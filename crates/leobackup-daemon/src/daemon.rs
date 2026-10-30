//! Orquestração do daemon: worker, watcher, fila e servidor local.
//!
//! A inicialização segue uma ordem que evita estados parciais visíveis:
//!
//! 1. valida a configuração e resolve os diretórios de origem;
//! 2. abre o SQLite e cria o [`BackupEngine`];
//! 3. reserva a porta de loopback (se o servidor estiver habilitado);
//! 4. inicia o worker, que configura o watcher e confirma a inicialização;
//! 5. só então inicia o servidor local.
//!
//! Se qualquer etapa falhar, os recursos já criados são liberados e o erro é
//! devolvido ao chamador. O token nunca é impresso nem incluído em erros.

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;

use leobackup_core::{EncryptionKey, Settings};
use leobackup_engine::BackupEngine;
use leobackup_storage::{ChunkStore, MetadataStore, RepositoryLayout};

use crate::client::Client;
use crate::error::DaemonError;
use crate::event::{is_within, path_key};
use crate::protocol::BackupSummary;
use crate::queue::EventQueue;
use crate::server::{CommandHandler, IpcServer};
use crate::status::StatusState;
use crate::token::ensure_token;
use crate::worker::{self, WorkerCommand, WorkerConfig};

/// Parâmetros necessários para iniciar o daemon.
#[derive(Debug)]
pub struct DaemonConfig {
    /// Diretório raiz do repositório de backup.
    pub repository: PathBuf,
    /// Configuração já carregada de `config/settings.json`.
    pub settings: Settings,
    /// Chave de criptografia, obrigatória se `settings.encryption.enabled`.
    pub key: Option<EncryptionKey>,
    /// Porta de loopback; `Some(0)` pede uma porta efêmera (testes).
    ///
    /// Quando `None`, usa `settings.daemon.port`.
    pub port: Option<u16>,
}

impl DaemonConfig {
    /// Cria a configuração usando a porta padrão de `Settings`.
    pub fn new(
        repository: impl Into<PathBuf>,
        settings: Settings,
        key: Option<EncryptionKey>,
    ) -> Self {
        Self {
            repository: repository.into(),
            settings,
            key,
            port: None,
        }
    }
}

/// Sinal de encerramento compartilhável com manipuladores de sinal.
#[derive(Clone)]
pub struct ShutdownSignal {
    shutdown: Arc<AtomicBool>,
    queue: EventQueue,
}

impl ShutdownSignal {
    /// Solicita o encerramento; é seguro chamar mais de uma vez.
    pub fn trigger(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Acorda o worker caso ele esteja ocioso no condvar da fila.
        self.queue.wake();
    }

    /// Indica se o encerramento já foi solicitado.
    pub fn is_triggered(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }
}

/// Ponte entre o servidor local e o worker.
struct RemoteHandler {
    commands: Sender<WorkerCommand>,
    signal: ShutdownSignal,
}

impl CommandHandler for RemoteHandler {
    fn backup_now(&self) -> Result<BackupSummary, String> {
        let (reply, response) = mpsc::channel();
        self.commands
            .send(WorkerCommand::BackupNow(reply))
            .map_err(|_| "worker encerrado; backup não executado".to_string())?;
        response
            .recv()
            .map_err(|_| "worker encerrou sem responder ao backup".to_string())?
    }

    fn shutdown(&self) -> Result<(), String> {
        // O flag é a fonte de verdade: o worker o observa entre arquivos e
        // encerra mesmo que o comando ainda não tenha sido consumido.
        self.signal.trigger();
        // Acorda o worker se ele estiver bloqueado aguardando eventos. Um
        // eventual erro aqui significa que ele já terminou, o que é aceitável
        // em um encerramento idempotente.
        let _ = self.commands.send(WorkerCommand::Shutdown);
        Ok(())
    }
}

/// Daemon em execução.
pub struct Daemon;

impl Daemon {
    /// Inicia o daemon e devolve um handle já operacional.
    pub fn start(config: DaemonConfig) -> Result<DaemonHandle, DaemonError> {
        config.settings.validate()?;

        let layout = RepositoryLayout::new(&config.repository)?;
        layout.ensure()?;

        let repo_root = fs::canonicalize(layout.root())
            .map_err(|source| DaemonError::io(layout.root(), source))?;
        let source_dirs = resolve_sources(&config.settings.source_dirs, &repo_root)?;

        let store = MetadataStore::open(&layout.database_path())?;
        let chunks = ChunkStore::new(&layout);
        let engine = BackupEngine::new(store, chunks, config.settings.clone(), config.key)?;

        let listener = if config.settings.daemon.enabled {
            let port = config.port.unwrap_or(config.settings.daemon.port);
            let listener = TcpListener::bind(("127.0.0.1", port)).map_err(|error| {
                DaemonError::Server(format!(
                    "não foi possível escutar em 127.0.0.1:{port}: {error}"
                ))
            })?;
            Some(listener)
        } else {
            None
        };

        let token = match &listener {
            Some(_) => ensure_token(&layout.token_path())?,
            None => String::new(),
        };

        let queue = EventQueue::new(config.settings.monitoring.queue_capacity);
        let status = StatusState::new(source_dirs.len());
        let shutdown = Arc::new(AtomicBool::new(false));
        let signal = ShutdownSignal {
            shutdown: Arc::clone(&shutdown),
            queue: queue.clone(),
        };

        let (commands, command_rx) = mpsc::channel();
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);

        let worker = worker::spawn(
            engine,
            queue.clone(),
            command_rx,
            Arc::clone(&status),
            WorkerConfig {
                settings: config.settings.clone(),
                source_dirs,
                repo_root: repo_root.clone(),
            },
            Arc::clone(&shutdown),
            startup_tx,
        )
        .map_err(|source| {
            DaemonError::Internal(format!("falha ao criar thread do worker: {source}"))
        })?;

        match startup_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                signal.trigger();
                let _ = worker.join();
                return Err(DaemonError::Watcher(message));
            }
            Err(_) => {
                signal.trigger();
                let _ = worker.join();
                return Err(DaemonError::Internal(
                    "worker encerrou durante a inicialização".to_string(),
                ));
            }
        }

        let server = match listener {
            Some(listener) => {
                let handler = RemoteHandler {
                    commands: commands.clone(),
                    signal: signal.clone(),
                };
                match IpcServer::start(
                    listener,
                    token.clone(),
                    Arc::clone(&status),
                    queue.clone(),
                    Arc::new(handler),
                ) {
                    Ok(server) => Some(server),
                    Err(error) => {
                        signal.trigger();
                        let _ = worker.join();
                        return Err(error);
                    }
                }
            }
            None => None,
        };

        let addr = server.as_ref().map(IpcServer::local_addr);

        Ok(DaemonHandle {
            addr,
            token,
            token_path: layout.token_path(),
            signal,
            shutdown,
            queue,
            status,
            server,
            worker: Some(worker),
        })
    }
}

/// Handle de um daemon em execução.
pub struct DaemonHandle {
    addr: Option<SocketAddr>,
    token: String,
    token_path: PathBuf,
    signal: ShutdownSignal,
    shutdown: Arc<AtomicBool>,
    queue: EventQueue,
    status: Arc<StatusState>,
    server: Option<IpcServer>,
    worker: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for DaemonHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonHandle")
            .field("addr", &self.addr)
            .field("token_path", &self.token_path)
            .finish_non_exhaustive()
    }
}

impl DaemonHandle {
    /// Endereço de escuta, quando o servidor local está habilitado.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.addr
    }

    /// Caminho do token de autenticação.
    pub fn token_path(&self) -> &Path {
        &self.token_path
    }

    /// Cria um cliente para este daemon.
    ///
    /// Retorna erro se o servidor local estiver desabilitado.
    pub fn client(&self) -> Result<Client, DaemonError> {
        let addr = self
            .addr
            .ok_or_else(|| DaemonError::InvalidConfig("servidor local desabilitado".to_string()))?;
        Client::new(addr, self.token.clone())
    }

    /// Sinal compartilhável para `ctrlc` ou encerramento programático.
    pub fn signal(&self) -> ShutdownSignal {
        self.signal.clone()
    }

    /// Estado observável atual.
    pub fn status(&self) -> crate::protocol::DaemonStatus {
        self.status.snapshot(&self.queue)
    }

    /// Indica se o encerramento já foi solicitado.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Solicita o encerramento sem bloquear.
    pub fn request_shutdown(&self) {
        self.signal.trigger();
    }

    /// Aguarda o worker terminar e libera o servidor local.
    ///
    /// Bloqueia até que um encerramento seja solicitado (via
    /// [`Self::request_shutdown`], [`Self::shutdown`] ou comando remoto).
    pub fn wait(mut self) -> Result<(), DaemonError> {
        let result = match self.worker.take() {
            Some(worker) => worker
                .join()
                .map_err(|_| DaemonError::Internal("worker terminou com panic".to_string())),
            None => Ok(()),
        };

        // O servidor é liberado mesmo se o worker falhar; caso contrário a
        // porta e a thread de accept continuariam ativas após o erro.
        if let Some(server) = self.server.take() {
            server.stop_and_join();
        }

        result
    }

    /// Solicita o encerramento e aguarda a finalização.
    pub fn shutdown(self) -> Result<(), DaemonError> {
        self.request_shutdown();
        self.wait()
    }
}

/// Resolve e valida os diretórios de origem.
///
/// Caminhos são canonicalizados para que o watcher, a varredura e a filtragem
/// do repositório usem a mesma representação. Origens sobrepostas são
/// deduplicadas; origens dentro do repositório são rejeitadas para evitar
/// varreduras que não fariam backup de nada.
fn resolve_sources(configured: &[PathBuf], repo_root: &Path) -> Result<Vec<PathBuf>, DaemonError> {
    if configured.is_empty() {
        return Err(DaemonError::InvalidConfig(
            "nenhum diretório de origem configurado em source_dirs".to_string(),
        ));
    }

    let mut resolved = Vec::with_capacity(configured.len());
    let mut keys = Vec::with_capacity(configured.len());

    for dir in configured {
        let canonical = fs::canonicalize(dir).map_err(|source| DaemonError::io(dir, source))?;

        let metadata =
            fs::metadata(&canonical).map_err(|source| DaemonError::io(&canonical, source))?;
        if !metadata.is_dir() {
            return Err(DaemonError::InvalidConfig(format!(
                "origem não é um diretório: {}",
                canonical.display()
            )));
        }

        if is_within(&canonical, repo_root) {
            return Err(DaemonError::InvalidConfig(format!(
                "origem {} está dentro do repositório {}; isso causaria um backup vazio ou recursivo",
                canonical.display(),
                repo_root.display()
            )));
        }

        let key = path_key(&canonical);
        if keys.contains(&key) {
            continue;
        }
        keys.push(key);
        resolved.push(canonical);
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use leobackup_core::config::{ChunkingSettings, EncryptionSettings, MonitoringSettings};
    use leobackup_core::{Settings, SnapshotStatus};
    use leobackup_storage::MetadataStore;
    use std::thread;
    use std::time::{Duration, Instant};

    struct Fixture {
        dir: tempfile::TempDir,
        source: PathBuf,
        repo: PathBuf,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src");
        fs::create_dir_all(&source).unwrap();
        let repo = dir.path().join("repo");
        Fixture { dir, source, repo }
    }

    fn settings(fixture: &Fixture) -> Settings {
        Settings {
            source_dirs: vec![fixture.source.clone()],
            chunking: ChunkingSettings {
                min_size: 1_024,
                avg_size: 4_096,
                max_size: 16_384,
            },
            monitoring: MonitoringSettings {
                debounce_ms: 50,
                queue_capacity: 64,
                ..MonitoringSettings::default()
            },
            encryption: EncryptionSettings { enabled: false },
            ..Settings::default()
        }
    }

    fn start(fixture: &Fixture, settings: Settings) -> DaemonHandle {
        Daemon::start(DaemonConfig {
            repository: fixture.repo.clone(),
            settings,
            key: None,
            port: Some(0),
        })
        .unwrap()
    }

    fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("condição não satisfeita em {:?}", timeout);
    }

    fn open_store(fixture: &Fixture) -> MetadataStore {
        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        MetadataStore::open(&layout.database_path()).unwrap()
    }

    #[test]
    fn manual_backup_roundtrips_through_ipc() {
        let fixture = fixture();
        fs::write(fixture.source.join("a.txt"), b"primeiro").unwrap();
        fs::create_dir_all(fixture.source.join("sub")).unwrap();
        fs::write(fixture.source.join("sub").join("b.txt"), b"segundo").unwrap();

        let mut settings = settings(&fixture);
        settings.monitoring.enabled = false;
        settings.monitoring.initial_scan = false;
        let handle = start(&fixture, settings);
        let client = handle.client().unwrap();

        let summary = client.backup_now().unwrap();
        assert_eq!(summary.files_backed_up, 2);
        assert_eq!(summary.failures, 0);
        assert_eq!(summary.snapshot_status.as_deref(), Some("completed"));

        let status = client.status().unwrap();
        assert_eq!(status.source_dirs, 1);
        assert_eq!(status.files_backed_up, 2);
        assert_eq!(status.snapshots_created, 1);

        handle.shutdown().unwrap();

        let store = open_store(&fixture);
        let snapshots = store.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].status, SnapshotStatus::Completed);
        assert_eq!(store.list_files(snapshots[0].id).unwrap().len(), 2);
    }

    #[test]
    fn initial_scan_backs_up_existing_files() {
        let fixture = fixture();
        fs::write(fixture.source.join("existente.txt"), b"conteudo").unwrap();

        let mut settings = settings(&fixture);
        settings.monitoring.enabled = false;
        settings.monitoring.initial_scan = true;
        let handle = start(&fixture, settings);
        let client = handle.client().unwrap();

        wait_until(Duration::from_secs(10), || {
            client
                .status()
                .map(|status| status.files_backed_up >= 1)
                .unwrap_or(false)
        });

        handle.shutdown().unwrap();

        let store = open_store(&fixture);
        let snapshots = store.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].status, SnapshotStatus::Completed);
        let files = store.list_files(snapshots[0].id).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].path.ends_with("existente.txt"));
    }

    #[test]
    fn watcher_backs_up_new_file() {
        let fixture = fixture();

        let mut settings = settings(&fixture);
        settings.monitoring.enabled = true;
        settings.monitoring.initial_scan = false;
        let handle = start(&fixture, settings);
        let client = handle.client().unwrap();

        fs::write(fixture.source.join("novo.txt"), b"criado em tempo real").unwrap();

        wait_until(Duration::from_secs(15), || {
            client
                .status()
                .map(|status| status.files_backed_up >= 1)
                .unwrap_or(false)
        });

        handle.shutdown().unwrap();

        let store = open_store(&fixture);
        let snapshots = store.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
        let files = store.list_files(snapshots[0].id).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].path.ends_with("novo.txt"));
    }

    #[test]
    fn rejects_wrong_token_over_ipc() {
        let fixture = fixture();
        let mut settings = settings(&fixture);
        settings.monitoring.enabled = false;
        settings.monitoring.initial_scan = false;
        let handle = start(&fixture, settings);

        let wrong = crate::token::generate_token().unwrap();
        let intruder = Client::new(handle.local_addr().unwrap(), wrong).unwrap();
        let error = intruder.status().unwrap_err();
        assert!(matches!(error, DaemonError::Rejected(_)));

        handle.shutdown().unwrap();
    }

    #[test]
    fn remote_shutdown_stops_the_worker() {
        let fixture = fixture();
        let mut settings = settings(&fixture);
        settings.monitoring.enabled = false;
        settings.monitoring.initial_scan = false;
        let handle = start(&fixture, settings);
        let client = handle.client().unwrap();

        client.shutdown().unwrap();
        handle.wait().unwrap();
    }

    #[test]
    fn rejects_missing_source_dir() {
        let fixture = fixture();
        let mut settings = settings(&fixture);
        settings.source_dirs = vec![fixture.dir.path().join("nao-existe")];

        let error = Daemon::start(DaemonConfig {
            repository: fixture.repo.clone(),
            settings,
            key: None,
            port: Some(0),
        })
        .unwrap_err();
        assert!(matches!(error, DaemonError::Io { .. }));
    }

    #[test]
    fn rejects_source_inside_repository() {
        let fixture = fixture();
        let nested = fixture.repo.join("origem");
        fs::create_dir_all(&nested).unwrap();

        let mut settings = settings(&fixture);
        settings.source_dirs = vec![nested];

        let error = Daemon::start(DaemonConfig {
            repository: fixture.repo.clone(),
            settings,
            key: None,
            port: Some(0),
        })
        .unwrap_err();
        assert!(matches!(error, DaemonError::InvalidConfig(_)));
    }

    #[test]
    fn rejects_empty_source_list() {
        let fixture = fixture();
        let mut settings = settings(&fixture);
        settings.source_dirs.clear();

        let error = Daemon::start(DaemonConfig {
            repository: fixture.repo.clone(),
            settings,
            key: None,
            port: Some(0),
        })
        .unwrap_err();
        assert!(matches!(error, DaemonError::InvalidConfig(_)));
    }

    #[test]
    fn daemon_without_ipc_still_scans() {
        let fixture = fixture();
        fs::write(fixture.source.join("a.txt"), b"a").unwrap();

        let mut settings = settings(&fixture);
        settings.daemon.enabled = false;
        settings.monitoring.enabled = false;
        settings.monitoring.initial_scan = true;
        let handle = start(&fixture, settings);

        assert!(handle.local_addr().is_none());
        assert!(handle.client().is_err());

        wait_until(Duration::from_secs(10), || {
            handle.status().files_backed_up >= 1
        });
        handle.shutdown().unwrap();
    }
}
