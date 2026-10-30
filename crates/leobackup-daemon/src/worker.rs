//! Worker único que consome a fila, executa backups e responde comandos.
//!
//! O pipeline e o SQLite são serializados neste worker: apenas uma thread
//! toca no `BackupEngine`. O watcher apenas enfileira caminhos; o servidor
//! local apenas enfileira comandos. Isso evita concorrência sobre a conexão
//! `rusqlite` e mantém a ordem dos commits.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TryRecvError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use leobackup_core::config::Settings;
use leobackup_engine::{BackupEngine, EngineError};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};

use crate::event::{map_notify_event, Containment};
use crate::protocol::BackupSummary;
use crate::queue::{EventQueue, PushOutcome};
use crate::scanner::{walk_files, ScanControl, MAX_DEPTH};
use crate::status::{BatchOutcome, StatusState};

/// Intervalo máximo de espera ociosa do worker.
///
/// Não é polling do sistema de arquivos: o watcher usa notificações nativas.
/// O limite existe apenas para observar comandos mesmo sem novos eventos.
const IDLE_WAIT: Duration = Duration::from_millis(100);

/// Número máximo de caminhos retirados da fila por ciclo.
const MAX_EVENTS_PER_BATCH: usize = 256;

/// Comando enviado ao worker pelo servidor local ou pelo sinal de encerramento.
pub enum WorkerCommand {
    /// Executa uma varredura completa e devolve o resumo.
    BackupNow(Sender<Result<BackupSummary, String>>),
    /// Interrompe o processamento e encerra o worker.
    Shutdown,
}

/// Parâmetros de execução do worker.
pub struct WorkerConfig {
    /// Configuração validada do repositório.
    pub settings: Settings,
    /// Diretórios monitorados.
    pub source_dirs: Vec<PathBuf>,
    /// Raiz do repositório, excluída das varreduras.
    pub repo_root: PathBuf,
}

/// Inicia o worker em uma thread própria.
///
/// O resultado da configuração do watcher é enviado por `startup` antes de
/// qualquer processamento, permitindo que a inicialização do daemon falhe de
/// forma explícita.
pub fn spawn(
    engine: BackupEngine,
    queue: EventQueue,
    commands: Receiver<WorkerCommand>,
    status: Arc<StatusState>,
    config: WorkerConfig,
    shutdown: Arc<AtomicBool>,
    startup: SyncSender<Result<(), String>>,
) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("leobackup-worker".to_string())
        .spawn(move || run(engine, queue, commands, status, config, shutdown, startup))
}

fn run(
    mut engine: BackupEngine,
    queue: EventQueue,
    commands: Receiver<WorkerCommand>,
    status: Arc<StatusState>,
    config: WorkerConfig,
    shutdown: Arc<AtomicBool>,
    startup: SyncSender<Result<(), String>>,
) {
    let WorkerConfig {
        settings,
        source_dirs,
        repo_root,
    } = config;

    let debounce = Duration::from_millis(settings.monitoring.debounce_ms);
    let watcher = match build_watcher(&settings, &source_dirs, &repo_root, &queue, &status) {
        Ok(watcher) => {
            if startup.send(Ok(())).is_err() {
                return;
            }
            watcher
        }
        Err(error) => {
            status.record_error(&error);
            let _ = startup.send(Err(error));
            return;
        }
    };
    let _watcher: Option<RecommendedWatcher> = watcher;

    if settings.monitoring.initial_scan && !shutdown.load(Ordering::SeqCst) {
        let outcome = full_scan(&mut engine, &source_dirs, &repo_root, &shutdown);
        status.record_outcome(&outcome);
    }

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        if handle_commands(
            &mut engine,
            &queue,
            &commands,
            &status,
            &source_dirs,
            &repo_root,
            &shutdown,
        ) {
            break;
        }

        if queue.take_rescan() {
            let outcome = full_scan(&mut engine, &source_dirs, &repo_root, &shutdown);
            status.record_outcome(&outcome);
            continue;
        }

        let events = queue.take_due(debounce, MAX_EVENTS_PER_BATCH);
        if events.is_empty() {
            queue.wait_for_work(debounce, IDLE_WAIT);
            continue;
        }

        let outcome = process_events(&mut engine, events, &repo_root, &shutdown);
        status.record_outcome(&outcome);
    }

    queue.close();
}

fn build_watcher(
    settings: &Settings,
    source_dirs: &[PathBuf],
    repo_root: &Path,
    queue: &EventQueue,
    status: &Arc<StatusState>,
) -> Result<Option<RecommendedWatcher>, String> {
    if !settings.monitoring.enabled {
        return Ok(None);
    }

    if source_dirs.is_empty() {
        return Err(
            "monitoramento habilitado, mas nenhum diretório de origem foi configurado".to_string(),
        );
    }

    for dir in source_dirs {
        let metadata = fs::metadata(dir)
            .map_err(|error| format!("origem inacessível {}: {error}", dir.display()))?;
        if !metadata.is_dir() {
            return Err(format!("origem não é um diretório: {}", dir.display()));
        }
    }

    let containment = Containment::new(repo_root);
    let queue = queue.clone();
    let status = Arc::clone(status);

    let mut watcher =
        notify::recommended_watcher(move |result: notify::Result<notify::Event>| match result {
            Ok(event) => {
                if event.need_rescan() {
                    queue.require_rescan();
                    return;
                }

                for mapped in map_notify_event(&event) {
                    if containment.contains(&mapped.path) {
                        continue;
                    }
                    if queue.push(mapped) == PushOutcome::Overflowed {
                        status
                            .record_error("fila de eventos cheia; varredura completa foi agendada");
                    }
                }
            }
            Err(error) => {
                status.record_error(format!("watcher: {error}"));
                queue.require_rescan();
            }
        })
        .map_err(|error| format!("falha ao criar watcher: {error}"))?;

    for dir in source_dirs {
        watcher
            .watch(dir, RecursiveMode::Recursive)
            .map_err(|error| format!("falha ao monitorar {}: {error}", dir.display()))?;
    }

    Ok(Some(watcher))
}

/// Processa comandos pendentes. Retorna `true` se o encerramento foi pedido.
fn handle_commands(
    engine: &mut BackupEngine,
    queue: &EventQueue,
    commands: &Receiver<WorkerCommand>,
    status: &Arc<StatusState>,
    source_dirs: &[PathBuf],
    repo_root: &Path,
    shutdown: &AtomicBool,
) -> bool {
    loop {
        match commands.try_recv() {
            Ok(WorkerCommand::BackupNow(reply)) => {
                if shutdown.load(Ordering::SeqCst) {
                    let _ = reply.send(Err("encerrando; backup manual não executado".to_string()));
                    return true;
                }

                queue.take_rescan();
                let outcome = full_scan(engine, source_dirs, repo_root, shutdown);
                status.record_outcome(&outcome);
                let result = if outcome.fatal {
                    Err(outcome
                        .error
                        .clone()
                        .unwrap_or_else(|| "backup manual falhou".to_string()))
                } else if outcome.aborted {
                    Err("backup manual interrompido pelo encerramento".to_string())
                } else {
                    Ok(summary_from(&outcome))
                };
                let _ = reply.send(result);
            }
            Ok(WorkerCommand::Shutdown) => {
                shutdown.store(true, Ordering::SeqCst);
                queue.wake();
                return true;
            }
            Err(TryRecvError::Empty) => return false,
            Err(TryRecvError::Disconnected) => {
                shutdown.store(true, Ordering::SeqCst);
                return true;
            }
        }
    }
}

fn summary_from(outcome: &BatchOutcome) -> BackupSummary {
    BackupSummary {
        snapshot_id: outcome.snapshot_id,
        snapshot_status: outcome.snapshot_status.clone(),
        files_backed_up: outcome.files_backed_up,
        failures: outcome.failures,
        deleted: outcome.deleted,
    }
}

fn full_scan(
    engine: &mut BackupEngine,
    source_dirs: &[PathBuf],
    repo_root: &Path,
    shutdown: &AtomicBool,
) -> BatchOutcome {
    let mut batch = Batch::new(engine, repo_root, shutdown);
    let summary = walk_files(source_dirs, &[repo_root.to_path_buf()], MAX_DEPTH, |path| {
        if batch.shutdown_requested() {
            batch.abort = true;
            return ScanControl::Stop;
        }

        batch.backup_regular_file(path);
        if batch.fatal.is_some() {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        }
    });

    if summary.errors > 0 {
        batch.failures += summary.errors;
        batch.last_error.get_or_insert_with(|| {
            format!(
                "{} entrada(s) não puderam ser lidas durante a varredura",
                summary.errors
            )
        });
    }

    batch.finish()
}

fn process_events(
    engine: &mut BackupEngine,
    events: Vec<crate::event::SourceEvent>,
    repo_root: &Path,
    shutdown: &AtomicBool,
) -> BatchOutcome {
    let mut batch = Batch::new(engine, repo_root, shutdown);

    for event in events {
        if batch.shutdown_requested() {
            batch.abort = true;
            break;
        }

        match event.kind {
            crate::event::SourceEventKind::Deleted => batch.deleted += 1,
            crate::event::SourceEventKind::Upsert => batch.backup_path(&event.path),
        }

        if batch.fatal.is_some() {
            break;
        }
    }

    batch.finish()
}

/// Estado de um lote que compartilha um único snapshot.
struct Batch<'a> {
    engine: &'a mut BackupEngine,
    repo_root: &'a Path,
    shutdown: &'a AtomicBool,
    snapshot_id: Option<i64>,
    files_backed_up: u64,
    failures: u64,
    deleted: u64,
    last_error: Option<String>,
    fatal: Option<String>,
    abort: bool,
}

impl<'a> Batch<'a> {
    fn new(engine: &'a mut BackupEngine, repo_root: &'a Path, shutdown: &'a AtomicBool) -> Self {
        Self {
            engine,
            repo_root,
            shutdown,
            snapshot_id: None,
            files_backed_up: 0,
            failures: 0,
            deleted: 0,
            last_error: None,
            fatal: None,
            abort: false,
        }
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    fn ensure_snapshot(&mut self) -> Option<i64> {
        if self.fatal.is_some() {
            return None;
        }
        if let Some(id) = self.snapshot_id {
            return Some(id);
        }

        match self.engine.begin_snapshot() {
            Ok(snapshot) => {
                self.snapshot_id = Some(snapshot.id);
                Some(snapshot.id)
            }
            Err(error) => {
                self.fatal = Some(format!("falha ao criar snapshot: {error}"));
                None
            }
        }
    }

    /// Processa um caminho que o watcher marcou como criado/modificado.
    fn backup_path(&mut self, path: &Path) {
        if self.fatal.is_some() {
            return;
        }

        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // O arquivo pode ter sido removido entre o evento e o
                // processamento; não é uma falha de backup.
                self.deleted += 1;
                return;
            }
            Err(error) => {
                self.failures += 1;
                self.last_error = Some(format!("{}: {error}", path.display()));
                return;
            }
        };

        if metadata.is_file() {
            self.backup_regular_file(path);
        } else if metadata.is_dir() {
            self.backup_directory(path);
        }
        // Entradas especiais (sockets, dispositivos) são ignoradas.
    }

    fn backup_directory(&mut self, dir: &Path) {
        let summary = walk_files(
            &[dir.to_path_buf()],
            &[self.repo_root.to_path_buf()],
            MAX_DEPTH,
            |path| {
                if self.shutdown_requested() {
                    self.abort = true;
                    return ScanControl::Stop;
                }

                self.backup_regular_file(path);
                if self.fatal.is_some() {
                    ScanControl::Stop
                } else {
                    ScanControl::Continue
                }
            },
        );

        if summary.errors > 0 {
            self.failures += summary.errors;
            self.last_error.get_or_insert_with(|| {
                format!(
                    "{} entrada(s) não puderam ser lidas em {}",
                    summary.errors,
                    dir.display()
                )
            });
        }
    }

    fn backup_regular_file(&mut self, path: &Path) {
        if self.fatal.is_some() {
            return;
        }

        let Some(snapshot_id) = self.ensure_snapshot() else {
            return;
        };

        match self.engine.backup_file(snapshot_id, path) {
            Ok(_) => self.files_backed_up += 1,
            Err(EngineError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                // Removido durante a leitura: o evento seguinte ou a próxima
                // varredura tratam o estado; não conta como falha.
                self.deleted += 1;
            }
            Err(error) => {
                self.failures += 1;
                self.last_error = Some(format!("{}: {error}", path.display()));
            }
        }
    }

    fn finish(mut self) -> BatchOutcome {
        let mut snapshot_status = None;
        let fatal = self.fatal.is_some();

        if let Some(id) = self.snapshot_id {
            let failed = fatal || self.failures > 0 || self.abort;
            let updated = if failed {
                self.engine.fail_snapshot(id)
            } else {
                self.engine.complete_snapshot(id)
            };

            match updated {
                Ok(snapshot) => snapshot_status = Some(snapshot.status.to_string()),
                Err(error) => {
                    if self.fatal.is_none() {
                        self.fatal = Some(format!("falha ao finalizar snapshot: {error}"));
                    }
                }
            }
        }

        let fatal = fatal || self.fatal.is_some();
        let error = self.fatal.take().or(self.last_error.take());
        BatchOutcome {
            snapshot_id: self.snapshot_id,
            snapshot_status,
            files_backed_up: self.files_backed_up,
            failures: self.failures,
            deleted: self.deleted,
            fatal,
            error,
            aborted: self.abort,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leobackup_core::{EncryptionKey, Settings};
    use leobackup_storage::{ChunkStore, MetadataStore, RepositoryLayout};
    use std::sync::mpsc;

    struct Repo {
        dir: tempfile::TempDir,
        source: PathBuf,
        engine: BackupEngine,
    }

    fn repo() -> Repo {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src");
        fs::create_dir_all(&source).unwrap();
        let layout = RepositoryLayout::new(dir.path().join("repo")).unwrap();
        layout.ensure().unwrap();

        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        let mut settings = Settings::default();
        settings.chunking.min_size = 1_024;
        settings.chunking.avg_size = 4_096;
        settings.chunking.max_size = 16_384;
        settings.encryption.enabled = false;
        let engine = BackupEngine::new(store, chunks, settings, None::<EncryptionKey>).unwrap();

        Repo {
            dir,
            source,
            engine,
        }
    }

    fn shutdown_flag() -> AtomicBool {
        AtomicBool::new(false)
    }

    #[test]
    fn full_scan_backs_up_existing_files_once() {
        let mut repo = repo();
        fs::write(repo.source.join("a.txt"), b"conteudo").unwrap();
        fs::write(repo.source.join("b.txt"), b"outro").unwrap();
        let flag = shutdown_flag();

        let outcome = full_scan(
            &mut repo.engine,
            &[repo.source.clone()],
            &repo.dir.path().join("repo"),
            &flag,
        );

        assert_eq!(outcome.files_backed_up, 2);
        assert_eq!(outcome.failures, 0);
        assert!(!outcome.fatal);
        assert_eq!(outcome.snapshot_status.as_deref(), Some("completed"));
        assert_eq!(repo.engine.store().list_snapshots().unwrap().len(), 1);
    }

    #[test]
    fn scan_excludes_repository_inside_source() {
        let mut repo = repo();
        let nested_repo = repo.source.join("repo");
        let layout = RepositoryLayout::new(&nested_repo).unwrap();
        layout.ensure().unwrap();
        fs::write(repo.source.join("a.txt"), b"a").unwrap();
        fs::write(nested_repo.join("data").join("x"), b"x").unwrap();

        let flag = shutdown_flag();
        let outcome = full_scan(
            &mut repo.engine,
            &[repo.source.clone()],
            &nested_repo,
            &flag,
        );

        assert_eq!(outcome.files_backed_up, 1);
        assert_eq!(repo.engine.store().list_files(1).unwrap().len(), 1);
    }

    #[test]
    fn directory_event_scans_all_files() {
        let mut repo = repo();
        let nested = repo.source.join("sub");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("a.txt"), b"a").unwrap();
        fs::write(nested.join("b.txt"), b"b").unwrap();
        let flag = shutdown_flag();

        let outcome = process_events(
            &mut repo.engine,
            vec![crate::event::SourceEvent::new(
                repo.source.clone(),
                crate::event::SourceEventKind::Upsert,
            )],
            &repo.dir.path().join("repo"),
            &flag,
        );

        assert_eq!(outcome.files_backed_up, 2);
        assert_eq!(outcome.snapshot_status.as_deref(), Some("completed"));
    }

    #[test]
    fn deleted_events_do_not_create_snapshots() {
        let mut repo = repo();
        let flag = shutdown_flag();

        let outcome = process_events(
            &mut repo.engine,
            vec![crate::event::SourceEvent::new(
                repo.source.join("nao-existe.txt"),
                crate::event::SourceEventKind::Deleted,
            )],
            &repo.dir.path().join("repo"),
            &flag,
        );

        assert_eq!(outcome.deleted, 1);
        assert!(outcome.snapshot_id.is_none());
        assert!(repo.engine.store().list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn shutdown_aborts_scan_and_marks_snapshot_failed() {
        let mut repo = repo();
        for index in 0..16 {
            fs::write(repo.source.join(format!("f{index}.txt")), b"dados").unwrap();
        }
        let flag = AtomicBool::new(true);

        let outcome = full_scan(
            &mut repo.engine,
            &[repo.source.clone()],
            &repo.dir.path().join("repo"),
            &flag,
        );

        assert!(outcome.aborted);
        // O primeiro arquivo pode ter sido iniciado antes da checagem.
        if let Some(id) = outcome.snapshot_id {
            let snapshot = repo.engine.store().get_snapshot(id).unwrap().unwrap();
            assert_eq!(snapshot.status, leobackup_core::SnapshotStatus::Failed);
        }
    }

    #[test]
    fn command_shutdown_stops_the_loop() {
        let mut repo = repo();
        let queue = EventQueue::new(8);
        let (tx, rx) = mpsc::channel();
        let status = StatusState::new(1);
        let flag = AtomicBool::new(false);

        tx.send(WorkerCommand::Shutdown).unwrap();

        let stop = handle_commands(
            &mut repo.engine,
            &queue,
            &rx,
            &status,
            &[repo.source.clone()],
            &repo.dir.path().join("repo"),
            &flag,
        );

        assert!(stop);
        assert!(flag.load(Ordering::SeqCst));
    }
}
