//! Servidor de comunicação local.
//!
//! Escuta apenas em loopback (`127.0.0.1`), aceita uma requisição JSON por
//! conexão e exige o token de `config/daemon.token`. Cada conexão é tratada
//! em uma thread própria, com limite de conexões simultâneas, para que uma
//! requisição lenta não bloqueie as demais.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::error::DaemonError;
use crate::protocol::{BackupSummary, CommandName, DaemonResponse, Request, MAX_REQUEST_BYTES};
use crate::queue::EventQueue;
use crate::status::StatusState;
use crate::token::constant_time_eq;

/// Máximo de conexões tratadas em paralelo.
const MAX_CONNECTIONS: usize = 16;

/// Intervalo de espera do `accept` não bloqueante.
///
/// O servidor é ocioso na maior parte do tempo; 20 verificações por segundo
/// têm custo desprezível e permitem encerramento sem depender de sinais do
/// sistema operacional.
const ACCEPT_WAIT: Duration = Duration::from_millis(50);

/// Timeout de leitura/escrita de cada conexão.
const IO_TIMEOUT: Duration = Duration::from_secs(30);

/// Tempo máximo aguardando conexões em andamento no encerramento.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Operações que o servidor delega ao worker.
pub trait CommandHandler: Send + Sync + 'static {
    /// Executa um backup completo e retorna o resumo.
    fn backup_now(&self) -> Result<BackupSummary, String>;

    /// Solicita encerramento gracioso.
    fn shutdown(&self) -> Result<(), String>;
}

/// Servidor local já em execução.
pub struct IpcServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    thread: Option<JoinHandle<()>>,
}

impl IpcServer {
    /// Inicia o servidor em uma thread própria.
    pub fn start(
        listener: TcpListener,
        token: String,
        status: Arc<StatusState>,
        queue: EventQueue,
        handler: Arc<dyn CommandHandler>,
    ) -> Result<Self, DaemonError> {
        listener
            .set_nonblocking(true)
            .map_err(|error| DaemonError::Server(format!("modo não bloqueante: {error}")))?;
        let addr = listener
            .local_addr()
            .map_err(|error| DaemonError::Server(format!("endereço local: {error}")))?;

        let stop = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));

        let thread = {
            let stop = Arc::clone(&stop);
            let active = Arc::clone(&active);
            let token = Arc::new(token);
            thread::Builder::new()
                .name("leobackup-ipc".to_string())
                .spawn(move || {
                    accept_loop(listener, token, status, queue, handler, stop, active);
                })
                .map_err(|error| DaemonError::Server(format!("thread do servidor: {error}")))?
        };

        Ok(Self {
            addr,
            stop,
            active,
            thread: Some(thread),
        })
    }

    /// Endereço efetivo de escuta.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Sinaliza o encerramento do `accept`.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Encerra o `accept`, aguarda conexões em andamento e junta a thread.
    pub fn stop_and_join(mut self) {
        self.shutdown();

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }

        let deadline = Instant::now() + DRAIN_TIMEOUT;
        while self.active.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
    }
}

fn accept_loop(
    listener: TcpListener,
    token: Arc<String>,
    status: Arc<StatusState>,
    queue: EventQueue,
    handler: Arc<dyn CommandHandler>,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                if active.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS {
                    active.fetch_sub(1, Ordering::AcqRel);
                    let _ = write_response(
                        &mut stream,
                        &DaemonResponse::failure("servidor ocupado; tente novamente"),
                    );
                    continue;
                }

                let token = Arc::clone(&token);
                let status = Arc::clone(&status);
                let queue = queue.clone();
                let handler = Arc::clone(&handler);
                let stop = Arc::clone(&stop);
                let active_for_thread = Arc::clone(&active);

                let spawned = thread::Builder::new()
                    .name("leobackup-ipc-client".to_string())
                    .spawn(move || {
                        handle_connection(stream, &token, &status, &queue, handler.as_ref(), &stop);
                        active_for_thread.fetch_sub(1, Ordering::AcqRel);
                    });

                if spawned.is_err() {
                    active.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_WAIT);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => {
                status.record_error(format!("servidor local: {error}"));
                thread::sleep(ACCEPT_WAIT);
            }
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    token: &str,
    status: &StatusState,
    queue: &EventQueue,
    handler: &dyn CommandHandler,
    stop: &AtomicBool,
) {
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));

    let reader_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(error) => {
            let _ = write_response(
                &mut stream,
                &DaemonResponse::failure(format!("falha interna: {error}")),
            );
            return;
        }
    };

    let mut reader = BufReader::new(reader_stream);
    let mut line = String::new();
    match reader
        .by_ref()
        .take((MAX_REQUEST_BYTES + 1) as u64)
        .read_line(&mut line)
    {
        Ok(0) => return,
        Ok(_) => {}
        Err(_) => {
            let _ = write_response(
                &mut stream,
                &DaemonResponse::failure("falha ao ler a requisição"),
            );
            return;
        }
    }

    if line.len() > MAX_REQUEST_BYTES || !line.ends_with('\n') {
        let response = DaemonResponse::failure("requisição excede o tamanho máximo");
        let _ = write_response(&mut stream, &response);
        return;
    }

    let request = match Request::parse(&line) {
        Ok(request) => request,
        Err(_) => {
            let _ = write_response(&mut stream, &DaemonResponse::failure("requisição inválida"));
            return;
        }
    };

    if !constant_time_eq(&request.token, token) {
        let _ = write_response(&mut stream, &DaemonResponse::failure("não autorizado"));
        return;
    }

    let response = match request.command {
        CommandName::Status => DaemonResponse::with_status(status.snapshot(queue)),
        CommandName::BackupNow => match handler.backup_now() {
            Ok(summary) => DaemonResponse::with_backup(summary),
            Err(error) => DaemonResponse::failure(error),
        },
        CommandName::Shutdown => match handler.shutdown() {
            Ok(()) => {
                stop.store(true, Ordering::SeqCst);
                DaemonResponse::ok()
            }
            Err(error) => DaemonResponse::failure(error),
        },
    };

    let _ = write_response(&mut stream, &response);
}

fn write_response(stream: &mut TcpStream, response: &DaemonResponse) -> std::io::Result<()> {
    let mut encoded = serde_json::to_vec(response)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    encoded.push(b'\n');
    stream.write_all(&encoded)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::DaemonStatus;
    use std::sync::atomic::AtomicUsize;

    struct FakeHandler {
        backups: AtomicUsize,
        shutdowns: AtomicUsize,
    }

    impl FakeHandler {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                backups: AtomicUsize::new(0),
                shutdowns: AtomicUsize::new(0),
            })
        }
    }

    impl CommandHandler for FakeHandler {
        fn backup_now(&self) -> Result<BackupSummary, String> {
            self.backups.fetch_add(1, Ordering::SeqCst);
            Ok(BackupSummary {
                snapshot_id: Some(42),
                snapshot_status: Some("completed".to_string()),
                files_backed_up: 3,
                failures: 0,
                deleted: 1,
            })
        }

        fn shutdown(&self) -> Result<(), String> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn start_server() -> (IpcServer, String, Arc<FakeHandler>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let token = crate::token::generate_token().unwrap();
        let handler = FakeHandler::new();
        let server = IpcServer::start(
            listener,
            token.clone(),
            StatusState::new(2),
            EventQueue::new(8),
            handler.clone(),
        )
        .unwrap();
        (server, token, handler)
    }

    fn send(addr: SocketAddr, body: &str) -> DaemonResponse {
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(body.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }

    fn request(token: &str, command: &str) -> String {
        format!(r#"{{"token":"{token}","command":"{command}"}}"#)
    }

    #[test]
    fn status_is_answered() {
        let (server, token, _) = start_server();
        let response = send(server.local_addr(), &request(&token, "status"));

        assert!(response.ok);
        let status: DaemonStatus = response.status.unwrap();
        assert_eq!(status.source_dirs, 2);
        assert_eq!(status.queued_events, 0);

        server.stop_and_join();
    }

    #[test]
    fn rejects_wrong_token() {
        let (server, _, handler) = start_server();
        let wrong = crate::token::generate_token().unwrap();
        let response = send(server.local_addr(), &request(&wrong, "backup_now"));

        assert!(!response.ok);
        assert_eq!(response.error.as_deref(), Some("não autorizado"));
        assert_eq!(handler.backups.load(Ordering::SeqCst), 0);

        server.stop_and_join();
    }

    #[test]
    fn backup_now_reaches_handler() {
        let (server, token, handler) = start_server();
        let response = send(server.local_addr(), &request(&token, "backup_now"));

        assert!(response.ok);
        let backup = response.backup.unwrap();
        assert_eq!(backup.snapshot_id, Some(42));
        assert_eq!(backup.files_backed_up, 3);
        assert_eq!(handler.backups.load(Ordering::SeqCst), 1);

        server.stop_and_join();
    }

    #[test]
    fn shutdown_stops_the_server() {
        let (server, token, handler) = start_server();
        let response = send(server.local_addr(), &request(&token, "shutdown"));

        assert!(response.ok);
        assert_eq!(handler.shutdowns.load(Ordering::SeqCst), 1);
        server.stop_and_join();
    }

    #[test]
    fn rejects_oversized_requests() {
        let (server, token, _) = start_server();
        let mut body = request(&token, "status");
        body.push_str(&"x".repeat(MAX_REQUEST_BYTES));

        let response = send(server.local_addr(), &body);
        assert!(!response.ok);
        assert!(response
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("tamanho máximo"));

        server.stop_and_join();
    }

    #[test]
    fn rejects_unknown_fields() {
        let (server, token, _) = start_server();
        let body = format!(r#"{{"token":"{token}","command":"status","extra":true}}"#);
        let response = send(server.local_addr(), &body);

        assert!(!response.ok);
        assert_eq!(response.error.as_deref(), Some("requisição inválida"));

        server.stop_and_join();
    }
}
