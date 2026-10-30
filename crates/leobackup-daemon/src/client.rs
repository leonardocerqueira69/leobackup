//! Cliente do servidor local do daemon.
//!
//! O cliente abre uma conexão TCP curta por comando, envia uma linha JSON com
//! o token de `config/daemon.token` e lê uma única linha de resposta. A
//! biblioteca é usada pela CLI e pelos testes de integração; o token nunca é
//! incluído em mensagens de erro ou logs.

use std::io::{BufRead, BufReader, Read as _, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use crate::error::DaemonError;
use crate::protocol::{
    BackupSummary, CommandName, DaemonResponse, DaemonStatus, Request, MAX_REQUEST_BYTES,
};
use crate::token::is_valid_token;

/// Tamanho máximo aceito para uma resposta.
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Timeout de conexão com o daemon.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout de leitura/escrita.
///
/// É generoso porque `backup_now` só responde depois que o lote termina.
const IO_TIMEOUT: Duration = Duration::from_secs(300);

/// Cliente do daemon.
#[derive(Clone)]
pub struct Client {
    addr: SocketAddr,
    token: String,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Cria um cliente exigindo um token no formato esperado.
    pub fn new(addr: SocketAddr, token: impl Into<String>) -> Result<Self, DaemonError> {
        let token = token.into();
        if !is_valid_token(&token) {
            return Err(DaemonError::InvalidConfig(
                "token do daemon inválido; leia config/daemon.token novamente".to_string(),
            ));
        }
        Ok(Self { addr, token })
    }

    /// Endereço do daemon.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Consulta o estado do daemon.
    pub fn status(&self) -> Result<DaemonStatus, DaemonError> {
        let response = self.request(CommandName::Status)?;
        response
            .status
            .ok_or_else(|| DaemonError::Protocol("resposta sem estado do daemon".to_string()))
    }

    /// Solicita uma varredura completa e aguarda o resumo.
    pub fn backup_now(&self) -> Result<BackupSummary, DaemonError> {
        let response = self.request(CommandName::BackupNow)?;
        response
            .backup
            .ok_or_else(|| DaemonError::Protocol("resposta sem resumo de backup".to_string()))
    }

    /// Solicita o encerramento gracioso do daemon.
    pub fn shutdown(&self) -> Result<(), DaemonError> {
        self.request(CommandName::Shutdown).map(|_| ())
    }

    fn request(&self, command: CommandName) -> Result<DaemonResponse, DaemonError> {
        let mut stream = TcpStream::connect_timeout(&self.addr, CONNECT_TIMEOUT)
            .map_err(|error| DaemonError::Server(format!("conexão recusada: {error}")))?;
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .map_err(|error| DaemonError::Server(format!("timeout de leitura: {error}")))?;
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .map_err(|error| DaemonError::Server(format!("timeout de escrita: {error}")))?;

        let request = Request {
            token: self.token.clone(),
            command,
        };
        let mut encoded = serde_json::to_vec(&request)?;
        if encoded.len() > MAX_REQUEST_BYTES {
            return Err(DaemonError::Protocol(
                "requisição excede o tamanho máximo".to_string(),
            ));
        }
        encoded.push(b'\n');
        stream
            .write_all(&encoded)
            .and_then(|()| stream.flush())
            .map_err(|error| DaemonError::Server(format!("falha ao enviar requisição: {error}")))?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader
            .by_ref()
            .take((MAX_RESPONSE_BYTES + 1) as u64)
            .read_line(&mut line)
            .map_err(|error| DaemonError::Server(format!("falha ao ler resposta: {error}")))?;

        if line.is_empty() {
            return Err(DaemonError::Server(
                "o daemon encerrou a conexão sem responder".to_string(),
            ));
        }
        if line.len() > MAX_RESPONSE_BYTES || !line.ends_with('\n') {
            return Err(DaemonError::Protocol(
                "resposta excede o tamanho máximo ou não é uma linha JSON".to_string(),
            ));
        }

        let response: DaemonResponse = serde_json::from_str(&line)?;
        if response.ok {
            Ok(response)
        } else {
            Err(DaemonError::Rejected(
                response
                    .error
                    .unwrap_or_else(|| "erro não especificado".to_string()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::EventQueue;
    use crate::server::{CommandHandler, IpcServer};
    use crate::status::StatusState;
    use std::net::TcpListener;
    use std::sync::Arc;

    struct Handler;

    impl CommandHandler for Handler {
        fn backup_now(&self) -> Result<BackupSummary, String> {
            Ok(BackupSummary {
                snapshot_id: Some(1),
                snapshot_status: Some("completed".to_string()),
                files_backed_up: 1,
                failures: 0,
                deleted: 0,
            })
        }

        fn shutdown(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn server() -> (IpcServer, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let token = crate::token::generate_token().unwrap();
        let server = IpcServer::start(
            listener,
            token.clone(),
            StatusState::new(0),
            EventQueue::new(8),
            Arc::new(Handler),
        )
        .unwrap();
        (server, token)
    }

    #[test]
    fn rejects_malformed_token_before_connecting() {
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let error = Client::new(addr, "curto").unwrap_err();
        assert!(matches!(error, DaemonError::InvalidConfig(_)));
    }

    #[test]
    fn calls_status_and_backup() {
        let (server, token) = server();
        let client = Client::new(server.local_addr(), token).unwrap();

        let status = client.status().unwrap();
        assert_eq!(status.source_dirs, 0);

        let backup = client.backup_now().unwrap();
        assert_eq!(backup.snapshot_id, Some(1));
        assert_eq!(backup.files_backed_up, 1);

        server.stop_and_join();
    }

    #[test]
    fn surfaces_server_rejection() {
        let (server, _) = server();
        let wrong = crate::token::generate_token().unwrap();
        let client = Client::new(server.local_addr(), wrong).unwrap();

        let error = client.status().unwrap_err();
        assert!(matches!(error, DaemonError::Rejected(_)));

        server.stop_and_join();
    }

    #[test]
    fn rejects_oversized_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Consome a requisição inteira antes de responder; fechar com
            // dados não lidos pode provocar RST no Windows.
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            let _ = reader.read_line(&mut request);
            drop(reader);

            let oversized = format!("{}\n", "x".repeat(MAX_RESPONSE_BYTES + 10));
            let _ = stream.write_all(oversized.as_bytes());
        });

        let client = Client::new(addr, crate::token::generate_token().unwrap()).unwrap();
        let error = client.status().unwrap_err();
        assert!(matches!(error, DaemonError::Protocol(_)));
        handle.join().unwrap();
    }

    #[test]
    fn reports_closed_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || drop(listener));

        let client = Client::new(addr, crate::token::generate_token().unwrap()).unwrap();
        assert!(client.status().is_err());
        handle.join().unwrap();
    }
}
