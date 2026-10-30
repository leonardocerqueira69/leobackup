//! Erros do daemon (watcher, fila, servidor local e inicialização).

use std::io;
use std::path::PathBuf;

use leobackup_core::CoreError;
use leobackup_engine::EngineError;
use leobackup_storage::StorageError;

/// Erros do daemon.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// Falha de I/O, com o caminho que a originou.
    #[error("erro de I/O em {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Erro das primitivas do LeoBackup.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Erro da camada de armazenamento.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// Erro do pipeline de backup.
    #[error(transparent)]
    Engine(#[from] EngineError),

    /// Configuração inválida para iniciar o daemon.
    #[error("configuração inválida: {0}")]
    InvalidConfig(String),

    /// Falha ao configurar ou operar o watcher.
    #[error("watcher: {0}")]
    Watcher(String),

    /// Falha no servidor de comunicação local.
    #[error("servidor local: {0}")]
    Server(String),

    /// Requisição ou resposta fora do protocolo esperado.
    #[error("protocolo: {0}")]
    Protocol(String),

    /// O daemon recusou a requisição (autenticação, comando indisponível).
    #[error("daemon recusou: {0}")]
    Rejected(String),

    /// Falha ao ler o arquivo de chave.
    #[error("chave: {0}")]
    Key(String),

    /// Estado interno inesperado (thread encerrada, canal fechado etc.).
    #[error("estado interno: {0}")]
    Internal(String),
}

impl DaemonError {
    /// Constrói um erro de I/O com contexto de caminho.
    pub fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

impl From<serde_json::Error> for DaemonError {
    fn from(error: serde_json::Error) -> Self {
        Self::Protocol(format!("JSON inválido: {error}"))
    }
}
