use std::io;
use std::path::PathBuf;

/// Erros da camada de armazenamento.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Falha de I/O, com o caminho que a originou.
    #[error("erro de I/O em {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Caminho de repositório inválido.
    #[error("caminho inválido: {0}")]
    InvalidPath(String),
}
