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

    /// Falha reportada pelo SQLite.
    #[error("erro de banco de dados: {0}")]
    Database(#[from] rusqlite::Error),

    /// Falha de configuração ou aplicação de migrações.
    #[error("erro de migração: {0}")]
    Migration(String),

    /// Dado persistido ou recebido em formato inconsistente.
    #[error("dado inválido: {0}")]
    InvalidData(String),

    /// Snapshot inexistente ou fora do estado que permite commit de arquivos.
    #[error("snapshot {0} não está disponível para commit (inexistente ou fora de in-progress)")]
    SnapshotUnavailable(i64),
}
