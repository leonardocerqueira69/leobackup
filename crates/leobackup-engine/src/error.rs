use std::io;
use std::path::PathBuf;

use leobackup_core::{CoreError, SnapshotId};
use leobackup_storage::StorageError;

/// Erros do pipeline de backup.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Falha de I/O, com o caminho que a originou.
    #[error("erro de I/O em {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Erro das primitivas de chunking/hash/compressão/criptografia.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Erro da camada de armazenamento.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// O caminho informado não é um arquivo regular.
    #[error("o caminho não é um arquivo regular: {}", .0.display())]
    NotAFile(PathBuf),

    /// O arquivo mudou entre a leitura inicial e o commit.
    #[error("o arquivo de origem mudou durante o backup: {}", .0.display())]
    SourceChanged(PathBuf),

    /// O caminho não pode ser representado como UTF-8.
    #[error("o caminho não é UTF-8 válido: {}", .0.display())]
    NonUtf8Path(PathBuf),

    /// Criptografia habilitada sem chave configurada.
    #[error("criptografia habilitada, mas nenhuma chave foi fornecida")]
    MissingEncryptionKey,

    /// Chave fornecida com criptografia desabilitada.
    #[error("uma chave foi fornecida, mas a criptografia está desabilitada")]
    UnexpectedEncryptionKey,

    /// Falha ao ler o relógio do sistema.
    #[error("falha ao obter o horário do sistema: {0}")]
    Clock(String),

    /// Hash inválido produzido pelo pipeline.
    #[error("hash de chunk inválido gerado: {0}")]
    InvalidHash(String),

    /// Snapshot desapareceu entre a criação e o commit.
    #[error("snapshot {0} desapareceu após o backup")]
    SnapshotLost(SnapshotId),
}
