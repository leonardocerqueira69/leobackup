use std::io;
use std::path::PathBuf;

use leobackup_core::{CoreError, FileId, SnapshotId};
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

    /// Snapshot referenciado não existe nos metadados.
    #[error("snapshot {0} não encontrado")]
    SnapshotNotFound(SnapshotId),

    /// Arquivo referenciado não existe nos metadados.
    #[error("arquivo {0} não encontrado nos metadados")]
    FileNotFound(FileId),

    /// O snapshot não está concluído e exigiria uma opção explícita.
    #[error("snapshot {id} está '{status}' e não pode ser restaurado sem permissão explícita")]
    SnapshotNotCompleted {
        /// Identificador do snapshot.
        id: SnapshotId,
        /// Estado persistido (`in-progress` ou `failed`).
        status: &'static str,
    },

    /// O destino já existe e a sobrescrita não foi autorizada.
    #[error("destino já existe: {}", .0.display())]
    DestinationExists(PathBuf),

    /// Caminho de restauração vazio, absoluto, com `..` ou fora da raiz.
    #[error("caminho de restauração inválido: {0}")]
    InvalidRestorePath(String),

    /// Chunk referenciado por `file_chunks` não existe em `chunks`.
    #[error("chunk {0} referenciado pelo arquivo não existe nos metadados")]
    ChunkMetadataMissing(String),

    /// Falha de I/O ao ler o chunk armazenado.
    #[error("falha ao ler o chunk {hash}: {source}")]
    ChunkRead {
        /// Hash do chunk.
        hash: String,
        /// Erro da camada de armazenamento.
        #[source]
        source: StorageError,
    },

    /// Falha de autenticação, descompressão ou formato do chunk.
    #[error("falha ao decodificar o chunk {hash}: {source}")]
    ChunkDecode {
        /// Hash do chunk.
        hash: String,
        /// Erro das primitivas de envelope/criptografia/compressão.
        #[source]
        source: CoreError,
    },

    /// O conteúdo restaurado não corresponde ao hash registrado.
    #[error("chunk {expected} não confere: conteúdo restaurado tem hash {actual}")]
    ChunkHashMismatch {
        /// Hash registrado nos metadados.
        expected: String,
        /// Hash recalculado sobre o conteúdo decodificado.
        actual: String,
    },

    /// Posições de `file_chunks` não são contíguas a partir de zero.
    #[error(
        "ordem de chunks inconsistente no arquivo {file_id}: esperado {expected}, encontrado {actual}"
    )]
    ChunkOrder {
        /// Arquivo cujos chunks estão fora de ordem.
        file_id: FileId,
        /// Posição esperada.
        expected: u32,
        /// Posição encontrada.
        actual: u32,
    },

    /// O total restaurado difere de `files.size`.
    #[error("arquivo restaurado tem {actual} bytes, esperado {expected}")]
    SizeMismatch {
        /// Tamanho registrado no backup.
        expected: u64,
        /// Total efetivamente escrito.
        actual: u64,
    },
}
