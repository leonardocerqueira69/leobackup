//! Tipos de domínio dos metadados de backup.
//!
//! Estes tipos são independentes da persistência: a crate
//! `leobackup-storage` os converte de/para as linhas do SQLite.

use std::str::FromStr;

use crate::error::CoreError;

/// Identificador de um snapshot.
pub type SnapshotId = i64;

/// Identificador de um arquivo dentro de um snapshot.
pub type FileId = i64;

/// Estado de um snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotStatus {
    /// O snapshot está sendo montado.
    InProgress,
    /// Todos os chunks e metadados foram persistidos com sucesso.
    Completed,
    /// O snapshot falhou e não deve ser considerado válido.
    Failed,
}

impl SnapshotStatus {
    /// Representação persistida no SQLite.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InProgress => "in-progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for SnapshotStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "in-progress" => Ok(Self::InProgress),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(CoreError::InvalidData(format!(
                "status de snapshot desconhecido: {other}"
            ))),
        }
    }
}

impl std::fmt::Display for SnapshotStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Um snapshot (ponto no tempo) do conjunto monitorado.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Identificador gerado pelo banco.
    pub id: SnapshotId,
    /// Instante de criação, em segundos desde a época Unix (UTC).
    pub timestamp: i64,
    /// Estado atual.
    pub status: SnapshotStatus,
}

/// Metadados de um arquivo pertencente a um snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    /// Identificador gerado pelo banco.
    pub id: FileId,
    /// Snapshot ao qual o arquivo pertence.
    pub snapshot_id: SnapshotId,
    /// Caminho como registrado no snapshot.
    pub path: String,
    /// Bits de permissão, quando conhecidos (semântica do SO).
    pub permissions: Option<u32>,
    /// Instante da última modificação, em segundos desde a época Unix (UTC).
    pub modification_time: Option<i64>,
    /// Tamanho do arquivo original, em bytes.
    pub size: u64,
}

/// Um chunk único armazenado no repositório.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRecord {
    /// Hash BLAKE3 do conteúdo, usado como chave primária.
    pub hash_id: String,
    /// Tamanho do chunk original, em bytes.
    pub size: u64,
    /// Tamanho após compressão/criptografia, em bytes.
    pub compressed_size: u64,
    /// Número de ocorrências em `file_chunks`.
    pub reference_count: u64,
}

/// Associação ordenada entre um arquivo e um chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChunkLink {
    /// Arquivo dono da associação.
    pub file_id: FileId,
    /// Chunk referenciado.
    pub chunk_id: String,
    /// Posição do chunk na reconstrução do arquivo (base zero).
    pub position: u32,
}
