//! Tipos base e utilitários compartilhados do LeoBackup.
//!
//! Esta crate concentra o que é independente de I/O de backup, do sistema
//! operacional e do transporte de comunicação: erros comuns, configuração,
//! modelos de domínio e as primitivas do pipeline (chunking, hashing,
//! compressão, criptografia e envelope de chunk).

pub mod chunk_file;
pub mod chunking;
pub mod compression;
pub mod config;
pub mod crypto;
pub mod error;
pub mod hashing;
pub mod model;

pub use chunk_file::{decode_chunk, encode_chunk, EncodeOptions};
pub use chunking::{chunk_stream, Chunk, ChunkingParams};
pub use config::Settings;
pub use crypto::EncryptionKey;
pub use error::CoreError;
pub use model::{
    ChunkRecord, FileChunkLink, FileId, FileRecord, Snapshot, SnapshotId, SnapshotStatus,
};
