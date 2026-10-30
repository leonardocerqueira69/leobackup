//! Tipos base e utilitários compartilhados do LeoBackup.
//!
//! Esta crate concentra o que é independente de I/O pesado, do sistema
//! operacional e do transporte de comunicação: erros comuns e o modelo de
//! configuração. Os módulos de chunking, hashing, compressão e criptografia
//! serão adicionados nas próximas fases.

pub mod config;
pub mod error;
pub mod model;

pub use config::Settings;
pub use error::CoreError;
pub use model::{ChunkRecord, FileChunkLink, FileRecord, Snapshot, SnapshotStatus};
