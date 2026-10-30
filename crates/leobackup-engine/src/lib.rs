//! Pipeline de backup e restauração do LeoBackup.
//!
//! A Fase 3 implementa o fluxo de backup (FastCDC, BLAKE3, deduplicação,
//! Zstandard, AES-GCM, escrita atômica e commit transacional). A
//! reconstrução completa será exposta na Fase 5; o envelope de chunk já é
//! decodificável por `leobackup_core::chunk_file`.

pub mod backup;
pub mod error;

pub use backup::{BackupEngine, BackupReport, FileFailure};
pub use error::EngineError;
