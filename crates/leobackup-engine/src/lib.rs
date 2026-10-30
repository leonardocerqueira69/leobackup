//! Pipeline de backup e restauração do LeoBackup.
//!
//! A Fase 3 implementa o fluxo de backup (FastCDC, BLAKE3, deduplicação,
//! Zstandard, AES-GCM, escrita atômica e commit transacional). A Fase 5
//! adiciona o fluxo de restauração: consulta de snapshots/arquivos,
//! reconstrução ordenada dos chunks com validação de hash e publicação
//! atômica no destino, preservando permissões e `modification_time`.

pub mod backup;
pub mod error;
pub mod maintenance;
pub mod restore;

pub use backup::{BackupEngine, BackupReport, FileFailure};
pub use error::EngineError;
pub use maintenance::{run_maintenance, MaintenanceReport, ScrubResult};
pub use restore::{RestoreEngine, RestoreOptions, RestoreReport};
