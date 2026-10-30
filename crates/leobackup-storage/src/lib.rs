//! Persistência do LeoBackup.
//!
//! Nesta fase a crate expõe apenas o layout canônico do repositório de
//! backup e a rotina idempotente que o inicializa. A conexão SQLite e os
//! repositórios de metadados serão adicionados na Fase 2.

pub mod error;
pub mod layout;

pub use error::StorageError;
pub use layout::RepositoryLayout;
