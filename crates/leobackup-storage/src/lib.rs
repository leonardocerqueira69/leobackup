//! Persistência do LeoBackup.
//!
//! Expõe o layout canônico do repositório de backup, o banco de metadados
//! SQLite (`metadata/index.db`) com migrações versionadas e a camada CRUD
//! usada pelo restante do sistema, além do armazenamento físico dos chunks.

pub mod chunk_store;
pub mod error;
pub mod layout;
pub mod migrations;
pub mod store;

pub use chunk_store::ChunkStore;
pub use error::StorageError;
pub use layout::RepositoryLayout;
pub use store::MetadataStore;
