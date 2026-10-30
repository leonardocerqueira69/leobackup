//! Layout canônico do repositório de backup.
//!
//! ```text
//! <raiz>/
//! ├── data/                 # chunks comprimidos/criptografados
//! ├── metadata/
//! │   └── index.db          # metadados (SQLite, Fase 2)
//! └── config/
//!     ├── settings.json     # configuração
//!     └── daemon.token      # token do servidor local (Fase 4)
//! ```
//!
//! A inicialização cria os diretórios ausentes de forma idempotente e nunca
//! remove ou sobrescreve dados existentes. O token é criado pelo daemon, não
//! por `ensure()`.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::StorageError;

/// Nome do subdiretório de chunks.
pub const DATA_DIR: &str = "data";

/// Nome do subdiretório de metadados.
pub const METADATA_DIR: &str = "metadata";

/// Nome do subdiretório de configuração.
pub const CONFIG_DIR: &str = "config";

/// Nome do arquivo de configuração.
pub const SETTINGS_FILE: &str = "settings.json";

/// Nome do banco de metadados.
pub const DB_FILE: &str = "index.db";

/// Nome do arquivo com o token de autenticação do servidor local.
pub const TOKEN_FILE: &str = "daemon.token";

/// Layout do repositório de backup, derivado de sua raiz.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryLayout {
    root: PathBuf,
}

impl RepositoryLayout {
    /// Cria a descrição do layout para `root`.
    ///
    /// Retorna erro se a raiz for um caminho vazio.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(StorageError::InvalidPath(
                "a raiz do repositório não pode ser vazia".to_string(),
            ));
        }
        Ok(Self { root })
    }

    /// Raiz do repositório.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Diretório `data/`.
    pub fn data_dir(&self) -> PathBuf {
        self.root.join(DATA_DIR)
    }

    /// Diretório `metadata/`.
    pub fn metadata_dir(&self) -> PathBuf {
        self.root.join(METADATA_DIR)
    }

    /// Diretório `config/`.
    pub fn config_dir(&self) -> PathBuf {
        self.root.join(CONFIG_DIR)
    }

    /// Caminho de `config/settings.json`.
    pub fn settings_path(&self) -> PathBuf {
        self.config_dir().join(SETTINGS_FILE)
    }

    /// Caminho de `metadata/index.db`.
    pub fn database_path(&self) -> PathBuf {
        self.metadata_dir().join(DB_FILE)
    }

    /// Caminho de `config/daemon.token`.
    ///
    /// O arquivo é criado pelo daemon na primeira execução e deve ter
    /// permissões restritas ao usuário atual.
    pub fn token_path(&self) -> PathBuf {
        self.config_dir().join(TOKEN_FILE)
    }

    /// Garante que `data/`, `metadata/` e `config/` existam.
    ///
    /// É idempotente: pode ser chamada repetidamente e não apaga arquivos
    /// já presentes no repositório.
    pub fn ensure(&self) -> Result<(), StorageError> {
        for dir in [self.data_dir(), self.metadata_dir(), self.config_dir()] {
            fs::create_dir_all(&dir).map_err(|source| StorageError::Io {
                path: dir.clone(),
                source,
            })?;
        }
        Ok(())
    }

    /// Indica se a estrutura básica do repositório já existe.
    pub fn is_initialized(&self) -> bool {
        self.data_dir().is_dir() && self.metadata_dir().is_dir() && self.config_dir().is_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_creates_structure() {
        let dir = tempfile::tempdir().unwrap();
        let layout = RepositoryLayout::new(dir.path()).unwrap();

        assert!(!layout.is_initialized());
        layout.ensure().unwrap();

        assert!(layout.data_dir().is_dir());
        assert!(layout.metadata_dir().is_dir());
        assert!(layout.config_dir().is_dir());
        assert!(layout.is_initialized());
    }

    #[test]
    fn ensure_is_idempotent_and_preserves_files() {
        let dir = tempfile::tempdir().unwrap();
        let layout = RepositoryLayout::new(dir.path()).unwrap();
        layout.ensure().unwrap();

        let marker = layout.data_dir().join("keep.bin");
        fs::write(&marker, b"dados existentes").unwrap();

        layout.ensure().unwrap();
        assert_eq!(fs::read(&marker).unwrap(), b"dados existentes");
    }

    #[test]
    fn rejects_empty_root() {
        assert!(matches!(
            RepositoryLayout::new(""),
            Err(StorageError::InvalidPath(_))
        ));
    }

    #[test]
    fn builds_expected_paths() {
        let layout = RepositoryLayout::new("repo").unwrap();
        assert_eq!(layout.data_dir(), Path::new("repo").join("data"));
        assert_eq!(
            layout.settings_path(),
            Path::new("repo").join("config").join("settings.json")
        );
        assert_eq!(
            layout.database_path(),
            Path::new("repo").join("metadata").join("index.db")
        );
        assert_eq!(
            layout.token_path(),
            Path::new("repo").join("config").join("daemon.token")
        );
    }
}
