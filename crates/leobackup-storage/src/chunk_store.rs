//! Armazenamento físico dos chunks no diretório `data/`.
//!
//! Cada chunk é gravado em `data/<2 primeiros hex do hash>/<hash>` usando
//! um arquivo temporário no mesmo diretório seguido de `persist` (rename
//! atômico). O hash é validado antes de compor qualquer caminho.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use leobackup_core::hashing::is_valid_hash;
use tempfile::NamedTempFile;

use crate::error::StorageError;
use crate::layout::RepositoryLayout;

/// Acesso aos arquivos de chunk dentro de `data/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkStore {
    data_dir: PathBuf,
}

impl ChunkStore {
    /// Cria o acesso a partir do layout do repositório.
    pub fn new(layout: &RepositoryLayout) -> Self {
        Self {
            data_dir: layout.data_dir(),
        }
    }

    /// Diretório raiz dos chunks (`data/`).
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Caminho de armazenamento de um chunk.
    ///
    /// O hash precisa ser BLAKE3 hexadecimal minúsculo de 64 caracteres.
    pub fn chunk_path(&self, hash: &str) -> Result<PathBuf, StorageError> {
        if !is_valid_hash(hash) {
            return Err(StorageError::InvalidData(format!(
                "hash de chunk inválido: {hash}"
            )));
        }
        Ok(self.data_dir.join(&hash[..2]).join(hash))
    }

    /// Indica se o chunk já está armazenado.
    pub fn exists(&self, hash: &str) -> Result<bool, StorageError> {
        Ok(self.chunk_path(hash)?.is_file())
    }

    /// Grava um chunk e retorna o tamanho ocupado no disco.
    ///
    /// Se o arquivo já existir, não o sobrescreve: o hash garante que o
    /// conteúdo é o mesmo.
    pub fn write_chunk(&self, hash: &str, bytes: &[u8]) -> Result<u64, StorageError> {
        let final_path = self.chunk_path(hash)?;

        if final_path.is_file() {
            let metadata = fs::metadata(&final_path).map_err(|source| StorageError::Io {
                path: final_path,
                source,
            })?;
            return Ok(metadata.len());
        }

        let dir = final_path
            .parent()
            .ok_or_else(|| StorageError::InvalidPath("chunk sem diretório pai".to_string()))?;
        fs::create_dir_all(dir).map_err(|source| StorageError::Io {
            path: dir.to_path_buf(),
            source,
        })?;

        let mut temp = NamedTempFile::new_in(dir).map_err(|source| StorageError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        temp.write_all(bytes).map_err(|source| StorageError::Io {
            path: temp.path().to_path_buf(),
            source,
        })?;
        temp.as_file()
            .sync_all()
            .map_err(|source| StorageError::Io {
                path: temp.path().to_path_buf(),
                source,
            })?;

        match temp.persist(&final_path) {
            Ok(_) => {}
            Err(error) => {
                if final_path.is_file() {
                    // Corrida com outro writer: o chunk já publicado tem o
                    // mesmo hash/conteúdo. O temporário é descartado.
                } else {
                    return Err(StorageError::Io {
                        path: final_path,
                        source: error.error,
                    });
                }
            }
        }

        let metadata = fs::metadata(&final_path).map_err(|source| StorageError::Io {
            path: final_path,
            source,
        })?;
        Ok(metadata.len())
    }

    /// Lista todos os hashes armazenados fisicamente no disco (`data/`).
    pub fn list_stored_hashes(&self) -> Result<Vec<String>, StorageError> {
        let mut hashes = Vec::new();
        for entry in fs::read_dir(self.data_dir()).map_err(|source| StorageError::Io {
            path: self.data_dir().to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| StorageError::Io {
                path: self.data_dir().to_path_buf(),
                source,
            })?;
            if entry
                .file_type()
                .map_err(|source| StorageError::Io {
                    path: entry.path(),
                    source,
                })?
                .is_dir()
            {
                for chunk_file in fs::read_dir(entry.path()).map_err(|source| StorageError::Io {
                    path: entry.path(),
                    source,
                })? {
                    let chunk_file = chunk_file.map_err(|source| StorageError::Io {
                        path: entry.path(),
                        source,
                    })?;
                    if chunk_file
                        .file_type()
                        .map_err(|source| StorageError::Io {
                            path: chunk_file.path(),
                            source,
                        })?
                        .is_file()
                    {
                        if let Ok(name) = chunk_file.file_name().into_string() {
                            if is_valid_hash(&name) {
                                hashes.push(name);
                            }
                        }
                    }
                }
            }
        }
        Ok(hashes)
    }

    /// Lê o conteúdo armazenado de um chunk.
    pub fn read_chunk(&self, hash: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.chunk_path(hash)?;
        fs::read(&path).map_err(|source| StorageError::Io { path, source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH_A: &str = "aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    const HASH_B: &str = "bb1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

    fn store(dir: &Path) -> ChunkStore {
        let layout = RepositoryLayout::new(dir).unwrap();
        layout.ensure().unwrap();
        ChunkStore::new(&layout)
    }

    #[test]
    fn rejects_invalid_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        assert!(matches!(
            store.chunk_path("curto"),
            Err(StorageError::InvalidData(_))
        ));
        assert!(matches!(
            store.chunk_path("AA1349B9F5F9A1A6A0404DEA36DCC9499BCB25C9ADC112B7CC9A93CAE41F3262"),
            Err(StorageError::InvalidData(_))
        ));
    }

    #[test]
    fn builds_prefixed_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        let path = store.chunk_path(HASH_A).unwrap();
        assert_eq!(path, dir.path().join("data").join("aa").join(HASH_A));
    }

    #[test]
    fn write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        let bytes = b"conteudo do chunk";

        let written = store.write_chunk(HASH_A, bytes).unwrap();
        assert_eq!(written, bytes.len() as u64);
        assert!(store.exists(HASH_A).unwrap());
        assert_eq!(store.read_chunk(HASH_A).unwrap(), bytes);
    }

    #[test]
    fn existing_chunk_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());

        store.write_chunk(HASH_A, b"original").unwrap();
        let size = store.write_chunk(HASH_A, b"outro conteudo").unwrap();

        assert_eq!(size, b"original".len() as u64);
        assert_eq!(store.read_chunk(HASH_A).unwrap(), b"original");
    }

    #[test]
    fn write_leaves_no_temporary_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        store.write_chunk(HASH_A, b"dados").unwrap();

        let prefix_dir = store.data_dir().join("aa");
        let entries: Vec<_> = fs::read_dir(&prefix_dir).unwrap().collect();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn missing_chunk_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path());
        assert!(matches!(
            store.read_chunk(HASH_B),
            Err(StorageError::Io { .. })
        ));
    }
}
