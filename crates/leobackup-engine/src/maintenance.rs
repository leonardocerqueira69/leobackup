//! Rotinas de resiliência, manutenção e integridade (Fase 6).
//!
//! - **Crash recovery** (`clean_orphan_chunks`): identifica chunks físicos
//!   no disco (`data/`) que não possuem referência no banco (`chunks`)
//!   e os remove.
//! - **Pruning** (`prune_chunks`): identifica chunks com
//!   `reference_count = 0` no banco, remove o registro e o arquivo físico.
//! - **Scrubbing** (`scrub_chunks`): lê chunks armazenados, descriptografa
//!   (se aplicável), recalcula BLAKE3 e compara com o hash registrado.
//!   Se falhar, registra em `corrupted_chunks`.

use leobackup_core::hashing;
use leobackup_core::EncryptionKey;
use leobackup_storage::{ChunkStore, MetadataStore};

use crate::error::EngineError;

/// Resultado das três rotinas de manutenção.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceReport {
    /// Quantos chunks físicos foram removidos como órfãos.
    pub orphaned_chunks_removed: usize,
    /// Hashes removidos pelo pruning.
    pub pruned_chunks: Vec<String>,
    /// Resultado do scrubbing.
    pub scrub: ScrubResult,
}

/// Resultado de uma verificação de integridade.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScrubResult {
    /// Chunks verificados com sucesso.
    pub checked: usize,
    /// Hashes corrompidos detectados.
    pub corrupted: Vec<String>,
    /// Erros de leitura (não necessariamente corrupção).
    pub read_errors: Vec<String>,
}

/// Executa as rotinas de manutenção.
pub fn run_maintenance(
    store: &mut MetadataStore,
    chunks: &ChunkStore,
    key: Option<&EncryptionKey>,
    scrub_sample_limit: Option<usize>,
) -> Result<MaintenanceReport, EngineError> {
    store.recompute_reference_counts()?;

    let orphaned = clean_orphan_chunks(store, chunks)?;
    let pruned = prune_chunks(store, chunks)?;
    let scrub = scrub_chunks(store, chunks, key, scrub_sample_limit)?;

    Ok(MaintenanceReport {
        orphaned_chunks_removed: orphaned,
        pruned_chunks: pruned,
        scrub,
    })
}

/// Limpa chunks físicos que não têm registro em `chunks`.
fn clean_orphan_chunks(store: &MetadataStore, chunks: &ChunkStore) -> Result<usize, EngineError> {
    use std::fs;
    let mut removed = 0usize;

    let stored = chunks.list_stored_hashes()?;
    for hash_id in stored {
        if store.get_chunk(&hash_id)?.is_none() {
            let path = chunks.chunk_path(&hash_id).map_err(|_e| {
                EngineError::Storage(leobackup_storage::StorageError::InvalidData(format!(
                    "hash inválido para chunk_path: {hash_id}"
                )))
            })?;
            if path.is_file() {
                fs::remove_file(&path).map_err(|e| EngineError::Io {
                    path: path.clone(),
                    source: e,
                })?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

/// Remove chunks com `reference_count = 0` do banco e do disco.
fn prune_chunks(
    store: &mut MetadataStore,
    chunks: &ChunkStore,
) -> Result<Vec<String>, EngineError> {
    use std::fs;
    let mut pruned = Vec::new();

    let to_prune: Vec<String> = store
        .list_chunks()?
        .into_iter()
        .filter(|c| c.reference_count == 0)
        .map(|c| c.hash_id)
        .collect();

    for hash_id in to_prune {
        let path = chunks.chunk_path(&hash_id).map_err(|e| match e {
            leobackup_storage::StorageError::InvalidData(_) => {
                EngineError::InvalidRestorePath(format!("hash inválido no pruning: {hash_id}"))
            }
            other => EngineError::Storage(other),
        })?;

        if path.is_file() {
            fs::remove_file(&path).map_err(|e| EngineError::Io {
                path: path.clone(),
                source: e,
            })?;
        }

        store.delete_chunk(&hash_id)?;
        pruned.push(hash_id);
    }

    Ok(pruned)
}

/// Verifica integridade de uma amostra (ou todos) os chunks armazenados.
fn scrub_chunks(
    store: &mut MetadataStore,
    chunks: &ChunkStore,
    key: Option<&EncryptionKey>,
    sample_limit: Option<usize>,
) -> Result<ScrubResult, EngineError> {
    use leobackup_core::chunk_file::decode_chunk;

    let mut checked = 0usize;
    let mut corrupted = Vec::new();
    let mut read_errors = Vec::new();

    let chunk_ids: Vec<String> = chunks.list_stored_hashes()?;
    let hashes: Vec<String> = match sample_limit {
        Some(limit) => chunk_ids.into_iter().take(limit).collect(),
        None => chunk_ids,
    };

    for hash_id in hashes {
        checked += 1;
        let stored = match chunks.read_chunk(&hash_id) {
            Ok(data) => data,
            Err(_) => {
                read_errors.push(hash_id.clone());
                continue;
            }
        };

        let record = match store.get_chunk(&hash_id) {
            Ok(Some(r)) => r,
            Ok(None) => {
                // Chunk físico sem metadado: será removido pelo crash recovery.
                read_errors.push(hash_id.clone());
                continue;
            }
            Err(_) => {
                read_errors.push(hash_id.clone());
                continue;
            }
        };

        let decoded = match decode_chunk(&stored, record.size, key) {
            Ok(data) => data,
            Err(_) => {
                store
                    .mark_chunk_corrupted(&hash_id)
                    .map_err(EngineError::Storage)?;
                corrupted.push(hash_id);
                continue;
            }
        };

        let actual = hashing::hash_chunk(&decoded);
        if actual != hash_id {
            store
                .mark_chunk_corrupted(&hash_id)
                .map_err(EngineError::Storage)?;
            corrupted.push(hash_id);
        }
    }

    Ok(ScrubResult {
        checked,
        corrupted,
        read_errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use leobackup_storage::{ChunkStore, MetadataStore, RepositoryLayout};
    use std::fs;

    fn setup_maintenance() -> (tempfile::TempDir, MetadataStore, ChunkStore) {
        let dir = tempfile::tempdir().unwrap();
        let layout = RepositoryLayout::new(dir.path()).unwrap();
        layout.ensure().unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        (dir, store, chunks)
    }

    #[test]
    fn crash_recovery_removes_orphan_chunks() {
        let (_dir, mut store, chunks) = setup_maintenance();
        let chunk_path = chunks
            .chunk_path("aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            .unwrap();
        fs::create_dir_all(chunk_path.parent().unwrap()).unwrap();
        fs::write(&chunk_path, b"dados").unwrap();

        assert!(!store
            .get_chunk("aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            .unwrap()
            .is_some());

        let report = run_maintenance(&mut store, &chunks, None, None).unwrap();
        assert_eq!(report.orphaned_chunks_removed, 1);
        assert!(!chunk_path.exists());
    }

    #[test]
    fn pruning_removes_unreferenced_chunks() {
        let (_dir, mut store, chunks) = setup_maintenance();
        let snapshot = store.create_snapshot(10).unwrap();
        store
            .insert_chunk(
                "aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
                4,
                8,
            )
            .unwrap();
        store
            .commit_file(
                snapshot.id,
                "/test/file.txt",
                None,
                None,
                5,
                &["aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"],
            )
            .unwrap();

        store.delete_snapshot(snapshot.id).unwrap();
        store.recompute_reference_counts().unwrap();

        let chunk_path = chunks
            .chunk_path("aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            .unwrap();
        fs::create_dir_all(chunk_path.parent().unwrap()).unwrap();
        fs::write(&chunk_path, b"dados").unwrap();

        let report = run_maintenance(&mut store, &chunks, None, None).unwrap();
        assert!(report.pruned_chunks.contains(
            &"aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262".to_string()
        ));
        assert!(!chunk_path.exists());
    }

    #[test]
    fn scrub_detects_corruption() {
        let (_dir, mut store, chunks) = setup_maintenance();
        let snapshot = store.create_snapshot(10).unwrap();
        store
            .insert_chunk(
                "aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
                4,
                8,
            )
            .unwrap();
        store
            .commit_file(
                snapshot.id,
                "/test/file.txt",
                None,
                None,
                4,
                &["aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"],
            )
            .unwrap();

        let chunk_path = chunks
            .chunk_path("aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            .unwrap();
        fs::create_dir_all(chunk_path.parent().unwrap()).unwrap();
        fs::write(&chunk_path, b"corrompido").unwrap();

        let report = run_maintenance(&mut store, &chunks, None, Some(10)).unwrap();
        assert!(report.scrub.corrupted.contains(
            &"aa1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262".to_string()
        ));
    }
}
