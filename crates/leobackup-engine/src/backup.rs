//! Pipeline de backup.
//!
//! Fluxo por arquivo:
//!
//! 1. Validar que a origem é um arquivo regular e capturar seus metadados.
//! 2. Ler em fluxo e dividir com FastCDC.
//! 3. Calcular BLAKE3 de cada chunk.
//! 4. Deduplicar: se o chunk já está no banco **e** no disco, reutilizar.
//! 5. Caso contrário, comprimir, criptografar (se habilitado) e gravar de
//!    forma atômica em `data/`.
//! 6. Ao final, confirmar que a origem não mudou e commitar o arquivo e suas
//!    associações em uma única transação.
//!
//! A ordem garante que os metadados nunca apontem para chunks ausentes: o
//! commit só ocorre depois de todos os chunks estarem persistidos.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use leobackup_core::chunk_file::{self, EncodeOptions};
use leobackup_core::chunking::{chunk_stream, ChunkingParams};
use leobackup_core::hashing;
use leobackup_core::model::{FileRecord, Snapshot, SnapshotId};
use leobackup_core::{CoreError, EncryptionKey, Settings};
use leobackup_storage::{ChunkStore, MetadataStore};

use crate::error::EngineError;

/// Motor de backup.
pub struct BackupEngine {
    store: MetadataStore,
    chunks: ChunkStore,
    settings: Settings,
    key: Option<EncryptionKey>,
    chunking: ChunkingParams,
}

impl BackupEngine {
    /// Cria o motor a partir das dependências já abertas e da configuração.
    ///
    /// Exige consistência entre `settings.encryption.enabled` e a presença da
    /// chave, evitando backups silenciosamente sem criptografia.
    pub fn new(
        store: MetadataStore,
        chunks: ChunkStore,
        settings: Settings,
        key: Option<EncryptionKey>,
    ) -> Result<Self, EngineError> {
        settings.validate()?;

        match (settings.encryption.enabled, key.is_some()) {
            (true, false) => return Err(EngineError::MissingEncryptionKey),
            (false, true) => return Err(EngineError::UnexpectedEncryptionKey),
            _ => {}
        }

        let chunking = chunking_params(&settings)?;

        Ok(Self {
            store,
            chunks,
            settings,
            key,
            chunking,
        })
    }

    /// Acesso somente leitura aos metadados.
    pub fn store(&self) -> &MetadataStore {
        &self.store
    }

    /// Acesso mutável aos metadados (operacional).
    pub fn store_mut(&mut self) -> &mut MetadataStore {
        &mut self.store
    }

    /// Acesso ao armazenamento de chunks.
    pub fn chunk_store(&self) -> &ChunkStore {
        &self.chunks
    }

    /// Cria um snapshot vazio em `in-progress`.
    ///
    /// Útil para lotes que processam vários arquivos em um mesmo ponto no
    /// tempo. O snapshot só deve ser finalizado depois que todos os arquivos
    /// pretendidos foram commitados.
    pub fn begin_snapshot(&mut self) -> Result<Snapshot, EngineError> {
        Ok(self.store.create_snapshot(now_epoch_seconds()?)?)
    }

    /// Marca um snapshot como `completed` e retorna seu estado atualizado.
    pub fn complete_snapshot(&mut self, id: SnapshotId) -> Result<Snapshot, EngineError> {
        self.store.complete_snapshot(id)?;
        self.snapshot_after_update(id)
    }

    /// Marca um snapshot como `failed` e retorna seu estado atualizado.
    pub fn fail_snapshot(&mut self, id: SnapshotId) -> Result<Snapshot, EngineError> {
        self.store.fail_snapshot(id)?;
        self.snapshot_after_update(id)
    }

    fn snapshot_after_update(&self, id: SnapshotId) -> Result<Snapshot, EngineError> {
        self.store
            .get_snapshot(id)?
            .ok_or(EngineError::SnapshotLost(id))
    }

    /// Cria um snapshot, faz backup dos arquivos e o finaliza.
    ///
    /// Erros por arquivo não interrompem os demais. Se qualquer arquivo falhar,
    /// o snapshot é marcado como `failed` e o relatório lista as falhas.
    pub fn backup_files(&mut self, files: &[PathBuf]) -> Result<BackupReport, EngineError> {
        let created = self.store.create_snapshot(now_epoch_seconds()?)?;

        let mut backed_up = Vec::new();
        let mut failures = Vec::new();

        for path in files {
            match self.backup_file(created.id, path) {
                Ok(record) => backed_up.push(record),
                Err(error) => failures.push(FileFailure {
                    path: path.clone(),
                    message: error.to_string(),
                }),
            }
        }

        if failures.is_empty() {
            self.store.complete_snapshot(created.id)?;
        } else {
            self.store.fail_snapshot(created.id)?;
        }

        let snapshot = self
            .store
            .get_snapshot(created.id)?
            .ok_or(EngineError::SnapshotLost(created.id))?;

        Ok(BackupReport {
            snapshot,
            backed_up,
            failures,
        })
    }

    /// Faz backup de um único arquivo em um snapshot `in-progress`.
    ///
    /// Os chunks são persistidos antes do commit dos metadados. O arquivo é
    /// rejeitado se mudar durante a leitura.
    pub fn backup_file(
        &mut self,
        snapshot_id: SnapshotId,
        path: &Path,
    ) -> Result<FileRecord, EngineError> {
        let before = fs::metadata(path).map_err(|source| EngineError::Io {
            path: path.to_path_buf(),
            source,
        })?;

        if !before.is_file() {
            return Err(EngineError::NotAFile(path.to_path_buf()));
        }

        let path_str = path
            .to_str()
            .ok_or_else(|| EngineError::NonUtf8Path(path.to_path_buf()))?
            .to_owned();

        let permissions = permissions_of(&before);
        let modification_time = modification_time_of(&before);
        let size = before.len();

        let file = File::open(path).map_err(|source| EngineError::Io {
            path: path.to_path_buf(),
            source,
        })?;

        let mut hashes = Vec::new();

        for chunk in chunk_stream(file, self.chunking) {
            let chunk = chunk?;
            let hash = hashing::hash_chunk(chunk.data());
            if !hashing::is_valid_hash(&hash) {
                return Err(EngineError::InvalidHash(hash));
            }

            let needs_write = match self.store.get_chunk(&hash)? {
                // Metadado existe, mas o arquivo pode estar ausente (corrupção):
                // nesse caso, regravamos o chunk a partir do conteúdo atual.
                Some(_) => !self.chunks.exists(&hash)?,
                None => true,
            };

            if needs_write {
                let stored_size = {
                    let options = EncodeOptions {
                        compression_level: self.settings.compression.level,
                        encryption: if self.settings.encryption.enabled {
                            self.key.as_ref()
                        } else {
                            None
                        },
                    };
                    let stored = chunk_file::encode_chunk(chunk.data(), &options)?;
                    self.chunks.write_chunk(&hash, &stored)?
                };

                self.store
                    .insert_chunk(&hash, chunk.data().len() as u64, stored_size)?;
            }

            hashes.push(hash);
        }

        let after = fs::metadata(path).map_err(|source| EngineError::Io {
            path: path.to_path_buf(),
            source,
        })?;

        if after.len() != before.len() || modification_time_of(&after) != modification_time {
            return Err(EngineError::SourceChanged(path.to_path_buf()));
        }

        // Commit só depois de todos os chunks estarem no disco.
        let record = self.store.commit_file(
            snapshot_id,
            &path_str,
            permissions,
            modification_time,
            size,
            &hashes,
        )?;

        Ok(record)
    }
}

/// Resultado de uma execução de backup.
#[derive(Debug, Clone)]
pub struct BackupReport {
    /// Snapshot criado (`completed` se não houve falhas, senão `failed`).
    pub snapshot: Snapshot,
    /// Arquivos cujo backup foi concluído.
    pub backed_up: Vec<FileRecord>,
    /// Arquivos que falharam, com a mensagem correspondente.
    pub failures: Vec<FileFailure>,
}

/// Falha de backup de um arquivo específico.
#[derive(Debug, Clone)]
pub struct FileFailure {
    /// Caminho do arquivo.
    pub path: PathBuf,
    /// Mensagem de erro.
    pub message: String,
}

fn chunking_params(settings: &Settings) -> Result<ChunkingParams, EngineError> {
    let min_size = u32::try_from(settings.chunking.min_size)
        .map_err(|_| CoreError::InvalidConfig("chunking.min_size excede u32::MAX".to_string()))?;
    let avg_size = u32::try_from(settings.chunking.avg_size)
        .map_err(|_| CoreError::InvalidConfig("chunking.avg_size excede u32::MAX".to_string()))?;
    let max_size = u32::try_from(settings.chunking.max_size)
        .map_err(|_| CoreError::InvalidConfig("chunking.max_size excede u32::MAX".to_string()))?;

    Ok(ChunkingParams::new(min_size, avg_size, max_size)?)
}

fn now_epoch_seconds() -> Result<i64, EngineError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| EngineError::Clock(error.to_string()))?;
    i64::try_from(elapsed.as_secs())
        .map_err(|_| EngineError::Clock("timestamp excede i64::MAX".to_string()))
}

fn modification_time_of(metadata: &fs::Metadata) -> Option<i64> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
}

#[cfg(unix)]
fn permissions_of(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode())
}

#[cfg(not(unix))]
fn permissions_of(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use leobackup_core::chunk_file::{decode_chunk, FLAG_ENCRYPTED};
    use leobackup_core::crypto::KEY_LEN;
    use leobackup_core::SnapshotStatus;
    use leobackup_storage::RepositoryLayout;
    use std::path::Path as StdPath;

    struct Repo {
        _dir: tempfile::TempDir,
        engine: BackupEngine,
        key: Option<EncryptionKey>,
    }

    fn setup(encrypted: bool) -> Repo {
        let dir = tempfile::tempdir().unwrap();
        let layout = RepositoryLayout::new(dir.path()).unwrap();
        layout.ensure().unwrap();

        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);

        let mut settings = Settings::default();
        settings.chunking.min_size = 1_024;
        settings.chunking.avg_size = 4_096;
        settings.chunking.max_size = 16_384;
        settings.encryption.enabled = encrypted;

        let key = if encrypted {
            Some(EncryptionKey::from_bytes([5u8; KEY_LEN]))
        } else {
            None
        };
        let engine = BackupEngine::new(store, chunks, settings, key.clone()).unwrap();

        Repo {
            _dir: dir,
            engine,
            key,
        }
    }

    fn write_file(dir: &StdPath, name: &str, data: &[u8]) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, data).unwrap();
        path
    }

    fn pseudo_random(len: usize) -> Vec<u8> {
        let mut data = vec![0u8; len];
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for byte in &mut data {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        data
    }

    #[test]
    fn backs_up_empty_and_small_files() {
        let mut repo = setup(false);
        let root = repo._dir.path();
        let empty = write_file(root, "empty.txt", b"");
        let small = write_file(root, "small.txt", b"conteudo");

        let report = repo
            .engine
            .backup_files(&[empty.clone(), small.clone()])
            .unwrap();

        assert_eq!(report.snapshot.status, SnapshotStatus::Completed);
        assert_eq!(report.backed_up.len(), 2);
        assert!(report.failures.is_empty());

        let files = repo.engine.store().list_files(report.snapshot.id).unwrap();
        assert_eq!(files.len(), 2);

        let empty_record = files
            .iter()
            .find(|f| f.path.ends_with("empty.txt"))
            .unwrap();
        assert_eq!(empty_record.size, 0);
        assert!(repo
            .engine
            .store()
            .list_file_chunks(empty_record.id)
            .unwrap()
            .is_empty());

        assert_eq!(repo.engine.store().list_chunks().unwrap().len(), 1);
    }

    #[test]
    fn deduplicates_identical_files_in_snapshot() {
        let mut repo = setup(false);
        let root = repo._dir.path();
        let data = b"mesmo conteudo em dois arquivos";
        let first = write_file(root, "a.txt", data);
        let second = write_file(root, "b.txt", data);

        let report = repo.engine.backup_files(&[first, second]).unwrap();
        assert_eq!(report.snapshot.status, SnapshotStatus::Completed);

        let chunks = repo.engine.store().list_chunks().unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].reference_count, 2);
    }

    #[test]
    fn deduplicates_across_snapshots() {
        let mut repo = setup(false);
        let root = repo._dir.path();
        let path = write_file(root, "a.txt", b"conteudo incremental");

        let first = repo
            .engine
            .backup_files(std::slice::from_ref(&path))
            .unwrap();
        assert_eq!(repo.engine.store().list_chunks().unwrap().len(), 1);

        let second = repo.engine.backup_files(&[path]).unwrap();
        let chunks = repo.engine.store().list_chunks().unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].reference_count, 2);

        let first_file = &first.backed_up[0];
        let second_file = &second.backed_up[0];
        assert_eq!(
            repo.engine.store().list_file_chunks(first_file.id).unwrap()[0].chunk_id,
            repo.engine
                .store()
                .list_file_chunks(second_file.id)
                .unwrap()[0]
                .chunk_id
        );
    }

    #[test]
    fn restore_roundtrip_reconstructs_bytes() {
        let mut repo = setup(false);
        let root = repo._dir.path();
        let data = pseudo_random(120_000);
        let path = write_file(root, "large.bin", &data);

        let report = repo.engine.backup_files(&[path]).unwrap();
        assert_eq!(report.snapshot.status, SnapshotStatus::Completed);
        let file = &report.backed_up[0];

        let links = repo.engine.store().list_file_chunks(file.id).unwrap();
        assert!(links.len() > 1, "o arquivo deveria gerar múltiplos chunks");

        let mut rebuilt = Vec::with_capacity(data.len());
        for link in links {
            let chunk = repo
                .engine
                .store()
                .get_chunk(&link.chunk_id)
                .unwrap()
                .unwrap();
            let stored = repo
                .engine
                .chunk_store()
                .read_chunk(&link.chunk_id)
                .unwrap();
            let decoded = decode_chunk(&stored, chunk.size, None).unwrap();
            rebuilt.extend_from_slice(&decoded);
        }

        assert_eq!(rebuilt, data);
    }

    #[test]
    fn encrypted_chunks_roundtrip_and_require_key() {
        let mut repo = setup(true);
        let root = repo._dir.path();
        let data = b"conteudo sensivel".repeat(100);
        let path = write_file(root, "secret.txt", &data);

        let report = repo.engine.backup_files(&[path]).unwrap();
        let file = &report.backed_up[0];
        let link = &repo.engine.store().list_file_chunks(file.id).unwrap()[0];

        let stored = repo
            .engine
            .chunk_store()
            .read_chunk(&link.chunk_id)
            .unwrap();
        assert_ne!(stored[4] & FLAG_ENCRYPTED, 0);

        let chunk = repo
            .engine
            .store()
            .get_chunk(&link.chunk_id)
            .unwrap()
            .unwrap();
        let decoded = decode_chunk(&stored, chunk.size, repo.key.as_ref()).unwrap();
        assert_eq!(decoded, data);

        let wrong = EncryptionKey::from_bytes([6u8; KEY_LEN]);
        assert!(decode_chunk(&stored, chunk.size, Some(&wrong)).is_err());
    }

    #[test]
    fn encryption_requires_key_at_construction() {
        let dir = tempfile::tempdir().unwrap();
        let layout = RepositoryLayout::new(dir.path()).unwrap();
        layout.ensure().unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);

        let settings = Settings::default();
        assert!(settings.encryption.enabled);
        assert!(matches!(
            BackupEngine::new(store, chunks, settings, None),
            Err(EngineError::MissingEncryptionKey)
        ));
    }

    #[test]
    fn partial_failure_marks_snapshot_failed() {
        let mut repo = setup(false);
        let root = repo._dir.path();
        let good = write_file(root, "good.txt", b"ok");
        let missing = root.join("missing.txt");

        let report = repo.engine.backup_files(&[good, missing]).unwrap();

        assert_eq!(report.snapshot.status, SnapshotStatus::Failed);
        assert_eq!(report.backed_up.len(), 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(
            repo.engine
                .store()
                .list_files(report.snapshot.id)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn rejects_directories() {
        let mut repo = setup(false);
        let root = repo._dir.path().to_path_buf();
        let report = repo.engine.backup_files(&[root]).unwrap();

        assert_eq!(report.snapshot.status, SnapshotStatus::Failed);
        assert_eq!(report.failures.len(), 1);
    }

    #[test]
    fn commit_requires_in_progress_snapshot() {
        let mut repo = setup(false);
        let root = repo._dir.path();
        let path = write_file(root, "a.txt", b"dados");

        let snapshot = repo.engine.store_mut().create_snapshot(0).unwrap();
        repo.engine
            .store_mut()
            .complete_snapshot(snapshot.id)
            .unwrap();

        assert!(repo.engine.backup_file(snapshot.id, &path).is_err());
    }
}
