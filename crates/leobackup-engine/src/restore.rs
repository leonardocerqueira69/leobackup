//! Pipeline de restauração.
//!
//! Fluxo por arquivo:
//!
//! 1. Carregar o arquivo e o snapshot nos metadados e recusar snapshots que
//!    não estejam `completed`, salvo permissão explícita.
//! 2. Calcular o caminho final relativo à raiz de destino, rejeitando
//!    caminhos absolutos, com `..` ou que escapem da raiz por symlink.
//! 3. Criar um arquivo temporário no diretório de destino.
//! 4. Ler os chunks na ordem de `file_chunks`, descriptografar, descomprimir,
//!    validar o tamanho e o BLAKE3 e concatenar no temporário.
//! 5. Validar o tamanho total, aplicar permissões/mtime e só então publicar o
//!    temporário no caminho final (com ou sem sobrescrita, conforme a opção).
//!
//! O arquivo final nunca fica parcialmente escrito: ou o temporário completo é
//! renomeado para o destino, ou o destino permanece intocado.

use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use leobackup_core::chunk_file::decode_chunk;
use leobackup_core::hashing;
use leobackup_core::model::{
    FileChunkLink, FileId, FileRecord, Snapshot, SnapshotId, SnapshotStatus,
};
use leobackup_core::EncryptionKey;
use leobackup_storage::{ChunkStore, MetadataStore};
use tempfile::NamedTempFile;

use crate::error::EngineError;

/// Motor de restauração.
///
/// A chave é opcional e independente de `settings.encryption.enabled`: chunks
/// antigos podem ter sido gravados criptografados mesmo que a configuração
/// atual esteja desabilitada.
pub struct RestoreEngine {
    store: MetadataStore,
    chunks: ChunkStore,
    key: Option<EncryptionKey>,
}

impl RestoreEngine {
    /// Cria o motor a partir das dependências já abertas e da chave.
    pub fn new(store: MetadataStore, chunks: ChunkStore, key: Option<EncryptionKey>) -> Self {
        Self { store, chunks, key }
    }

    /// Acesso somente leitura aos metadados.
    pub fn store(&self) -> &MetadataStore {
        &self.store
    }

    /// Acesso ao armazenamento de chunks.
    pub fn chunk_store(&self) -> &ChunkStore {
        &self.chunks
    }

    /// Lista todos os snapshots persistidos, em ordem de identificador.
    pub fn list_snapshots(&self) -> Result<Vec<Snapshot>, EngineError> {
        Ok(self.store.list_snapshots()?)
    }

    /// Lista os arquivos de um snapshot, em ordem de caminho.
    pub fn list_files(&self, snapshot_id: SnapshotId) -> Result<Vec<FileRecord>, EngineError> {
        Ok(self.store.list_files(snapshot_id)?)
    }

    /// Busca um arquivo pelo caminho exato dentro de um snapshot.
    pub fn find_file(
        &self,
        snapshot_id: SnapshotId,
        path: &str,
    ) -> Result<Option<FileRecord>, EngineError> {
        Ok(self.store.find_file_by_path(snapshot_id, path)?)
    }

    /// Restaura um arquivo para `destination_root` (ou para um subcaminho
    /// relativo definido em [`RestoreOptions::relative_path`]).
    ///
    /// O diretório de destino é criado quando necessário. Por padrão o nome
    /// final é o nome do arquivo registrado no snapshot e a sobrescrita é
    /// recusada.
    pub fn restore_file(
        &self,
        file_id: FileId,
        destination_root: &Path,
        options: &RestoreOptions,
    ) -> Result<RestoreReport, EngineError> {
        let file = self
            .store
            .get_file(file_id)?
            .ok_or(EngineError::FileNotFound(file_id))?;

        let snapshot = self
            .store
            .get_snapshot(file.snapshot_id)?
            .ok_or(EngineError::SnapshotNotFound(file.snapshot_id))?;

        if snapshot.status != SnapshotStatus::Completed && !options.allow_incomplete_snapshot {
            return Err(EngineError::SnapshotNotCompleted {
                id: snapshot.id,
                status: snapshot.status.as_str(),
            });
        }

        let relative = match &options.relative_path {
            Some(path) => sanitize_relative(path)?,
            None => default_relative_path(&file)?,
        };

        let target = resolve_target(destination_root, &relative)?;

        if target.is_dir() || (!options.overwrite && target.symlink_metadata().is_ok()) {
            return Err(EngineError::DestinationExists(target));
        }

        let parent = target.parent().ok_or_else(|| {
            EngineError::InvalidRestorePath(format!(
                "destino sem diretório pai: {}",
                target.display()
            ))
        })?;

        let mut temp = NamedTempFile::new_in(parent).map_err(|source| io_error(parent, source))?;
        let temp_path = temp.path().to_path_buf();

        let links = self.store.list_file_chunks(file.id)?;
        let (total, chunks_restored) =
            self.write_chunks(&links, file.id, temp.as_file_mut(), &temp_path)?;

        if total != file.size {
            return Err(EngineError::SizeMismatch {
                expected: file.size,
                actual: total,
            });
        }

        temp.as_file()
            .sync_all()
            .map_err(|source| io_error(&temp_path, source))?;

        apply_permissions(&temp_path, file.permissions)?;
        apply_modification_time(&temp_path, file.modification_time)?;

        let destination = persist(temp, &target, options.overwrite)?;

        Ok(RestoreReport {
            file,
            snapshot,
            destination,
            bytes_written: total,
            chunks_restored,
        })
    }

    /// Lê, valida e concatena os chunks na ordem registrada.
    fn write_chunks(
        &self,
        links: &[FileChunkLink],
        file_id: FileId,
        writer: &mut fs::File,
        writer_path: &Path,
    ) -> Result<(u64, usize), EngineError> {
        let mut total = 0u64;

        for (index, link) in links.iter().enumerate() {
            let expected = u32::try_from(index).unwrap_or(u32::MAX);
            if link.position != expected {
                return Err(EngineError::ChunkOrder {
                    file_id,
                    expected,
                    actual: link.position,
                });
            }

            let record = self
                .store
                .get_chunk(&link.chunk_id)?
                .ok_or_else(|| EngineError::ChunkMetadataMissing(link.chunk_id.clone()))?;

            let stored = self.chunks.read_chunk(&link.chunk_id).map_err(|source| {
                EngineError::ChunkRead {
                    hash: link.chunk_id.clone(),
                    source,
                }
            })?;

            let data = decode_chunk(&stored, record.size, self.key.as_ref()).map_err(|source| {
                EngineError::ChunkDecode {
                    hash: link.chunk_id.clone(),
                    source,
                }
            })?;

            let actual = hashing::hash_chunk(&data);
            if actual != link.chunk_id {
                return Err(EngineError::ChunkHashMismatch {
                    expected: link.chunk_id.clone(),
                    actual,
                });
            }

            writer
                .write_all(&data)
                .map_err(|source| io_error(writer_path, source))?;
            total = total.saturating_add(data.len() as u64);
        }

        Ok((total, links.len()))
    }
}

/// Opções de restauração de um arquivo.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreOptions {
    /// Subcaminho relativo à raiz de destino. Quando `None`, usa o nome do
    /// arquivo registrado no snapshot.
    pub relative_path: Option<PathBuf>,
    /// Autoriza substituir um destino existente.
    pub overwrite: bool,
    /// Permite restaurar snapshots `in-progress`/`failed`.
    pub allow_incomplete_snapshot: bool,
}

impl RestoreOptions {
    /// Opções padrão: nome do arquivo, sem sobrescrita e somente snapshots
    /// concluídos.
    pub fn new() -> Self {
        Self::default()
    }

    /// Define o subcaminho relativo de destino.
    pub fn with_relative_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.relative_path = Some(path.into());
        self
    }

    /// Autoriza ou não a sobrescrita do destino.
    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Autoriza ou não a restauração de snapshots incompletos.
    pub fn with_incomplete_snapshot(mut self, allow: bool) -> Self {
        self.allow_incomplete_snapshot = allow;
        self
    }
}

/// Resultado de uma restauração.
#[derive(Debug, Clone)]
pub struct RestoreReport {
    /// Metadados do arquivo restaurado.
    pub file: FileRecord,
    /// Snapshot de origem.
    pub snapshot: Snapshot,
    /// Caminho final publicado.
    pub destination: PathBuf,
    /// Total de bytes escritos (igual a `file.size`).
    pub bytes_written: u64,
    /// Quantidade de chunks reconstruídos.
    pub chunks_restored: usize,
}

fn sanitize_relative(path: &Path) -> Result<PathBuf, EngineError> {
    let mut sanitized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Normal(name) => {
                #[cfg(windows)]
                validate_windows_component(name, path)?;
                sanitized.push(name);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(EngineError::InvalidRestorePath(format!(
                    "caminho deve ser relativo e sem '..': {}",
                    path.display()
                )));
            }
        }
    }

    if sanitized.as_os_str().is_empty() {
        return Err(EngineError::InvalidRestorePath(format!(
            "caminho de restauração vazio: {}",
            path.display()
        )));
    }

    Ok(sanitized)
}

#[cfg(windows)]
fn validate_windows_component(name: &std::ffi::OsStr, original: &Path) -> Result<(), EngineError> {
    let text = name.to_string_lossy();

    // `nome:fluxo` cria um fluxo alternativo NTFS em vez de um arquivo comum.
    if text.contains(':') {
        return Err(EngineError::InvalidRestorePath(format!(
            "fluxo alternativo NTFS não é permitido: {}",
            original.display()
        )));
    }

    if is_windows_reserved_name(&text) {
        return Err(EngineError::InvalidRestorePath(format!(
            "nome reservado do Windows não é permitido: {}",
            original.display()
        )));
    }

    Ok(())
}

#[cfg(windows)]
fn is_windows_reserved_name(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).trim_end_matches(' ');
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

fn default_relative_path(file: &FileRecord) -> Result<PathBuf, EngineError> {
    Path::new(&file.path)
        .file_name()
        .map(PathBuf::from)
        .ok_or_else(|| {
            EngineError::InvalidRestorePath(format!(
                "não foi possível derivar um nome de arquivo de '{}'",
                file.path
            ))
        })
}

/// Cria a raiz e os diretórios intermediários, garantindo que nenhum
/// componente existente (inclusive symlink) escape da raiz canônica.
fn resolve_target(root: &Path, relative: &Path) -> Result<PathBuf, EngineError> {
    fs::create_dir_all(root).map_err(|source| io_error(root, source))?;
    let canonical_root = fs::canonicalize(root).map_err(|source| io_error(root, source))?;

    let mut current = canonical_root.clone();
    let mut components: Vec<_> = relative.iter().collect();
    let file_name = components.pop().ok_or_else(|| {
        EngineError::InvalidRestorePath(format!(
            "caminho de restauração vazio: {}",
            relative.display()
        ))
    })?;

    for component in components {
        let next = current.join(component);
        match fs::metadata(&next) {
            Ok(metadata) => {
                let canonical_next =
                    fs::canonicalize(&next).map_err(|source| io_error(&next, source))?;
                if !canonical_next.starts_with(&canonical_root) {
                    return Err(EngineError::InvalidRestorePath(format!(
                        "caminho escapa da raiz de destino: {}",
                        next.display()
                    )));
                }
                if !metadata.is_dir() {
                    return Err(EngineError::InvalidRestorePath(format!(
                        "componente do caminho não é diretório: {}",
                        next.display()
                    )));
                }
                current = canonical_next;
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&next).map_err(|source| io_error(&next, source))?;
                current = next;
            }
            Err(source) => return Err(io_error(&next, source)),
        }
    }

    Ok(current.join(file_name))
}

fn persist(temp: NamedTempFile, target: &Path, overwrite: bool) -> Result<PathBuf, EngineError> {
    if overwrite {
        temp.persist(target)
            .map_err(|error| io_error(target, error.error))?;
    } else {
        match temp.persist_noclobber(target) {
            Ok(_) => {}
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(EngineError::DestinationExists(target.to_path_buf()));
            }
            Err(error) => return Err(io_error(target, error.error)),
        }
    }

    Ok(target.to_path_buf())
}

#[cfg(unix)]
fn apply_permissions(path: &Path, permissions: Option<u32>) -> Result<(), EngineError> {
    use std::os::unix::fs::PermissionsExt;

    if let Some(mode) = permissions {
        let permissions = fs::Permissions::from_mode(mode & 0o7777);
        fs::set_permissions(path, permissions).map_err(|source| io_error(path, source))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn apply_permissions(_path: &Path, _permissions: Option<u32>) -> Result<(), EngineError> {
    Ok(())
}

fn apply_modification_time(path: &Path, timestamp: Option<i64>) -> Result<(), EngineError> {
    if let Some(seconds) = timestamp {
        let time = filetime::FileTime::from_unix_time(seconds, 0);
        filetime::set_file_mtime(path, time).map_err(|source| io_error(path, source))?;
    }
    Ok(())
}

fn io_error(path: &Path, source: io::Error) -> EngineError {
    EngineError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::BackupEngine;
    use filetime::FileTime;
    use leobackup_core::crypto::KEY_LEN;
    use leobackup_core::Settings;
    use leobackup_storage::RepositoryLayout;

    struct Fixture {
        _base: tempfile::TempDir,
        repo: PathBuf,
        source: PathBuf,
        destination: PathBuf,
        key: Option<EncryptionKey>,
    }

    fn fixture(encrypted: bool) -> Fixture {
        let base = tempfile::tempdir().unwrap();
        let repo = base.path().join("repo");
        let source = base.path().join("src");
        let destination = base.path().join("dst");

        fs::create_dir_all(&repo).unwrap();
        fs::create_dir_all(&source).unwrap();

        let layout = RepositoryLayout::new(&repo).unwrap();
        layout.ensure().unwrap();

        let key = encrypted.then(|| EncryptionKey::from_bytes([7u8; KEY_LEN]));

        Fixture {
            _base: base,
            repo,
            source,
            destination,
            key,
        }
    }

    fn settings_for(fixture: &Fixture) -> Settings {
        let mut settings = Settings::default();
        settings.chunking.min_size = 1_024;
        settings.chunking.avg_size = 4_096;
        settings.chunking.max_size = 16_384;
        settings.encryption.enabled = fixture.key.is_some();
        settings
    }

    fn backup(fixture: &Fixture, paths: &[PathBuf]) -> (Snapshot, Vec<FileRecord>) {
        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        let mut engine =
            BackupEngine::new(store, chunks, settings_for(fixture), fixture.key.clone()).unwrap();
        let report = engine.backup_files(paths).unwrap();
        (report.snapshot, report.backed_up)
    }

    fn backup_in_progress(fixture: &Fixture, path: &Path) -> (Snapshot, FileRecord) {
        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        let mut engine =
            BackupEngine::new(store, chunks, settings_for(fixture), fixture.key.clone()).unwrap();
        let snapshot = engine.begin_snapshot().unwrap();
        let file = engine.backup_file(snapshot.id, path).unwrap();
        (snapshot, file)
    }

    fn fail_snapshot(fixture: &Fixture, snapshot_id: SnapshotId) {
        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        let mut engine =
            BackupEngine::new(store, chunks, settings_for(fixture), fixture.key.clone()).unwrap();
        engine.fail_snapshot(snapshot_id).unwrap();
    }

    fn restore_with(
        fixture: &Fixture,
        key: Option<EncryptionKey>,
        file_id: FileId,
        options: &RestoreOptions,
    ) -> Result<RestoreReport, EngineError> {
        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        let engine = RestoreEngine::new(store, chunks, key);
        engine.restore_file(file_id, &fixture.destination, options)
    }

    fn restore(
        fixture: &Fixture,
        file_id: FileId,
        options: &RestoreOptions,
    ) -> Result<RestoreReport, EngineError> {
        restore_with(fixture, fixture.key.clone(), file_id, options)
    }

    fn write_source(fixture: &Fixture, name: &str, data: &[u8]) -> PathBuf {
        let path = fixture.source.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
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
    fn restores_empty_and_small_files_byte_for_byte() {
        let fixture = fixture(false);
        let empty = write_source(&fixture, "empty.txt", b"");
        let small = write_source(&fixture, "small.txt", b"conteudo restaurado");
        let (_, files) = backup(&fixture, &[empty, small]);

        let empty_report = restore(&fixture, files[0].id, &RestoreOptions::default()).unwrap();
        assert_eq!(empty_report.destination.file_name().unwrap(), "empty.txt");
        assert_eq!(empty_report.bytes_written, 0);
        assert_eq!(empty_report.chunks_restored, 0);
        assert_eq!(fs::read(&empty_report.destination).unwrap(), b"");

        let small_report = restore(&fixture, files[1].id, &RestoreOptions::default()).unwrap();
        assert_eq!(small_report.destination.file_name().unwrap(), "small.txt");
        assert_eq!(small_report.bytes_written, 19);
        assert_eq!(
            fs::read(&small_report.destination).unwrap(),
            b"conteudo restaurado"
        );
    }

    #[test]
    fn restores_large_file_and_preserves_chunk_order() {
        let fixture = fixture(false);
        let data = pseudo_random(300 * 1024);
        let source = write_source(&fixture, "large.bin", &data);
        let (_, files) = backup(&fixture, &[source]);

        let report = restore(
            &fixture,
            files[0].id,
            &RestoreOptions::new().with_relative_path("restored.bin"),
        )
        .unwrap();

        assert!(report.chunks_restored > 1);
        assert_eq!(report.bytes_written, data.len() as u64);
        assert_eq!(fs::read(&report.destination).unwrap(), data);
    }

    #[test]
    fn restores_unicode_names_in_deep_relative_path() {
        let fixture = fixture(false);
        let name = "relatório final.txt";
        let data = "conteúdo com acentuação".as_bytes();
        let source = write_source(&fixture, name, data);
        let (_, files) = backup(&fixture, &[source]);

        let relative = PathBuf::from("a").join("b").join("c").join("d").join(name);
        let report = restore(
            &fixture,
            files[0].id,
            &RestoreOptions::new().with_relative_path(relative.clone()),
        )
        .unwrap();

        assert!(report.destination.ends_with(&relative));
        assert_eq!(fs::read(&report.destination).unwrap(), data);
    }

    #[test]
    fn refuses_overwrite_without_option_and_replaces_with_it() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "a.txt", b"novo");
        let (_, files) = backup(&fixture, &[source]);

        fs::create_dir_all(&fixture.destination).unwrap();
        let target = fixture.destination.join("a.txt");
        fs::write(&target, b"antigo").unwrap();

        let error = restore(&fixture, files[0].id, &RestoreOptions::default()).unwrap_err();
        match error {
            EngineError::DestinationExists(path) => {
                assert_eq!(path.file_name().unwrap(), "a.txt");
            }
            other => panic!("erro inesperado: {other:?}"),
        }
        assert_eq!(fs::read(&target).unwrap(), b"antigo");

        let report = restore(
            &fixture,
            files[0].id,
            &RestoreOptions::new().with_overwrite(true),
        )
        .unwrap();
        assert_eq!(report.bytes_written, 4);
        assert_eq!(fs::read(&target).unwrap(), b"novo");
    }

    #[test]
    fn refuses_to_replace_a_directory() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "a.txt", b"conteudo");
        let (_, files) = backup(&fixture, &[source]);

        fs::create_dir_all(fixture.destination.join("a.txt")).unwrap();

        let error = restore(
            &fixture,
            files[0].id,
            &RestoreOptions::new().with_overwrite(true),
        )
        .unwrap_err();
        assert!(matches!(error, EngineError::DestinationExists(_)));
    }

    #[test]
    fn rejects_unsafe_relative_paths() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "a.txt", b"x");
        let (_, files) = backup(&fixture, &[source]);

        for relative in [
            PathBuf::from(""),
            PathBuf::from("."),
            PathBuf::from(".."),
            PathBuf::from("../escape.txt"),
            PathBuf::from("a/../../escape.txt"),
            fixture.destination.join("abs.txt"),
        ] {
            let error = restore(
                &fixture,
                files[0].id,
                &RestoreOptions::new().with_relative_path(relative),
            )
            .unwrap_err();
            assert!(
                matches!(error, EngineError::InvalidRestorePath(_)),
                "esperava caminho inválido, obtive {error:?}"
            );
        }

        assert!(!fixture._base.path().join("escape.txt").exists());
        assert!(!fixture.destination.join("abs.txt").exists());
    }

    #[cfg(windows)]
    #[test]
    fn rejects_windows_device_names_and_alternate_streams() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "a.txt", b"x");
        let (_, files) = backup(&fixture, &[source]);

        for relative in [PathBuf::from("NUL"), PathBuf::from("file:stream")] {
            let error = restore(
                &fixture,
                files[0].id,
                &RestoreOptions::new().with_relative_path(relative),
            )
            .unwrap_err();
            assert!(matches!(error, EngineError::InvalidRestorePath(_)));
        }
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let fixture = fixture(false);
        let source = write_source(&fixture, "a.txt", b"x");
        let (_, files) = backup(&fixture, &[source]);

        let outside = tempfile::tempdir().unwrap();
        fs::create_dir_all(&fixture.destination).unwrap();
        symlink(outside.path(), fixture.destination.join("link")).unwrap();

        let error = restore(
            &fixture,
            files[0].id,
            &RestoreOptions::new().with_relative_path("link/a.txt"),
        )
        .unwrap_err();
        assert!(matches!(error, EngineError::InvalidRestorePath(_)));
        assert!(!outside.path().join("a.txt").exists());
    }

    #[test]
    fn fails_when_stored_chunk_is_missing() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "a.txt", b"conteudo");
        let (_, files) = backup(&fixture, &[source]);

        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        let hash = store
            .list_file_chunks(files[0].id)
            .unwrap()
            .first()
            .unwrap()
            .chunk_id
            .clone();
        fs::remove_file(chunks.chunk_path(&hash).unwrap()).unwrap();

        let error = restore(&fixture, files[0].id, &RestoreOptions::default()).unwrap_err();
        match error {
            EngineError::ChunkRead { hash: actual, .. } => assert_eq!(actual, hash),
            other => panic!("erro inesperado: {other:?}"),
        }
        assert!(!fixture.destination.join("a.txt").exists());
    }

    #[test]
    fn fails_when_stored_chunk_is_corrupted() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "a.txt", b"conteudo");
        let (_, files) = backup(&fixture, &[source]);

        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let chunks = ChunkStore::new(&layout);
        let hash = store
            .list_file_chunks(files[0].id)
            .unwrap()
            .first()
            .unwrap()
            .chunk_id
            .clone();
        fs::write(chunks.chunk_path(&hash).unwrap(), b"corrompido").unwrap();

        let error = restore(&fixture, files[0].id, &RestoreOptions::default()).unwrap_err();
        match error {
            EngineError::ChunkDecode { hash: actual, .. } => assert_eq!(actual, hash),
            other => panic!("erro inesperado: {other:?}"),
        }
        assert!(!fixture.destination.join("a.txt").exists());
    }

    #[test]
    fn fails_with_wrong_or_missing_key() {
        let fixture = fixture(true);
        let source = write_source(&fixture, "secret.txt", b"segredo");
        let (_, files) = backup(&fixture, &[source]);

        let wrong = Some(EncryptionKey::from_bytes([9u8; KEY_LEN]));
        let error =
            restore_with(&fixture, wrong, files[0].id, &RestoreOptions::default()).unwrap_err();
        assert!(matches!(error, EngineError::ChunkDecode { .. }));

        let error =
            restore_with(&fixture, None, files[0].id, &RestoreOptions::default()).unwrap_err();
        assert!(matches!(error, EngineError::ChunkDecode { .. }));

        let report = restore(&fixture, files[0].id, &RestoreOptions::default()).unwrap();
        assert_eq!(fs::read(&report.destination).unwrap(), b"segredo");
    }

    #[test]
    fn requires_explicit_permission_for_incomplete_snapshot() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "partial.txt", b"parcial");
        let (snapshot, file) = backup_in_progress(&fixture, &source);
        assert_eq!(snapshot.status, SnapshotStatus::InProgress);

        let error = restore(&fixture, file.id, &RestoreOptions::default()).unwrap_err();
        match error {
            EngineError::SnapshotNotCompleted { id, status } => {
                assert_eq!(id, snapshot.id);
                assert_eq!(status, "in-progress");
            }
            other => panic!("erro inesperado: {other:?}"),
        }

        let report = restore(
            &fixture,
            file.id,
            &RestoreOptions::new().with_incomplete_snapshot(true),
        )
        .unwrap();
        assert_eq!(fs::read(&report.destination).unwrap(), b"parcial");
    }

    #[test]
    fn refuses_failed_snapshot_by_default() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "failed.txt", b"parcial");
        let (snapshot, file) = backup_in_progress(&fixture, &source);
        fail_snapshot(&fixture, snapshot.id);

        let error = restore(&fixture, file.id, &RestoreOptions::default()).unwrap_err();
        match error {
            EngineError::SnapshotNotCompleted { id, status } => {
                assert_eq!(id, snapshot.id);
                assert_eq!(status, "failed");
            }
            other => panic!("erro inesperado: {other:?}"),
        }
    }

    #[test]
    fn lists_snapshots_files_and_finds_by_path() {
        let fixture = fixture(false);
        let first = write_source(&fixture, "a.txt", b"a");
        let second = write_source(&fixture, "b.txt", b"b");
        let (snapshot, files) = backup(&fixture, &[first, second]);

        let layout = RepositoryLayout::new(&fixture.repo).unwrap();
        let store = MetadataStore::open(&layout.database_path()).unwrap();
        let engine = RestoreEngine::new(store, ChunkStore::new(&layout), None);

        let snapshots = engine.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, snapshot.id);

        let listed = engine.list_files(snapshot.id).unwrap();
        assert_eq!(listed.len(), 2);

        let found = engine
            .find_file(snapshot.id, &files[0].path)
            .unwrap()
            .unwrap();
        assert_eq!(found.id, files[0].id);
        assert!(engine
            .find_file(snapshot.id, "nao/existe")
            .unwrap()
            .is_none());
    }

    #[test]
    fn reports_missing_file() {
        let fixture = fixture(false);
        let error = restore(&fixture, 999, &RestoreOptions::default()).unwrap_err();
        assert!(matches!(error, EngineError::FileNotFound(999)));
    }

    #[test]
    fn preserves_modification_time() {
        let fixture = fixture(false);
        let source = write_source(&fixture, "stamp.txt", b"tempo");
        let stamp = FileTime::from_unix_time(1_600_000_000, 0);
        filetime::set_file_mtime(&source, stamp).unwrap();

        let (_, files) = backup(&fixture, &[source]);
        assert_eq!(files[0].modification_time, Some(1_600_000_000));

        let report = restore(&fixture, files[0].id, &RestoreOptions::default()).unwrap();
        let metadata = fs::metadata(&report.destination).unwrap();
        let restored = FileTime::from_last_modification_time(&metadata);
        assert_eq!(restored.unix_seconds(), 1_600_000_000);
    }

    #[cfg(unix)]
    #[test]
    fn preserves_unix_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = fixture(false);
        let source = write_source(&fixture, "script.sh", b"#!/bin/sh\n");
        fs::set_permissions(&source, fs::Permissions::from_mode(0o640)).unwrap();

        let (_, files) = backup(&fixture, &[source]);
        let report = restore(&fixture, files[0].id, &RestoreOptions::default()).unwrap();
        let mode = fs::metadata(&report.destination)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(mode, 0o640);
    }
}
