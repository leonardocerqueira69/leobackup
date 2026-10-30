//! Acesso ao banco de metadados (`metadata/index.db`).
//!
//! `MetadataStore` concentra a abertura da conexão, a configuração de
//! integridade e as operações CRUD usadas pelo restante do sistema.
//!
//! ## Regras de `chunks.reference_count`
//!
//! - Conta **ocorrências** em `file_chunks` (um chunk repetido no mesmo
//!   arquivo conta mais de uma vez).
//! - É mantido por triggers `AFTER INSERT`/`AFTER DELETE` em `file_chunks`,
//!   portanto remoções em cascata também atualizam a contagem.
//! - Nunca fica negativo: a coluna tem `CHECK (reference_count >= 0)`.
//! - `recompute_reference_counts` permite reconstruir a contagem a partir de
//!   `file_chunks`, útil para verificação/recuperação.

use std::path::Path;
use std::time::Duration;

use leobackup_core::model::{
    ChunkRecord, FileChunkLink, FileId, FileRecord, Snapshot, SnapshotId, SnapshotStatus,
};
use rusqlite::{params, Connection, OptionalExtension};

use crate::error::StorageError;
use crate::migrations;

/// Tempo máximo de espera por um lock do SQLite.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Conexão e operações CRUD sobre o banco de metadados.
pub struct MetadataStore {
    conn: Connection,
}

impl MetadataStore {
    /// Abre (ou cria) o banco em `path` e aplica as migrações pendentes.
    ///
    /// Cria o diretório pai quando necessário. A conexão é configurada com
    /// `foreign_keys=ON`, `journal_mode=WAL` e `synchronous=FULL`.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|source| StorageError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let mut conn = Connection::open(path)?;
        configure(&conn)?;
        migrations::apply(&mut conn)?;

        Ok(Self { conn })
    }

    /// Acesso à conexão subjacente (para consultas avançadas e diagnósticos).
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Versão de esquema atualmente aplicada.
    pub fn schema_version(&self) -> Result<u32, StorageError> {
        let version: i64 = self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        u32::try_from(version)
            .map_err(|_| StorageError::InvalidData(format!("user_version inválido: {version}")))
    }

    /// Indica se as chaves estrangeiras estão ativas nesta conexão.
    pub fn foreign_keys_enabled(&self) -> Result<bool, StorageError> {
        let enabled: i64 = self
            .conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
        Ok(enabled == 1)
    }

    // ----- snapshots -----

    /// Cria um snapshot no estado `InProgress`.
    pub fn create_snapshot(&mut self, timestamp: i64) -> Result<Snapshot, StorageError> {
        let status = SnapshotStatus::InProgress;
        self.conn.execute(
            "INSERT INTO snapshots (timestamp, status) VALUES (?1, ?2)",
            params![timestamp, status.as_str()],
        )?;

        Ok(Snapshot {
            id: self.conn.last_insert_rowid(),
            timestamp,
            status,
        })
    }

    /// Atualiza o estado de um snapshot. Retorna `false` se o id não existir.
    pub fn set_snapshot_status(
        &mut self,
        id: SnapshotId,
        status: SnapshotStatus,
    ) -> Result<bool, StorageError> {
        let changed = self.conn.execute(
            "UPDATE snapshots SET status = ?1 WHERE id = ?2",
            params![status.as_str(), id],
        )?;
        Ok(changed == 1)
    }

    /// Marca um snapshot como concluído.
    pub fn complete_snapshot(&mut self, id: SnapshotId) -> Result<bool, StorageError> {
        self.set_snapshot_status(id, SnapshotStatus::Completed)
    }

    /// Marca um snapshot como falho.
    pub fn fail_snapshot(&mut self, id: SnapshotId) -> Result<bool, StorageError> {
        self.set_snapshot_status(id, SnapshotStatus::Failed)
    }

    /// Busca um snapshot por id.
    pub fn get_snapshot(&self, id: SnapshotId) -> Result<Option<Snapshot>, StorageError> {
        let row = self
            .conn
            .query_row(
                "SELECT id, timestamp, status FROM snapshots WHERE id = ?1",
                params![id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;

        row.map(map_snapshot).transpose()
    }

    /// Lista todos os snapshots em ordem de criação.
    pub fn list_snapshots(&self) -> Result<Vec<Snapshot>, StorageError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, timestamp, status FROM snapshots ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(map_snapshot(row?)?);
        }
        Ok(snapshots)
    }

    /// Remove um snapshot. Arquivos e associações são removidos em cascata.
    ///
    /// Chunks que ficarem sem referência **não** são apagados aqui; a
    /// remoção física de chunks órfãos pertence ao pruning (Fase 6).
    pub fn delete_snapshot(&mut self, id: SnapshotId) -> Result<bool, StorageError> {
        let changed = self
            .conn
            .execute("DELETE FROM snapshots WHERE id = ?1", params![id])?;
        Ok(changed == 1)
    }

    // ----- arquivos -----

    /// Insere os metadados de um arquivo em um snapshot.
    pub fn insert_file(
        &mut self,
        snapshot_id: SnapshotId,
        path: &str,
        permissions: Option<u32>,
        modification_time: Option<i64>,
        size: u64,
    ) -> Result<FileRecord, StorageError> {
        if path.is_empty() {
            return Err(StorageError::InvalidData(
                "files.path não pode ser vazio".to_string(),
            ));
        }

        let size_i64 = i64_from_u64(size, "files.size")?;
        let permissions_i64 = permissions.map(i64::from);

        self.conn.execute(
            "INSERT INTO files (snapshot_id, path, permissions, modification_time, size)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                snapshot_id,
                path,
                permissions_i64,
                modification_time,
                size_i64
            ],
        )?;

        Ok(FileRecord {
            id: self.conn.last_insert_rowid(),
            snapshot_id,
            path: path.to_string(),
            permissions,
            modification_time,
            size,
        })
    }

    /// Busca um arquivo por id.
    pub fn get_file(&self, id: FileId) -> Result<Option<FileRecord>, StorageError> {
        let row = self
            .conn
            .query_row(
                "SELECT id, snapshot_id, path, permissions, modification_time, size
                   FROM files WHERE id = ?1",
                params![id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?;

        row.map(map_file).transpose()
    }

    /// Commit atômico dos metadados de um arquivo e de seus chunks.
    ///
    /// Só aceita snapshots em `in-progress`. A operação insere o arquivo e
    /// todas as associações ordenadas na mesma transação; qualquer falha
    /// (snapshot inválido, caminho duplicado, chunk inexistente) reverte
    /// tudo. Os chunks devem já estar persistidos no disco antes da chamada.
    pub fn commit_file<S: AsRef<str>>(
        &mut self,
        snapshot_id: SnapshotId,
        path: &str,
        permissions: Option<u32>,
        modification_time: Option<i64>,
        size: u64,
        chunk_ids: &[S],
    ) -> Result<FileRecord, StorageError> {
        if path.is_empty() {
            return Err(StorageError::InvalidData(
                "files.path não pode ser vazio".to_string(),
            ));
        }

        let size_i64 = i64_from_u64(size, "files.size")?;
        let permissions_i64 = permissions.map(i64::from);

        let tx = self.conn.transaction()?;

        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM snapshots WHERE id = ?1",
                params![snapshot_id],
                |row| row.get(0),
            )
            .optional()?;
        if status.as_deref() != Some("in-progress") {
            return Err(StorageError::SnapshotUnavailable(snapshot_id));
        }

        tx.execute(
            "INSERT INTO files (snapshot_id, path, permissions, modification_time, size)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                snapshot_id,
                path,
                permissions_i64,
                modification_time,
                size_i64
            ],
        )?;
        let file_id = tx.last_insert_rowid();

        {
            let mut stmt = tx.prepare(
                "INSERT INTO file_chunks (file_id, chunk_id, position) VALUES (?1, ?2, ?3)",
            )?;
            for (position, chunk_id) in chunk_ids.iter().enumerate() {
                let position_i64 = i64::try_from(position).map_err(|_| {
                    StorageError::InvalidData("posição de chunk excede i64::MAX".to_string())
                })?;
                stmt.execute(params![file_id, chunk_id.as_ref(), position_i64])?;
            }
        }

        tx.commit()?;

        Ok(FileRecord {
            id: file_id,
            snapshot_id,
            path: path.to_string(),
            permissions,
            modification_time,
            size,
        })
    }

    /// Lista os arquivos de um snapshot, em ordem de caminho.
    pub fn list_files(&self, snapshot_id: SnapshotId) -> Result<Vec<FileRecord>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, snapshot_id, path, permissions, modification_time, size
               FROM files WHERE snapshot_id = ?1 ORDER BY path",
        )?;
        let rows = stmt.query_map(params![snapshot_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;

        let mut files = Vec::new();
        for row in rows {
            files.push(map_file(row?)?);
        }
        Ok(files)
    }

    // ----- chunks -----

    /// Registra um chunk, se ainda não existir.
    ///
    /// Retorna `true` quando o chunk foi inserido e `false` quando já havia
    /// um registro com o mesmo `hash_id` (deduplicação).
    pub fn insert_chunk(
        &mut self,
        hash_id: &str,
        size: u64,
        compressed_size: u64,
    ) -> Result<bool, StorageError> {
        if hash_id.is_empty() {
            return Err(StorageError::InvalidData(
                "chunks.hash_id não pode ser vazio".to_string(),
            ));
        }

        let size_i64 = i64_from_u64(size, "chunks.size")?;
        let compressed_i64 = i64_from_u64(compressed_size, "chunks.compressed_size")?;

        let changed = self.conn.execute(
            "INSERT INTO chunks (hash_id, size, compressed_size) VALUES (?1, ?2, ?3)
             ON CONFLICT(hash_id) DO NOTHING",
            params![hash_id, size_i64, compressed_i64],
        )?;

        Ok(changed == 1)
    }

    /// Busca um chunk por hash.
    pub fn get_chunk(&self, hash_id: &str) -> Result<Option<ChunkRecord>, StorageError> {
        let row = self
            .conn
            .query_row(
                "SELECT hash_id, size, compressed_size, reference_count
                   FROM chunks WHERE hash_id = ?1",
                params![hash_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;

        row.map(map_chunk).transpose()
    }

    /// Lista todos os chunks conhecidos.
    pub fn list_chunks(&self) -> Result<Vec<ChunkRecord>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT hash_id, size, compressed_size, reference_count FROM chunks ORDER BY hash_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;

        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(map_chunk(row?)?);
        }
        Ok(chunks)
    }

    // ----- associações arquivo/chunk -----

    /// Associa uma sequência ordenada de chunks a um arquivo.
    ///
    /// A posição é derivada da ordem do slice e a operação é atômica. Os
    /// chunks precisam existir; a contagem de referências é atualizada pelos
    /// triggers do esquema.
    pub fn link_file_chunks<S: AsRef<str>>(
        &mut self,
        file_id: FileId,
        chunk_ids: &[S],
    ) -> Result<(), StorageError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO file_chunks (file_id, chunk_id, position) VALUES (?1, ?2, ?3)",
            )?;
            for (position, chunk_id) in chunk_ids.iter().enumerate() {
                let position_i64 = i64::try_from(position).map_err(|_| {
                    StorageError::InvalidData("posição de chunk excede i64::MAX".to_string())
                })?;
                stmt.execute(params![file_id, chunk_id.as_ref(), position_i64])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Associa um único chunk a uma posição explícita.
    pub fn append_file_chunk(
        &mut self,
        file_id: FileId,
        chunk_id: &str,
        position: u32,
    ) -> Result<(), StorageError> {
        self.conn.execute(
            "INSERT INTO file_chunks (file_id, chunk_id, position) VALUES (?1, ?2, ?3)",
            params![file_id, chunk_id, i64::from(position)],
        )?;
        Ok(())
    }

    /// Lista as associações de um arquivo em ordem de posição.
    pub fn list_file_chunks(&self, file_id: FileId) -> Result<Vec<FileChunkLink>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT file_id, chunk_id, position FROM file_chunks
               WHERE file_id = ?1 ORDER BY position",
        )?;
        let rows = stmt.query_map(params![file_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        let mut links = Vec::new();
        for row in rows {
            let (file_id, chunk_id, position) = row?;
            links.push(FileChunkLink {
                file_id,
                chunk_id,
                position: u32_from_i64(position, "file_chunks.position")?,
            });
        }
        Ok(links)
    }

    // ----- manutenção -----

    /// Recalcula `chunks.reference_count` a partir de `file_chunks`.
    ///
    /// Útil para verificação de consistência e recuperação. A operação é
    /// atômica.
    pub fn recompute_reference_counts(&mut self) -> Result<(), StorageError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE chunks
                SET reference_count = (
                    SELECT COUNT(*) FROM file_chunks
                     WHERE file_chunks.chunk_id = chunks.hash_id
                )",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }
}

fn configure(conn: &Connection) -> Result<(), StorageError> {
    conn.pragma_update(None, "foreign_keys", true)?;

    let journal_mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(StorageError::Migration(format!(
            "não foi possível habilitar o modo WAL (modo atual: {journal_mode})"
        )));
    }

    // 2 = FULL: prioriza durabilidade sobre desempenho.
    conn.pragma_update(None, "synchronous", 2_i64)?;
    conn.busy_timeout(BUSY_TIMEOUT)?;

    let enabled: i64 = conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    if enabled != 1 {
        return Err(StorageError::Migration(
            "não foi possível habilitar foreign_keys".to_string(),
        ));
    }

    Ok(())
}

fn map_snapshot(row: (i64, i64, String)) -> Result<Snapshot, StorageError> {
    let (id, timestamp, status) = row;
    let status = status
        .parse::<SnapshotStatus>()
        .map_err(|err| StorageError::InvalidData(err.to_string()))?;
    Ok(Snapshot {
        id,
        timestamp,
        status,
    })
}

fn map_file(
    row: (i64, i64, String, Option<i64>, Option<i64>, i64),
) -> Result<FileRecord, StorageError> {
    let (id, snapshot_id, path, permissions, modification_time, size) = row;
    let permissions = match permissions {
        Some(value) => Some(u32::try_from(value).map_err(|_| {
            StorageError::InvalidData(format!("files.permissions fora do intervalo u32: {value}"))
        })?),
        None => None,
    };
    Ok(FileRecord {
        id,
        snapshot_id,
        path,
        permissions,
        modification_time,
        size: u64_from_i64(size, "files.size")?,
    })
}

fn map_chunk(row: (String, i64, i64, i64)) -> Result<ChunkRecord, StorageError> {
    let (hash_id, size, compressed_size, reference_count) = row;
    Ok(ChunkRecord {
        hash_id,
        size: u64_from_i64(size, "chunks.size")?,
        compressed_size: u64_from_i64(compressed_size, "chunks.compressed_size")?,
        reference_count: u64_from_i64(reference_count, "chunks.reference_count")?,
    })
}

fn i64_from_u64(value: u64, field: &str) -> Result<i64, StorageError> {
    i64::try_from(value)
        .map_err(|_| StorageError::InvalidData(format!("{field} excede i64::MAX: {value}")))
}

fn u64_from_i64(value: i64, field: &str) -> Result<u64, StorageError> {
    u64::try_from(value)
        .map_err(|_| StorageError::InvalidData(format!("{field} é negativo: {value}")))
}

fn u32_from_i64(value: i64, field: &str) -> Result<u32, StorageError> {
    u32::try_from(value)
        .map_err(|_| StorageError::InvalidData(format!("{field} fora do intervalo u32: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (tempfile::TempDir, MetadataStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata").join("index.db");
        let store = MetadataStore::open(&path).unwrap();
        (dir, store)
    }

    #[test]
    fn open_configures_integrity_and_schema() {
        let (_dir, store) = open_temp();
        assert_eq!(
            store.schema_version().unwrap(),
            migrations::CURRENT_SCHEMA_VERSION
        );
        assert!(store.foreign_keys_enabled().unwrap());
    }

    #[test]
    fn snapshot_lifecycle() {
        let (_dir, mut store) = open_temp();

        let snapshot = store.create_snapshot(1_700_000_000).unwrap();
        assert_eq!(snapshot.status, SnapshotStatus::InProgress);

        assert!(store.complete_snapshot(snapshot.id).unwrap());
        let loaded = store.get_snapshot(snapshot.id).unwrap().unwrap();
        assert_eq!(loaded.status, SnapshotStatus::Completed);

        let listed = store.list_snapshots().unwrap();
        assert_eq!(listed, vec![loaded]);
    }

    #[test]
    fn missing_snapshot_returns_none_and_false() {
        let (_dir, mut store) = open_temp();
        assert!(store.get_snapshot(999).unwrap().is_none());
        assert!(!store.complete_snapshot(999).unwrap());
        assert!(!store.fail_snapshot(999).unwrap());
        assert!(!store.delete_snapshot(999).unwrap());
    }

    #[test]
    fn files_are_listed_and_unique_per_snapshot() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(10).unwrap();

        let file = store
            .insert_file(snapshot.id, "/home/user/a.txt", Some(0o644), Some(123), 42)
            .unwrap();
        assert_eq!(file.size, 42);
        assert_eq!(store.get_file(file.id).unwrap().unwrap(), file);

        let listed = store.list_files(snapshot.id).unwrap();
        assert_eq!(listed.len(), 1);

        assert!(store
            .insert_file(snapshot.id, "/home/user/a.txt", None, None, 1)
            .is_err());
    }

    #[test]
    fn insert_file_requires_existing_snapshot() {
        let (_dir, mut store) = open_temp();
        assert!(store.insert_file(123, "/f", None, None, 1).is_err());
    }

    #[test]
    fn empty_paths_and_hashes_are_rejected() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(0).unwrap();
        assert!(matches!(
            store.insert_file(snapshot.id, "", None, None, 0),
            Err(StorageError::InvalidData(_))
        ));
        assert!(matches!(
            store.insert_chunk("", 1, 1),
            Err(StorageError::InvalidData(_))
        ));
    }

    #[test]
    fn chunk_insert_is_deduplicated() {
        let (_dir, mut store) = open_temp();
        assert!(store.insert_chunk("hash-a", 100, 80).unwrap());
        assert!(!store.insert_chunk("hash-a", 100, 80).unwrap());
        assert_eq!(store.list_chunks().unwrap().len(), 1);
    }

    #[test]
    fn u64_overflow_is_rejected() {
        let (_dir, mut store) = open_temp();
        assert!(matches!(
            store.insert_chunk("big", u64::MAX, 1),
            Err(StorageError::InvalidData(_))
        ));
    }

    #[test]
    fn linking_chunks_maintains_reference_count() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(0).unwrap();
        let file = store
            .insert_file(snapshot.id, "/f", None, None, 30)
            .unwrap();

        store.insert_chunk("a", 10, 8).unwrap();
        store.insert_chunk("b", 10, 8).unwrap();

        store.link_file_chunks(file.id, &["a", "b", "a"]).unwrap();

        let links = store.list_file_chunks(file.id).unwrap();
        assert_eq!(links.len(), 3);
        assert_eq!(links[0].position, 0);
        assert_eq!(links[2].chunk_id, "a");

        assert_eq!(store.get_chunk("a").unwrap().unwrap().reference_count, 2);
        assert_eq!(store.get_chunk("b").unwrap().unwrap().reference_count, 1);
    }

    #[test]
    fn linking_unknown_chunk_fails_atomically() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(0).unwrap();
        let file = store.insert_file(snapshot.id, "/f", None, None, 0).unwrap();
        store.insert_chunk("a", 1, 1).unwrap();

        assert!(store.link_file_chunks(file.id, &["a", "missing"]).is_err());
        // A transação foi revertida: nenhuma associação ficou gravada.
        assert!(store.list_file_chunks(file.id).unwrap().is_empty());
    }

    #[test]
    fn deleting_snapshot_cascades_and_decrements_references() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(0).unwrap();
        let file = store
            .insert_file(snapshot.id, "/f", None, None, 20)
            .unwrap();
        store.insert_chunk("a", 10, 8).unwrap();
        store.insert_chunk("b", 10, 8).unwrap();
        store.link_file_chunks(file.id, &["a", "b"]).unwrap();

        assert!(store.delete_snapshot(snapshot.id).unwrap());

        assert!(store.list_files(snapshot.id).unwrap().is_empty());
        assert!(store.list_file_chunks(file.id).unwrap().is_empty());
        assert_eq!(store.get_chunk("a").unwrap().unwrap().reference_count, 0);
        assert_eq!(store.get_chunk("b").unwrap().unwrap().reference_count, 0);
    }

    #[test]
    fn referenced_chunk_cannot_be_deleted() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(0).unwrap();
        let file = store
            .insert_file(snapshot.id, "/f", None, None, 10)
            .unwrap();
        store.insert_chunk("a", 10, 8).unwrap();
        store.link_file_chunks(file.id, &["a"]).unwrap();

        let result = store
            .connection()
            .execute("DELETE FROM chunks WHERE hash_id = ?1", params!["a"]);
        assert!(result.is_err());
    }

    #[test]
    fn foreign_keys_and_status_constraints_are_enforced() {
        let (_dir, store) = open_temp();

        let bad_status = store.connection().execute(
            "INSERT INTO snapshots (timestamp, status) VALUES (0, 'bogus')",
            [],
        );
        assert!(bad_status.is_err());

        let bad_file = store.connection().execute(
            "INSERT INTO files (snapshot_id, path, size) VALUES (999, '/x', 0)",
            [],
        );
        assert!(bad_file.is_err());
    }

    #[test]
    fn reference_counts_can_be_recomputed() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(0).unwrap();
        let file = store
            .insert_file(snapshot.id, "/f", None, None, 30)
            .unwrap();
        store.insert_chunk("a", 10, 8).unwrap();
        store.link_file_chunks(file.id, &["a", "a"]).unwrap();

        store
            .connection()
            .execute("UPDATE chunks SET reference_count = 0", [])
            .unwrap();
        store.recompute_reference_counts().unwrap();

        assert_eq!(store.get_chunk("a").unwrap().unwrap().reference_count, 2);
    }

    #[test]
    fn data_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata").join("index.db");

        let id = {
            let mut store = MetadataStore::open(&path).unwrap();
            store.create_snapshot(42).unwrap().id
        };

        let store = MetadataStore::open(&path).unwrap();
        let snapshot = store.get_snapshot(id).unwrap().unwrap();
        assert_eq!(snapshot.timestamp, 42);
        assert_eq!(
            store.schema_version().unwrap(),
            migrations::CURRENT_SCHEMA_VERSION
        );
    }

    #[test]
    fn commit_file_is_atomic_and_requires_open_snapshot() {
        let (_dir, mut store) = open_temp();
        let snapshot = store.create_snapshot(0).unwrap();
        store.insert_chunk("a", 1, 1).unwrap();

        let file = store
            .commit_file(snapshot.id, "/f", None, None, 3, &["a"])
            .unwrap();
        assert_eq!(store.list_file_chunks(file.id).unwrap().len(), 1);

        // Caminho duplicado no mesmo snapshot: falha sem deixar resíduos.
        assert!(store
            .commit_file(snapshot.id, "/f", None, None, 3, &["a"])
            .is_err());
        assert_eq!(store.list_files(snapshot.id).unwrap().len(), 1);

        // Chunk inexistente: reverte o arquivo e as associações.
        assert!(store
            .commit_file(snapshot.id, "/g", None, None, 3, &["missing"])
            .is_err());
        assert_eq!(store.list_files(snapshot.id).unwrap().len(), 1);

        // Snapshot concluído não aceita novos commits.
        assert!(store.complete_snapshot(snapshot.id).unwrap());
        assert!(matches!(
            store.commit_file(snapshot.id, "/h", None, None, 0, &[] as &[&str]),
            Err(StorageError::SnapshotUnavailable(_))
        ));
    }

    #[test]
    fn newer_schema_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata").join("index.db");

        {
            let store = MetadataStore::open(&path).unwrap();
            store
                .connection()
                .pragma_update(None, "user_version", 99_i64)
                .unwrap();
        }

        assert!(matches!(
            MetadataStore::open(&path),
            Err(StorageError::Migration(_))
        ));
    }
}
