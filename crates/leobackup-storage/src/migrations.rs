//! Migrações versionadas do esquema SQLite.
//!
//! A versão aplicada é controlada por `PRAGMA user_version`. Cada elemento de
//! `MIGRATIONS` corresponde a uma versão incremental; a migração é aplicada
//! dentro de uma transação e só é considerada concluída após o commit do
//! `user_version`.

use rusqlite::Connection;

use crate::error::StorageError;

/// Esquema inicial: snapshots, arquivos, chunks e associações ordenadas.
const MIGRATION_V1: &str = r#"
CREATE TABLE snapshots (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    status    TEXT    NOT NULL CHECK (status IN ('in-progress', 'completed', 'failed'))
);

CREATE INDEX idx_snapshots_timestamp ON snapshots(timestamp);

CREATE TABLE files (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id       INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    path              TEXT    NOT NULL,
    permissions       INTEGER,
    modification_time INTEGER,
    size              INTEGER NOT NULL CHECK (size >= 0),
    UNIQUE (snapshot_id, path)
);

CREATE INDEX idx_files_snapshot ON files(snapshot_id);
CREATE INDEX idx_files_path ON files(path);

CREATE TABLE chunks (
    hash_id         TEXT    PRIMARY KEY,
    size            INTEGER NOT NULL CHECK (size >= 0),
    compressed_size INTEGER NOT NULL CHECK (compressed_size >= 0),
    reference_count INTEGER NOT NULL DEFAULT 0 CHECK (reference_count >= 0)
);

CREATE TABLE file_chunks (
    file_id  INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    chunk_id TEXT    NOT NULL REFERENCES chunks(hash_id) ON DELETE RESTRICT,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (file_id, position)
);

CREATE INDEX idx_file_chunks_chunk ON file_chunks(chunk_id);

-- Mantém chunks.reference_count coerente com o número de ocorrências em
-- file_chunks. Assim, remoções em cascata (ex.: prune de snapshots) também
-- atualizam as contagens corretamente.
CREATE TRIGGER trg_file_chunks_insert
AFTER INSERT ON file_chunks
BEGIN
    UPDATE chunks
       SET reference_count = reference_count + 1
     WHERE hash_id = NEW.chunk_id;
END;

CREATE TRIGGER trg_file_chunks_delete
AFTER DELETE ON file_chunks
BEGIN
    UPDATE chunks
       SET reference_count = reference_count - 1
     WHERE hash_id = OLD.chunk_id;
END;
"#;

/// Migrações conhecidas, em ordem. O índice + 1 é a versão de destino.
const MIGRATIONS: &[&str] = &[MIGRATION_V1];

/// Versão de esquema suportada por esta build.
pub const CURRENT_SCHEMA_VERSION: u32 = MIGRATIONS.len() as u32;

/// Aplica as migrações pendentes em `conn`.
pub fn apply(conn: &mut Connection) -> Result<(), StorageError> {
    let current: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let mut version = u32::try_from(current)
        .map_err(|_| StorageError::Migration(format!("user_version inválido: {current}")))?;

    if version > CURRENT_SCHEMA_VERSION {
        return Err(StorageError::Migration(format!(
            "banco na versão {version}, mas esta build suporta até {CURRENT_SCHEMA_VERSION}"
        )));
    }

    while version < CURRENT_SCHEMA_VERSION {
        let next = version + 1;
        let sql = MIGRATIONS[(next - 1) as usize];

        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", i64::from(next))?;
        tx.commit()?;

        version = next;
    }

    debug_assert_eq!(version, CURRENT_SCHEMA_VERSION);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_list_matches_schema_version() {
        assert_eq!(CURRENT_SCHEMA_VERSION as usize, MIGRATIONS.len());
    }
}
