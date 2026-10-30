//! Modelo de configuração persistido em `config/settings.json`.
//!
//! O arquivo é lido e gravado pela crate `leobackup-core` e consumido pelas
//! demais camadas (engine, daemon e CLI). A configuração é versionada por
//! `schema_version`: campos novos devem ser adicionados com valores padrão
//! via `#[serde(default)]` para preservar a compatibilidade de leitura.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::chunking::ChunkingParams;
use crate::error::CoreError;

/// Versão atual do esquema persistido de `settings.json`.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Um Kibibyte (1024 bytes).
pub const KIB: u64 = 1024;

/// Um Mebibyte (1024 KiB).
pub const MIB: u64 = 1024 * KIB;

/// Nível máximo aceito pelo Zstandard.
const ZSTD_MAX_LEVEL: i32 = 22;

/// Janela máxima de debounce aceita para eventos do watcher.
const MAX_DEBOUNCE_MS: u64 = 60_000;

/// Capacidade máxima aceita para a fila de eventos do watcher.
const MAX_QUEUE_CAPACITY: usize = 1_000_000;

/// Configuração persistida do LeoBackup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    /// Versão do esquema deste arquivo.
    pub schema_version: u32,
    /// Diretórios de origem monitorados/backupeados.
    pub source_dirs: Vec<PathBuf>,
    /// Parâmetros do chunking baseado em conteúdo (FastCDC).
    pub chunking: ChunkingSettings,
    /// Limites de concorrência do pipeline.
    pub concurrency: ConcurrencySettings,
    /// Política de retenção de snapshots.
    pub retention: RetentionSettings,
    /// Configuração do watcher de sistema de arquivos.
    pub monitoring: MonitoringSettings,
    /// Parâmetros de compressão.
    pub compression: CompressionSettings,
    /// Parâmetros de criptografia em repouso.
    pub encryption: EncryptionSettings,
    /// Configuração do servidor local do daemon.
    pub daemon: DaemonSettings,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            source_dirs: Vec::new(),
            chunking: ChunkingSettings::default(),
            concurrency: ConcurrencySettings::default(),
            retention: RetentionSettings::default(),
            monitoring: MonitoringSettings::default(),
            compression: CompressionSettings::default(),
            encryption: EncryptionSettings::default(),
            daemon: DaemonSettings::default(),
        }
    }
}

impl Settings {
    /// Valida as invariantes estruturais da configuração.
    ///
    /// A existência dos diretórios de origem não é verificada aqui: a
    /// configuração pode ser criada antes de eles existirem. Essa checagem
    /// pertence ao início de um backup.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.schema_version > CURRENT_SCHEMA_VERSION {
            return Err(CoreError::UnsupportedConfigVersion(self.schema_version));
        }

        self.chunking.validate()?;
        self.concurrency.validate()?;
        self.monitoring.validate()?;
        self.compression.validate()?;
        self.daemon.validate()?;

        for dir in &self.source_dirs {
            if dir.as_os_str().is_empty() {
                return Err(CoreError::InvalidConfig(
                    "source_dirs contém um caminho vazio".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Lê e valida `settings.json` a partir de `path`.
    pub fn load(path: &Path) -> Result<Self, CoreError> {
        let bytes = fs::read(path).map_err(|source| CoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let settings: Settings = serde_json::from_slice(&bytes)?;
        settings.validate()?;
        Ok(settings)
    }

    /// Serializa e grava `settings.json` em `path`.
    ///
    /// A escrita é feita em um arquivo temporário no mesmo diretório e
    /// renomeada ao final, evitando que leitores observem um arquivo
    /// parcialmente gravado.
    pub fn save(&self, path: &Path) -> Result<(), CoreError> {
        self.validate()?;

        let json = serde_json::to_vec_pretty(self)?;

        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).map_err(|source| CoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let file_name = path.file_name().ok_or_else(|| {
            CoreError::InvalidConfig(format!(
                "caminho de configuração inválido: {}",
                path.display()
            ))
        })?;
        let mut tmp_name = file_name.to_os_string();
        tmp_name.push(".tmp");
        let tmp_path = path.with_file_name(tmp_name);

        fs::write(&tmp_path, &json).map_err(|source| CoreError::Io {
            path: tmp_path.clone(),
            source,
        })?;
        fs::rename(&tmp_path, path).map_err(|source| CoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;

        Ok(())
    }
}

/// Parâmetros do chunking baseado em conteúdo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChunkingSettings {
    /// Tamanho mínimo de um chunk, em bytes.
    pub min_size: u64,
    /// Tamanho médio esperado de um chunk, em bytes.
    pub avg_size: u64,
    /// Tamanho máximo de um chunk, em bytes.
    pub max_size: u64,
}

impl Default for ChunkingSettings {
    fn default() -> Self {
        Self {
            min_size: MIB,
            avg_size: 2 * MIB,
            max_size: 4 * MIB,
        }
    }
}

impl ChunkingSettings {
    fn validate(&self) -> Result<(), CoreError> {
        let min_size = u32::try_from(self.min_size).map_err(|_| {
            CoreError::InvalidConfig("chunking.min_size excede u32::MAX".to_string())
        })?;
        let avg_size = u32::try_from(self.avg_size).map_err(|_| {
            CoreError::InvalidConfig("chunking.avg_size excede u32::MAX".to_string())
        })?;
        let max_size = u32::try_from(self.max_size).map_err(|_| {
            CoreError::InvalidConfig("chunking.max_size excede u32::MAX".to_string())
        })?;

        ChunkingParams::new(min_size, avg_size, max_size)?;
        Ok(())
    }
}

/// Limites de concorrência do pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConcurrencySettings {
    /// Número máximo de workers simultâneos.
    pub workers: u32,
}

impl Default for ConcurrencySettings {
    fn default() -> Self {
        Self { workers: 4 }
    }
}

impl ConcurrencySettings {
    fn validate(&self) -> Result<(), CoreError> {
        if self.workers == 0 {
            return Err(CoreError::InvalidConfig(
                "concurrency.workers deve ser maior que zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Política de retenção de snapshots.
///
/// Os valores representam por quanto tempo cada granularidade é mantida.
/// A aplicação efetiva do pruning ocorre na Fase 6.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RetentionSettings {
    /// Quantos dias de snapshots diários manter.
    pub daily_days: u32,
    /// Quantas semanas de snapshots semanais manter.
    pub weekly_weeks: u32,
    /// Quantos meses de snapshots mensais manter.
    pub monthly_months: u32,
}

impl Default for RetentionSettings {
    fn default() -> Self {
        Self {
            daily_days: 7,
            weekly_weeks: 4,
            monthly_months: 12,
        }
    }
}

/// Configuração do watcher de sistema de arquivos.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MonitoringSettings {
    /// Se o monitoramento em tempo real está habilitado.
    pub enabled: bool,
    /// Janela de coalescência de eventos, em milissegundos.
    pub debounce_ms: u64,
    /// Capacidade máxima de caminhos pendentes na fila de eventos.
    pub queue_capacity: usize,
    /// Se uma varredura completa deve ocorrer na inicialização.
    pub initial_scan: bool,
}

impl Default for MonitoringSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            debounce_ms: 500,
            queue_capacity: 10_000,
            initial_scan: true,
        }
    }
}

impl MonitoringSettings {
    fn validate(&self) -> Result<(), CoreError> {
        if self.debounce_ms > MAX_DEBOUNCE_MS {
            return Err(CoreError::InvalidConfig(format!(
                "monitoring.debounce_ms excede o máximo de {MAX_DEBOUNCE_MS} ms"
            )));
        }
        if !(1..=MAX_QUEUE_CAPACITY).contains(&self.queue_capacity) {
            return Err(CoreError::InvalidConfig(format!(
                "monitoring.queue_capacity deve estar entre 1 e {MAX_QUEUE_CAPACITY} (atual: {})",
                self.queue_capacity
            )));
        }
        Ok(())
    }
}

/// Parâmetros de compressão.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionSettings {
    /// Nível do Zstandard (1 a 22).
    pub level: i32,
}

impl Default for CompressionSettings {
    fn default() -> Self {
        Self { level: 3 }
    }
}

impl CompressionSettings {
    fn validate(&self) -> Result<(), CoreError> {
        if !(1..=ZSTD_MAX_LEVEL).contains(&self.level) {
            return Err(CoreError::InvalidConfig(format!(
                "compression.level deve estar entre 1 e {ZSTD_MAX_LEVEL} (atual: {})",
                self.level
            )));
        }
        Ok(())
    }
}

/// Parâmetros de criptografia em repouso.
///
/// Nenhuma chave é armazenada aqui. O gerenciamento de chaves será definido
/// e documentado antes de a criptografia ser implementada.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EncryptionSettings {
    /// Se os chunks devem ser criptografados antes de gravar no disco.
    pub enabled: bool,
}

impl Default for EncryptionSettings {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Configuração do servidor de comunicação local do daemon.
///
/// O servidor escuta apenas em `127.0.0.1`; `port` não pode ser zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonSettings {
    /// Se o servidor local deve ser iniciado.
    pub enabled: bool,
    /// Porta TCP em `127.0.0.1`.
    pub port: u16,
}

impl Default for DaemonSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 47_821,
        }
    }
}

impl DaemonSettings {
    fn validate(&self) -> Result<(), CoreError> {
        if self.port == 0 {
            return Err(CoreError::InvalidConfig(
                "daemon.port deve ser maior que zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid_and_roundtrips() {
        let settings = Settings::default();
        assert_eq!(settings.schema_version, CURRENT_SCHEMA_VERSION);
        settings.validate().unwrap();

        let json = serde_json::to_string(&settings).unwrap();
        let parsed: Settings = serde_json::from_str(&json).unwrap();
        assert_eq!(settings, parsed);
    }

    #[test]
    fn missing_fields_use_defaults() {
        let parsed: Settings = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed, Settings::default());
    }

    #[test]
    fn rejects_invalid_chunk_order() {
        let mut settings = Settings::default();
        settings.chunking.min_size = 5 * MIB;
        settings.chunking.avg_size = 2 * MIB;
        settings.chunking.max_size = MIB;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_zero_min_chunk() {
        let mut settings = Settings::default();
        settings.chunking.min_size = 0;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_zero_workers() {
        let mut settings = Settings::default();
        settings.concurrency.workers = 0;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_invalid_queue_capacity() {
        let mut settings = Settings::default();
        settings.monitoring.queue_capacity = 0;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));

        settings.monitoring.queue_capacity = MAX_QUEUE_CAPACITY + 1;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_zero_daemon_port() {
        let mut settings = Settings::default();
        settings.daemon.port = 0;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_invalid_compression_level() {
        let mut settings = Settings::default();
        settings.compression.level = 0;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));

        settings.compression.level = ZSTD_MAX_LEVEL + 1;
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let settings = Settings {
            schema_version: CURRENT_SCHEMA_VERSION + 1,
            ..Settings::default()
        };
        assert!(matches!(
            settings.validate(),
            Err(CoreError::UnsupportedConfigVersion(_))
        ));
    }

    #[test]
    fn rejects_empty_source_dir() {
        let mut settings = Settings::default();
        settings.source_dirs.push(PathBuf::new());
        assert!(matches!(
            settings.validate(),
            Err(CoreError::InvalidConfig(_))
        ));
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config").join("settings.json");

        let settings = Settings::default();
        settings.save(&path).unwrap();
        assert!(path.is_file());
        assert!(!path.with_extension("json.tmp").exists());

        let loaded = Settings::load(&path).unwrap();
        assert_eq!(settings, loaded);
    }

    #[test]
    fn load_missing_file_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        assert!(matches!(Settings::load(&path), Err(CoreError::Io { .. })));
    }

    #[test]
    fn load_invalid_json_returns_json_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        fs::write(&path, b"{ nao e json }").unwrap();
        assert!(matches!(Settings::load(&path), Err(CoreError::Json(_))));
    }
}
