use std::io;
use std::path::PathBuf;

/// Erros comuns da crate `leobackup-core`.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// Falha de I/O, com o caminho que a originou.
    #[error("erro de I/O em {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Falha ao ler ou gravar JSON.
    #[error("erro ao processar JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuração estruturalmente inválida.
    #[error("configuração inválida: {0}")]
    InvalidConfig(String),

    /// Versão de configuração mais nova do que a suportada.
    #[error("versão de configuração não suportada: {0}")]
    UnsupportedConfigVersion(u32),

    /// Dado persistido ou recebido em formato inesperado.
    #[error("dado inválido: {0}")]
    InvalidData(String),
}
