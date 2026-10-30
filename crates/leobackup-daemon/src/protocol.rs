//! Protocolo do servidor local: JSON por linha sobre TCP em loopback.
//!
//! Cada conexão transporta exatamente uma requisição e uma resposta, ambas
//! terminadas por `\n` e limitadas em tamanho. O token de autenticação é o
//! primeiro campo verificado; a mensagem de erro de autenticação não revela
//! detalhes da requisição.
//!
//! A escolha por TCP em loopback + JSON (em vez de gRPC ou sockets de domínio)
//! está documentada no README: é portátil, sem dependências pesadas e o
//! serviço nunca é exposto fora de `127.0.0.1`.

use serde::{Deserialize, Serialize};

use crate::error::DaemonError;

/// Tamanho máximo de uma requisição, incluindo o token.
pub const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// Comando aceito pelo daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandName {
    /// Consulta o estado do daemon sem tocar no pipeline.
    Status,
    /// Executa uma varredura completa das origens.
    BackupNow,
    /// Solicita encerramento gracioso.
    Shutdown,
}

/// Requisição recebida do cliente.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Token de autenticação (hexadecimal de 64 caracteres).
    pub token: String,
    /// Comando solicitado.
    pub command: CommandName,
}

impl Request {
    /// Faz o parse de uma linha JSON, rejeitando campos desconhecidos.
    pub fn parse(line: &str) -> Result<Self, DaemonError> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Err(DaemonError::Protocol("requisição vazia".to_string()));
        }
        serde_json::from_str(trimmed)
            .map_err(|_| DaemonError::Protocol("requisição inválida".to_string()))
    }
}

/// Resumo de um backup executado pelo worker.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupSummary {
    /// Identificador do snapshot criado, se houve arquivos processados.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// Estado final do snapshot (`completed` ou `failed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_status: Option<String>,
    /// Arquivos gravados com sucesso.
    pub files_backed_up: u64,
    /// Arquivos que falharam.
    pub failures: u64,
    /// Eventos de remoção observados (não geram snapshot por si só).
    pub deleted: u64,
}

/// Estado resumido do daemon, seguro para expor ao cliente.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    /// Tempo desde a inicialização, em segundos.
    pub uptime_secs: u64,
    /// Quantidade de diretórios de origem configurados.
    pub source_dirs: usize,
    /// Caminhos aguardando processamento na fila.
    pub queued_events: usize,
    /// Se uma varredura completa está pendente.
    pub rescan_required: bool,
    /// Arquivos gravados com sucesso desde a inicialização.
    pub files_backed_up: u64,
    /// Falhas de arquivo desde a inicialização.
    pub failures: u64,
    /// Eventos de remoção observados desde a inicialização.
    pub deleted_events: u64,
    /// Snapshots criados desde a inicialização.
    pub snapshots_created: u64,
    /// Última mensagem de erro, quando houver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Resposta do daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonResponse {
    /// Se o comando foi aceito e concluído.
    pub ok: bool,
    /// Mensagem de erro, quando `ok` é falso.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Estado do daemon, para o comando `status`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DaemonStatus>,
    /// Resumo do backup, para o comando `backup_now`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupSummary>,
}

impl DaemonResponse {
    /// Resposta de sucesso sem payload.
    pub fn ok() -> Self {
        Self {
            ok: true,
            error: None,
            status: None,
            backup: None,
        }
    }

    /// Resposta de sucesso com o estado do daemon.
    pub fn with_status(status: DaemonStatus) -> Self {
        Self {
            ok: true,
            error: None,
            status: Some(status),
            backup: None,
        }
    }

    /// Resposta de sucesso com o resumo de backup.
    pub fn with_backup(backup: BackupSummary) -> Self {
        Self {
            ok: true,
            error: None,
            status: None,
            backup: Some(backup),
        }
    }

    /// Resposta de erro.
    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(message.into()),
            status: None,
            backup: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_commands() {
        let request = Request::parse(r#"{"token":"abc","command":"backup_now"}"#).unwrap();
        assert_eq!(request.token, "abc");
        assert_eq!(request.command, CommandName::BackupNow);

        let status = Request::parse(r#"{"token":"abc","command":"status"}"#).unwrap();
        assert_eq!(status.command, CommandName::Status);

        let shutdown = Request::parse(r#"{"token":"abc","command":"shutdown"}"#).unwrap();
        assert_eq!(shutdown.command, CommandName::Shutdown);
    }

    #[test]
    fn rejects_unknown_fields_and_commands() {
        assert!(Request::parse(r#"{"token":"abc","command":"status","extra":1}"#).is_err());
        assert!(Request::parse(r#"{"token":"abc","command":"apagar_tudo"}"#).is_err());
        assert!(Request::parse("").is_err());
        assert!(Request::parse("nao e json").is_err());
    }

    #[test]
    fn response_omits_absent_payloads() {
        let encoded = serde_json::to_string(&DaemonResponse::ok()).unwrap();
        assert_eq!(encoded, r#"{"ok":true}"#);

        let failure = serde_json::to_string(&DaemonResponse::failure("indisponível")).unwrap();
        assert!(failure.contains("\"ok\":false"));
        assert!(failure.contains("indisponível"));
    }

    #[test]
    fn status_roundtrips() {
        let status = DaemonStatus {
            uptime_secs: 1,
            source_dirs: 2,
            queued_events: 3,
            rescan_required: false,
            files_backed_up: 4,
            failures: 0,
            deleted_events: 5,
            snapshots_created: 6,
            last_error: None,
        };
        let response = DaemonResponse::with_status(status.clone());
        let encoded = serde_json::to_string(&response).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.status, Some(status));
    }

    #[test]
    fn backup_summary_roundtrips() {
        let summary = BackupSummary {
            snapshot_id: Some(7),
            snapshot_status: Some("completed".to_string()),
            files_backed_up: 3,
            failures: 1,
            deleted: 2,
        };
        let encoded = serde_json::to_string(&DaemonResponse::with_backup(summary.clone())).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.backup, Some(summary));
    }
}
