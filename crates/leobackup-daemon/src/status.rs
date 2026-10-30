//! Contadores compartilhados de estado do daemon.
//!
//! O servidor local responde `status` sem consultar o worker nem o SQLite:
//! lê estes contadores, que são atualizados ao final de cada lote. Isso
//! mantém o comando responsivo mesmo durante um backup.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::protocol::DaemonStatus;
use crate::queue::EventQueue;

/// Resultado de um lote processado pelo worker.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchOutcome {
    /// Snapshot criado pelo lote, se houve arquivos.
    pub snapshot_id: Option<i64>,
    /// Estado final do snapshot.
    pub snapshot_status: Option<String>,
    /// Arquivos gravados com sucesso.
    pub files_backed_up: u64,
    /// Arquivos que falharam.
    pub failures: u64,
    /// Caminhos removidos observados.
    pub deleted: u64,
    /// Erro geral do lote (falha de banco, encerramento etc.).
    pub error: Option<String>,
    /// Se o erro é fatal para o lote (impede novos arquivos).
    pub fatal: bool,
    /// Se o lote foi interrompido por encerramento.
    pub aborted: bool,
}

#[derive(Debug, Default)]
struct Counters {
    files_backed_up: u64,
    failures: u64,
    deleted_events: u64,
    snapshots_created: u64,
    last_error: Option<String>,
}

/// Estado observável do daemon.
pub struct StatusState {
    started: Instant,
    source_dirs: usize,
    counters: Mutex<Counters>,
}

impl StatusState {
    /// Cria o estado observável.
    pub fn new(source_dirs: usize) -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            source_dirs,
            counters: Mutex::new(Counters::default()),
        })
    }

    /// Registra um erro avulso (watcher, configuração, lote).
    pub fn record_error(&self, message: impl Into<String>) {
        self.lock().last_error = Some(message.into());
    }

    /// Incorpora o resultado de um lote.
    pub fn record_outcome(&self, outcome: &BatchOutcome) {
        let mut counters = self.lock();
        counters.files_backed_up += outcome.files_backed_up;
        counters.failures += outcome.failures;
        counters.deleted_events += outcome.deleted;

        if outcome.snapshot_id.is_some() {
            counters.snapshots_created += 1;
        }

        if let Some(error) = &outcome.error {
            counters.last_error = Some(error.clone());
        } else if outcome.failures > 0 {
            counters.last_error = Some(format!(
                "{} arquivo(s) falharam no último lote",
                outcome.failures
            ));
        } else if !outcome.aborted {
            counters.last_error = None;
        }
    }

    /// Produz a visão serializável do estado atual.
    pub fn snapshot(&self, queue: &EventQueue) -> DaemonStatus {
        let counters = self.lock();
        DaemonStatus {
            uptime_secs: self.started.elapsed().as_secs(),
            source_dirs: self.source_dirs,
            queued_events: queue.len(),
            rescan_required: queue.rescan_required(),
            files_backed_up: counters.files_backed_up,
            failures: counters.failures,
            deleted_events: counters.deleted_events,
            snapshots_created: counters.snapshots_created,
            last_error: counters.last_error.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Counters> {
        self.counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_counters_and_reports_queue() {
        let status = StatusState::new(2);
        let queue = EventQueue::new(8);

        status.record_outcome(&BatchOutcome {
            snapshot_id: Some(1),
            snapshot_status: Some("completed".to_string()),
            files_backed_up: 3,
            failures: 0,
            deleted: 1,
            error: None,
            fatal: false,
            aborted: false,
        });

        let snapshot = status.snapshot(&queue);
        assert_eq!(snapshot.source_dirs, 2);
        assert_eq!(snapshot.files_backed_up, 3);
        assert_eq!(snapshot.deleted_events, 1);
        assert_eq!(snapshot.snapshots_created, 1);
        assert_eq!(snapshot.queued_events, 0);
        assert!(snapshot.last_error.is_none());
    }

    #[test]
    fn tracks_and_clears_last_error() {
        let status = StatusState::new(0);
        let queue = EventQueue::new(8);

        status.record_error("falha de teste");
        assert_eq!(
            status.snapshot(&queue).last_error.as_deref(),
            Some("falha de teste")
        );

        status.record_outcome(&BatchOutcome {
            files_backed_up: 1,
            ..BatchOutcome::default()
        });
        assert!(status.snapshot(&queue).last_error.is_none());
    }

    #[test]
    fn failed_batch_keeps_an_error_visible() {
        let status = StatusState::new(0);
        let queue = EventQueue::new(8);

        status.record_outcome(&BatchOutcome {
            snapshot_id: Some(9),
            snapshot_status: Some("failed".to_string()),
            failures: 2,
            ..BatchOutcome::default()
        });

        let snapshot = status.snapshot(&queue);
        assert_eq!(snapshot.failures, 2);
        assert!(snapshot.last_error.is_some());
    }
}
