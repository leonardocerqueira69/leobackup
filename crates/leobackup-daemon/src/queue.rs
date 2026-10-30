//! Fila de eventos em memória com coalescência, debounce e limite.
//!
//! O callback do watcher nunca bloqueia: ele apenas chama [`EventQueue::push`],
//! que faz uma inserção curta sob mutex. O worker consome os caminhos quando
//! eles ficam estáveis por `debounce` (nenhum evento novo para o mesmo
//! caminho).
//!
//! Quando a capacidade é atingida, o evento é descartado de forma explícita e
//! a fila sinaliza `rescan_required`. O worker responde com uma varredura
//! completa das origens, garantindo que nenhuma alteração seja esquecida em
//! silêncio.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::event::{SourceEvent, SourceEventKind};

/// Resultado de uma tentativa de inserir um evento.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    /// O caminho entrou na fila.
    Enqueued,
    /// O caminho já estava pendente e foi atualizado.
    Coalesced,
    /// A fila estava cheia; o evento foi descartado e um rescan foi marcado.
    Overflowed,
    /// A fila foi encerrada.
    Closed,
}

#[derive(Debug)]
struct Pending {
    kind: SourceEventKind,
    last_seen: Instant,
}

struct QueueShared {
    inner: Mutex<QueueInner>,
    ready: Condvar,
    capacity: usize,
}

struct QueueInner {
    pending: HashMap<PathBuf, Pending>,
    order: VecDeque<PathBuf>,
    rescan_required: bool,
    closed: bool,
}

/// Fila de eventos compartilhável entre o watcher e o worker.
#[derive(Clone)]
pub struct EventQueue {
    shared: Arc<QueueShared>,
}

impl EventQueue {
    /// Cria uma fila com capacidade máxima de caminhos pendentes.
    pub fn new(capacity: usize) -> Self {
        Self {
            shared: Arc::new(QueueShared {
                inner: Mutex::new(QueueInner {
                    pending: HashMap::new(),
                    order: VecDeque::new(),
                    rescan_required: false,
                    closed: false,
                }),
                ready: Condvar::new(),
                capacity: capacity.max(1),
            }),
        }
    }

    /// Insere ou coalesce um evento. Nunca bloqueia por debounce ou I/O.
    pub fn push(&self, event: SourceEvent) -> PushOutcome {
        let mut inner = self.lock();

        if inner.closed {
            return PushOutcome::Closed;
        }

        let now = Instant::now();
        if let Some(pending) = inner.pending.get_mut(&event.path) {
            pending.kind = event.kind;
            pending.last_seen = now;
            drop(inner);
            self.shared.ready.notify_all();
            return PushOutcome::Coalesced;
        }

        if inner.pending.len() >= self.shared.capacity {
            inner.rescan_required = true;
            drop(inner);
            self.shared.ready.notify_all();
            return PushOutcome::Overflowed;
        }

        inner.pending.insert(
            event.path.clone(),
            Pending {
                kind: event.kind,
                last_seen: now,
            },
        );
        inner.order.push_back(event.path);
        drop(inner);
        self.shared.ready.notify_all();
        PushOutcome::Enqueued
    }

    /// Marca que uma varredura completa é necessária.
    pub fn require_rescan(&self) {
        self.lock().rescan_required = true;
        self.shared.ready.notify_all();
    }

    /// Indica se um rescan foi solicitado.
    pub fn rescan_required(&self) -> bool {
        self.lock().rescan_required
    }

    /// Consome a solicitação de rescan.
    pub fn take_rescan(&self) -> bool {
        let mut inner = self.lock();
        std::mem::replace(&mut inner.rescan_required, false)
    }

    /// Acorda o worker (usado ao enfileirar comandos).
    pub fn wake(&self) {
        self.shared.ready.notify_all();
    }

    /// Número de caminhos pendentes.
    pub fn len(&self) -> usize {
        self.lock().pending.len()
    }

    /// Indica se não há caminhos pendentes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Aguarda até haver trabalho, o prazo do debounce ou `max_wait`.
    ///
    /// `max_wait` limita a espera para que comandos do servidor sejam
    /// observados mesmo sem um evento novo. Não faz polling do sistema de
    /// arquivos: o watcher continua notificando de forma nativa.
    pub fn wait_for_work(&self, debounce: Duration, max_wait: Duration) {
        let inner = self.lock();
        let now = Instant::now();

        if inner.closed || inner.rescan_required || inner.has_due(now, debounce) {
            return;
        }

        let timeout = inner
            .next_deadline(debounce)
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(max_wait)
            .min(max_wait);

        let _ = self.shared.ready.wait_timeout(inner, timeout);
    }

    /// Remove até `limit` eventos cujo debounce já expirou.
    pub fn take_due(&self, debounce: Duration, limit: usize) -> Vec<SourceEvent> {
        let mut inner = self.lock();
        if limit == 0 {
            return Vec::new();
        }

        let now = Instant::now();
        let mut due = Vec::new();
        let mut kept = VecDeque::with_capacity(inner.order.len());

        while let Some(path) = inner.order.pop_front() {
            match inner.pending.get(&path) {
                Some(pending)
                    if due.len() < limit
                        && now.saturating_duration_since(pending.last_seen) >= debounce =>
                {
                    let pending = inner
                        .pending
                        .remove(&path)
                        .expect("caminho presente na fila e no índice");
                    due.push(SourceEvent {
                        path,
                        kind: pending.kind,
                    });
                }
                _ => kept.push_back(path),
            }
        }

        inner.order = kept;
        due
    }

    /// Encerra a fila; novas inserções passam a retornar `Closed`.
    pub fn close(&self) {
        self.lock().closed = true;
        self.shared.ready.notify_all();
    }

    /// Indica se a fila foi encerrada.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueueInner> {
        self.shared
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl QueueInner {
    fn has_due(&self, now: Instant, debounce: Duration) -> bool {
        self.pending
            .values()
            .any(|pending| now.saturating_duration_since(pending.last_seen) >= debounce)
    }

    fn next_deadline(&self, debounce: Duration) -> Option<Instant> {
        self.pending
            .values()
            .map(|pending| pending.last_seen + debounce)
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn event(path: &str, kind: SourceEventKind) -> SourceEvent {
        SourceEvent::new(path, kind)
    }

    #[test]
    fn coalesces_same_path_and_keeps_latest_kind() {
        let queue = EventQueue::new(8);
        assert_eq!(
            queue.push(event("/a", SourceEventKind::Upsert)),
            PushOutcome::Enqueued
        );
        assert_eq!(
            queue.push(event("/a", SourceEventKind::Deleted)),
            PushOutcome::Coalesced
        );
        assert_eq!(queue.len(), 1);

        let due = queue.take_due(Duration::ZERO, 10);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].kind, SourceEventKind::Deleted);
        assert!(queue.is_empty());
    }

    #[test]
    fn respects_capacity_and_marks_rescan() {
        let queue = EventQueue::new(2);
        assert_eq!(
            queue.push(event("/a", SourceEventKind::Upsert)),
            PushOutcome::Enqueued
        );
        assert_eq!(
            queue.push(event("/b", SourceEventKind::Upsert)),
            PushOutcome::Enqueued
        );
        assert_eq!(
            queue.push(event("/c", SourceEventKind::Upsert)),
            PushOutcome::Overflowed
        );
        assert_eq!(queue.len(), 2);
        assert!(queue.rescan_required());
        assert!(queue.take_rescan());
        assert!(!queue.rescan_required());
    }

    #[test]
    fn debounce_delays_readiness() {
        let queue = EventQueue::new(8);
        queue.push(event("/a", SourceEventKind::Upsert));

        assert!(queue.take_due(Duration::from_millis(30), 10).is_empty());
        thread::sleep(Duration::from_millis(40));
        assert_eq!(queue.take_due(Duration::from_millis(30), 10).len(), 1);
    }

    #[test]
    fn update_restarts_debounce_window() {
        let queue = EventQueue::new(8);
        queue.push(event("/a", SourceEventKind::Upsert));
        thread::sleep(Duration::from_millis(20));
        queue.push(event("/a", SourceEventKind::Upsert));
        thread::sleep(Duration::from_millis(20));

        // 40 ms desde a primeira inserção, mas apenas 20 ms desde a última.
        assert!(queue.take_due(Duration::from_millis(30), 10).is_empty());
    }

    #[test]
    fn take_due_limits_batch() {
        let queue = EventQueue::new(8);
        for path in ["/a", "/b", "/c"] {
            queue.push(event(path, SourceEventKind::Upsert));
        }
        let first = queue.take_due(Duration::ZERO, 2);
        assert_eq!(first.len(), 2);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn wait_returns_when_event_arrives() {
        let queue = EventQueue::new(8);
        let producer = queue.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            producer.push(event("/a", SourceEventKind::Upsert));
        });

        queue.wait_for_work(Duration::ZERO, Duration::from_millis(500));
        assert!(!queue.is_empty());
        handle.join().unwrap();
    }

    #[test]
    fn closed_queue_rejects_events() {
        let queue = EventQueue::new(8);
        queue.close();
        assert!(queue.is_closed());
        assert_eq!(
            queue.push(event("/a", SourceEventKind::Upsert)),
            PushOutcome::Closed
        );
    }
}
