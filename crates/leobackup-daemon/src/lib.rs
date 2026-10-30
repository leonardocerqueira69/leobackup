//! Daemon do LeoBackup.
//!
//! A Fase 4 entrega o serviço em background: um worker serializado que
//! consome a fila de eventos do watcher, o watcher baseado em `notify` e um
//! servidor local autenticado por token (TCP em `127.0.0.1` com JSON por
//! linha). A restauração e o agendamento de retenção ficam para as fases
//! seguintes.

pub mod client;
pub mod daemon;
pub mod error;
pub mod event;
pub mod protocol;
pub mod queue;
pub mod scanner;
pub mod server;
pub mod status;
pub mod token;
pub mod worker;

pub use client::Client;
pub use daemon::{Daemon, DaemonConfig, DaemonHandle, ShutdownSignal};
pub use error::DaemonError;
pub use event::{SourceEvent, SourceEventKind};
pub use protocol::{BackupSummary, CommandName, DaemonResponse, DaemonStatus};
pub use queue::{EventQueue, PushOutcome};
pub use scanner::{walk_files, ScanControl, ScanSummary};
pub use server::{CommandHandler, IpcServer};
pub use status::{BatchOutcome, StatusState};

/// Versão do protocolo local, documentada no README.
pub const PROTOCOL_VERSION: u32 = 1;
