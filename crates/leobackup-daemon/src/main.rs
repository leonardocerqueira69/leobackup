//! Binário do daemon do LeoBackup.
//!
//! Uso típico:
//!
//! ```text
//! leobackup-daemon --repo D:\Backups\LeoBackup --key-file D:\chave.hex
//! ```
//!
//! O token de acesso é criado em `config/daemon.token` na primeira execução.
//! Ele nunca é impresso: o cliente deve ler o arquivo.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use leobackup_core::{EncryptionKey, Settings};
use leobackup_daemon::{Daemon, DaemonConfig, DaemonError};
use leobackup_storage::RepositoryLayout;

/// Comprimento de uma chave AES-256 em bytes.
const KEY_BYTES: usize = 32;

#[derive(Debug, Parser)]
#[command(
    name = "leobackup-daemon",
    version,
    about = "Daemon de monitoramento e backup do LeoBackup (Fase 4)"
)]
struct Args {
    /// Diretório raiz do repositório inicializado com `leobackup init`.
    #[arg(long, default_value = ".")]
    repo: PathBuf,

    /// Arquivo com a chave de criptografia (64 caracteres hex ou 32 bytes).
    #[arg(long)]
    key_file: Option<PathBuf>,

    /// Porta de loopback; use 0 para uma porta efêmera.
    #[arg(long)]
    port: Option<u16>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("leobackup-daemon: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), DaemonError> {
    let args = Args::parse();

    let layout = RepositoryLayout::new(&args.repo)?;
    let settings_path = layout.settings_path();
    if !settings_path.exists() {
        return Err(DaemonError::InvalidConfig(format!(
            "{} não encontrado; execute `leobackup init --repo {}` primeiro",
            settings_path.display(),
            args.repo.display()
        )));
    }

    let settings = Settings::load(&settings_path)?;
    let key = match &args.key_file {
        Some(path) => Some(load_key(path)?),
        None => None,
    };

    println!("LeoBackup daemon");
    println!("  repositório: {}", layout.root().display());
    for dir in &settings.source_dirs {
        println!("  origem: {}", dir.display());
    }

    let handle = Daemon::start(DaemonConfig {
        repository: args.repo,
        settings,
        key,
        port: args.port,
    })?;

    match handle.local_addr() {
        Some(addr) => println!("  servidor local: {addr} (TCP em loopback, JSON por linha)"),
        None => println!("  servidor local desabilitado (daemon.enabled=false)"),
    }
    println!(
        "  token de acesso: {} (leia o arquivo; ele não é exibido)",
        handle.token_path().display()
    );

    let signal = handle.signal();
    if let Err(error) = ctrlc::set_handler(move || signal.trigger()) {
        let _ = handle.shutdown();
        return Err(DaemonError::Internal(format!(
            "falha ao instalar Ctrl+C: {error}"
        )));
    }

    // Bloqueia até o encerramento ser solicitado por Ctrl+C ou pelo comando
    // `shutdown`. O worker finaliza o lote corrente e o servidor é liberado.
    handle.wait()
}

/// Lê a chave de um arquivo, aceitando hexadecimal (64 caracteres) ou 32 bytes
/// brutos. O conteúdo nunca aparece em mensagens de erro.
fn load_key(path: &Path) -> Result<EncryptionKey, DaemonError> {
    let bytes = fs::read(path).map_err(|source| DaemonError::io(path, source))?;

    if let Ok(text) = std::str::from_utf8(&bytes) {
        let text = text.trim();
        if text.len() == KEY_BYTES * 2 {
            return EncryptionKey::from_hex(text).map_err(DaemonError::Core);
        }
    }

    if bytes.len() == KEY_BYTES {
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        return EncryptionKey::from_hex(&hex).map_err(DaemonError::Core);
    }

    Err(DaemonError::Key(format!(
        "{} deve conter 64 caracteres hexadecimais ou {KEY_BYTES} bytes brutos",
        path.display()
    )))
}
