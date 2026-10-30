//! Interface de linha de comando do LeoBackup.
//!
//! Nesta fase a CLI expõe apenas o necessário para inicializar o repositório
//! e inspecionar a configuração. Os comandos de backup, list, restore, prune
//! e scrub serão adicionados na Fase 7.

use std::path::Path;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use leobackup_core::Settings;
use leobackup_storage::{MetadataStore, RepositoryLayout};

#[derive(Debug, Parser)]
#[command(
    name = "leobackup",
    version,
    about = "Backup com deduplicação e snapshots (Fase 1: infraestrutura)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inicializa um repositório de backup no caminho informado.
    Init {
        /// Diretório raiz do repositório.
        #[arg(long, default_value = ".")]
        repo: std::path::PathBuf,
        /// Sobrescreve config/settings.json caso ele já exista.
        #[arg(long)]
        force: bool,
    },
    /// Exibe a configuração atual do repositório.
    Config {
        /// Diretório raiz do repositório.
        #[arg(long, default_value = ".")]
        repo: std::path::PathBuf,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("erro: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Init { repo, force } => init(&repo, force),
        Command::Config { repo } => show_config(&repo),
    }
}

fn init(repo: &Path, force: bool) -> anyhow::Result<()> {
    let layout = RepositoryLayout::new(repo)?;
    layout.ensure()?;

    let settings_path = layout.settings_path();
    if settings_path.exists() && !force {
        println!(
            "configuração existente preservada: {}",
            settings_path.display()
        );
    } else {
        Settings::default().save(&settings_path)?;
        println!("configuração criada: {}", settings_path.display());
    }

    let store = MetadataStore::open(&layout.database_path())?;
    println!(
        "banco de metadados pronto: {} (schema v{})",
        layout.database_path().display(),
        store.schema_version()?
    );

    println!("repositório inicializado em {}", layout.root().display());
    Ok(())
}

fn show_config(repo: &Path) -> anyhow::Result<()> {
    let layout = RepositoryLayout::new(repo)?;
    let settings = Settings::load(&layout.settings_path())?;
    println!("{}", serde_json::to_string_pretty(&settings)?);
    Ok(())
}
