# LeoBackup

Mecanismo de backup em Rust com chunking baseado em conteúdo, hashing,
deduplicação, compressão, criptografia e metadados em SQLite.

Este repositório segue o PRD (`prd_sistema_de_backup_inteligente.md`),
as especificações técnicas (`technical_specifications.md`), o backlog
(`task_list_backlog.md`) e o guia `AGENTS.md` mantidos no diretório de
documentos do projeto.

## Status: Fase 1 concluída

A Fase 1 (Configuração Inicial e Infraestrutura) entrega:

- Workspace Rust com a estrutura de crates documentada.
- Dependências fundamentais declaradas no `Cargo.toml`.
- Layout do repositório de backup (`data/`, `metadata/`, `config/`) com
  rotina de inicialização idempotente e não destrutiva.
- Parser de leitura/escrita de `config/settings.json` com validação.

As fases 2 a 8 ainda não foram implementadas.

## Estrutura

```text
crates/
├── leobackup-core/      # tipos base, erro comum e configuração (Fase 1)
├── leobackup-storage/   # layout do repositório; SQLite virá na Fase 2
├── leobackup-engine/    # pipeline de backup/restauração (Fase 3/5)
├── leobackup-daemon/    # watcher e servidor local (Fase 4)
└── leobackup-cli/       # interface de linha de comando (Fase 1: init/config)
```

## Repositório de backup

```text
<raiz>/
├── data/                 # chunks (Fase 3)
├── metadata/
│   └── index.db          # metadados SQLite (Fase 2)
└── config/
    └── settings.json     # configuração
```

## Configuração (`config/settings.json`)

O arquivo é versionado por `schema_version` (atual: `1`). Campos ausentes
usam valores padrão, o que permite adicionar campos em fases futuras sem
migração. Valores padrão relevantes:

| Campo | Padrão |
|---|---|
| `chunking.min_size` | 1 MiB |
| `chunking.avg_size` | 2 MiB |
| `chunking.max_size` | 4 MiB |
| `concurrency.workers` | 4 |
| `retention.daily_days` | 7 |
| `retention.weekly_weeks` | 4 |
| `retention.monthly_months` | 12 |
| `monitoring.enabled` | `true` |
| `monitoring.debounce_ms` | 500 |
| `compression.level` | 3 |
| `encryption.enabled` | `true` |

Validações implementadas: `min_size > 0`, `min_size <= avg_size <= max_size`,
`workers > 0`, `compression.level` entre 1 e 22, `debounce_ms <= 60000`,
`schema_version <= 1` e ausência de caminhos vazios em `source_dirs`.

## Uso

```bash
cargo run -p leobackup-cli -- init --repo ./repo
cargo run -p leobackup-cli -- config --repo ./repo
```

`init` nunca sobrescreve uma configuração existente sem `--force`.

## Verificações

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Decisões e suposições da Fase 1

- **Configuração em `leobackup-core`**: a crate documentada não prevê uma
  crate dedicada a configuração; ela é pequena e independente de I/O de
  backup, então fica em `core`. Se crescer, pode ser extraída sem alterar o
  formato persistido.
- **Dependências pesadas**: `rusqlite`, `notify`, `fastcdc`, `blake3`,
  `zstd` e `aes-gcm` estão declaradas em `[workspace.dependencies]`, mas só
  serão vinculadas às crates donas quando cada fase as utilizar. Isso segue
  o item "Gerenciamento de Dependências" do backlog sem compilar código
  ainda não usado.
- **Escrita atômica de configuração**: `settings.json` é gravado em um
  temporário no mesmo diretório e renomeado ao final.
- **Validação de `source_dirs`**: apenas estrutura (caminho não vazio). A
  existência e a acessibilidade dos diretórios serão verificadas no início
  de cada backup.
- **Criptografia habilitada por padrão**: prioriza segurança em repouso
  (RNF03). O gerenciamento de chaves ainda não foi definido e será
  documentado antes da implementação.
