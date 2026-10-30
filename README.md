# LeoBackup

Mecanismo de backup em Rust com chunking baseado em conteúdo, hashing,
deduplicação, compressão, criptografia e metadados em SQLite.

Este repositório segue o PRD (`prd_sistema_de_backup_inteligente.md`),
as especificações técnicas (`technical_specifications.md`), o backlog
(`task_list_backlog.md`) e o guia `AGENTS.md` mantidos no diretório de
documentos do projeto.

## Status

### Fase 1 — Configuração Inicial e Infraestrutura (concluída)

- Workspace Rust com a estrutura de crates documentada.
- Dependências fundamentais declaradas no `Cargo.toml`.
- Layout do repositório de backup (`data/`, `metadata/`, `config/`) com
  rotina de inicialização idempotente e não destrutiva.
- Parser de leitura/escrita de `config/settings.json` com validação.

### Fase 2 — Banco de Dados e Metadados (concluída)

- Conexão SQLite em `metadata/index.db` com `foreign_keys=ON`,
  `journal_mode=WAL` e `synchronous=FULL`.
- Migrações versionadas via `PRAGMA user_version`.
- Esquema `snapshots`, `files`, `chunks` e `file_chunks` com chaves
  estrangeiras, restrições e índices.
- Camada CRUD transacional em `MetadataStore`, com `reference_count`
  mantido por triggers.
- `init` cria e migra o banco automaticamente.

As fases 3 a 8 ainda não foram implementadas.

## Estrutura

```text
crates/
├── leobackup-core/      # tipos base, erro, configuração e modelos de domínio
├── leobackup-storage/   # layout do repositório e SQLite (migrações + CRUD)
├── leobackup-engine/    # pipeline de backup/restauração (Fase 3/5)
├── leobackup-daemon/    # watcher e servidor local (Fase 4)
└── leobackup-cli/       # interface de linha de comando (init/config)
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

## Banco de metadados (Fase 2)

O esquema é criado por migrações versionadas (`PRAGMA user_version`, versão
atual `1`) em `metadata/index.db`:

| Tabela | Colunas principais |
|---|---|
| `snapshots` | `id`, `timestamp` (epoch UTC), `status` (`in-progress`/`completed`/`failed`) |
| `files` | `id`, `snapshot_id`, `path`, `permissions`, `modification_time`, `size` |
| `chunks` | `hash_id` (PK), `size`, `compressed_size`, `reference_count` |
| `file_chunks` | `file_id`, `chunk_id`, `position` |

Regras e garantias:

- `foreign_keys=ON` é verificado na abertura da conexão.
- `files` tem `UNIQUE (snapshot_id, path)`; remover um snapshot remove
  arquivos e associações em cascata.
- `chunks` usa `hash_id` como chave primária e `ON DELETE RESTRICT` em
  `file_chunks`, impedindo remover chunk ainda referenciado.
- `reference_count` conta **ocorrências** em `file_chunks` e é mantido por
  triggers `AFTER INSERT`/`AFTER DELETE`; nunca fica negativo.
- `recompute_reference_counts()` reconstrói a contagem a partir de
  `file_chunks` para verificação/recuperação.
- `link_file_chunks()` associa uma sequência ordenada de chunks de forma
  atômica; qualquer chunk inexistente reverte a transação.
- Caminhos são persistidos como `TEXT` (UTF-8). Caminhos que não possam ser
  representados como UTF-8 não são suportados nesta fase.

## Uso


```bash
cargo run -p leobackup-cli -- init --repo ./repo
cargo run -p leobackup-cli -- config --repo ./repo
```

`init` nunca sobrescreve uma configuração existente sem `--force` e
cria/migra `metadata/index.db`.

## Verificações

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Decisões e suposições

### Fase 1

- **Configuração em `leobackup-core`**: a crate documentada não prevê uma
  crate dedicada a configuração; ela é pequena e independente de I/O de
  backup, então fica em `core`. Se crescer, pode ser extraída sem alterar o
  formato persistido.
- **Dependências pesadas**: `notify`, `fastcdc`, `blake3`, `zstd` e
  `aes-gcm` permanecem declaradas em `[workspace.dependencies]` e só serão
  vinculadas às crates donas quando cada fase as utilizar. `rusqlite` foi
  vinculada na Fase 2. Isso segue o item "Gerenciamento de Dependências" do
  backlog sem compilar código ainda não usado.
- **Escrita atômica de configuração**: `settings.json` é gravado em um
  temporário no mesmo diretório e renomeado ao final.
- **Validação de `source_dirs`**: apenas estrutura (caminho não vazio). A
  existência e a acessibilidade dos diretórios serão verificadas no início
  de cada backup.
- **Criptografia habilitada por padrão**: prioriza segurança em repouso
  (RNF03). O gerenciamento de chaves ainda não foi definido e será
  documentado antes da implementação.

### Fase 2

- **`reference_count` por triggers**: centraliza a consistência da contagem,
  inclusive em exclusões em cascata. As regras estão documentadas em
  `crates/leobackup-storage/src/store.rs`.
- **Uma conexão por `MetadataStore`**: suficiente para o CRUD desta fase. A
  estratégia de concorrência/pool será definida junto com o pipeline
  (Fase 3) e o daemon (Fase 4).
- **Transações por operação composta**: `link_file_chunks` e
  `recompute_reference_counts` já são atômicas. O commit de um arquivo
  completo (metadados + chunks + snapshot) será uma operação composta na
  Fase 3, mantendo I/O de chunks fora da transação.
- **`synchronous=FULL`**: prioriza durabilidade sobre desempenho. Pode ser
  reavaliado com medições se necessário.
- **Caminhos como `TEXT`/UTF-8**: decisão atual; suporte a caminhos não-UTF-8
  exigiria persistência como BLOB e uma migração.
- **Sem exclusão física de chunks órfãos**: remover um snapshot zera
  `reference_count`, mas o arquivo/chunk só é removido pelo pruning
  (Fase 6).
