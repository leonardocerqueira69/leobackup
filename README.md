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

### Fase 3 — Engine de Processamento (concluída)

- Chunking com FastCDC (variante 2020) sobre fluxo, com parâmetros
  validados contra os limites da crate.
- BLAKE3 hexadecimal minúsculo como identidade de cada chunk.
- Deduplicação consultando o índice **e** confirmando a presença do arquivo
  em `data/`; chunks já armazenados não são recomprimidos nem recriptografados.
- Compressão Zstandard e criptografia AES-256-GCM com nonce aleatório por
  operação, em um envelope versionado (`LBK1`).
- Escrita atômica de chunks: temporário no mesmo diretório, `sync_all` e
  renomeação; chunk existente não é sobrescrito.
- Commit transacional do arquivo e de `file_chunks` apenas depois que todos
  os chunks estão no disco.
- Detecção de origem alterada durante a leitura (tamanho/mtime) e falhas por
  arquivo sem interromper o restante; snapshot vira `failed` se algo falhar.

As fases 4 a 8 ainda não foram implementadas.

## Estrutura

```text
crates/
├── leobackup-core/      # tipos base, erro, configuração, modelos e primitivas
├── leobackup-storage/   # layout, SQLite (migrações + CRUD) e ChunkStore
├── leobackup-engine/    # pipeline de backup/restauração (Fase 3/5)
├── leobackup-daemon/    # watcher e servidor local (Fase 4)
└── leobackup-cli/       # interface de linha de comando (init/config)
```

`leobackup-core` concentra as primitivas do pipeline, independentes de I/O de
backup: `chunking` (FastCDC), `hashing` (BLAKE3), `compression` (Zstandard),
`crypto` (AES-256-GCM) e `chunk_file` (envelope persistido).

## Repositório de backup

```text
<raiz>/
├── data/                 # chunks: data/<2 hex>/<hash> (Fase 3)
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

Validações implementadas: limites do FastCDC (`min_size` 64 B–1 MiB,
`avg_size` 256 B–4 MiB, `max_size` 1 KiB–16 MiB), `min_size <= avg_size <=
max_size`, `workers > 0`, `compression.level` entre 1 e 22, `debounce_ms <=
60000`, `schema_version <= 1` e ausência de caminhos vazios em `source_dirs`.
O motor também exige coerência entre criptografia e chave: habilitada sem
chave (ou vice-versa) é erro de construção.

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

## Pipeline de backup (Fase 3)

`BackupEngine` executa, por arquivo:

1. valida que a origem é arquivo regular e captura tamanho, mtime e
   permissões (Unix);
2. lê em fluxo e divide com FastCDC;
3. calcula BLAKE3 de cada chunk;
4. reutiliza o chunk se ele existe no índice **e** em `data/`; caso o
   metadado exista sem o arquivo, regrava o conteúdo;
5. comprime com Zstandard, criptografa se habilitado e grava de forma atômica;
6. confirma que a origem não mudou e faz o commit transacional do arquivo e
   de `file_chunks`.

`backup_files` cria um snapshot, continua após erros por arquivo e o finaliza
como `completed` ou `failed`, devolvendo um `BackupReport` com os arquivos
gravados e as falhas.

### Envelope de chunk (`LBK1`)

```text
0    4   magic "LBK1"
4    1   flags: bit0 = criptografado, bit1 = comprimido
5   12   nonce AES-GCM (zeros quando não criptografado)
17   N   payload (ciphertext + tag, ou bytes comprimidos)
```

`compressed_size` em `chunks` corresponde ao tamanho total do envelope em
disco. Mudanças de formato exigem nova versão de magic.

## Uso

```bash
cargo run -p leobackup-cli -- init --repo ./repo
cargo run -p leobackup-cli -- config --repo ./repo
```

`init` nunca sobrescreve uma configuração existente sem `--force` e
cria/migra `metadata/index.db`.

O backup pela CLI será exposto na Fase 7. Por enquanto o pipeline é usado
programaticamente: `BackupEngine::new(store, chunks, settings, key)` seguido
de `backup_files(&caminhos)`.

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
  `aes-gcm` permanecem declaradas em `[workspace.dependencies]` e são
  vinculadas às crates donas quando cada fase as utiliza: `rusqlite` na
  Fase 2; `fastcdc`, `blake3`, `zstd` e `aes-gcm` na Fase 3 (em `core`).
  `notify` será vinculada na Fase 4. Isso segue o item "Gerenciamento de
  Dependências" do backlog sem compilar código ainda não usado.
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

### Fase 3

- **Primitivas em `core`**: chunking, hashing, compressão, criptografia e
  envelope ficam em `leobackup-core` para teste isolado e reuso pela
  restauração (Fase 5), sem depender de SQLite.
- **Envelope sempre comprimido**: a flag de compressão existe para permitir
  payloads não comprimidos no futuro, mas `encode_chunk` sempre comprime.
- **Deduplicação exige disco + índice**: um metadado órfão não é considerado
  suficiente; o chunk é regravado antes do commit.
- **Commit após persistência**: os chunks são gravados fora da transação
  SQLite; a transação só publica `files`/`file_chunks` quando todos existem.
  Um crash entre a gravação e o commit deixa chunks sem referência, tratados
  pela rotina de recuperação da Fase 6.
- **Detecção de mudança por tamanho/mtime**: heurística barata para a fase;
  não cobre alterações com mesmo tamanho e mtime preservado.
- **Falha por arquivo não aborta o lote**: o snapshot é marcado `failed` se
  qualquer arquivo falhar, mesmo que os demais tenham sido gravados, para
  evitar declarar um snapshot completo incorretamente.
- **Chave em memória**: `EncryptionKey` redige o `Debug` e zera os bytes no
  `Drop` (best-effort). Derivação/armazenamento/rotação de chaves continuam
  pendentes e serão decididos antes de expor a CLI de chaves.
- **Snapshots de arquivo único**: `backup_file` exige snapshot `in-progress`,
  mantendo a mesma garantia usada por `backup_files`.
