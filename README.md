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

### Fase 4 — Daemon e Monitoramento (concluída)

- Watcher baseado em `notify` para criação, modificação, remoção e renomeação,
  com filtragem de eventos do próprio repositório.
- Fila de eventos em memória com capacidade limitada, coalescência por
  caminho, debounce por caminho e sinalização de `rescan` quando a fila
  transborda (nenhuma alteração é descartada em silêncio).
- Worker único e serializado: somente ele acessa `BackupEngine`/SQLite; o
  watcher e o servidor apenas enfileiram trabalho.
- Varredura inicial opcional (`monitoring.initial_scan`) e varredura de
  diretórios criados/movidos, sem carregar a lista completa de arquivos em
  memória.
- Servidor local autenticado por token, escutando apenas em `127.0.0.1`, com
  comandos `status`, `backup_now` e `shutdown`.
- Encerramento gracioso por Ctrl+C ou comando remoto, com finalização segura
  do lote corrente.

### Fase 5 — Restauração (implementada, ainda não integrada ao daemon/CLI)

- `RestoreEngine` com `RestoreOptions` (`relative_path`, `overwrite`,
  `allow_incomplete_snapshot`) e `RestoreReport`.
- `MetadataStore::find_file_by_path` para busca por caminho exato dentro de
  um snapshot; `list_snapshots` e `list_files` para navegação.
- Pipeline de reconstrução: ler `file_chunks` na ordem, validar posição,
  ler o chunk de `data/`, descriptografar/descomprimir com `decode_chunk`,
  recalcular `hashing::hash_chunk` e comparar com o hash registrado; só
  concatenar se o hash confere e o tamanho corresponde ao esperado.
- Validação de `chunk_path`: rejeita `..`, caminhos absolutos, nomes reservados
  no Windows (`NUL`, `COM1` etc.) e fluxos alternativos NTFS (`nome:fluxo`).
  A raiz de destino é canonicalizada (`canonical_root`) e cada componente
  intermediário é verificado: `symlink` que escape da raiz é rejeitado.
- Escrita atômica: `NamedTempFile` no mesmo sistema de arquivos do destino,
  `sync_all`, aplicação de permissões e `modification_time` no temporário,
  e só então `persist_noclobber` (padrão) ou `persist` (quando `overwrite`).
- Rejeita snapshots `in-progress`/`failed` por padrão; só restaura com
  `allow_incomplete_snapshot=true`.
- Erros explícitos por tipo: `SnapshotNotFound`, `FileNotFound`,
  `SnapshotNotCompleted`, `DestinationExists`, `InvalidRestorePath`,
  `ChunkMetadataMissing`, `ChunkRead`, `ChunkDecode`, `ChunkHashMismatch`,
  `ChunkOrder`, `SizeMismatch`. Nenhum erro é silenciado com `let _`.
- `RestoreEngine` aceita chave opcional, independente de
  `encryption.enabled`: chunks antigos podem ser criptografados mesmo com a
  configuração atual desabilitada.
- `README.md` atualizado e `clippy` limpo.

A integração com daemon/CLI será feita na Fase 7.

As fases 6 a 8 ainda não foram implementadas.

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
    ├── settings.json     # configuração
    └── daemon.token      # token local criado na primeira execução (Fase 4)
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
| `monitoring.queue_capacity` | 10000 |
| `monitoring.initial_scan` | `true` |
| `compression.level` | 3 |
| `encryption.enabled` | `true` |
| `daemon.enabled` | `true` |
| `daemon.port` | 47821 |

Validações implementadas: limites do FastCDC (`min_size` 64 B–1 MiB,
`avg_size` 256 B–4 MiB, `max_size` 1 KiB–16 MiB), `min_size <= avg_size <=
max_size`, `workers > 0`, `compression.level` entre 1 e 22, `debounce_ms <=
60000`, `queue_capacity` entre 1 e 1.000.000, `daemon.port > 0`,
`schema_version <= 1` e ausência de caminhos vazios em `source_dirs`.
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

## Daemon e comunicação local (Fase 4)

### Protocolo escolhido

O contrato entre CLI e daemon é **TCP em `127.0.0.1` com JSON delimitado por
linha e token de 256 bits**. Cada conexão transporta exatamente uma
requisição e uma resposta:

```json
{"token":"<64 caracteres hex>","command":"status"}
{"ok":true,"status":{"uptime_secs":12,"source_dirs":1,"queued_events":0,...}}
```

Comandos disponíveis:

| Comando | Efeito |
|---|---|
| `status` | Lê contadores em memória; não bloqueia durante um backup |
| `backup_now` | Solicita uma varredura completa e devolve o resumo do lote |
| `shutdown` | Solicita encerramento gracioso |

Respostas de erro têm `{"ok":false,"error":"..."}`. Limites: requisição de até
8 KiB, resposta de até 64 KiB, 16 conexões simultâneas e timeouts de I/O.

A escolha foi feita antes de construir o cliente:

- **TCP em loopback + JSON**: portátil (Windows/Linux/macOS), sem dependência
  nativa, depurável com ferramentas comuns e suficiente para comandos
  pequenos. O bind é fixo em `127.0.0.1`, nunca em todas as interfaces.
- **IPC por socket de domínio/named pipe**: mais restrito por ACL do sistema,
  porém exige código específico por plataforma e permissões diferentes em cada
  uma; fica como evolução se o loopback não bastar.
- **gRPC/REST local**: traria dependências e um servidor HTTP completo sem
  ganho para três comandos; o custo de manutenção não se justifica nesta fase.

O token é gerado com o RNG do sistema na primeira execução, gravado em
`config/daemon.token` com `create_new` (nunca sobrescrito) e modo `0600` em
Unix. No Windows as permissões são herdadas do diretório do repositório. A
comparação é feita em tempo constante e o token nunca é impresso pelo daemon,
pela CLI ou incluído em mensagens de erro.

### Funcionamento

- `Daemon::start` valida a configuração, canonicaliza as origens, abre o
  SQLite, reserva a porta, inicia o worker (que configura o watcher e confirma
  a inicialização) e só então inicia o servidor.
- Origens dentro do repositório são rejeitadas, para evitar varreduras que não
  fariam backup de nada. Origens duplicadas são deduplicadas.
- O watcher usa `notify` de forma recursiva. Eventos de acesso puro são
  ignorados; renomeações geram remoção + inclusão; eventos com
  `need_rescan` disparam varredura completa.
- A fila coalesce eventos do mesmo caminho, aplica `debounce_ms` e tem
  capacidade máxima (`queue_capacity`). Ao transbordar, marca `rescan_required`
  e o worker faz uma varredura completa: o evento é descartado, mas a
  alteração não é perdida em silêncio.
- Cada lote de eventos (até 256 caminhos) ou varredura completa cria **um
  snapshot**: os arquivos são gravados com o pipeline da Fase 3, o snapshot é
  `completed` se todos terminaram bem e `failed` se qualquer arquivo falhou.
- Remoções são contabilizadas (`deleted_events`), mas ainda não geram
  tombstones no esquema; a semântica de exclusão será definida com a
  restauração e a retenção.
- `BackupNow` e a varredura inicial usam a mesma rotina de varredura do
  watcher. Diretórios criados/movidos são varridos no momento do evento.
- Encerramento por Ctrl+C, comando `shutdown` ou queda do canal do worker:
  o flag é observado entre arquivos, o lote corrente é finalizado (snapshot
  `failed` se interrompido), a fila é fechada e o servidor drena conexões.

### Limitações desta fase

- Snapshot por lote, não um manifesto completo da árvore. A restauração
  (Fase 5) precisará combinar snapshots ou o modelo deverá evoluir para
  registrar a árvore inteira.
- `concurrency.workers` continua apenas informativo: há um worker para
  preservar a consistência da conexão `rusqlite` e a ordem dos commits.
- Sem VSS (item previsto para leitura segura) e sem retenção/scrub, que
  pertencem às fases seguintes.
- Sem TLS: o transporte é loopback e autenticado por token; escutar em outra
  interface exigiria uma decisão de segurança própria.
- `daemon.enabled=false` inicia o daemon apenas para monitoramento/backup
  inicial, sem servidor local.

## Uso

```bash
cargo run -p leobackup-cli -- init --repo ./repo
cargo run -p leobackup-cli -- config --repo ./repo
```

`init` nunca sobrescreve uma configuração existente sem `--force` e
cria/migra `metadata/index.db`.

Para iniciar o daemon:

```bash
cargo run -p leobackup-daemon -- --repo ./repo --key-file ./chave.hex
# porta alternativa (ou 0 para efêmera):
cargo run -p leobackup-daemon -- --repo ./repo --key-file ./chave.hex --port 47822
```

- `--key-file` aceita 64 caracteres hexadecimais ou 32 bytes brutos. É
  obrigatório quando `encryption.enabled=true` (padrão). A geração e o
  armazenamento seguro de chaves serão expostos na Fase 7; até lá, forneça um
  arquivo gerado por uma fonte criptograficamente segura ou desabilite a
  criptografia apenas em ambiente de desenvolvimento.
- O daemon imprime o endereço e o **caminho** do token, nunca o token.
- Ctrl+C ou o comando `shutdown` encerram de forma graciosa.

O cliente programático fica em `leobackup_daemon::Client`:

```rust
let client = handle.client()?;
let status = client.status()?;
let summary = client.backup_now()?;
```

O backup pela CLI será exposto na Fase 7. Por enquanto o pipeline também pode
ser usado diretamente: `BackupEngine::new(store, chunks, settings, key)`
seguido de `backup_files(&caminhos)`.

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
  `notify` foi vinculada na Fase 4. Isso segue o item "Gerenciamento de
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

### Fase 4

- **Protocolo**: TCP em loopback + JSON por linha + token, comparado com IPC
  nativo e gRPC/REST na seção acima. Cada conexão trata um comando, o que
  simplifica o encerramento e evita estado de sessão.
- **Token persistido, nunca sobrescrito**: um arquivo inválido exige remoção
  manual; o daemon não gera um novo token silenciosamente e não invalida
  clientes existentes.
- **Worker único**: o `RecommendedWatcher` do Windows não é `Send`, então ele
  é criado dentro da thread do worker. A mesma thread é dona exclusiva do
  `BackupEngine` e da conexão SQLite. `concurrency.workers` fica advisory até
  existir um pool de conexões/coordenação de chunks.
- **Fila com overflow explícito**: em vez de crescer sem limite, a fila marca
  `rescan_required`; o worker responde com varredura completa, que é mais
  cara, mas recupera a consistência sem perder alterações.
- **Debounce por caminho**: o prazo reinicia a cada evento do mesmo caminho,
  agregando gravações típicas de editores e compiladores.
- **Snapshot por lote**: mantém a garantia da Fase 3 (nenhum snapshot
  `completed` com falhas) e evita transações longas. A consequência é que
  `files` descreve apenas o que o lote tocou; a Fase 5 deve definir como
  restaurar uma árvore completa.
- **Remoções apenas contabilizadas**: o esquema ainda não tem tombstones.
  Excluir arquivos do backup exige uma decisão de retenção/restauração que não
  foi tomada nesta fase.
- **Servidor independente do monitoramento**: `daemon.enabled=false` permite
  executar o worker sem IPC; útil para diagnóstico e para ambientes em que a
  porta não está disponível.
- **`accept` não bloqueante**: o servidor verifica o flag de encerramento a
  cada 50 ms. Não é polling do sistema de arquivos — o watcher continua
  orientado a eventos nativos.
- **Chave via arquivo**: a CLI do daemon lê a chave de `--key-file`; não há
  argumento de chave inline (evita exposição no histórico do shell). A
  gestão completa de chaves (geração, rotação, recuperação) fica para a
  Fase 7.

### Fase 5

- **API por arquivo (não árvore completa nesta fase)**: `restore_file` recebe
  `file_id`, a raiz de destino e `RestoreOptions`. A restauração de uma
  árvore completa fica para a Fase 7, com integração ao daemon/CLI.
- **Chave opcional e independente de `encryption.enabled`**: aceita chave mesmo
  quando `encryption.enabled = false`, pois chunks antigos podem ter sido
  gravados com criptografia; se o chunk não for criptografado, a chave é
  ignorada; se for criptografado e a chave estiver ausente, `decode_chunk`
  retorna erro explícito (`ChunkDecode`).
- **Sanitização defensiva**: `sanitize_relative` rejeita componentes
  `ParentDir`, `RootDir`, `Prefix` e caminhos vazios. No Windows, rejeita
  nomes reservados (`NUL`, `COM1`...`COM9`, `LPT1`...`LPT9`) e fluxos
  alternativos (`nome:fluxo`).
- **Canonicalização com contenção**: `resolve_target` canonicaliza o
  diretório pai e exige que cada componente intermediário esteja contido em
  `canonical_root`; symlinks que escapem são rejeitados antes da criação do
  arquivo.
- **Publicação atômica**: `NamedTempFile` no mesmo sistema de arquivos,
  `sync_all`, aplicação de permissões/`mtime`, e só então `persist` ou
  `persist_noclobber`. Nenhum arquivo parcialmente escrito é publicado.
- **Validação por hash**: `hashing::hash_chunk` é recalculado sobre cada chunk
  restaurado; se o hash não confere, o pipeline é interrompido
  (`ChunkHashMismatch`) sem escrever o arquivo final. Nenhum dado não
  autenticado é aceito.
- **Ordem de chunks verificada**: `link.position` deve ser contíguo a partir
  de zero; se não for, retorna `ChunkOrder` com `expected`/`actual`.
- **Não sobrescreve silenciosamente**: `overwrite=false` por padrão; quando
  `true`, `persist` substitui; quando `false` e o destino existe,
  `persist_noclobber` retorna `DestinationExists` sem modificar o destino.
- **Snapshots incompletos recusados por padrão**: só restaura
  `in-progress`/`failed` com `allow_incomplete_snapshot=true`. Isso evita
  restaurar dados parciais sem consentimento explícito.
- **Nenhum `unwrap()` ou `expect()` em caminhos externos**: todos os I/O,
  SQLite, chunking e criptografia usam `?`. Os testes incluem casos de erro
  relevantes (chunk ausente, corrupto, chave errada, snapshot incompleto,
  traversal de caminho, symlink escape, conflito de destino).

### Fase 6 — Resiliência, Manutenção e Limpeza (implementada, ainda não integrada ao daemon/CLI)

- **Crash recovery** (`clean_orphan_chunks`): identifica chunks físicos no
  disco (`data/`) que não possuem referência em `chunks` no banco e os
  remove. A rotina é segura para execução repetida.
- **Pruning** (`prune_chunks`): identifica chunks com `reference_count = 0`
  no banco, remove o registro (`delete_chunk`) e o arquivo físico. Só
  executa após confirmar que `reference_count` é zero (evita exclusão
  prematura).
- **Scrubbing** (`scrub_chunks`): lê chunks armazenados (amostra ou todos),
  descriptografa (quando aplicável), recalcula `hashing::hash_chunk` e
  compara com o hash registrado (`hash_id`). Se houver falha, registra
  o chunk em `corrupted_chunks` (`mark_chunk_corrupted`). Nenhum dado não
  autenticado é aceito.
- **Migração V2** (`corrupted_chunks`): adiciona a tabela `corrupted_chunks`
  e o índice `idx_corrupted_chunks_chunk`, com `ON DELETE CASCADE` para
  manter consistência quando um chunk é removido pelo pruning.
- **Recompute antes de limpar**: `run_maintenance` executa
  `recompute_reference_counts()` antes do pruning, garantindo que a
  contagem esteja atualizada após qualquer alteração no banco.
- Testes adicionados (`crash_recovery_removes_orphan_chunks`,
  `pruning_removes_unreferenced_chunks`, `scrub_detects_corruption`).

A integração com daemon/CLI será feita na Fase 7.

As fases 7 e 8 ainda não foram implementadas.
