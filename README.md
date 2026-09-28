# PerunCS Cluster

Embedded Eclipse Store clustering: one writer and N reader nodes, replicated
over Aeron. Each node is fully consistent locally; the cluster is eventually
consistent. Architecture and the reasons behind every rule below are recorded
in [`module-info.java`](src/main/java/module-info.java) (the ADR). This file
covers only building, configuring, and operating a node.

## Build

Requires Java 27 exactly and Maven 3.9+. Every JVM running a node needs:

```text
--enable-preview --add-modules jdk.incubator.vector --add-exports java.base/jdk.internal.misc=ALL-UNNAMED
```

The first two flags enable Java 27's preview Vector API, which Eclipse
Store's JVector index uses for SIMD acceleration ([configuration guide](https://docs.eclipsestore.io/manual/gigamap/indexing/jvector/configuration.html)).
Maven applies these flags to its test and integration-test JVMs.

On the module path, target the export at `org.eclipse.serializer.base`
instead of `ALL-UNNAMED`.

```bash
mvn test                      # default gate
mvn verify -Pintegration      # embedded Store/Aeron integration
mvn verify -Pcrashmatrix      # forked crash matrix
mvn verify -Psoak             # writer/reader soak
```

The build tracks Eclipse Store/Serializer `5.0.0-SNAPSHOT`. Deploy every node
from the same dated snapshot. This is a pre-release artifact, not published
to Maven Central.

## Use

```xml
<dependency>
    <groupId>peruncs</groupId>
    <artifactId>peruncs-cluster</artifactId>
</dependency>
```

```java
try (var node = ClusterNode.open(NodeOptions.of(MyRoot::new))) {
    ClusterStorageManager<MyRoot> storage = node.storageManager();
    // Writer: mutate and persist inside a write section.
    int size = storage.graphBoundary().write(() -> {
        MyRoot root = storage.root().get();
        root.add("value");
        storage.store(root);
        return root.size();
    });
    // Reader: traverse only inside a read section; copy out what you need.
    int seen = storage.graphBoundary().read(() -> storage.root().get().size());
    NodeStatus status = node.status();
}
```

- If a write callback may have changed the graph before failing, call
  `graphBoundary().invalidate(cause)` before leaving the section.
- `ClusterNode.close()` and `storageManager().shutdown()` are equivalent and
  idempotent.
- Exceptions from `peruncs.cluster.errors`:
  - `ReaderWriteRejectedException`: a write on a reader;
  - `WriterFencedException`: this writer lost its lease;
  - `StorageLimitReachedException`: the Store is full;
  - `GraphInvalidatedException`: the graph is latched invalid; restart or
    reseed;
  - `ReseedRequiredException`: seed the node again.

## Configuration

Settings come from the process environment by default. An application can
supply its own `NodeSettingsSource` through
`NodeOptions.withNodeSettingsSource(...)` to read any other configuration
system. All keys use the `ECLIPSE_DATAGRID_` prefix. Legacy unprefixed and
`MSCNL_*` names are still read as a fallback and logged as deprecated.

### Required on every replicated node

```text
ECLIPSE_DATAGRID_PROD_MODE=true
ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron
ECLIPSE_DATAGRID_REPLICATION_ROLE=writer|reader|backup-reader
ECLIPSE_DATAGRID_AERON_CLUSTER_ID=<stable cluster UUID>
ECLIPSE_DATAGRID_AERON_NODE_ID=<stable node UUID>
ECLIPSE_DATAGRID_AERON_STORE_GENERATION=<Store generation UUID>
ECLIPSE_DATAGRID_AERON_WIRE_NONCE=<same non-zero value on all nodes>
ECLIPSE_DATAGRID_STORAGE_LIMIT_GB=<limit>                              # writer and reader
ECLIPSE_DATAGRID_STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES=<minutes>      # writer and reader
```

Readers discover the writer's recording through the `alias=` of the live
channel. Set `ECLIPSE_DATAGRID_AERON_RECORDING_ID` only to override
discovery. Without `ECLIPSE_DATAGRID_PROD_MODE=true`, a node starts as a
standalone development Store, and replication settings are rejected.

### Storage paths

Put every live path on node-local storage: a local disk, or a block volume
attached to this host only. Never put them on NFS, SMB, or another shared
network file system. The backup volume is the only path that may be shared.

| Setting | Default | Holds | Media |
| --- | --- | --- | --- |
| `ECLIPSE_DATAGRID_STORAGE_PATH` | `storage` | Store (`<path>/storage`) and reader cursor (`<path>/offset`) | local |
| `ECLIPSE_DATAGRID_AERON_DIRECTORY` | `/tmp/eclipse-datagrid-aeron` | Aeron MediaDriver files (recreated at start) | local; tmpfs such as `/dev/shm` recommended; `/tmp` rejected in production |
| `ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY` | `<aeron dir>.archive` | Aeron Archive recording | local, durable |
| `ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH` | `<aeron dir>.writer.checkpoint` | writer checkpoint | local, durable, owner-only |
| `ECLIPSE_DATAGRID_BACKUP_PATH` | `backups` | backup archives; writer fencing lease | may be a shared network volume |

- Production paths must be absolute.
- The driver, archive, and checkpoint paths must not overlap.
- Keep the Store, cursor, Archive, and checkpoint of one node together:
  losing any one of them means a reseed.
- Writer only, until the lease moves to local storage (see the ADR):
  `ECLIPSE_DATAGRID_BACKUP_PATH` must be a pre-provisioned NFSv4 export
  shared by every potential writer, and
  `ECLIPSE_DATAGRID_AERON_SHARED_LEASE_FILESYSTEM=true` must be set. Keep
  writer hosts on synchronized clocks (NTP/chrony), because lease freshness
  compares wall-clock timestamps.

### Network

PerunCS replication deliberately has no node authentication or transport
encryption, and it does not require either. Replace loopback channel defaults
with addresses routable between peers; production mode rejects loopback and
wildcard endpoints.

- The development live channel is
  `control=localhost:40123|control-mode=dynamic|fc=max|term-length=16m|alias=datagrid-<cluster>`.
- **Several clusters on one network:** give each cluster its own
  `ECLIPSE_DATAGRID_AERON_CLUSTER_ID`, its own control, replay, and watermark
  endpoints, and its own stream ids.

| Setting group | Keys (`ECLIPSE_DATAGRID_AERON_…`) | Defaults |
| --- | --- | --- |
| Channels | `LIVE_CHANNEL`, `REPLAY_CHANNEL`, `CONTROL_CHANNEL`, `CONTROL_RESPONSE_CHANNEL`, `ARCHIVE_REPLICATION_CHANNEL`, `WATERMARK_CHANNEL`, `STREAM_ID`, `WATERMARK_STREAM_ID` | loopback; stream `1001` |
| Wire | `TERM_LENGTH`, `MTU_LENGTH`, `CHUNK_SIZE`, `MAX_TRANSACTION_BYTES` | 16 MiB, 1,408 B, 128 KiB, 64 MiB. Constraint: `CHUNK_SIZE + 84 ≤ min(TERM_LENGTH / 8, 16 MiB)` |
| Timeouts | `OFFER_TIMEOUT_NANOS`, `RECORDING_START_TIMEOUT_NANOS`, `RECORDED_POSITION_TIMEOUT_NANOS`, `RECORDING_STOP_TIMEOUT_NANOS`, `READER_STOP_TIMEOUT_NANOS`, `LIVE_WITHHOLD_TIMEOUT_NANOS`, `RECONNECT_TIMEOUT_NANOS`, `ARCHIVE_CONTROL_TIMEOUT_NANOS`, `WATERMARK_CLOSE_TIMEOUT_NANOS`, `LEASE_LOCK_TIMEOUT_MILLIS` | live-withhold and reconnect 30 s; Archive control, watermark close, and lease lock 5 s |
| Archive | `FILE_SYNC_LEVEL`, `ARCHIVE_SEGMENT_FILE_LENGTH`, `ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD`, `MAX_CONCURRENT_REPLAYS`, `MIN_ARCHIVE_FREE_BYTES` | sync level `1` (`0` rejected in production) |
| Threading | `THREADING_MODE` | dedicated in production, shared otherwise |
| Epoch | `EPOCH` | `1` |

### Node settings

| Key (`ECLIPSE_DATAGRID_…`) | Default | Purpose |
| --- | --- | --- |
| `GRAPH_DRAIN_TIMEOUT_MILLIS` | 5,000 | Close waits this long for active graph sections |
| `BACKUP_CLOSE_TIMEOUT_MILLIS` | 60,000 | Close waits this long for a running backup |
| `KEPT_BACKUPS_COUNT` | 3 | Scheduled backups retained |
| `BACKUP_INTERVAL_MINUTES` | 120 | Backup-reader backup cadence |
| `AERON_RETENTION_INTERVAL_MINUTES` | 1 | Writer Archive retention cadence; lagging/incomplete reader quorums preserve history |
| `GC_INTERVAL_MINUTES` | 60 (30 on backup-reader) | Full Store GC and cache check cadence |
| `DATA_MERGER_TIMEOUT`, `DATA_MERGER_LIMIT`, `DATA_MERGER_APPLY_TIMEOUT` | built-in | Reader apply tuning |
| `INDEX_VALIDATION_MAX_OBJECTS` | 65,536 | Maximum index-relevant objects and collection entries inspected per validation scan |
| `AERON_LEASE_STALENESS_MILLIS` | built-in | Writer lease staleness bound |

## Operations

### Seeding a reader

Before a reader (or a backup-reader without an upload) starts for the first
time, give it a matching Store and cursor, by either:

- restoring a compatible backup from the backup volume (done automatically
  at startup when one exists), or
- with the writer stopped, copying the writer's `<storage path>/storage` and
  `<storage path>/offset` to the reader.

A node reporting `RESEED_REQUIRED` needs the same procedure.

### Status and control

`ClusterNode` exposes:

- `status()`: an immutable `NodeStatus` with the role, `ready`, `healthy`,
  storage bytes, `ReplicationStatus` (state, sequences, lag), and backup-reader
  status for backup and post-publication maintenance failures. Operator
  actions:
  - `FAILED`: stop serving and inspect;
  - `DEGRADED`: may serve, but fix the dependency;
  - `RESEED_REQUIRED`: stop and reseed.
- `startStorageChecks()`: periodic Store checks (writer and reader).
- `createBackup(BackupSlot)`: backup-reader only; returns a
  `CompletableFuture<BackupInfo>`. A concurrent request completes the future
  exceptionally with `BackupBusyException`.

These are in-process calls without authentication. Cluster replication
deliberately has no node authentication or transport encryption. If you expose
these calls through a separate network service, endpoint policy belongs to the
application. A typical mapping:

- `BackupBusyException` → 409;
- not ready or unhealthy → 503;
- anything else → 500.

### Backups

- Backups are written to `ECLIPSE_DATAGRID_BACKUP_PATH` as
  `<timestamp>.zip` (scheduled) or `<timestamp>.manual.zip`.
- To bootstrap a cluster from an existing Store, place it as
  `user-uploaded-storage.zip` in the backup volume and start the
  backup-reader. It installs the upload, publishes a starter backup, and then
  deletes the upload.

### Archive retention and capacity

- **Enabling retention:** set `ECLIPSE_DATAGRID_AERON_RETENTION_READERS=<reader UUIDs, comma-separated>`
  on the writer only, and set the watermark channel and stream on every node.
- **Behaviour:** the writer checks retention on its maintenance schedule and
  deletes complete Archive segments only after every listed reader has
  confirmed the boundary. Without the list or a complete quorum, history is
  kept.
- **Capacity procedure:**
  1. Alert when `archiveUsableSpaceBytes` approaches
     `ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES` (below it, writes are
     rejected).
  2. Take a Store and Archive backup.
  3. Stop the writer.
  4. Enlarge or replace the Archive's local storage.
  5. Restart with the same recording and checkpoint.
- Never delete recording segments or edit a cursor by hand. If the Archive is
  lost, start a new epoch and reseed every reader.

## Testing

Test design, invariants, and fault models are documented in the test classes'
Javadoc. This section lists commands and knobs only.

### Writer/reader soak (`AeronWriterReaderSoakIT`)

```text
mvn verify -Psoak -Dsoak.seconds=30 -Dsoak.restarts=10
for seed in 1 2 3 4 5 6 7 8 9 10; do
  mvn failsafe:verify -Psoak -Dsoak.seconds=15 -Dsoak.seed=$seed
done
```

Prefer many seeds over one long run. Outputs:
- `target/soak-events.jsonl`: the per-run chaos schedule;
- `target/soak.jfr`: summarize with `jfr summary` or `SoakJfrReport`.

| Property | Default | Purpose |
| --- | ---: | --- |
| `soak.seed` | `1` | Workload values and chaos rotation offset |
| `soak.seconds` | `30` | Workload duration before convergence |
| `soak.readers` | `3` | Reader count (tested with 5–6) |
| `soak.writer.threads` | `3` | Writer workload threads |
| `soak.query.threads` | `2` | Query threads per reader |
| `soak.payloadBytes` | `0` | Padded transaction body size; `0` = compact |
| `soak.restarts` | `10` | Chaos-work budget; heavy operations count as two |
| `soak.lagSlots` | `100` | Reader lag allowance in replication slots |
| `soak.miniCensus` | `20` | Entities per mid-run census |
| `soak.pollDelayMs` | `2` | Slow-reader applied delay |
| `soak.pollStallMs` | `800` | Slow-reader burst duration |
| `soak.fsyncDelayMs` | `3` | Injected fsync delay |
| `soak.maxTornReads` | `64` | Benign query retries allowed; `0` = strict |
| `soak.corrupt` | `true` | Cursor, rollback, and Archive-tail corruption |
| `soak.gc` | `true` | Forced-GC bursts |
| `soak.writerRestart` | `true` | Writer restarts |
| `soak.reseed` | `true` | Reseed and rejoin |
| `soak.retention` | `true` | Watermark retention and purge |
| `soak.events` | `target/soak-events.jsonl` | Event log path |
| `soak.jfr.fail` | `false` | Fail on calibrated JFR budget warnings |

### Crash matrix (`ProviderCrashMatrixIT`)

```text
mvn verify -Pcrashmatrix
mvn -q -Pcrashmatrix -DskipTests \
  -Dit.test=ProviderCrashMatrixIT#recordedCommitAcrossTinyTermBoundaryRequiresReseed \
  org.apache.maven.plugins:maven-failsafe-plugin:3.6.0:integration-test \
  org.apache.maven.plugins:maven-failsafe-plugin:3.6.0:verify
```

A failed cell leaves a sibling `*.evidence` directory with the reason,
control files, child output, and a fixture listing. Look there first.

| Property | Default | Purpose |
| --- | ---: | --- |
| `crash.matrix.seed` | `1` | Randomized scenario seed |
| `crash.matrix.random.iterations` | `10` | Seeded kill iterations; `0` disables |
| `crash.matrix.random.seeds` | `1` | Consecutive seeds |
| `crash.matrix.budget.iterations` | `3` | Randomized chunk-budget cells |
| `crash.matrix.subscriber` | `true` | `false` = no-subscriber back-pressure variant |
| `crash.matrix.termLength` | `1048576` | Child term length |
| `crash.matrix.chunkSize` | derived | Child chunk size override |
| `crash.budget.startup` | `120000` ms | Child startup/outcome wait |
| `crash.budget.milestone` | `60000` ms | Barrier wait |
| `crash.budget.archiveStop` | `30000` ms | Active-Archive retry window |
| `crash.budget.cell` | `600000` ms | Per-cell ceiling |
