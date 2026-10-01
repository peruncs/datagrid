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
instead of `ALL-UNNAMED`, and open the Store's vector package to the cluster module so the
reader can retire stale JVector graphs:
`--add-opens org.eclipes.store.gigamap.jvector/org.eclipse.store.gigamap.jvector=peruncs.cluster`
(the upstream module name really is spelled `eclipes`). A node that cannot do so fails at startup.

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
try (var node = ClusterStorage.<MyRoot>Foundation().setRootSupplier(MyRoot::new).startNode()) {
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
  - `StorageLimitReachedException`: the Store is full;
  - `GraphInvalidatedException`: the graph is latched invalid; restart or
    reseed;
- `ReseedRequiredException`: seed the node again.

## Migrating from Eclipse Store

| Eclipse Store | PerunCS Cluster |
| --- | --- |
| `EmbeddedStorage.start(root, path)` | `ClusterStorage.start(root)` using `NodeConfig.fromEnvironment()` |
| `EmbeddedStorage.Foundation()…start()` | `ClusterStorage.Foundation()…start()` |
| Tune an `EmbeddedStorageFoundation` | Pass it to `setEmbeddedStorageFoundation`; the node still owns the live path from `NodeConfig.storage().root()` |
| Obtain a `StorageManager` | Use the returned `ClusterStorageManager<T>` |

Set a custom configuration with `setNodeConfig(NodeConfig.fromMap(values))` or
`setNodeConfig(NodeConfig.builder()...)`. Use `startNode()` when the application
also needs status and backup operations. The node handle and its storage
manager share one lifecycle; closing either closes the node.

The manager follows Eclipse Store's common API with these cluster constraints:

- Call `graphBoundary().read(...)` for graph traversal and
  `graphBoundary().write(...)` for application mutations.
- `setRoot` requires a `Lazy` root. `viewRoots()` omits the internal
  `peruncs.replication` root.
- `importData` and `importFiles` are unsupported. Replicated roles reject raw
  persistence-target writes and metadata changes; reader writes raise
  `ReaderWriteRejectedException`.
- `shutdown()` closes the complete node. See `ClusterStorageManager` Javadoc
  for the method-level role behavior.

## Using PerunCS inside a container

Create the `ClusterNode` from the container's application-start hook and close
it from the matching disposal hook. Do not rely on a JVM shutdown hook. Give
the application `node.storageManager()` wherever it expects an Eclipse Store
`StorageManager`, and use `NodeConfig.fromMap(...)` to adapt framework settings.

Map node status to container health as follows:

- readiness follows `status.ready()`;
- liveness fails for replication states `FAILED` and `RESEED_REQUIRED`;
- report `DEGRADED` and a failed last backup without stopping a live node;
- include `status.role()` in health details. Standalone nodes report
  `NOT_CONFIGURED` replication.

## Configuration

Settings come from the process environment by default. Applications can pass
`NodeConfig.fromMap(...)` to `ClusterStorage.Foundation().setNodeConfig(...)`,
or create a `NodeConfig` with its builder. This keeps framework-specific
configuration outside the cluster library. All environment keys use the
`PERUNCS_` prefix; old unprefixed and `MSCNL_*` aliases are not read.

### Required on every replicated node

```text
PERUNCS_PROD_MODE=true
PERUNCS_REPLICATION_TRANSPORT=aeron
PERUNCS_REPLICATION_ROLE=writer|reader|backup-reader
PERUNCS_AERON_CLUSTER_ID=<stable cluster UUID>
PERUNCS_AERON_NODE_ID=<stable node UUID>
PERUNCS_AERON_STORE_GENERATION=<Store generation UUID>
PERUNCS_STORAGE_LIMIT_GB=<limit>                              # writer and reader
PERUNCS_STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES=<minutes>      # writer and reader
```

Readers discover the writer's recording through the `alias=` of the live
channel. Set `PERUNCS_AERON_RECORDING_ID` only to override discovery. Without
`PERUNCS_PROD_MODE=true`, a node starts as a standalone Store unless
`PERUNCS_REPLICATION_TRANSPORT=aeron` is explicitly selected. Production mode
enables stricter channel, filesystem, and Archive checks. To run a development
cluster, set the Aeron transport and role; a standalone Store leaves transport
at `none` (the default) and has no role. Production standalone status reports
role `STANDALONE` and replication state `NOT_CONFIGURED`.

### Storage paths

Put every live path on node-local storage: a local disk, or a block volume
attached to this host only. Never put them on NFS, SMB, or another shared
network file system. The backup volume is the only path that may be shared.

| Setting | Default | Holds | Media |
| --- | --- | --- | --- |
| `PERUNCS_STORAGE_PATH` | `storage` | Store and writer lock (`<path>/storage`) | local |
| `PERUNCS_AERON_DIRECTORY` | `<PERUNCS_STORAGE_PATH>/aeron` | Aeron MediaDriver files (recreated at start) | local; tmpfs such as `/dev/shm` recommended; `/tmp` rejected in production |
| `PERUNCS_AERON_ARCHIVE_DIRECTORY` | `<aeron dir>.archive` | Aeron Archive recording | local, durable |
| `PERUNCS_BACKUP_PATH` | `backups` | finished backup archives | may be a shared network volume |
| `PERUNCS_BACKUP_WORKSPACE_PATH` | `<PERUNCS_STORAGE_PATH>/backup-workspace` | uncompressed export and compression of a backup | local; needs room for the Store plus its archive |

- Production paths must be absolute.
- The driver, Archive, and Store paths must not overlap.
- Keep the Store and Archive of one node together; losing either requires a
  new writer epoch and reader reseed.
- A writer locks `<PERUNCS_STORAGE_PATH>/storage/writer.lock` before
  opening the Store. This prevents a second process using the same Store path;
  deployment must ensure only one host is configured as writer because locks
  on separate Store copies cannot coordinate.

### Network

Project requirement: PerunCS replication has no node authentication or
transport encryption. Replace loopback channel defaults with addresses
routable between peers; production mode rejects loopback and wildcard
endpoints.

- The development live channel is
  `control=localhost:40123|control-mode=dynamic|fc=max|term-length=16m|alias=peruncs-<cluster>`.
- **Several clusters on one network:** give each cluster its own
  `PERUNCS_AERON_CLUSTER_ID`, its own control, replay, and watermark
  endpoints, and its own stream ids.

### Environment key reference

This table is generated by `NodeConfig.settingsMarkdown()`. `unset` means no raw
value is supplied; paths and channels with derived defaults are described above.

| Setting | Default |
| --- | --- |
| `PERUNCS_PROD_MODE` | `false` |
| `PERUNCS_REPLICATION_TRANSPORT` | `none` |
| `PERUNCS_REPLICATION_ROLE` | `unset` |
| `PERUNCS_STORAGE_PATH` | `storage` |
| `PERUNCS_BACKUP_PATH` | `backups` |
| `PERUNCS_BACKUP_WORKSPACE_PATH` | `unset` |
| `PERUNCS_BACKUP_MAX_ENTRIES` | `1048576` |
| `PERUNCS_BACKUP_PUBLICATION_LOCK_TIMEOUT_MILLIS` | `30000` |
| `PERUNCS_KEPT_BACKUPS_COUNT` | `3` |
| `PERUNCS_STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES` | `unset` |
| `PERUNCS_GC_INTERVAL_MINUTES` | `unset` |
| `PERUNCS_BACKUP_INTERVAL_MINUTES` | `120` |
| `PERUNCS_STORAGE_LIMIT_GB` | `unset` |
| `PERUNCS_DATA_MERGER_TIMEOUT` | `10000` |
| `PERUNCS_DATA_MERGER_APPLY_TIMEOUT` | `60000` |
| `PERUNCS_DATA_MERGER_LIMIT` | `67108864` |
| `PERUNCS_POOL_MAX_RETAINED_BYTES` | `33554432` |
| `PERUNCS_GRAPH_DRAIN_TIMEOUT_MILLIS` | `5000` |
| `PERUNCS_BACKUP_CLOSE_TIMEOUT_MILLIS` | `60000` |
| `PERUNCS_BACKUP_STOP_TIMEOUT_MILLIS` | `60000` |
| `PERUNCS_BACKUP_STOP_POLL_INTERVAL_MILLIS` | `100` |
| `PERUNCS_BACKUP_RETENTION_RETRY_ATTEMPTS` | `3` |
| `PERUNCS_BACKUP_RETENTION_RETRY_DELAY_MILLIS` | `100` |
| `PERUNCS_BACKUP_PUBLICATION_RETRY_ATTEMPTS` | `3` |
| `PERUNCS_MAINTENANCE_FAILURE_THRESHOLD` | `3` |
| `PERUNCS_WRITER_RECOVERY_ATTEMPTS` | `3` |
| `PERUNCS_MAINTENANCE_CLOSE_TIMEOUT_MILLIS` | `5000` |
| `PERUNCS_STORAGE_CHECK_CLOSE_TIMEOUT_MILLIS` | `5000` |
| `PERUNCS_INDEX_REFRESH_TIMEOUT_MILLIS` | `600000` |
| `PERUNCS_INDEX_VALIDATION_MAX_OBJECTS` | `65536` |
| `PERUNCS_AERON_ARCHIVE_DIRECTORY` | `unset` |
| `PERUNCS_AERON_DIRECTORY` | `unset` |
| `PERUNCS_AERON_RETENTION_INTERVAL_MINUTES` | `1` |
| `PERUNCS_AERON_TERM_LENGTH` | `16777216` |
| `PERUNCS_AERON_MTU_LENGTH` | `1408` |
| `PERUNCS_AERON_CHUNK_SIZE` | `131072` |
| `PERUNCS_AERON_MAX_TRANSACTION_BYTES` | `67108864` |
| `PERUNCS_AERON_OFFER_TIMEOUT_NANOS` | `30000000000` |
| `PERUNCS_AERON_RECORDING_START_TIMEOUT_NANOS` | `30000000000` |
| `PERUNCS_AERON_RECORDED_POSITION_TIMEOUT_NANOS` | `30000000000` |
| `PERUNCS_AERON_ABORT_RECORDED_POSITION_TIMEOUT_NANOS` | `5000000000` |
| `PERUNCS_AERON_RETRY_IDLE_MAX_PARK_NANOS` | `1000000` |
| `PERUNCS_AERON_RETRY_JITTER_CAP_NANOS` | `1000000` |
| `PERUNCS_AERON_RETRY_ARCHIVE_PROBE_DELAY_NANOS` | `10000000` |
| `PERUNCS_AERON_RETENTION_OPERATION_TIMEOUT_MILLIS` | `60000` |
| `PERUNCS_AERON_RECORDING_STOP_TIMEOUT_NANOS` | `30000000000` |
| `PERUNCS_AERON_READER_STOP_TIMEOUT_NANOS` | `30000000000` |
| `PERUNCS_AERON_RECONNECT_TIMEOUT_NANOS` | `30000000000` |
| `PERUNCS_AERON_NODE_ID` | `unset` |
| `PERUNCS_AERON_STORE_GENERATION` | `unset` |
| `PERUNCS_AERON_CLUSTER_ID` | `unset` |
| `PERUNCS_AERON_EPOCH` | `1` |
| `PERUNCS_AERON_STREAM_ID` | `1001` |
| `PERUNCS_AERON_RECORDING_ID` | `-1` |
| `PERUNCS_AERON_WATERMARK_STREAM_ID` | `unset` |
| `PERUNCS_AERON_FILE_SYNC_LEVEL` | `1` |
| `PERUNCS_AERON_MIN_ARCHIVE_FREE_BYTES` | `0` |
| `PERUNCS_AERON_ARCHIVE_SEGMENT_FILE_LENGTH` | `134217728` |
| `PERUNCS_AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD` | `134217728` |
| `PERUNCS_AERON_MAX_CONCURRENT_REPLAYS` | `20` |
| `PERUNCS_AERON_DRIVER_TIMEOUT_MILLIS` | `10000` |
| `PERUNCS_AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS` | `5000000000` |
| `PERUNCS_AERON_WATERMARK_CLOSE_TIMEOUT_NANOS` | `5000000000` |
| `PERUNCS_AERON_LIVE_CHANNEL` | `unset` |
| `PERUNCS_AERON_REPLAY_CHANNEL` | `unset` |
| `PERUNCS_AERON_ARCHIVE_REPLICATION_CHANNEL` | `unset` |
| `PERUNCS_AERON_WATERMARK_CHANNEL` | `unset` |
| `PERUNCS_AERON_CONTROL_CHANNEL` | `unset` |
| `PERUNCS_AERON_CONTROL_RESPONSE_CHANNEL` | `unset` |
| `PERUNCS_AERON_RETENTION_READERS` | `` |
| `PERUNCS_AERON_THREADING_MODE` | `unset` |

## Operations

### Seeding a reader

Before a reader (or a backup-reader without an upload) starts for the first
time, give it a matching Store, by either:

- restoring a compatible backup from the backup volume (done automatically
  at startup when one exists), or
- with the writer stopped, copying the writer's Store directory to the reader.

A node reporting `RESEED_REQUIRED` needs the same procedure.

### Status and control

`ClusterNode` exposes:

- `status()`: an immutable `NodeStatus` with the role, `ready`, `healthy`,
  storage bytes, `ReplicationStatus` (state, sequences, lag), and backup-reader
  status for backup and post-publication maintenance failures. Operator
  actions:
  - standalone nodes report `ReplicationState.NOT_CONFIGURED`;
  - `FAILED`: stop serving and inspect; a node whose Archive stayed unreachable past
    the reconnect budget also reports `FAILED` and resumes from the same Store mark
    after a restart;
  - `DEGRADED`: may serve, but fix the dependency;
  - `RESEED_REQUIRED`: stop and reseed (the recording no longer covers the Store mark).
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

- Backups are written to `PERUNCS_BACKUP_PATH` as
  `<timestamp>.zip` (scheduled) or `<timestamp>.manual.zip`.
- To bootstrap a cluster from an existing Store, place it as
  `user-uploaded-storage.zip` in the backup volume and start the
  backup-reader. It installs the upload, publishes a starter backup, and then
  deletes the upload.

### Archive retention and capacity

- **Enabling retention:** set `PERUNCS_AERON_RETENTION_READERS=<reader UUIDs, comma-separated>`
  on the writer only, and set the watermark channel and stream on every node.
- **Behaviour:** the writer checks retention on its maintenance schedule and
  deletes complete Archive segments only after every listed reader has
  confirmed the boundary. Without the list or a complete quorum, history is
  kept. Each reader reports the position its restart would resume at (the
  prepare start of the transaction in its Store mark), and the writer keeps the
  Archive segment that holds the oldest such position.
- **Capacity procedure:**
  1. Alert when `archiveUsableSpaceBytes` approaches
     `PERUNCS_AERON_MIN_ARCHIVE_FREE_BYTES` (below it, writes are
     rejected).
  2. Take a Store and Archive backup.
  3. Stop the writer.
  4. Enlarge or replace the Archive's local storage.
  5. Restart with the same Store mark and Archive recording.
- Never delete recording segments or edit the Store mark by hand. If the Archive is
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
| `soak.maxTornReads` | `64` | Benign query retries allowed; `0` = strict |
| `soak.corrupt` | `true` | Store-mark rollback and Archive-tail corruption |
| `soak.gc` | `true` | Forced-GC bursts |
| `soak.writerRestart` | `true` | Writer restarts |
| `soak.reseed` | `true` | Reseed and rejoin |
| `soak.retention` | `true` | Watermark retention and purge |
| `soak.events` | `target/soak-events.jsonl` | Event log path |
| `soak.jfr.fail` | `false` | Fail on calibrated JFR budget warnings |

### Store mark crash matrix (`ReplicationMarkCrashMatrixIT`)

```text
mvn verify -Pcrashmatrix
mvn verify -Pcrashmatrix \
  -Dit.test=ReplicationMarkCrashMatrixIT#namedMarkAndFourMegabytesOfStoreDataRecoverTogetherAfterProcessKill
```

Each of the 200 trials seeds the production `ReplicationMark`, kills a child JVM
at a random point inside its 4 MiB Store commit, then verifies the mark and
payload recover together. The delay uses a fixed seed for repeatability.
