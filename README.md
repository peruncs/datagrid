# Peruncs Data Grid

Peruncs Data Grid is an in-memory data processing layer to speed up database
applications and relieve the database. It combines distributed caching,
high-speed in-memory searching, and complex data manipulation on the native
Java object model, persisted transaction-safe by Eclipse Store and moved
between nodes over Aeron. It is based on a single-writer approach: each node
is fully consistent locally, while the cluster model is eventual consistency.

The repository is a single Maven project producing the `peruncs-cluster`
artifact: node lifecycle, backup, Store replication, and indexes over Aeron,
documented in the cluster section below.

## Build

Requires an exact Java 26 runtime and Maven 3.9+. This artifact uses Java 26
preview APIs, so both compilation and every consumer JVM must enable preview:
`--enable-preview`. It is intentionally a pre-release build and is not ready
for Maven Central publication until the preview dependency is removed or the
release policy explicitly supports it.

```bash
mvn test
mvn package
```

Heavier suites live behind profiles (see below for knobs): `mvn verify -Psoak`
for the writer/reader soak and `mvn verify -Pcrashmatrix` for the forked crash
matrix.

The checkout is aligned with the locally installed Eclipse Store/Serializer
`5.0.0-SNAPSHOT` artifacts; use one dated snapshot repository state for a
deployment and treat the snapshot as pre-release. Do not mix daily snapshot
metadata across nodes.

## Use

```xml
<dependency>
    <groupId>peruncs</groupId>
    <artifactId>peruncs-cluster</artifactId>
</dependency>
```

## Cluster node with Aeron replication
`peruncs-cluster` runs a Data Grid node with Aeron replication, including the
Store binary transport and the embedded index policy. Add it and set:

```text
ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron
ECLIPSE_DATAGRID_REPLICATION_ROLE=writer|reader|backup-reader
ECLIPSE_DATAGRID_AERON_CLUSTER_ID=<stable-cluster-uuid>
ECLIPSE_DATAGRID_AERON_RECORDING_ID=<writer archive recording id>
ECLIPSE_DATAGRID_AERON_NODE_ID=<stable-node-uuid>
ECLIPSE_DATAGRID_AERON_STORE_GENERATION=<store-generation-uuid>
```

The remaining `ECLIPSE_DATAGRID_AERON_*` settings select the UDP live,
Archive-control, replay and Archive-replication channels, directory, archive
directory, writer checkpoint, file-sync, term, MTU, chunk, transaction,
threading, segment, low-storage, and replay-concurrency limits. Production
defaults to dedicated MediaDriver/Archive threads; development and tests use
shared threads. Override with `ECLIPSE_DATAGRID_AERON_THREADING_MODE` when the
deployment deliberately chooses another supported mode. Set
`ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH` to a durable, owner-only path. The
MediaDriver directory is recreated by Aeron on startup, so archive and
checkpoint paths must not be children of `ECLIPSE_DATAGRID_AERON_DIRECTORY`.
Production deployments must replace the loopback channel defaults with
routable node/Service addresses; the provider rejects loopback and wildcard
endpoints when production mode is enabled.

Every node setting uses the `ECLIPSE_DATAGRID_` prefix. The earlier bare names
(`IS_BACKUP_NODE`, `GC_INTERVAL_MINUTES`, ...) and the `MSCNL_*` names are
still accepted as a fallback when the prefixed name is unset; the prefixed name
wins when both are set, and a consumed legacy name is logged once as
deprecated so operators can migrate.

For a reader, set `ECLIPSE_DATAGRID_AERON_RECORDING_ID` to the writer's
recording. Reader identity/checkpoint persistence is supplied by the
node deployment; this provider does not invent an identity from the
network address.

Before a reader (or a backup node without a user upload) starts, seed it
with a matching Store directory plus its durable replication cursor — either
restore a compatible backup on the shared volume or copy the writer's Store
directory and offset file while the writer is stopped. (Why a seed is
required at all is a design invariant; see the module documentation.)
One provider instance owns one configured replication stream; use separate
provider instances/channels for multiple streams.

### Running multiple clusters on one network

Clusters may share a VPN or other routed network, but each cluster must have
its own Aeron traffic namespace. Configure distinct live-channel control
endpoints, distinct replay and watermark endpoints when those channels are
shared, and distinct stream IDs for every cluster. Every cluster must also use
a unique `ECLIPSE_DATAGRID_AERON_CLUSTER_ID` and, when explicitly configured,
the same `ECLIPSE_DATAGRID_AERON_WIRE_NONCE` on every participant. (Why namespaces can't be shared
is a design constraint; see the module documentation.)

Network policy must still restrict which nodes can publish to
each cluster's live and watermark endpoints. Archive control authentication is
separate and does not isolate live replication traffic.

The development live-channel default is a dynamic MDC loopback channel
(`control=localhost:40123|control-mode=dynamic|fc=max|term-length=16m|alias=datagrid-<cluster>`)
so multiple readers can attach. The replay default points at the same local
control endpoint with a dynamic response stream. Production deployments must
configure routable control and replay endpoints.
The default wire tuning is a 16 MiB term, 1 MiB Store chunk, 1,408-byte MTU,
and 64 MiB transaction limit; override with the full environment keys
`ECLIPSE_DATAGRID_AERON_TERM_LENGTH`,
`ECLIPSE_DATAGRID_AERON_MTU_LENGTH`,
`ECLIPSE_DATAGRID_AERON_CHUNK_SIZE`,
`ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES`, and
`ECLIPSE_DATAGRID_AERON_OFFER_TIMEOUT_NANOS`. Archive recording startup,
recorded-position, and stop waits are independently configurable with
`ECLIPSE_DATAGRID_AERON_RECORDING_START_TIMEOUT_NANOS`,
`ECLIPSE_DATAGRID_AERON_RECORDED_POSITION_TIMEOUT_NANOS`, and
`ECLIPSE_DATAGRID_AERON_RECORDING_STOP_TIMEOUT_NANOS`; reader shutdown uses
`ECLIPSE_DATAGRID_AERON_READER_STOP_TIMEOUT_NANOS`.
Replication must run on an isolated network
(VPN, firewall rules, or Kubernetes NetworkPolicies): any host that can reach
the live channel can publish well-formed frames; the nonce only rejects
accidental cross-wiring and is not authentication. Every writer and reader must
use the same cluster id, epoch, and fencing lineage; a mismatch fails closed
before Store data is applied. Writer lease freshness uses shared wall-clock
timestamps, so all writer hosts must run synchronized NTP/chrony clocks. A
heartbeat far in the future is rejected as clock skew instead of being treated
as an indefinitely fresh lease.
Archive runtime tuning is controlled by
`ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL`,
`ECLIPSE_DATAGRID_AERON_ARCHIVE_SEGMENT_FILE_LENGTH`,
`ECLIPSE_DATAGRID_AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD`, and
`ECLIPSE_DATAGRID_AERON_MAX_CONCURRENT_REPLAYS`. The provider maps the
configured threading mode to matching MediaDriver and Archive threading.
`CHUNK_SIZE + 84` must fit Aeron's publication maximum (`term-length / 8`,
capped at 16 MiB). Store bytes are sent directly inside the fixed replication
envelope; no SBE or second serialization pass is required.

Writer checkpoint persistence is enabled in the provider. Quorum-gated,
segment-boundary retention is available only when an embedded writer is started
with `ECLIPSE_DATAGRID_AERON_RETENTION_READERS` (a comma-separated list of
reader UUIDs). Name the quorum on the writer only: readers need no retention
list of their own. Configure `ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL` and
`ECLIPSE_DATAGRID_AERON_WATERMARK_STREAM_ID` on every participant. Each reader
first persists its recovery cursor and then sends an `AeronReaderWatermark`
covering reader, cluster, Store generation, epoch, recording, sequence, and
position over that dedicated stream. Readers publish unconditionally — the
stream is latest-value fire-and-forget, an unreceived value is retained for
retry until the writer subscribes (or discarded at close if none ever does),
and the writer records only quorum members, rejecting any other reader's
watermark. The writer records those
acknowledgements automatically; call `deleteThrough` with the ordinary durable
backup cursor naming the desired sequence. The configured reader quorum,
rather than the maintenance request itself, authorizes deletion. (How the
quorum gates deletion is a design decision; see the module documentation.)
An active replay defers maintenance without deleting data. External Archives and
incomplete reader quorums remain unsupported for deletion. Without a
configured reader list, retention is reported as
unsupported and history is preserved. Operators must monitor Archive capacity
and rotate or expand storage before it is exhausted.
The supported capacity procedure is: alert when
`archiveUsableSpaceBytes()` approaches the configured
`ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES`, stop acknowledged writes (the
provider will reject them below the threshold), take a matched Store+Archive
backup, stop the writer, provision or attach a larger Archive filesystem, and
restart with the same recording and checkpoint. Do not delete active recording
segments or manually advance a reader cursor; if the Archive cannot be
restored, initialize a new epoch and reseed every reader.

Aeron Archive control and replay channels support control-session authentication when
`ECLIPSE_DATAGRID_AERON_AUTH_ENABLED=true`. Configure the principal and
credentials with `ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL` and either
`ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS` or its file variant. Reader roles are
limited to discovery, position queries, and replay; writer roles additionally
receive recording and retention-maintenance actions. Production deployments
must still isolate those endpoints with private interfaces, firewall rules,
and Kubernetes NetworkPolicies/security groups. Cluster UUIDs and CRCs validate
data identity and integrity only; they are not credentials. Do not enable
ACK-driven deletion on an untrusted network.

When Archive authentication is enabled on a writer, configure a separate
reader identity with `ECLIPSE_DATAGRID_AERON_AUTH_READER_PRINCIPAL` and either
`ECLIPSE_DATAGRID_AERON_AUTH_READER_CREDENTIALS` or its file variant. Use that
reader identity on every reader node; sharing the writer identity would grant
the writer's recording permissions.

## Network boundary

Fencing tokens and CRC32C are correctness checks, not security: what each
control does and does not prove is a design decision recorded in the module
documentation. Operationally, none of them replaces the network boundary:

- Aeron Archive authentication gates control-plane commands (recording,
  retention, replay). It is defense in depth behind firewall rules and
  NetworkPolicies, which remain the primary boundary and the only per-node
  identity below full PKI.

Run replication on a VPN-contained network: the isolated network is the only
traffic boundary for replication frames and reader watermarks. Archive
control authentication is a separate protection domain and always needs its
own acknowledgement (`ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE`) when
disabled. Running without it never disables fencing or CRC32C.

Archive control credentials have no overlap mechanism: rotate those with a
coordinated restart.

A writer requires a shared `ECLIPSE_DATAGRID_BACKUP_PATH` for its fencing
lease; manual promotion and automated failover remain
deployment responsibilities, and the lease directory must not sit inside the
Aeron driver, archive, or checkpoint tree.

## Filesystem backups

Backups use the filesystem named by `ECLIPSE_DATAGRID_BACKUP_PATH` (default
`backups`). This can be a network-mounted volume, and it also holds the writer
fencing lease. Each generated backup is a single compressed archive named
`<timestamp>.zip` or `<timestamp>.manual.zip`; the archive contains `storage/`,
`manifest`, and `ready`. The complete archive is atomically moved into the
volume, so readers never select an in-progress export. Operator-provided
storage is kept as `user-uploaded-storage.zip` and is restored through its
separate API.

Archive extraction rejects traversal, symbolic-link paths, duplicate entries,
oversized content, missing storage, and incomplete generated metadata. The
backup operations below trigger and read these local-volume operations; no
backup HTTP transport or hosted backup target is configured by the node.

## Programmatic control boundary

The Boundary–Control–Entity separation itself is an architectural decision
recorded in the module documentation. What remains here is the operator
contract for driving the node through its control views:

- `storageNodeManager()` on a storage node returns a `StorageNodeControl`:
  role (`isDistributor`), liveness (`isHealthy`), readiness (`isReady`),
  storage size (`readStorageSizeBytes`), and raw replication observability
  (`replicationMetrics()` — transport, replay/live state, current/latest
  sequence, lag, readiness, health, including Archive or replay failures).
  The embedder renders these typed values as JSON or Prometheus text itself.
- `backupNodeManager()` on a backup node returns a `BackupNodeControl`:
  backup triggers (`createStorageBackup`, `isBackupRunning`) and reader
  pause/resume (`stopReadingAtLatestMessage`, `resumeReading`, `isReading`).

The views carry no `close()`, and both closes are idempotent, so a stray
borrower call stays harmless. The role is validated before anything starts,
so probing the wrong role never starts Store, Aeron, recovery, or background
threads.
The mutating operations (backups, storage checks, pausing and resuming
replication) carry no authentication of their own: the embedding application
MUST authenticate and authorize them before delegating. Only the health,
readiness, and read-only metric reads are safe to expose to an
unauthenticated probe endpoint. Map `BackupBusyException` to a conflict
response, unhealthy/not-ready to a retryable unavailable response, and any
other failure to an internal error.

## Store binary transport

The transport keeps Eclipse Serializer/Eclipse Store `Binary` bytes opaque
inside a versioned envelope. (Envelope framing and the Archive-first write
ordering are design decisions; see the module documentation.) A writer should use
`AeronStorageBinaryReplicationTarget` with an
`AeronReplicationWriteCoordinator`.

Readers use `AeronArchiveReader.New(...)` for replay, live
join, and reconnect. Persist the DataGrid cursor/checkpoint after each
completed commit. `AeronReplicationCheckpointStore` is provided for
deployments that persist the Aeron-specific identity and replay boundary.

Chunk size must remain below `min(termLength / 8, 16 MiB) - 84`. Aeron fragments each envelope
as needed for the selected MTU.

CRC32C detects accidental corruption; it does not authenticate a sender. Bind
UDP and Archive-control channels to private interfaces and restrict them with
firewall or network-policy rules; do not enable ACK-driven retention on an
untrusted network.

Run the transport and UDP/Archive integration tests with:

```text
mvn verify
```

The threaded writer/reader soak and the forked crash matrix live outside the
default gate. They are intentionally separate: the soak explores concurrent
load and recovery over time, while the crash matrix kills isolated child
processes at named durability boundaries and checks exact recovery outcomes.

### Writer/reader soak

`AeronWriterReaderSoakIT` runs one writer Store with three reader Stores. The
writer performs adds, updates, and removals while reader query threads access
the graph, Lucene, and JVector indexes. The test keeps a transaction model of
the expected title, body, vector, and title/body checksum. A reader is only
checked against entities whose own persisted cursor proves that the entity was
applied; this separates replication lag from data loss. Positive index checks
use a short refresh retry because graph materialization and searcher refresh
are not the same event.

The run is divided into setup, concurrent soak, convergence, and quiescent
verification:

1. Seed the writer with ordinary entities and three vector sentinels, copy the
   seed Store to each reader, and record the baseline writer sequence.
2. Run writer, query, audit, and chaos workers. The audit worker performs
   bounded-lag checks and mini-censuses; query workers verify ghost-title
   absence, graph bodies/checksums, Lucene hits, and non-sentinel JVector hits.
3. Run a seeded fixed rotation of single restart, dual restart, slow-reader,
   CPU/GC burst, cursor corruption, rollback cursor, and live Archive-tail
   corruption. The first chaos operation is always an abrupt restart. Every
   operation is logged as selected, effective, or skipped with a reason.
4. Stop writers, converge or explicitly park every reader, then run strict
   samples and an uncapped graph/Lucene/JVector census. A reader may be
   converged, reseed-parked, or corruption-parked; it may not disappear
   silently.

The soak fails on worker failures, lost event-log writes, phantom index hits,
bad checksums, unexpected exceptions, torn-boundary convergence, excessive
classified torn-read retries, lag-SLO violations, missing chaos coverage, or a
reader without a recorded fate. `target/soak-events.jsonl` is the authoritative
per-run schedule: each line has a run id and sequence number. The seed repeats
workload values and chaos distribution, but timing-dependent skips and reader
states can change the exact operation schedule. The fork also writes
`target/soak.jfr`; use `jfr summary` or the `SoakJfrReport` aggregation for
compact GC, monitor, allocation, and virtual-thread-pinning signals. JFR is
warn-first unless `-Dsoak.jfr.fail=true` is enabled after baseline calibration.

Run a single soak or a seed sweep:

```text
mvn verify -Psoak -Dsoak.seconds=30 -Dsoak.restarts=10
for seed in 1 2 3 4 5 6 7 8 9 10; do
  mvn failsafe:verify -Psoak -Dsoak.seconds=15 -Dsoak.seed=$seed
done
```

Soak controls are:

| Property | Default | Purpose |
| --- | ---: | --- |
| `soak.seed` | `1` | Workload values and chaos-rotation start offset |
| `soak.seconds` | `30` | Concurrent workload duration before convergence |
| `soak.writer.threads` | `3` | Writer workload threads, serialized by the one-writer test lock |
| `soak.query.threads` | `2` | Query threads per reader |
| `soak.restarts` | `10` | Effective chaos-work budget; heavy operations may count as two |
| `soak.lagSlots` | `100` | Reader lag-SLO allowance in replication slots |
| `soak.miniCensus` | `20` | Entities checked by each mid-soak mini-census |
| `soak.pollDelayMs` | `2` | Applied delay used by slow-reader chaos |
| `soak.pollStallMs` | `800` | Slow-reader burst duration; near the configured reader-stop timeout it also probes the sliding deadline |
| `soak.fsyncDelayMs` | `3` | Delay injected through the AtomicFileStore fsync hook |
| `soak.maxTornReads` | `64` | Maximum classified benign query retries; set to `0` for strict mode |
| `soak.corrupt` | `true` | Enable cursor, rollback, and Archive-tail corruption operations |
| `soak.events` | `target/soak-events.jsonl` | JSONL event-log destination |
| `soak.jfr.fail` | `false` | Turn calibrated JFR budget warnings into failures |

At the small default, `soak.pollStallMs` is primarily a backlog widener. The
sliding reader-stop deadline is probed only when it is configured near
`ECLIPSE_DATAGRID_AERON_READER_STOP_TIMEOUT_NANOS`.

`-Dsoak.corrupt=false` removes cursor, rollback, and Archive-tail corruption
from the rotation; it does not turn off restart or load chaos. A short run may
waive a chaos type only when parked readers caused the skip and that causal
reason is present in the event log. Use independent seeds rather than one very
long run: duration increases load, while seeds increase schedule coverage.

### Forked crash matrix

`-Pcrashmatrix` runs the provider crash matrix together with the external
Archive, Aeron transport, Store integration, and reader crash suites configured
in the Maven profile. The central `ProviderCrashMatrixIT` uses an isolated
temporary directory and a forked `ProviderCrashChildMain`:

1. Phase 1 starts the writer/Archive fixture, writes the requested payloads,
   and records `control/ready` and the exact `control/milestone.reached`.
2. The parent validates the phase-1 Store prefix expected at that boundary and
   forcibly kills the child.
3. Phase 2 restarts the same directory, retrying only transient active-driver
   startup failures while the Archive finishes stopping.
4. The parent parses `control/outcome` and checks the recovery policy, health,
   checkpoint identity/CRC, and exact Store records.

The oracle is deliberately two-valued. `CONTINUE` is allowed only when the
checkpoint and Archive position prove an unambiguous boundary; recovery must
be `LIVE` and append each missing transaction exactly once.
`RESEED_REQUIRED` is required when a prepare, data tail, commit offer, local
write, uncertain checkpoint, or corrupted artifact makes the boundary
ambiguous; recovery must fail closed and must not reuse the tail. Missing
milestones, harness errors, invalid outcomes, wrong Store prefixes, and an
unexpected policy are failures rather than acceptable crash variation.

Deterministic cells cover:

- publication, prepare, local-write, commit-offer, recorded-commit, abort,
  enqueue-then-Archive, and prepare-failure seams;
- checkpoint temp-write, rename, directory-sync, and committed-sequence seams;
- one-byte, chunk-minus-one, chunk-plus-one, multi-chunk, random, tiny-term,
  and 200 KiB payloads;
- randomized per-chunk budget kills between named milestones;
- checkpoint-byte-flip and Archive-tail corruption;
- reader inflight corruption and deleted cursors; and
- double- and triple-crash recovery chains.

Each cell appends `selection`, `milestone`, mutation, recovery, and `outcome`
records to `control/events.jsonl`. When a cell fails, the diagnostic collector
creates a sibling `*.evidence` directory containing the reason, control files,
child stdout/stderr, and a directory listing of the Store/checkpoint/Archive
fixture. This is the first place to look when a crash barrier or recovery
policy assertion fails.

Crash-matrix controls are:

| Property | Default | Purpose |
| --- | ---: | --- |
| `crash.matrix.seed` | `1` | Seed for randomized budget/scenario selection |
| `crash.matrix.random.iterations` | `10` in `-Pcrashmatrix` | Seeded process-kill iterations; `0` disables that test |
| `crash.matrix.random.seeds` | `1` | Number of consecutive seed values |
| `crash.matrix.budget.iterations` | `3` | Randomized chunk-budget cells |
| `crash.matrix.subscriber` | `true` | Disable for the no-subscriber backpressure variant |
| `crash.matrix.termLength` | `1048576` | Child Aeron term length in bytes |
| `crash.matrix.chunkSize` | derived | Overrides the child chunk size; default is bounded by term size |
| `crash.budget.startup` | `120000` ms | Child startup/outcome wait |
| `crash.budget.milestone` | `60000` ms | Barrier wait |
| `crash.budget.archiveStop` | `30000` ms | Active-Archive retry window |
| `crash.budget.cell` | `600000` ms | Overall cell wait ceiling |

Run the full profile, or a focused provider cell when investigating one
boundary:

```text
mvn verify -Pcrashmatrix
mvn -q -Pcrashmatrix -DskipTests \
  -Dit.test=ProviderCrashMatrixIT#recordedCommitAcrossTinyTermBoundaryRequiresReseed \
  org.apache.maven.plugins:maven-failsafe-plugin:3.6.0:integration-test \
  org.apache.maven.plugins:maven-failsafe-plugin:3.6.0:verify
```

The crash matrix intentionally does not claim to model arbitrary UDP loss or
duplication, disk-full ENOSPC, SIGSTOP, or network authentication. Those need
separate fault-injection mechanisms; these tests focus on durable ordering,
checkpoint truth, process death, and fail-closed recovery.

## Design

The architectural decisions — Archive-first replication, fixed roles,
durable cursors and checkpoints, seeding and reseed, quorum-gated retention,
in-graph indexes, and the boundary–control–entity separation — are recorded
in the module documentation:
[`src/main/java/module-info.java`](src/main/java/module-info.java).
