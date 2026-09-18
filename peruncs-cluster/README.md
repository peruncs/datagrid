# DataGrid cluster node with Aeron replication

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
The provider owns its embedded MediaDriver/Archive lifecycle and closes those
resources from the DataGrid storage-manager shutdown callback.

Every node setting uses the `ECLIPSE_DATAGRID_` prefix. The earlier bare names
(`IS_BACKUP_NODE`, `GC_INTERVAL_MINUTES`, ...) and the `MSCNL_*` names are
still accepted as a fallback when the prefixed name is unset; the prefixed name
wins when both are set, and a consumed legacy name is logged once as
deprecated so operators can migrate.

For a reader, set `ECLIPSE_DATAGRID_AERON_RECORDING_ID` to the writer's
recording. Reader identity/checkpoint persistence is supplied by the
node deployment; this provider does not invent an identity from the
network address.

A reader owns no authoritative Store image: replication ships deltas that
reference object ids the writer created, so a reader started against an empty
directory cannot reproduce pre-existing state and fails with
`ReseedRequiredException` instead of inventing a root. Before a reader (or a
backup node without a user upload) starts, seed it with a matching Store
directory plus its durable replication cursor — either restore a compatible
backup on the shared volume or copy the writer's Store directory and offset
file while the writer is stopped. The writer is the only role that may
manufacture a fresh root; the backup node may do so only for a user-uploaded
Store it then publishes as the starter backup.
One provider instance owns one configured replication stream; use separate
provider instances/channels for multiple streams.

### Running multiple clusters on one network

Clusters may share a VPN or other routed network, but each cluster must have
its own Aeron traffic namespace. Configure distinct live-channel control
endpoints, distinct replay and watermark endpoints when those channels are
shared, and distinct stream IDs for every cluster. Do not rely on the cluster
UUID to separate Aeron traffic: it is carried inside each frame and checked
only after a subscriber receives that frame.

Every cluster must also use a unique `ECLIPSE_DATAGRID_AERON_CLUSTER_ID`.
Readers reject frames, cursors, checkpoints, and watermarks whose cluster ID,
Store generation, or epoch does not match their configuration. If two clusters
accidentally subscribe to the same live channel and stream, the wrong
cluster's frame is rejected and the subscriber fails closed; separate channels
and stream IDs prevent that cross-talk and avoid turning a configuration error
into a cluster outage.

The cluster ID and CRC32C checks are identity and corruption checks, not
authentication. Network policy must still restrict which nodes can publish to
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
Replication integrity comes from header and payload CRC32C checks on every
frame; there is no per-frame key. Replication must run on an isolated network
(VPN, firewall rules, or Kubernetes NetworkPolicies): any host that can reach
the live channel can publish well-formed frames. Every writer and reader must
use the same cluster id, epoch, and fencing lineage; a mismatch fails closed
before Store data is applied.
Archive runtime tuning is controlled by
`ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL`,
`ECLIPSE_DATAGRID_AERON_ARCHIVE_SEGMENT_FILE_LENGTH`,
`ECLIPSE_DATAGRID_AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD`, and
`ECLIPSE_DATAGRID_AERON_MAX_CONCURRENT_REPLAYS`. The provider maps the
configured threading mode to matching MediaDriver and Archive threading.
`CHUNK_SIZE + 76` must fit Aeron's publication maximum (`term-length / 8`,
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
rather than the maintenance request itself, authorizes deletion.
The provider computes the least advanced reader position, pauses coordinator
admission, stops the recording, purges only complete segments, and extends the
same recording at its exact stop position before admitting another write. An
active replay defers maintenance without deleting data. External Archives and
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

Each control proves something different; none of them replaces the network
boundary:

- The writer fencing lease and fencing token are correctness, not security:
  they keep exactly one writer's history linear.
- CRC32C detects accidental corruption, never forgery. Any host that can reach
  the live channel can publish well-formed frames, and a staging cluster on
  the same network must use a different cluster id or its frames cross-talk.
- Aeron Archive authentication gates control-plane commands (recording,
  retention, replay). It is defense in depth behind firewall rules and
  NetworkPolicies, which remain the primary boundary and the only per-node
  identity below full PKI.

Run replication on a VPN-contained network: the isolated network is the only
traffic boundary for replication frames and reader watermarks. Archive
control authentication is a separate protection domain and always needs its
own acknowledgement (`ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE`) when
disabled. Running without it never disables fencing or CRC32C — those are
correctness checks, not network security.

Archive control credentials have no overlap mechanism: rotate those with a
coordinated restart.

Fixed-writer/no-consensus operation is intentional. The strict one-writer
invariant is enforced by a renewable writer lease in the shared backup volume:
a writer acquires `writer-lease-<cluster>-<generation>.lease` in
`ECLIPSE_DATAGRID_BACKUP_PATH`, renews its heartbeat, and publishes the lease's
monotonically increasing fencing token with every envelope, checkpoint, and
cursor. A different writer for the same cluster/generation fails acquisition while
the first heartbeat is fresh, and readers reject a lower token so a deposed
writer cannot interleave history. The same writer (the same stable node id)
restarting after a clean stop or a crash mints the next token immediately;
a live clone with a copied node id is deposed instead, when its next renewal
fails the holder check. A writer therefore requires a shared
`ECLIPSE_DATAGRID_BACKUP_PATH`; manual promotion and automated failover remain
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

The node ships no HTTP server and no HTTP types. The embedding application
owns the entire boundary — HTTP/OpenAPI routes, MCP tools, a web UI,
Prometheus rendering, authentication, and authorization — and drives the node
through the control views borrowed from `ClusterFoundation`:

- `storageNodeManager()` on a storage node returns a `StorageNodeControl`:
  role (`isDistributor`), liveness (`isHealthy`), readiness (`isReady`),
  storage size (`readStorageSizeBytes`), and raw replication observability
  (`replicationMetrics()` — transport, replay/live state, current/latest
  sequence, lag, readiness, health, including Archive or replay failures).
  The embedder renders these typed values as JSON or Prometheus text itself.
- `backupNodeManager()` on a backup node returns a `BackupNodeControl`:
  backup triggers (`createStorageBackup`, `isBackupRunning`) and reader
  pause/resume (`stopReadingAtLatestMessage`, `resumeReading`, `isReading`).

The views carry no `close()`: the foundation owns both managers and closes
them exactly once, and both closes are idempotent, so a stray borrower call
stays harmless. The role is validated before anything starts, so probing the
wrong role never starts Store, Aeron, recovery, or background threads. Roles
are fixed at startup — a writer serves the distributor, a reader or
backup-reader serves the reader, and there is deliberately no
reader-to-distributor promotion: a role change is a restart with a new role,
never a runtime transition.
The mutating operations (backups, storage checks, pausing and resuming
replication) carry no authentication of their own: the embedding application
MUST authenticate and authorize them before delegating. Only the health,
readiness, and read-only metric reads are safe to expose to an
unauthenticated probe endpoint. Map `BackupBusyException` to a conflict
response, unhealthy/not-ready to a retryable unavailable response, and any
other failure to an internal error.

## Store binary transport

The transport keeps Eclipse Serializer/Eclipse Store `Binary` bytes opaque and
adds a 76-byte version-4 envelope for cluster identity, fencing token,
sequence, chunking,
CRC32C, and commit/abort markers. A writer should use
`AeronStorageBinaryReplicationTarget` with an
`AeronReplicationWriteCoordinator` so the ordering is:

```text
Archive prepare chunks -> local Store enqueue -> Archive commit
```

Readers use `AeronArchiveReader.New(...)` for replay, live
join, and reconnect. Persist the DataGrid cursor/checkpoint after each
completed commit. `AeronReplicationCheckpointStore` is provided for
deployments that persist the Aeron-specific identity and replay boundary.

The envelope is deliberately not an SBE-generated second payload format:
Eclipse Serializer's `Binary` bytes remain the authoritative Store payload,
while the fixed header supplies only framing and validation. Chunk size must
remain below `min(termLength / 8, 16 MiB) - 76`. Aeron fragments each envelope
as needed for the selected MTU.

CRC32C detects accidental corruption; it does not authenticate a sender. Bind
UDP and Archive-control channels to private interfaces and restrict them with
firewall or network-policy rules; do not enable ACK-driven retention on an
untrusted network.

Run the transport and UDP/Archive integration tests with:

```text
mvn -pl peruncs-cluster -am verify
```

The threaded writer/reader soak and the forked crash matrix live outside the
default gate. The soak asserts every served query against the transaction
model, restarts readers (including overlapping dual restarts), injects
slow-reader/fsync/CPU chaos plus cursor and Archive-tail corruption, and
requires every reader to reach a planned outcome (converged or fail-closed
parked). Coverage scales with independent seeds, not duration — prefer sweeps:

```text
mvn -pl peruncs-cluster -am verify -Psoak -Dsoak.seconds=30 -Dsoak.restarts=10
for seed in 1 2 3 4 5 6 7 8 9 10; do
  mvn -pl peruncs-cluster failsafe:verify -Psoak -Dsoak.seconds=15 -Dsoak.seed=$seed
done
mvn -pl peruncs-cluster -am verify -Pcrashmatrix
```

Soak knobs: `-Dsoak.seed=`, `-Dsoak.seconds=`, `-Dsoak.writer.threads=`,
`-Dsoak.query.threads=`, `-Dsoak.restarts=`, `-Dsoak.lagSlots=` (bounded-lag
SLO), `-Dsoak.miniCensus=`, `-Dsoak.pollDelayMs=`/`-Dsoak.pollStallMs=`
(slow-reader injection), `-Dsoak.fsyncDelayMs=`, `-Dsoak.corrupt=` (disable all
corruption chaos). The sub-timeout stall only probes the sliding reader stop
deadline when `-Dsoak.pollStallMs=` is set near the configured
`ECLIPSE_DATAGRID_AERON_READER_STOP_TIMEOUT_NANOS`; at the small default it is
purely a backlog widener. Key transitions are appended to
`target/soak-events.jsonl` for replay. The soak fork also dumps `target/soak.jfr`; analyze it offline
with `SoakJfrReportTest` (warn-first; gate with `-Dsoak.jfr.fail=true` only
after calibrating budgets from nightly baselines).

## Design

The architectural decisions — Archive-first replication, fixed roles,
durable cursors and checkpoints, quorum-gated retention, in-graph indexes —
are recorded in the [module documentation](src/main/java/module-info.java).
