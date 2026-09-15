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

For a reader, set `ECLIPSE_DATAGRID_AERON_RECORDING_ID` to the writer's
recording. Reader identity/checkpoint persistence is supplied by the
nodelibrary deployment; this provider does not invent an identity from the
network address.
One provider instance owns one configured replication stream; use separate
provider instances/channels for multiple streams.
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
Archive runtime tuning is controlled by
`ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL`,
`ECLIPSE_DATAGRID_AERON_ARCHIVE_SEGMENT_FILE_LENGTH`,
`ECLIPSE_DATAGRID_AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD`, and
`ECLIPSE_DATAGRID_AERON_MAX_CONCURRENT_REPLAYS`. The provider maps the
configured threading mode to matching MediaDriver and Archive threading.
`CHUNK_SIZE + 64` must fit Aeron's publication maximum (`term-length / 8`,
capped at 16 MiB). Store bytes are sent directly inside the fixed replication
envelope; no SBE or second serialization pass is required.

Writer checkpoint persistence is enabled in the provider. Authenticated,
segment-boundary retention is available only when an embedded writer is started
with `ECLIPSE_DATAGRID_AERON_RETENTION_SECRET` (base64, at least 16 bytes) and
`ECLIPSE_DATAGRID_AERON_RETENTION_READERS` (a comma-separated list of reader
UUIDs). The secret may instead be supplied through
`ECLIPSE_DATAGRID_AERON_RETENTION_SECRET_FILE`, an owner-only regular file
containing the base64 key. Configure the same secret plus
`ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL` and
`ECLIPSE_DATAGRID_AERON_WATERMARK_STREAM_ID` on every participant. Each reader
first persists its recovery cursor and then sends an HMAC-SHA256
`AeronAuthenticatedWatermark` covering reader, cluster, Store generation,
epoch, recording, sequence, and position over that dedicated stream. The writer
records those acknowledgements automatically; call `deleteThrough` with the
ordinary durable backup cursor naming the desired sequence. The authenticated
reader quorum, rather than the maintenance request itself, authorizes deletion.
The provider computes the least advanced reader position, pauses coordinator
admission, stops the recording, purges only complete segments, and extends the
same recording at its exact stop position before admitting another write. An
active replay defers maintenance without deleting data. External Archives and
incomplete reader quorums remain unsupported for deletion. Without the secret
or reader list, retention is reported as
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

Aeron Archive control and replay channels have no application authentication in
this provider. Production deployments must isolate those endpoints with private
interfaces, firewall rules, and Kubernetes NetworkPolicies/security groups.
Cluster UUIDs and CRCs validate data identity and integrity only; they are not
credentials. Do not enable ACK-driven deletion on an untrusted network.

Fixed-writer/no-consensus operation is intentional. Writer fencing and manual
promotion remain deployment responsibilities.

The node exposes Aeron through the normal monitoring endpoints:
/eclipse-datagrid/health`, `/eclipse-datagrid/health/ready`, and the
Prometheus-compatible `/eclipse-datagrid/replication-metrics`. The latter
reports `transport="aeron"`, replay/live state, current/latest sequence, lag,
readiness, and health, including Archive or replay failures.

## Store binary transport

The transport keeps Eclipse Serializer/Eclipse Store `Binary` bytes opaque and
adds a 68-byte versioned envelope for cluster identity, sequence, chunking,
CRC32C, and commit/abort markers. A writer should use
`AeronStorageBinaryTargetDistributing` with an
`AeronReplicationWriteCoordinator` so the ordering is:

```text
Archive prepare chunks -> local Store enqueue -> Archive commit
```

Readers use `StorageBinaryDataClientAeronArchive.New(...)` for replay, live
join, and reconnect. Persist the DataGrid cursor/checkpoint after each
completed commit. `AeronReplicationCheckpointStore` is provided for
deployments that persist the Aeron-specific identity and replay boundary.

The envelope is deliberately not an SBE-generated second payload format:
Eclipse Serializer's `Binary` bytes remain the authoritative Store payload,
while the fixed header supplies only framing and validation. Chunk size must
remain below `min(termLength / 8, 16 MiB) - 64`; Aeron fragments each envelope
as needed for the selected MTU.

CRC32C detects corruption but does not authenticate a sender. Bind UDP and
Archive-control channels to private interfaces and restrict them with firewall
or network-policy rules; do not enable ACK-driven retention on an untrusted
network.

Run the transport and UDP/Archive integration tests with:

```text
mvn -pl cluster -am verify
```

## Decisions

- [ADR 0001](docs/adr/0001-archive-first-replication.md): Archive-first replication
- [ADR 0002](docs/adr/0002-fixed-roles-without-consensus.md): fixed roles
- [ADR 0003](docs/adr/0003-durable-cursors-and-checkpoints.md): durable cursors
- [ADR 0004](docs/adr/0004-quorum-gated-retention.md): quorum-gated retention
- [ADR 0005](docs/adr/0005-in-graph-index-policy.md): in-graph indexes
