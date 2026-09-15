# Aeron transport design

Status: implemented. Aeron is the only replication transport; there is no
transport selection beyond enabling it or running unreplicated.

Scope: fixed-topology clustering. Store replication is 1-writer/N-reader.
Cache invalidation is an N-writer/N-reader broadcast. Consensus, leader
election, and writer failover are intentionally out of scope: roles are fixed
at configuration and a reader can never promote itself to writer.

## Module map

| Directory | Artifact          | Java module                     | Contents                                                                    |
|-----------|-------------------|---------------------------------|-----------------------------------------------------------------------------|
| `cache`   | `peruncs-cache`   | `peruncs.datagrid.cache.clustered` | Hibernate clustered-cache region factory, timestamp invalidation over Aeron |
| `cluster` | `peruncs-cluster` | `peruncs.datagrid.cluster`         | Node lifecycle, backup, replication, Store transport, and indexes over Aeron |

The `cluster` module keeps the `cluster.nodelibrary.*` packages organized by
concern: `node` (foundation,
managers, configuration), `store` (Store adaptation), `backup` (backends,
manager, proxy client), `replication` (transport, cursors, health, retention),
`http` (route table and request controller), and `aeron` (the transport
implementation), plus `exceptions`.

## Cache invalidation

`ClusteredCacheRegionFactory` extends the Store Hibernate region factory. At
session-factory preparation it builds one serializer, one
`AeronClusteredCacheMessageComProvider`, one receiver, and one listener
configuration; the receiver starts before local cache events are redirected.

- The sender is synchronous: a listener callback offers one Aeron frame and
  waits for acceptance, retrying back pressure within the configured offer
  timeout. Failure fails the local cache write; nothing is silently dropped.
- Each frame carries a 16-byte sender identity and a per-sender sequence. A
  receiver ignores its own frames and fails closed on malformed frames,
  unappliable messages, or sequence gaps, because a volatile broadcast cannot
  repair a missed invalidation.
- The default channel `aeron:ipc` is single-host. Multi-host deployments must
  configure a UDP channel with `control-mode=dynamic`. Channels carry no
  authentication and must sit on an isolated network.
- Settings live in the Hibernate cache properties under the `aeron.*`
  clustered-cache keys (channel, stream id, node id, driver directory,
  embedded driver, offer/driver timeouts, max payload).

## Store replication

The transport keeps Eclipse Serializer/Eclipse Store `Binary` bytes opaque
behind a 68-byte versioned envelope (cluster identity, sequence, chunking,
CRC32C, commit/abort markers). Writers order Archive prepare chunks before
the local Store enqueue and the Archive commit (`AeronReplicationWriteCoordinator` with
`AeronStorageBinaryTargetDistributing`). Readers replay from the Archive, join
the live stream, and reconnect from a durable cursor.

- Cursors (`ReplicationCursor`) and checkpoints (`AeronReplicationCheckpointStore`) persist the recording id/position
  boundary; a restarted or late reader resumes without data loss.
- Authenticated retention: readers advertise durable boundaries as
  HMAC-signed watermarks; the writer deletes Archive history only through the
  complete configured reader quorum. Without the secret or quorum, history is
  preserved.
- Embedded writers reject new transactions below a configured Archive
  free-space threshold, reported through replication health.
- Backups carry a forced manifest/ready boundary plus the cursor; only
  complete, marked backups are selectable for restore.

## Node roles and lifecycle

`ECLIPSE_DATAGRID_REPLICATION_TRANSPORT` selects `aeron` or `none`
(unreplicated). `ECLIPSE_DATAGRID_REPLICATION_ROLE` selects `writer`,
`reader`, or `backup-reader`. `ClusterFoundation` assembles storage,
transport, backup, and maintenance services in dependency order and releases
them in reverse. Storage roots, backup targets, retention counts, storage
limits, and merger tuning come from `ECLIPSE_DATAGRID_*` environment
variables (see `NodelibraryPropertiesProvider`).

## Failure behavior

- Cache receiver gap or malformed frame: the timestamps region refuses all
  operations rather than serving stale query results.
- Aeron Archive or replay failure: surfaced through replication health and
  the metrics endpoint; affected readers stop instead of diverging.
- Incomplete reader quorum or active replay: retention maintenance defers
  without deleting data.

## Observability

`GET /eclipse-datagrid/replication-metrics` emits Prometheus gauges for
transport, current/latest sequence, lag, lifecycle state, readiness, and
health, including Archive and replay failures.

## Tests

Unit tests cover framing, envelope, checkpoints, coordinator, backup, and
lifecycle. Integration tests run real MediaDriver/Archive instances over
dynamic-MDC UDP (`*IT`). Forked process-crash suites (`-Pcrashmatrix`) cover
provider restart, driver failure, and reader crashes.
