# DataGrid cluster Aeron provider

`cluster-nodelibrary-aeron` is an optional implementation of the neutral
`cluster-nodelibrary` replication SPI. Install it together with
`storage-distributed-aeron` and set:

```text
ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron
ECLIPSE_DATAGRID_REPLICATION_ROLE=writer|reader|backup-reader
ECLIPSE_DATAGRID_AERON_CLUSTER_ID=<stable-cluster-uuid>
ECLIPSE_DATAGRID_AERON_RECORDING_ID=<writer archive recording id>
ECLIPSE_DATAGRID_AERON_NODE_ID=<stable-node-uuid>
ECLIPSE_DATAGRID_AERON_STORE_GENERATION=<store-generation-uuid>
```

The remaining `ECLIPSE_DATAGRID_AERON_*` settings select the UDP live,
Archive-control, replay, directory, archive directory, writer checkpoint,
file-sync, term, MTU, chunk, and transaction limits. Set
`ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH` to a durable, owner-only path. The
MediaDriver directory is recreated by Aeron on startup, so archive and
checkpoint paths must not be children of `ECLIPSE_DATAGRID_AERON_DIRECTORY`.
The provider owns its embedded MediaDriver/Archive lifecycle and closes those
resources from the DataGrid storage-manager shutdown callback.

For a reader, set `ECLIPSE_DATAGRID_AERON_RECORDING_ID` to the writer's
recording. Reader identity/checkpoint persistence is supplied by the
nodelibrary deployment; this provider does not invent an identity from the
network address.
One provider instance owns one configured replication stream; use separate
provider instances/channels for multiple streams.
The development live-channel default is a dynamic MDC loopback channel
(`control=localhost:40123|control-mode=dynamic|fc=max`) so multiple readers
can attach. Production deployments must configure a routable control endpoint.
The default wire tuning is a 16 MiB term, 1 MiB Store chunk, 1,408-byte MTU,
and 64 MiB transaction limit; override with the full environment keys
`ECLIPSE_DATAGRID_AERON_TERM_LENGTH`,
`ECLIPSE_DATAGRID_AERON_MTU_LENGTH`,
`ECLIPSE_DATAGRID_AERON_CHUNK_SIZE`,
`ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES`, and
`ECLIPSE_DATAGRID_AERON_OFFER_TIMEOUT_NANOS`.
`CHUNK_SIZE + 64` must fit Aeron's publication maximum (`term-length / 8`,
capped at 16 MiB). Store bytes are sent directly inside the fixed replication
envelope; no SBE or second serialization pass is required.

Writer checkpoint persistence is enabled in the provider. ACK-driven retention
is intentionally not part of this release: the unused ACK/tracker implementation
was removed rather than shipped as a misleading public surface. The nodelibrary
provider exposes a safe no-op retention policy until authenticated reader
identities and durable watermarks are designed and implemented.

Fixed-writer/no-consensus operation is intentional. Writer fencing and manual
promotion remain deployment responsibilities.

The framework adapters expose Aeron through the normal monitoring endpoints:
`/eclipse-datagrid/health`, `/eclipse-datagrid/health/ready`, and the
Prometheus-compatible `/eclipse-datagrid/replication-metrics`. The latter
reports `transport="aeron"`, replay/live state, current/latest sequence, lag,
readiness, and health, including Archive or replay failures.
