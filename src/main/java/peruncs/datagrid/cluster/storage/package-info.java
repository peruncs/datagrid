/// Store transport contracts and Aeron replication machinery.
///
/// The root package holds the neutral replication contract types —
/// [ReplicationCursor], [ReplicationDurabilityMode], [ReplicationRetry],
/// and [Crc32C]. The `binary` package carries the Store binary
/// distribution and materialization machinery, and the `index` package the
/// embedded index policy, so transport providers never duplicate the API
/// layer. The `io` package owns atomic metadata writes and path safety.
/// The `aeron` package implements the contracts over Aeron Archive
/// recordings with fencing, checkpoints, and quorum-gated retention.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage;
