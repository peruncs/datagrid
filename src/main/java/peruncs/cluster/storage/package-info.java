/// Store transport contracts and Aeron replication machinery.
///
/// The root package holds the neutral replication contract types —
/// [ReplicationPosition], [ReplicationRetry], and [Crc32C] — and the
/// [StorageGraphCoordinator]: Deliberately here rather than in
/// `node.store` because the merger in `binary` consumes it directly and
/// Store-side packages must not depend on the node layer. Prepare frames are
/// recorded by the Archive before the local Store write; the persisted
/// replication state lives on [ReplicationMark].
/// The `binary` package carries the Store binary
/// distribution and materialization machinery, and the `index` package the
/// embedded index policy, so transport providers never duplicate the API
/// layer. The `io` package owns atomic metadata writes and path safety.
/// The `aeron` package implements the contracts over Aeron Archive
/// recordings with fencing, Store marks, and quorum-gated retention.
///
/// @since 1.0
package peruncs.cluster.storage;

import peruncs.cluster.storage.aeron.mark.ReplicationMark;
