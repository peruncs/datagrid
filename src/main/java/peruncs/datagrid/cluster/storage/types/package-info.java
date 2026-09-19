/// This package defines the persistence replication contract and its
/// package-private machinery.
///
/// The public types carry Eclipse Store binary data and lifecycle callbacks
/// without depending on the Aeron client implementation. Writers publish
/// transaction boundaries; readers apply them in order; importers own binary
/// buffers until materialization completes.
///
/// [ReplicationCursor] is the transport-neutral durable boundary a reader
/// persists after every applied commit — the node layer stores it through
/// `ReplicationCursorStore`. It is deliberately distinct from the Aeron
/// provider's `AeronReplicationCursor` (in `storage.aeron.checkpoint`),
/// which encodes the transport-specific recording id and position inside
/// the neutral cursor's opaque provider-position bytes: the Aeron type is a
/// codec, never a persisted boundary in its own right.
///
/// Implementations must not expose a mutable transport buffer after the
/// callback that consumes it returns.
///
/// # Why contracts and machinery share one package
///
/// The public surface is deliberately small: [StorageBinaryDataDistributor],
/// [StorageBinaryDataClient], [StorageBinaryDataReceiver],
/// [StorageBinaryDataMerger] and its configuration, [ReplicationCursor],
/// [ReplicationDurabilityMode], [AtomicFileWriter], [PathSecurity],
/// [RejectingPersistenceTarget], [StorageGraphCoordinator], and
/// [ClusterStoreIndexes]. Transport providers in `storage.aeron` and the node
/// layer import these types directly.
///
/// Everything else — the merger implementation, materializer, importer,
/// buffer views, index validation policy, index maintenance, and the
/// reflective upstream-layout holder — is package-private. It is kept in this
/// package rather than moved to a `storage.internal` package because the
/// machinery and the contract implementations are mutually coupled through
/// package-private seams (`StorageBinaryDataMerger.Default` calls the
/// package-private importer/materializer, and `ClusterStoreIndexes` delegates
/// to package-private policy and maintenance types). A physical split would
/// force those seams public and break the transport imports that must stay
/// stable, so the boundary is enforced by visibility, not by package names.
///
/// Embedded Lucene and in-graph JVector policy lives in
/// [ClusterStoreIndexes] (facade), [ClusterIndexValidation] (validation
/// policy), [ClusterIndexMaintenance] (reader refresh/rebuild), and
/// [StoreIndexReflection] (upstream field layout).
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.types;
