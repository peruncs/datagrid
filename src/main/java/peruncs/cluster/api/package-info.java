/// Application-facing API of one Eclipse Store replication node.
///
/// Applications open a [ClusterNode] with [NodeOptions],
/// receive a [ClusterStorageManager] — a StorageManager-compatible facade
/// with documented role, root, coordination, and lifecycle restrictions — and
/// join graph access through [GraphBoundary]. Aeron, backup archive, cursor,
/// checkpoint, and Store-adapter types are implementation details; node
/// configuration enters programmatically through [NodeSettingsSource], with
/// the environment as the default source.
///
/// Deviations from plain Store: application imports are rejected outright,
/// persistence failures with uncertain durable outcome latch the graph
/// (fail closed until reload or reseed), and application shutdown of the
/// manager tears the whole node down in the owned teardown order.
///
/// No public or protected signature in this package references a
/// non-exported type: internal assembly, manager, control-view, and
/// storage-manager types appear only in private fields, method bodies, and
/// package-private constructors, so JPMS clients can compile against this
/// package alone. Observability crosses the boundary once through
/// [ReplicationStatus], whose
/// [ReplicationState] is the single lifecycle
/// definition shared by the internals — there is no second parallel state
/// enum to drift, and no free-form transport id: Aeron is the only transport,
/// and a node without replication simply reports no replication metrics.
package peruncs.cluster.api;

