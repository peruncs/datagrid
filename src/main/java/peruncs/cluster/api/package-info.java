/// Minimal embedding API for one Eclipse Store replication node.
///
/// Applications open a [ClusterNode], access its
/// guarded [ClusterStore], and render immutable
/// [NodeStatus] snapshots. Aeron, backup archive,
/// cursor, checkpoint, and Store-adapter types are implementation details.
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

