/// Minimal embedding API for one Eclipse Store replication node.
///
/// Applications open a [peruncs.datagrid.cluster.api.ClusterNode], access its
/// guarded [peruncs.datagrid.cluster.api.ClusterStore], and render immutable
/// [peruncs.datagrid.cluster.api.NodeStatus] snapshots. Aeron, backup archive,
/// cursor, checkpoint, and Store-adapter types are implementation details.
///
/// No public or protected signature in this package references a
/// non-exported type: internal assembly, manager, control-view, and
/// storage-manager types appear only in private fields, method bodies, and
/// package-private constructors, so JPMS clients can compile against this
/// package alone. Observability crosses the boundary once through
/// [peruncs.datagrid.cluster.api.ReplicationStatus], whose
/// [peruncs.datagrid.cluster.api.ReplicationState] is the single lifecycle
/// definition shared by the internals — there is no second parallel state
/// enum to drift, and no free-form transport id: Aeron is the only transport,
/// and a node without replication simply reports no replication metrics.
package peruncs.datagrid.cluster.api;
