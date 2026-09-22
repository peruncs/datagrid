/// Minimal embedding API for one Eclipse Store replication node.
///
/// Applications open a [peruncs.datagrid.cluster.api.ClusterNode], access its
/// guarded [peruncs.datagrid.cluster.api.ClusterStore], and render immutable
/// [peruncs.datagrid.cluster.api.NodeStatus] snapshots. Aeron, backup archive,
/// cursor, checkpoint, and Store-adapter types are implementation details.
package peruncs.datagrid.cluster.api;
