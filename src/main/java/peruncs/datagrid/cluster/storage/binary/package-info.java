/// Store binary distribution, reception, merging, import, and materialization.
///
/// The binary machinery carries Eclipse Store binary data and lifecycle
/// callbacks without depending on the Aeron client implementation. Writers
/// publish transactions through the replication publisher; readers replay
/// and apply them in order through the replication applier, receiver, and
/// merger; importers own binary buffers until materialization completes.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.binary;
