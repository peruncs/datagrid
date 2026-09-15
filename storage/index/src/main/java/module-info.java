/**
 * Defines the Store index policy used by clustered Data Grid applications.
 *
 * <p>Cluster replication carries the Eclipse Store object graph. Lucene data
 * must therefore live in the graph, and vector data must use JVector's
 * persisted in-graph state. Files in an application directory are not part
 * of a Store transaction and are rejected at this boundary.</p>
 *
 * <p>The policy applies to Aeron replication, which carries the same Store bytes
 * covered by these index rules.</p>
 *
 * @since 1.0
 */
module peruncs.datagrid.storage.distributed.index
{
	requires org.eclipse.store.gigamap;
	requires org.eclipse.store.gigamap.lucene;
	// The upstream module name is misspelled; keep the dependency aligned with
	// the published module descriptor.
	requires org.eclipes.store.gigamap.jvector;
	requires org.apache.lucene.core;

	exports peruncs.datagrid.storage.distributed.index;
}
