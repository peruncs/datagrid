/**
 * This package carries Aeron replication between nodes.
 *
 * <p>It owns the transport contract with its no-op implementation for nodes
 * with replication disabled, the binary distributor, client, merger, and
 * packet contracts, durable cursors with their store, health, retention, and
 * position views. A provider owns its transport resources, while the node owns
 * start and stop order.</p>
 *
 * @since 1.0
 */
package peruncs.datagrid.cluster.nodelibrary.replication;
