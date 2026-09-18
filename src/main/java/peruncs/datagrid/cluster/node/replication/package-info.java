/// This package carries Aeron replication between nodes.
///
/// It owns the transport contract with its no-op implementation for nodes
/// with replication disabled, plus durable cursors with their store, health,
/// retention, and position views. Binary distributor, client, merger, and
/// packet contracts live in the storage-types package so transport providers
/// do not create a duplicate API layer. A provider owns its transport
/// resources, while the node owns start and stop order.
///
/// @since 1.0
package peruncs.datagrid.cluster.node.replication;
