/// Neutral replication contracts shared by the node layer and its transports.
///
/// This package owns the transport contract with its no-op implementation
/// for nodes with replication disabled, plus Store-mark, health, retention,
/// and position-provider views. Binary apply, publish,
/// receive, and merger contracts live in `...cluster.storage.binary`, so
/// transport providers do not create a duplicate API layer. A provider owns
/// its transport resources, while the node owns start and stop order.
///
/// @since 1.0
package peruncs.cluster.node.replication;
