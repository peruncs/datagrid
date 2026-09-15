/// This package carries clustered cache invalidations through Aeron.
///
/// Each node publishes its timestamp updates on one Aeron publication and
/// consumes every other node's updates from one subscription. A receiver
/// ignores frames written by its own sender identity.
///
/// The sender is synchronous and fails the local cache operation when it cannot
/// publish. At the receiver,
/// malformed or undecodable frames, application failures, and sender
/// sequence gaps stop delivery and are exposed through failure state;
/// continuing after any of these conditions would leave a volatile broadcast
/// consumer permanently stale.
///
/// Topology and identity: cache invalidation is an N-writer/N-reader
/// broadcast, not the Store replication 1-writer/N-reader topology. The default
/// channel `aeron:ipc` is single-host; multi-host deployments must
/// configure a UDP channel with `control-mode=dynamic`. Self-suppression
/// uses a 16-byte sender id per provider, or a configured `node-id`
/// shared by every provider of one node. Payloads use a fixed UTF-8 schema and
/// CRC32C rather than dynamic object deserialization.
///
/// Loss and security: the Aeron stream is volatile, so invalidations are
/// lost while a receiver is down or over a missed burst; a receiver fails
/// closed when it detects that loss. The channel is assumed to sit on an isolated network; the
/// frames carry no authentication, and the provider
/// rejects wildcard, loopback, and non-MDC UDP channels to reduce the exposure.
/// See [AeronClusteredConfigurationPropertyNames] for the settings.
///
/// @since 1.0
package peruncs.datagrid.cache.aeron;
