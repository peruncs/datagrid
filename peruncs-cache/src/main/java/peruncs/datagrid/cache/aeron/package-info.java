/// This package carries clustered cache invalidations through Aeron.
///
/// Each node publishes its timestamp updates on one Aeron publication and
/// consumes every other node's updates from one subscription. A receiver
/// ignores frames written by its own sender identity.
///
/// The sender is synchronous and fails the local cache operation when it cannot
/// publish. An idle sender heartbeats on its own thread so quiet clusters
/// still prove liveness. At the receiver, malformed or undecodable frames,
/// application failures, sender sequence gaps, and silence past the freshness
/// deadline stop delivery and are exposed through failure state; continuing
/// after any of these conditions would leave a volatile broadcast consumer
/// silently stale.
///
/// Durability: startup invalidates the local caches before the receiver is
/// declared healthy, per-sender cursors persist in an atomic file so a
/// restart validates its first sequence instead of accepting anything, and a
/// failed receiver requires re-synchronization — invalidation plus a trusted
/// new baseline — before it serves reads again. Publication acceptance still
/// proves admission only, not delivery to every subscriber, and the loss of
/// one peer's traffic while other traffic flows is covered by the per-sender
/// heartbeat deadline; a deployment still needs an external membership system
/// if it must distinguish a crashed peer from a deliberately paused sender.
///
/// Topology and identity: cache invalidation is an N-writer/N-reader
/// broadcast, not the Store replication 1-writer/N-reader topology. The default
/// channel `aeron:ipc` is single-host; multi-host deployments must
/// configure a UDP channel with `control-mode=dynamic`. Self-suppression
/// uses a 16-byte sender id per provider, or a configured `node-id`
/// shared by every provider of one node. Payloads use a fixed UTF-8 schema and
/// CRC32C rather than dynamic object deserialization; an optional HMAC key
/// authenticates every frame and rejects unsigned traffic when configured.
///
/// Loss and security: the Aeron stream is volatile, so invalidations are
/// lost while a receiver is down or over a missed burst; a receiver fails
/// closed when it detects that loss — including one sender's silence past
/// the freshness deadline while other traffic flows. The channel is assumed
/// to sit on an isolated network; without a configured secret the frames
/// carry CRC32C and sequence checks only, while a configured secret makes
/// every frame HMAC-signed and rejects unsigned frames. The provider rejects
/// wildcard, loopback, and non-MDC UDP channels to reduce the exposure.
/// See [AeronClusteredCacheConfiguration] for the settings.
///
/// @since 1.0
package peruncs.datagrid.cache.aeron;
