/// This package connects clustered cache invalidation to the Aeron transport.
///
/// The sender publishes timestamp updates synchronously and fails the local
/// cache operation when it cannot publish. The receiver hands an update to the
/// acceptor, which applies it only when it is newer than the timestamp already
/// held by the local cache. An update for a cache that is not open yet is
/// buffered, never ignored, and replayed once the cache opens. The region
/// factory replays buffered updates when it creates the timestamps region,
/// and the receiver invalidates every cache on startup and on every
/// re-synchronization, so reads are never served from state that may have
/// missed traffic. Listener configuration owns the sender and releases it
/// when the configuration is disposed.
///
/// API surface: the region factory, acceptor, and serialization type
/// provider. Hibernate setting maps are translated once in the region factory;
/// the Aeron core takes an injected configuration object.
///
/// @since 1.0
package peruncs.datagrid.cache.types;

