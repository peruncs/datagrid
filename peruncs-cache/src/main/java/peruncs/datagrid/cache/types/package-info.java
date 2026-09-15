/// This package connects clustered cache invalidation to the Aeron transport.
///
/// The sender publishes timestamp updates synchronously and fails the local
/// cache operation when it cannot publish. The receiver hands an update to the
/// acceptor, which applies it only when it is newer than the timestamp already
/// held by the local cache. An unknown cache is ignored because a node may
/// receive an update before that cache is opened. Listener configuration owns
/// the sender and releases it when the configuration is disposed.
///
/// API surface: the region factory, acceptor, and serialization type
/// provider. Hibernate setting maps are translated once in the region factory;
/// the Aeron core takes an injected configuration object.
///
/// @since 1.0
package peruncs.datagrid.cache.types;

