/**
 * This package connects clustered cache invalidation to the Aeron transport.
 *
 * <p>The sender publishes timestamp updates synchronously and fails the local
 * cache operation when it cannot publish. The receiver hands an update to the
 * acceptor, which applies it only when it is newer than the timestamp already
 * held by the local cache. An unknown cache is ignored because a node may
 * receive an update before that cache is opened. Listener configuration owns
 * the sender and releases it when the configuration is disposed.</p>
 *
 * <p>API surface: the region factory, acceptor, configuration property names,
 * and serialization type provider. {@link ClusteredCachePropertyParsers} is an
 * internal parsing helper and is not meant for application code.</p>
 *
 * @since 1.0
 */
package peruncs.datagrid.cache.types;

