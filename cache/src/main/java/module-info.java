/// This module adds clustered invalidation to the Eclipse Store Hibernate
/// cache region factory, carried over Aeron.
///
/// The region factory, acceptor, and serialization type provider live in
/// `...cache.types`. The Aeron sender, receiver, codec, and provider
/// in `...cache.aeron` are the only transport; the region
/// factory creates the provider directly.
///
/// Cache invalidation is an N-writer/N-reader broadcast. The default channel
/// is single-host `aeron:ipc`; multi-host deployments must configure
/// dynamic MDC UDP. The stream is volatile and carries no authentication.
///
/// @since 1.0
module peruncs.datagrid.cache
{
    requires org.eclipse.serializer;
    requires org.eclipse.serializer.base;
    requires org.eclipse.store.cache;
    requires org.eclipse.store.cache.hibernate;
    requires org.hibernate.orm.core;
    requires cache.api;
    requires io.aeron.client;
    requires io.aeron.driver;
    requires org.agrona;

    exports peruncs.datagrid.cache.types;
    exports peruncs.datagrid.cache.aeron;
}
