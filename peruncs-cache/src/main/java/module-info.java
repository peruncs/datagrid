/// Clustered Hibernate second-level cache over Aeron.
///
/// `ClusteredCacheRegionFactory` extends the Store Hibernate region factory.
/// At session-factory preparation it builds one fixed-schema payload codec,
/// one Aeron provider, one receiver, and one listener configuration; the receiver
/// starts before local cache events are redirected to the cluster. The
/// region factory, acceptor, and serialization type provider live in
/// `...cache.types`; the Aeron sender, receiver, codec, and provider in
/// `...cache.aeron` are the only transport.
///
/// # Volatile broadcast
///
/// Invalidations are small, frequent, and newest-wins: each carries a table
/// name and a timestamp. They travel on a volatile Aeron N-writer/N-reader
/// broadcast, not a durable log — there is no broker, no archive, and no
/// replay. Each frame holds a 16-byte sender identity, a per-sender
/// sequence, a CRC32C, and a fixed UTF-8 payload schema; a node ignores frames
/// carrying its own identity, or the identity shared through a configured
/// `node-id`. The default channel `aeron:ipc` is single-host;
/// multi-host deployments must configure dynamic MDC UDP. Channels carry no
/// authentication and must sit on an isolated network.
///
/// # Synchronous, fail-fast sending
///
/// A JCache listener callback runs inside the local cache write, so a
/// dropped invalidation would leave peers on stale timestamps while the
/// local write succeeds. The sender therefore offers each frame and waits
/// until the publication accepts it, retrying back pressure within the
/// configured offer timeout; a closed publication or an expired timeout
/// fails the local operation instead. Cache semantics never depend on
/// transport timing: either peers can receive the invalidation or the write
/// does not happen.
///
/// # Fail-closed receiving
///
/// The broadcast cannot prove a bad frame was harmless and cannot
/// retransmit a missed one, so the receiver stops on the first malformed
/// or undeserializable frame, on a message the acceptor cannot apply, and
/// on any sender sequence gap. It retains the terminal failure, and the
/// timestamps region refuses every cache operation until the node restarts
/// into a clean state. Stale query results are impossible by construction
/// after transport faults.
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
