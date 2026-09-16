/// Clustered Hibernate second-level cache over Aeron.
///
/// `ClusteredCacheRegionFactory` extends the Store Hibernate region factory.
/// At session-factory preparation it builds one fixed-schema payload codec,
/// one Aeron provider, one receiver, and one listener configuration; the receiver
/// starts before local cache events are redirected to the cluster. The
/// region factory, acceptor, and update message live in
/// `...cache.types`; the Aeron sender, receiver, codec, cursor store, and
/// provider in `...cache.aeron` are the only transport.
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
/// # Fail-closed receiving with heartbeats, freshness, and cursors
///
/// The broadcast cannot prove a bad frame was harmless and cannot
/// retransmit a missed one, so the receiver stops on the first malformed
/// or undeserializable frame, on a message the acceptor cannot apply, on
/// any sender sequence gap, and on silence past the freshness deadline. It
/// retains the terminal failure, invalidates the local caches, and the
/// timestamps region refuses every cache operation until the receiver is
/// re-synchronized into a clean state.
///
/// Durability rests on four mechanisms, and on understanding what they do
/// not cover. Idle senders publish sequence-consuming heartbeats, so a quiet
/// cluster still proves liveness; a receiver that hears no frame or heartbeat
/// within the freshness timeout marks itself stale instead of serving reads
/// it cannot vouch for. Startup always invalidates the local caches before
/// the receiver is declared healthy, and per-sender cursors persisted in an
/// atomic file validate the first sequence after a restart — a mismatch
/// invalidates everything and fails closed. Updates for caches that are not
/// open yet are buffered, never silently ignored, and replayed once the
/// cache opens.
///
/// What remains best-effort: publication acceptance proves admission, not
/// delivery to every subscriber, and senders that were never heard from are
/// not missed — the receiver learns membership from traffic, so a sender
/// with no record cannot have a silence deadline without a membership
/// protocol. Every sender that has been heard from must keep proving
/// liveness: its silence past the freshness deadline fails the receiver
/// closed even while other traffic flows. Without a configured HMAC secret,
/// frames and heartbeats are CRC-protected only, so the channel must sit on
/// an isolated network; with a secret every frame is HMAC-signed and
/// unsigned frames are rejected. Production mode requires the secret unless
/// unsigned frames are explicitly acknowledged.
///
/// @since 1.0
module peruncs.datagrid.cache
{
    /* Transitive for modules whose types appear in exported API signatures:
     * Disposable is an implemented interface, CacheManager a public constructor
     * parameter, CacheRegionFactory the superclass, Hibernate settings and
     * storage-access types appear in public/protected methods, and
     * CacheEntryListenerConfiguration is a public return type. */
    requires transitive org.eclipse.serializer.base;
    requires transitive org.eclipse.store.cache;
    requires transitive org.eclipse.store.cache.hibernate;
    requires transitive org.hibernate.orm.core;
    requires transitive cache.api;
    requires io.aeron.client;
    requires io.aeron.driver;
    requires org.agrona;

    exports peruncs.datagrid.cache.types;
    exports peruncs.datagrid.cache.aeron;
}
