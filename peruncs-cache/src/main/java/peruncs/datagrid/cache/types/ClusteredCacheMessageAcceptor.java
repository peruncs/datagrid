package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.types.Cache;
import org.eclipse.store.cache.types.CacheManager;

import javax.cache.processor.EntryProcessor;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/// This acceptor applies remote timestamp updates to the local caches.
///
/// It keeps the greatest timestamp seen for each table. A message for a cache
/// that is not open yet is buffered, keeping only the newest timestamp per
/// table, and applied once the cache opens — either opportunistically on the
/// next update for that cache or explicitly through [#replayPending]. Updates
/// are never silently dropped. The comparison and mutation run through a
/// JCache `EntryProcessor` so they execute under the cache's internal table
/// lock; a concurrent local timestamp write therefore cannot move the value
/// backwards. The listener filter marks this synchronous remote update as
/// silent so it is never broadcast back to the cluster.
///
/// [#invalidateAll] drops every locally cached timestamp and every buffered
/// update. The receiver calls it on startup and before any re-synchronization
/// completes, so a node never serves reads from a cache that may have missed
/// traffic. Buffered updates are dropped there too: they predate the gap that
/// forced the invalidation and replaying them could resurrect stale values.
///
/// A replay racing an invalidation must never resurrect a pre-gap update: every
/// invalidation bumps a generation counter, every buffered update carries the
/// generation it was buffered under, and a replay applies an entry only when
/// both the entry and the acceptor are still on the generation observed at the
/// replay start. A direct application likewise drops its message when an
/// invalidation lands between the cache lookup and the update.
///
/// `Cache.putSilentIfGreater` would express the max-update directly and is the
/// desired replacement, because it applies the maximum under the same lock
/// without dispatching any listener at all. The published `eclipse-store`
/// snapshot this module builds against does not yet expose it, so until the
/// dependency provides it, the processor plus a filter is the only atomic,
/// non-rebroadcasting path.
public class ClusteredCacheMessageAcceptor {
    private static final System.Logger LOGGER =
            System.getLogger(ClusteredCacheMessageAcceptor.class.getName());
    private static final ScopedValue<Boolean> REMOTE_UPDATE = ScopedValue.newInstance();
    /* Hard bound on distinct buffered cache/table names. A real application's
     * schema stays far below this; the bound exists so a hostile peer cannot
     * flood arbitrary names into unbounded memory. Exceeding it is treated as
     * unreconcilable state and fails the receiver closed, mirroring the
     * tracked-sender bound. */
    private static final int MAX_PENDING_UNOPENED = 10_000;
    private final CacheManager cacheManager;
    /* Updates for caches that are not open here, coalesced to the newest
     * timestamp per table. Bounded by [MAX_PENDING_UNOPENED] distinct
     * cache/table names, the same order of state the cache itself would hold
     * once opened. */
    private final ConcurrentHashMap<CacheTable, StampedUpdate> pendingUnopened =
            new ConcurrentHashMap<>();
    /* Bumped by every invalidation; replays and applications carry the
     * generation they observed and are dropped once it moves on. */
    private final AtomicLong generation = new AtomicLong();
    /* Held across an invalidation's generation bump, buffer clear, and cache
     * wipe, and across one generation check plus its cache update. The
     * generation checks alone cannot close the check-then-act window around
     * the cache I/O, so this one lock orders every mutation: an update either
     * lands fully before an invalidation wipes it, or is dropped after it.
     * The critical sections are single map operations, never network calls. */
    private final ReentrantLock mutationLock = new ReentrantLock();
    /* Cache access and remote application take the read side before any
     * mutationLock acquisition; invalidation takes the write side first.
     * This orders an admitted cache operation wholly before or after a wipe. */
    private final ReentrantReadWriteLock cacheAccessLock = new ReentrantReadWriteLock(true);

    /// Returns the lock held for an entire admitted cache operation.
    ///
    /// @return the read side of the cache invalidation gate
    Lock cacheReadLock() {
        return this.cacheAccessLock.readLock();
    }

        /// Creates an acceptor for the supplied local cache manager.
    ///
    /// @param cacheManager local cache manager
    public ClusteredCacheMessageAcceptor(final CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

        /// Applies a remote timestamp when it is newer than the local value.
    /// A message for a cache that is not open here is buffered and applied
    /// once the cache opens; it is never silently dropped.
    ///
    /// @param message remote timestamp update
    public void accept(final TimestampsRegionUpdateMessage message) {
        Objects.requireNonNull(message, "message");
        this.cacheAccessLock.readLock().lock();
        try {
        if (this.cacheManager == null) {
            throw new IllegalStateException("No cache manager is configured for the clustered-cache acceptor");
        }
        final long observed = this.generation.get();
        var cache = this.cacheManager.getCache(message.cacheName());

        if (cache == null) {
            /* The cache is not open here. Buffer the newest timestamp per
             * table so no loss window opens before the cache is created. A
             * flood of distinct unknown names beyond the bound is
             * unreconcilable — dropping entries would open exactly the silent
             * loss window this buffer exists to prevent — so it fails the
             * receiver closed instead. */
            this.mutationLock.lock();
            try {
                /* The lookup happened before the lock. If an invalidation
                 * completed in that gap, this message belongs to the old
                 * generation and must not be stamped as live traffic. Recheck
                 * the cache as well so an open between the two calls is
                 * updated immediately instead of being buffered indefinitely. */
                if (this.generation.get() != observed) {
                    return;
                }
                cache = this.cacheManager.getCache(message.cacheName());
                if (cache != null) {
                    this.applyIfCurrent(cache, message, observed);
                    return;
                }
                final CacheTable key = new CacheTable(message.cacheName(), message.tableName());
                if (!this.pendingUnopened.containsKey(key) && this.pendingUnopened.size() >= MAX_PENDING_UNOPENED) {
                    throw new IllegalStateException(
                            "Aeron clustered-cache acceptor exceeded the pending-update bound of %s distinct cache tables".formatted(MAX_PENDING_UNOPENED));
                }
                this.pendingUnopened.merge(key, new StampedUpdate(message, observed),
                        (previous, incoming) -> incoming.message().timestamp() >= previous.message().timestamp()
                                ? incoming : previous);
            } finally {
                this.mutationLock.unlock();
            }
            LOGGER.log(System.Logger.Level.DEBUG,
                    "Buffering query-cache timestamp table=%s for unopened cache %s".formatted(message.tableName(), message.cacheName()));
            return;
        }
        this.replayPendingFor(message.cacheName(), observed);
        this.applyIfCurrent(cache, message, observed);
        } finally {
            this.cacheAccessLock.readLock().unlock();
        }
    }

        /// Applies buffered updates for caches that have opened since the
    /// updates arrived. Buffered entries for caches that are still unopened
    /// stay buffered. Entries predating a concurrent invalidation are dropped,
    /// never replayed.
    public void replayPending() {
        this.cacheAccessLock.readLock().lock();
        try {
        this.mutationLock.lock();
        try {
        final long observed = this.generation.get();
        for (final var pending : this.pendingUnopened.entrySet()) {
            final var cache = this.cacheManager == null ? null : this.cacheManager.getCache(pending.getKey().cacheName());
            if (cache == null) {
                continue;
            }
            if (this.pendingUnopened.remove(pending.getKey(), pending.getValue()) &&
                pending.getValue().generation() == observed) {
                this.applyIfCurrent(cache, pending.getValue().message(), observed);
            }
        }
        } finally {
            this.mutationLock.unlock();
        }
        } finally {
            this.cacheAccessLock.readLock().unlock();
        }
    }

        /// Drops every locally cached timestamp and every buffered update.
    /// After this call the next read reloads timestamps from the source, so a
    /// node that may have missed traffic never serves stale entries. Buffered
    /// updates are dropped as well: they predate the gap that forced the
    /// invalidation. The generation bump makes a concurrent replay drop the
    /// same entries instead of re-inserting them afterwards. A `null` cache
    /// manager holds no caches and is a no-op for the caches, but still bumps
    /// the generation so buffered updates are dropped.
    public void invalidateAll() {
        this.cacheAccessLock.writeLock().lock();
        try {
        this.mutationLock.lock();
        try {
            this.generation.incrementAndGet();
            this.pendingUnopened.clear();
            if (this.cacheManager == null) {
                return;
            }
            RuntimeException failure = null;
            for (final String cacheName : this.cacheManager.getCacheNames()) {
                try {
                    final var cache = this.cacheManager.getCache(cacheName);
                    if (cache != null) {
                        cache.removeAll();
                    }
                } catch (final RuntimeException error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            this.mutationLock.unlock();
        }
        } finally {
            this.cacheAccessLock.writeLock().unlock();
        }
    }

        /// Returns how many updates are buffered for unopened caches.
    ///
    /// @return buffered update count
    public int pendingUnopenedCount() {
        return this.pendingUnopened.size();
    }

        /// Applies buffered updates for one cache that has just been observed open.
    /// Entries predating a concurrent invalidation are dropped, never replayed
    /// or re-buffered.
    private void replayPendingFor(final String cacheName, final long observed) {
        this.mutationLock.lock();
        try {
        for (final var pending : this.pendingUnopened.entrySet()) {
            if (!pending.getKey().cacheName().equals(cacheName)) {
                continue;
            }
            if (this.pendingUnopened.remove(pending.getKey(), pending.getValue())) {
                if (pending.getValue().generation() != observed) {
                    continue;
                }
                final var cache = this.cacheManager.getCache(cacheName);
                if (cache != null) {
                    this.applyIfCurrent(cache, pending.getValue().message(), observed);
                } else if (pending.getValue().generation() == this.generation.get()) {
                    /* The cache closed concurrently with no invalidation in
                     * between; keep the update buffered. */
                    this.pendingUnopened.merge(pending.getKey(), pending.getValue(),
                            (previous, incoming) -> incoming.message().timestamp() >= previous.message().timestamp()
                                    ? incoming : previous);
                }
            }
        }
        } finally {
            this.mutationLock.unlock();
        }
    }

        /// Returns the current invalidation generation. Every [#invalidateAll]
    /// bumps it; replays and applications stamped with an older generation are
    /// dropped.
    ///
    /// @return invalidation generation
    long generation() {
        return this.generation.get();
    }

        /// Applies one update when the acceptor is still on the observed
    /// generation. The check and the update run under the mutation lock, so an
    /// invalidation racing this call either wipes the update afterwards or
    /// drops it beforehand — a pre-gap value can never be re-inserted after
    /// the wipe.
    private void applyIfCurrent(final Cache<Object, Object> cache,
                                final TimestampsRegionUpdateMessage message,
                                final long observed) {
        this.mutationLock.lock();
        try {
            if (this.generation.get() != observed) {
                return;
            }
            this.applyUpdate(cache, message);
        } finally {
            this.mutationLock.unlock();
        }
    }

    private void applyUpdate(final Cache<Object, Object> cache,
                             final TimestampsRegionUpdateMessage message) {
        final EntryProcessor<Object, Object, Boolean> update = (entry, ignored) -> {
            final Object stored = entry.getValue();
            final Long previousTimestamp = stored instanceof Long value ? value : null;
            if (stored != null && previousTimestamp == null) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Ignoring query-cache timestamp table=%s with a non-timestamp stored value of type %s".formatted(message.tableName(), stored.getClass().getName()));
                return false;
            }
            if (previousTimestamp != null && previousTimestamp >= message.timestamp()) {
                LOGGER.log(System.Logger.Level.DEBUG,
                        "Received outdated query-cache timestamp table=%s, timestamp=%s. Currently stored timestamp=%s".formatted(message.tableName(), message.timestamp(), previousTimestamp));
                return false;
            }
            entry.setValue(message.timestamp());
            return true;
        };
        final boolean updated = ScopedValue.where(REMOTE_UPDATE, Boolean.TRUE)
                .call(() -> Boolean.TRUE.equals(cache.invoke(message.tableName(), update)));
        if (updated) {
            LOGGER.log(System.Logger.Level.DEBUG,
                    "Updating query-cache timestamp table=%s, timestamp=%s%s".formatted(message.tableName(), message.timestamp(), '.'));
        }
    }

    static boolean isRemoteUpdate() {
        return Boolean.TRUE.equals(REMOTE_UPDATE.orElse(Boolean.FALSE));
    }

        /// Identifies one buffered update: the cache it targets and its table key.
    private record CacheTable(String cacheName, String tableName) {
    }

        /// One buffered update stamped with the invalidation generation it was
    /// buffered under. A replay applies it only when the stamp still matches.
    private record StampedUpdate(TimestampsRegionUpdateMessage message, long generation) {
    }
}
