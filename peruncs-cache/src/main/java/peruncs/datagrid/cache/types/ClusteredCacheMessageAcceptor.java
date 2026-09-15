package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.types.CacheManager;

import javax.cache.processor.EntryProcessor;
import java.util.Objects;

/// This acceptor applies remote timestamp updates to caches already open here.
///
/// It keeps the greatest timestamp seen for each table. A message for an
/// unopened cache is ignored because opening that cache will establish its own
/// local state. The comparison and mutation run through a JCache
/// `EntryProcessor` so they execute under the cache's internal table lock; a
/// concurrent local timestamp write therefore cannot move the value backwards.
/// The listener filter marks this synchronous remote update as silent so it is
/// never broadcast back to the cluster.
///
/// `Cache.putSilentIfGreater` would express this directly and is the desired
/// replacement, because it applies the maximum under the same lock without
/// dispatching any listener at all. The published `eclipse-store` snapshot this
/// module builds against does not yet expose it, so until the dependency
/// provides it, the processor plus a filter is the only atomic,
/// non-rebroadcasting path.
public class ClusteredCacheMessageAcceptor {
    private static final System.Logger LOGGER =
            System.getLogger(ClusteredCacheMessageAcceptor.class.getName());
    private static final ScopedValue<Boolean> REMOTE_UPDATE = ScopedValue.newInstance();
    private final CacheManager cacheManager;

        /// Creates an acceptor for the supplied local cache manager.
    ///
    /// @param cacheManager local cache manager
    public ClusteredCacheMessageAcceptor(final CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

        /// Applies a remote timestamp when it is newer than the local value.
    ///
    /// @param message remote timestamp update
    public void accept(final TimestampsRegionUpdateMessage message) {
        Objects.requireNonNull(message, "message");
        if (this.cacheManager == null) {
            throw new IllegalStateException("No cache manager is configured for the clustered-cache acceptor");
        }
        final var cache = this.cacheManager.getCache(message.cacheName());

        if (cache == null) {
            // we don't have this cache loaded
            return;
        }

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
}
