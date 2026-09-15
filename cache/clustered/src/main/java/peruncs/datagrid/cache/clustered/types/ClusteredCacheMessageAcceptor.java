package peruncs.datagrid.cache.clustered.types;

import org.eclipse.store.cache.types.CacheManager;

import java.util.Objects;

/**
 * This acceptor applies remote timestamp updates to caches already open here.
 *
 * <p>It keeps the greatest timestamp seen for each table. A message for an
 * unopened cache is ignored because opening that cache will establish its own
	 * local state. The compare-and-set is performed while holding the cache
	 * monitor, matching the monitor used by the cache's silent write operation;
	 * the silent update prevents a received invalidation from being sent back to
	 * the cluster.</p>
 */
public class ClusteredCacheMessageAcceptor
{
    private static final System.Logger LOGGER =
        System.getLogger(ClusteredCacheMessageAcceptor.class.getName());
    private final CacheManager cacheManager;

	/** Creates an acceptor for the supplied local cache manager.
	 *
	 * @param cacheManager local cache manager
	 */
	public ClusteredCacheMessageAcceptor(final CacheManager cacheManager)
    {
        this.cacheManager = cacheManager;
    }

	/** Applies a remote timestamp when it is newer than the local value.
	 *
	 * @param message remote timestamp update
	 */
	public void accept(final TimestampsRegionUpdateMessage message)
    {
        Objects.requireNonNull(message, "message");
        if (this.cacheManager == null)
        {
            throw new IllegalStateException("No cache manager is configured for the clustered-cache acceptor");
        }
        final var cache = this.cacheManager.getCache(message.cacheName());

        if (cache == null)
        {
            // we don't have this cache loaded
            return;
        }

		synchronized (cache)
		{
			final Object stored = cache.get(message.tableName());
			final Long previousTimestamp = stored instanceof final Long value ? value : null;
			if (stored != null && previousTimestamp == null)
			{
				LOGGER.log(System.Logger.Level.WARNING,
					"Ignoring query-cache timestamp table=" + message.tableName() +
						" with a non-timestamp stored value of type " + stored.getClass().getName());
				return;
			}

			if (previousTimestamp != null && previousTimestamp >= message.timestamp())
			{
				LOGGER.log(System.Logger.Level.DEBUG,
					"Received outdated query-cache timestamp table=" + message.tableName() +
						", timestamp=" + message.timestamp() + ". Currently stored timestamp=" + previousTimestamp);
				return;
			}

			cache.putSilent(message.tableName(), message.timestamp());
			LOGGER.log(System.Logger.Level.DEBUG,
				"Updating query-cache timestamp table=" + message.tableName() +
					", timestamp=" + message.timestamp() + '.');
		}
    }
}
