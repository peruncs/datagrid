package peruncs.datagrid.cache.clustered.types;

import peruncs.datagrid.cache.clustered.aeron.types.AeronClusteredCacheMessageSender;
import org.eclipse.serializer.typing.Disposable;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import java.util.Objects;

/**
 * This configuration connects cache events to the Aeron invalidation sender.
 *
 * <p>The JCache listener is synchronous and does not require the old value.
 * The enclosing configuration owns the sender and disposes it with the cache
 * region.</p>
 */
public class ClusteredCacheEntryListenerConfiguration implements Disposable
{
    private final CacheEntryListenerConfig updateTimestamps;

	/** Creates a configuration that owns the supplied sender.
	 *
	 * @param updateTimestampsSender sender for timestamp updates
	 */
	public ClusteredCacheEntryListenerConfiguration(final AeronClusteredCacheMessageSender updateTimestampsSender)
    {
        this.updateTimestamps = new CacheEntryListenerConfig(
            Objects.requireNonNull(updateTimestampsSender, "updateTimestampsSender"));
    }

	/**
	 * Returns the JCache listener configuration for timestamp updates.
	 *
	 * <p>The listener factory returns the single sender instance owned by this
	 * configuration. The reference is deliberately not snapshotted: the sender
	 * is the only listener instance, its dispose is idempotent, and it fails
	 * closed, so a late factory call after {@link #dispose()} yields a sender
	 * that rejects further publishes instead of a stale copy.</p>
	 *
	 * @return listener configuration for timestamp updates
	 */
	public CacheEntryListenerConfiguration<Object, Object> getUpdateTimestampsCacheEntryListenerConfiguration()
    {
        return this.updateTimestamps;
    }

    private boolean disposed;

    @Override
    public synchronized void dispose()
    {
        if (this.disposed)
        {
            return;
        }
        /* The sender's own dispose is idempotent; the reference stays valid for
         * the listener factory until this configuration is garbage. */
        this.updateTimestamps.sender.dispose();
        this.disposed = true;
    }

	/** The JCache configuration view backed by one sender instance.
	 *
	 * @param sender sender exposed by the listener factory
	 */
	private record CacheEntryListenerConfig(AeronClusteredCacheMessageSender sender)
		implements CacheEntryListenerConfiguration<Object, Object>
	{
        @Override
        public Factory<CacheEntryListener<? super Object, ? super Object>> getCacheEntryListenerFactory()
        {
            return () -> this.sender;
        }

        @Override
        public boolean isOldValueRequired()
        {
            return false;
        }

        @Override
        public Factory<CacheEntryEventFilter<? super Object, ? super Object>> getCacheEntryEventFilterFactory()
        {
            return null;
        }

        @Override
        public boolean isSynchronous()
        {
            return true;
        }
    }
}
