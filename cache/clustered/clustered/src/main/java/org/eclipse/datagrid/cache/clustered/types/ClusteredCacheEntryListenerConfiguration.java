package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.eclipse.serializer.typing.Disposable;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;

/**
 * This configuration connects cache events to a clustered message sender.
 *
 * <p>The JCache listener is synchronous and does not require the old value.
 * The enclosing configuration owns the sender and disposes it with the cache
 * region.</p>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public class ClusteredCacheEntryListenerConfiguration<K, V> implements Disposable
{
    private final CacheEntryListenerConfig updateTimestamps;

	/** Creates a configuration that owns the supplied sender.
	 *
	 * @param updateTimestampsSender sender for timestamp updates
	 */
	public ClusteredCacheEntryListenerConfiguration(final ClusteredCacheMessageSender<K, V> updateTimestampsSender)
    {
        this.updateTimestamps = new CacheEntryListenerConfig(updateTimestampsSender);
    }

	/** Returns the JCache listener configuration for timestamp updates.
	 *
	 * @return listener configuration for timestamp updates
	 */
	public CacheEntryListenerConfiguration<K, V> getUpdateTimestampsCacheEntryListenerConfiguration()
    {
        return this.updateTimestamps;
    }

    private boolean isOldValueRequired()
    {
        return false;
    }

    private Factory<CacheEntryEventFilter<? super K, ? super V>> getCacheEntryEventFilterFactory()
    {
        return null;
    }

    private boolean isSynchronous()
    {
        return true;
    }

    @Override
    public void dispose()
    {
        this.updateTimestamps.sender.dispose();
    }

    /** JCache view that exposes the sender as a listener factory. */
	public class CacheEntryListenerConfig implements CacheEntryListenerConfiguration<K, V>
	{
		/** Sender shared by the listener factory and the enclosing owner. */
		private final ClusteredCacheMessageSender<K, V> sender;

        private CacheEntryListenerConfig(final ClusteredCacheMessageSender<K, V> sender)
        {
            this.sender = sender;
        }

        @Override
        public Factory<CacheEntryListener<? super K, ? super V>> getCacheEntryListenerFactory()
        {
            return () -> this.sender;
        }

        @Override
        public boolean isOldValueRequired()
        {
            return ClusteredCacheEntryListenerConfiguration.this.isOldValueRequired();
        }

        @Override
        public Factory<CacheEntryEventFilter<? super K, ? super V>> getCacheEntryEventFilterFactory()
        {
            return ClusteredCacheEntryListenerConfiguration.this.getCacheEntryEventFilterFactory();
        }

        @Override
        public boolean isSynchronous()
        {
            return ClusteredCacheEntryListenerConfiguration.this.isSynchronous();
        }
    }
}
