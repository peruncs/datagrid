package org.eclipse.datagrid.cache.clustered.test;

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

import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.datagrid.cache.clustered.types.TimestampsRegionUpdateMessage;
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.SerializerFoundation;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared fixtures for the clustered-cache adapter tests.
 *
 * <p>The Kafka and Aeron adapter modules depend on this test-jar so both
 * transports exercise the same serializer, event construction, and publish
 * path.</p>
 */
public final class ClusteredCacheTestSupport
{
	private ClusteredCacheTestSupport()
	{
	}

	/** Creates the serializer used by the adapter tests. */
	public static Serializer<byte[]> serializer()
	{
		return Serializer.Bytes(SerializerFoundation.New()
			.registerEntityTypes(TimestampsRegionUpdateMessage.class));
	}

	/** Creates a timestamp-region cache event. */
	public static CacheEntryEvent<Object, Object> event(
		final String cacheName, final EventType eventType, final String tableName, final long timestamp)
	{
		return new TestCacheEntryEvent(new NamedCache(cacheName), eventType, tableName, timestamp);
	}

	/** Creates a cache event with arbitrary key and value, for validation tests. */
	public static CacheEntryEvent<Object, Object> eventWith(
		final String cacheName, final EventType eventType, final Object key, final Object value)
	{
		return new TestCacheEntryEvent(new NamedCache(cacheName), eventType, key, value);
	}

	/** Delivers one event to the sender through its JCache listener path. */
	@SuppressWarnings("unchecked") // The transport sender is selected as both JCache listener kinds by the provider.
	public static void publish(final ClusteredCacheMessageSender<Object, Object> sender, final EventType eventType,
		final String cacheName, final String tableName, final long timestamp)
	{
		final CacheEntryEvent<Object, Object> event = event(cacheName, eventType, tableName, timestamp);
		if (eventType == EventType.UPDATED)
		{
			((CacheEntryUpdatedListener<Object, Object>)sender).onUpdated(List.of(event));
		}
		else
		{
			((CacheEntryCreatedListener<Object, Object>)sender).onCreated(List.of(event));
		}
	}

	/** Cache stub that only answers {@link Cache#getName()}. */
	private static final class NamedCache implements Cache<Object, Object>
	{
		private final String name;

		private NamedCache(final String name)
		{
			this.name = name;
		}

		@Override
		public String getName()
		{
			return this.name;
		}

		@Override
		public Object get(final Object key)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Map<Object, Object> getAll(final Set<?> keys)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsKey(final Object key)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void put(final Object key, final Object value)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Object getAndPut(final Object key, final Object value)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void putAll(final Map<?, ?> entries)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean putIfAbsent(final Object key, final Object value)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(final Object key)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(final Object key, final Object oldValue)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Object getAndRemove(final Object key)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean replace(final Object key, final Object oldValue, final Object newValue)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean replace(final Object key, final Object value)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Object getAndReplace(final Object key, final Object value)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void removeAll(final Set<?> keys)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void removeAll()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public <C extends Configuration<Object, Object>> C getConfiguration(final Class<C> clazz)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> T unwrap(final Class<T> clazz)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void registerCacheEntryListener(final CacheEntryListenerConfiguration<Object, Object> configuration)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void deregisterCacheEntryListener(final CacheEntryListenerConfiguration<Object, Object> configuration)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<javax.cache.Cache.Entry<Object, Object>> iterator()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> T invoke(final Object key, final EntryProcessor<Object, Object, T> entryProcessor,
			final Object... arguments) throws EntryProcessorException
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> Map<Object, EntryProcessorResult<T>> invokeAll(final Set<?> keys,
			final EntryProcessor<Object, Object, T> entryProcessor, final Object... arguments)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void close()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isClosed()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void loadAll(final Set<?> keys, final boolean replaceExistingValues,
			final CompletionListener completionListener)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public javax.cache.CacheManager getCacheManager()
		{
			throw new UnsupportedOperationException();
		}
	}

	/** Minimal cache event used by the sender's listener path. */
	public static final class TestCacheEntryEvent extends CacheEntryEvent<Object, Object>
	{
		private final Object key;
		private final Object value;

		private TestCacheEntryEvent(final Cache<Object, Object> source, final EventType eventType,
			final Object key, final Object value)
		{
			super(source, eventType);
			this.key = key;
			this.value = value;
		}

		@Override
		public Object getKey()
		{
			return this.key;
		}

		@Override
		public Object getValue()
		{
			return this.value;
		}

		@Override
		public Object getOldValue()
		{
			return null;
		}

		@Override
		public boolean isOldValueAvailable()
		{
			return false;
		}

		@Override
		public <T> T unwrap(final Class<T> clazz)
		{
			throw new UnsupportedOperationException();
		}
	}
}
