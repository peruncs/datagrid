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

import org.junit.jupiter.api.Test;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the acceptor applies only newer timestamps and tolerates bad state. */
class ClusteredCacheMessageAcceptorTest
{
	@Test
	void newerTimestampIsApplied()
	{
		final StubCache cache = new StubCache("cache");
		final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

		acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

		assertEquals(42L, cache.entries.get("table"));
	}

	@Test
	void outdatedTimestampIsIgnored()
	{
		final StubCache cache = new StubCache("cache");
		cache.entries.put("table", 100L);
		final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

		acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

		assertEquals(100L, cache.entries.get("table"), "an older timestamp must not move the stored value backwards");
	}

	@Test
	void equalTimestampIsApplied()
	{
		final StubCache cache = new StubCache("cache");
		cache.entries.put("table", 42L);
		final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

		acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

		assertEquals(42L, cache.entries.get("table"));
	}

	@Test
	void unknownCacheIsIgnored()
	{
		final StubCache cache = new StubCache("cache");
		final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

		acceptor.accept(new TimestampsRegionUpdateMessage("other", "table", 42L));

		assertNull(cache.entries.get("table"));
	}

	@Test
	void nonTimestampStoredValueIsIgnored()
	{
		final StubCache cache = new StubCache("cache");
		cache.entries.put("table", "not-a-long");
		final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

		acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

		assertEquals("not-a-long", cache.entries.get("table"));
	}

	@Test
	void missingCacheManagerIsRejected()
	{
		final ClusteredCacheMessageAcceptor acceptor = new ClusteredCacheMessageAcceptor(null);

		assertThrows(IllegalStateException.class,
			() -> acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L)));
	}

	private static ClusteredCacheMessageAcceptor acceptor(final StubCache cache)
	{
		final StubCacheManager manager = new StubCacheManager();
		manager.caches.put(cache.getName(), cache);
		return new ClusteredCacheMessageAcceptor(manager);
	}

	/** Cache manager stub that only answers {@link #getCache(String)}. */
	private static final class StubCacheManager implements org.eclipse.store.cache.types.CacheManager
	{
		private final Map<String, StubCache> caches = new HashMap<>();

		@Override
		@SuppressWarnings("unchecked")
		public <K, V> org.eclipse.store.cache.types.Cache<K, V> getCache(final String cacheName)
		{
			return (org.eclipse.store.cache.types.Cache<K, V>)this.caches.get(cacheName);
		}

		@Override public <K, V> org.eclipse.store.cache.types.Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) { throw new UnsupportedOperationException(); }
		@Override public <K, V, C extends Configuration<K, V>> org.eclipse.store.cache.types.Cache<K, V> createCache(String cacheName, C configuration) { throw new UnsupportedOperationException(); }
		@Override public org.eclipse.store.cache.types.CachingProvider getCachingProvider() { throw new UnsupportedOperationException(); }
		@Override public void removeCache(String cacheName) { throw new UnsupportedOperationException(); }
		@Override public <T> T unwrap(Class<T> clazz) { throw new UnsupportedOperationException(); }
		@Override public URI getURI() { throw new UnsupportedOperationException(); }
		@Override public ClassLoader getClassLoader() { throw new UnsupportedOperationException(); }
		@Override public Properties getProperties() { throw new UnsupportedOperationException(); }
		@Override public Iterable<String> getCacheNames() { throw new UnsupportedOperationException(); }
		@Override public void destroyCache(String cacheName) { throw new UnsupportedOperationException(); }
		@Override public void enableManagement(String cacheName, boolean enabled) { throw new UnsupportedOperationException(); }
		@Override public void enableStatistics(String cacheName, boolean enabled) { throw new UnsupportedOperationException(); }
		@Override public void close() { throw new UnsupportedOperationException(); }
		@Override public boolean isClosed() { throw new UnsupportedOperationException(); }
	}

	/** Cache stub with a working {@link #get} and {@link #putSilent}. */
	private static final class StubCache implements org.eclipse.store.cache.types.Cache<Object, Object>
	{
		private final String name;
		private final Map<Object, Object> entries = new HashMap<>();

		private StubCache(final String name)
		{
			this.name = name;
		}

		@Override public String getName() { return this.name; }
		@Override public Object get(Object key) { return this.entries.get(key); }
		@Override public void putSilent(Object key, Object value) { this.entries.put(key, value); }
		@Override public Map<Object, Object> getAll(Set<?> keys) { throw new UnsupportedOperationException(); }
		@Override public boolean containsKey(Object key) { throw new UnsupportedOperationException(); }
		@Override public void put(Object key, Object value) { throw new UnsupportedOperationException(); }
		@Override public Object getAndPut(Object key, Object value) { throw new UnsupportedOperationException(); }
		@Override public void putAll(Map<?, ?> entries) { throw new UnsupportedOperationException(); }
		@Override public boolean putIfAbsent(Object key, Object value) { throw new UnsupportedOperationException(); }
		@Override public boolean remove(Object key) { throw new UnsupportedOperationException(); }
		@Override public boolean remove(Object key, Object oldValue) { throw new UnsupportedOperationException(); }
		@Override public Object getAndRemove(Object key) { throw new UnsupportedOperationException(); }
		@Override public boolean replace(Object key, Object oldValue, Object newValue) { throw new UnsupportedOperationException(); }
		@Override public boolean replace(Object key, Object value) { throw new UnsupportedOperationException(); }
		@Override public Object getAndReplace(Object key, Object value) { throw new UnsupportedOperationException(); }
		@Override public void removeAll(Set<?> keys) { throw new UnsupportedOperationException(); }
		@Override public void removeAll() { throw new UnsupportedOperationException(); }
		@Override public void clear() { throw new UnsupportedOperationException(); }
		@Override public <C extends Configuration<Object, Object>> C getConfiguration(Class<C> clazz) { throw new UnsupportedOperationException(); }
		@Override public <T> T unwrap(Class<T> clazz) { throw new UnsupportedOperationException(); }
		@Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<Object, Object> configuration) { throw new UnsupportedOperationException(); }
		@Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<Object, Object> configuration) { throw new UnsupportedOperationException(); }
		@Override public Iterator<Entry<Object, Object>> iterator() { throw new UnsupportedOperationException(); }
		@Override public <T> T invoke(Object key, EntryProcessor<Object, Object, T> entryProcessor, Object... arguments) throws EntryProcessorException { throw new UnsupportedOperationException(); }
		@Override public <T> Map<Object, EntryProcessorResult<T>> invokeAll(Set<?> keys, EntryProcessor<Object, Object, T> entryProcessor, Object... arguments) { throw new UnsupportedOperationException(); }
		@Override public void close() { throw new UnsupportedOperationException(); }
		@Override public boolean isClosed() { throw new UnsupportedOperationException(); }
		@Override public void loadAll(Set<?> keys, boolean replaceExistingValues, CompletionListener completionListener) { throw new UnsupportedOperationException(); }
		@Override public org.eclipse.store.cache.types.CacheManager getCacheManager() { throw new UnsupportedOperationException(); }
		@Override public org.eclipse.store.cache.types.CacheConfiguration<Object, Object> getConfiguration() { throw new UnsupportedOperationException(); }
		@Override public long size() { throw new UnsupportedOperationException(); }
		@Override public void putAll(Map<?, ?> entries, boolean batch, boolean synchronous) { throw new UnsupportedOperationException(); }
		@Override public void setManagementEnabled(boolean enabled) { throw new UnsupportedOperationException(); }
		@Override public void setStatisticsEnabled(boolean enabled) { throw new UnsupportedOperationException(); }
		@Override public void evict(Iterable<org.eclipse.serializer.typing.KeyValue<Object, org.eclipse.store.cache.types.CachedValue>> entries) { throw new UnsupportedOperationException(); }
	}
}