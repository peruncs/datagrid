package peruncs.datagrid.cache.types;

import org.junit.jupiter.api.Test;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the acceptor applies only newer timestamps and tolerates bad state.
class ClusteredCacheMessageAcceptorTest {
    private static ClusteredCacheMessageAcceptor acceptor(final StubCache cache) {
        final StubCacheManager manager = new StubCacheManager();
        manager.caches.put(cache.getName(), cache);
        return new ClusteredCacheMessageAcceptor(manager);
    }

    @Test
    void newerTimestampIsApplied() {
        final StubCache cache = new StubCache("cache");
        final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

        acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

        assertEquals(42L, cache.entries.get("table"));
        assertFalse(ClusteredCacheMessageAcceptor.isRemoteUpdate(),
                "remote-update context must not escape the cache invocation");
    }

    @Test
    void outdatedTimestampIsIgnored() {
        final StubCache cache = new StubCache("cache");
        cache.entries.put("table", 100L);
        final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

        acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

        assertEquals(100L, cache.entries.get("table"), "an older timestamp must not move the stored value backwards");
    }

    @Test
    void equalTimestampIsUnchanged() {
        final StubCache cache = new StubCache("cache");
        cache.entries.put("table", 42L);
        final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

        acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

        assertEquals(42L, cache.entries.get("table"));
    }

    @Test
    void unknownCacheIsIgnored() {
        final StubCache cache = new StubCache("cache");
        final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

        acceptor.accept(new TimestampsRegionUpdateMessage("other", "table", 42L));

        assertNull(cache.entries.get("table"));
    }

    @Test
    void nonTimestampStoredValueIsIgnored() {
        final StubCache cache = new StubCache("cache");
        cache.entries.put("table", "not-a-long");
        final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);

        acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L));

        assertEquals("not-a-long", cache.entries.get("table"));
    }

    @Test
    void missingCacheManagerIsRejected() {
        final ClusteredCacheMessageAcceptor acceptor = new ClusteredCacheMessageAcceptor(null);

        assertThrows(IllegalStateException.class,
                () -> acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", 42L)));
    }

    @Test
    void nullMessageIsRejected() {
        final ClusteredCacheMessageAcceptor acceptor = new ClusteredCacheMessageAcceptor(null);

        assertThrows(NullPointerException.class, () -> acceptor.accept(null));
    }

    @Test
    void concurrentDeliveryKeepsTheGreatestTimestamp() throws Exception {
        final StubCache cache = new StubCache("cache");
        final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);
        final int updates = 32;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] threads = new Thread[updates];
        for (int i = 0; i < updates; i++) {
            final long timestamp = i;
            threads[i] = Thread.ofVirtual().name("clustered-cache-acceptor-test-%s".formatted(i)).unstarted(() ->
            {
                try {
                    start.await();
                    acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", timestamp));
                } catch (final Throwable error) {
                    failure.set(error);
                    if (error instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (final Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(5L));
            assertFalse(thread.isAlive(), "timestamp delivery must not deadlock");
        }

        assertNull(failure.get(), "all concurrent timestamp deliveries must complete");
        assertEquals((long) updates - 1L, cache.entries.get("table"),
                "an older concurrent invalidation must never overwrite a newer timestamp");
    }

    @Test
    void remoteInvalidationCannotRegressAConcurrentLocalWrite() throws Exception {
        final StubCache cache = new StubCache("cache");
        final ClusteredCacheMessageAcceptor acceptor = acceptor(cache);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread local = Thread.ofVirtual().name("clustered-cache-local-writer-test").unstarted(() ->
        {
            try {
                start.await();
                for (long timestamp = 1_000L; timestamp < 1_064L; timestamp++) {
                    cache.putSilent("table", timestamp);
                }
            } catch (final Throwable error) {
                failure.set(error);
            }
        });
        final Thread remote = Thread.ofVirtual().name("clustered-cache-remote-writer-test").unstarted(() ->
        {
            try {
                start.await();
                for (long timestamp = 1L; timestamp < 64L; timestamp++) {
                    acceptor.accept(new TimestampsRegionUpdateMessage("cache", "table", timestamp));
                }
            } catch (final Throwable error) {
                failure.set(error);
            }
        });
        local.start();
        remote.start();
        start.countDown();
        local.join(TimeUnit.SECONDS.toMillis(5L));
        remote.join(TimeUnit.SECONDS.toMillis(5L));
        assertFalse(local.isAlive(), "local writer must complete");
        assertFalse(remote.isAlive(), "remote writer must complete");
        assertNull(failure.get(), "concurrent local and remote timestamp writes must complete");
        assertEquals(1_063L, cache.entries.get("table"),
                "the atomic max operation must preserve the greatest local timestamp");
    }

        /// Cache manager stub that only answers [#getCache(String)].
    private static final class StubCacheManager implements org.eclipse.store.cache.types.CacheManager {
        private final Map<String, StubCache> caches = new HashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> org.eclipse.store.cache.types.Cache<K, V> getCache(final String cacheName) {
            return (org.eclipse.store.cache.types.Cache<K, V>) this.caches.get(cacheName);
        }

        @Override
        public <K, V> org.eclipse.store.cache.types.Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K, V, C extends Configuration<K, V>> org.eclipse.store.cache.types.Cache<K, V> createCache(String cacheName, C configuration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.eclipse.store.cache.types.CachingProvider getCachingProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeCache(String cacheName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getURI() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClassLoader getClassLoader() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Properties getProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<String> getCacheNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroyCache(String cacheName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableManagement(String cacheName, boolean enabled) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableStatistics(String cacheName, boolean enabled) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClosed() {
            throw new UnsupportedOperationException();
        }
    }

        /// Cache stub with a working [#get] and [#putSilent].
    private static final class StubCache implements org.eclipse.store.cache.types.Cache<Object, Object> {
        private final String name;
        private final Map<Object, Object> entries = new ConcurrentHashMap<>();

        private StubCache(final String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public Object get(Object key) {
            return this.entries.get(key);
        }

        @Override
        public synchronized void putSilent(Object key, Object value) {
            this.entries.put(key, value);
        }

        @Override
        public Map<Object, Object> getAll(Set<?> keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsKey(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getAndPut(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<?, ?> entries) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean putIfAbsent(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object key, Object oldValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getAndRemove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean replace(Object key, Object oldValue, Object newValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean replace(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getAndReplace(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeAll(Set<?> keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeAll() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Configuration<Object, Object>> C getConfiguration(Class<C> clazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerCacheEntryListener(CacheEntryListenerConfiguration<Object, Object> configuration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<Object, Object> configuration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Entry<Object, Object>> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized <T> T invoke(Object key, EntryProcessor<Object, Object, T> entryProcessor, Object... arguments) throws EntryProcessorException {
            final MutableEntry<Object, Object> entry = new MutableEntry<>() {
                @Override
                public Object getKey() {
                    return key;
                }

                @Override
                public Object getValue() {
                    return StubCache.this.entries.get(key);
                }

                @Override
                public boolean exists() {
                    return StubCache.this.entries.containsKey(key);
                }

                @Override
                public void remove() {
                    StubCache.this.entries.remove(key);
                }

                @Override
                public void setValue(final Object value) {
                    StubCache.this.entries.put(key, value);
                }

                @Override
                public <T> T unwrap(final Class<T> clazz) {
                    throw new IllegalArgumentException("unsupported unwrap: " + clazz.getName());
                }
            };
            return entryProcessor.process(entry, arguments);
        }

        @Override
        public <T> Map<Object, EntryProcessorResult<T>> invokeAll(Set<?> keys, EntryProcessor<Object, Object, T> entryProcessor, Object... arguments) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClosed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void loadAll(Set<?> keys, boolean replaceExistingValues, CompletionListener completionListener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.eclipse.store.cache.types.CacheManager getCacheManager() {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.eclipse.store.cache.types.CacheConfiguration<Object, Object> getConfiguration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<?, ?> entries, boolean batch, boolean synchronous) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setManagementEnabled(boolean enabled) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setStatisticsEnabled(boolean enabled) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evict(Iterable<org.eclipse.serializer.typing.KeyValue<Object, org.eclipse.store.cache.types.CachedValue>> entries) {
            throw new UnsupportedOperationException();
        }
    }
}
