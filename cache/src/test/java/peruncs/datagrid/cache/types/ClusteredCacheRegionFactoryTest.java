package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.hibernate.cache.CacheException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the region factory health gate and serializer provider resolution.
class ClusteredCacheRegionFactoryTest {
    @Test
    void failClosedStorageAccessRefusesOperationsAfterReceiverFailure() {
        final boolean[] healthy = {true};
        final boolean[] used = {false};
        final StorageAccess delegate = new StorageAccess() {
            @Override
            public Object getFromCache(final Object key, final SharedSessionContractImplementor session) {
                used[0] = true;
                return null;
            }

            @Override
            public void putIntoCache(final Object key, final Object value, final SharedSessionContractImplementor session) {
                used[0] = true;
            }

            @Override
            public boolean contains(final Object key) {
                used[0] = true;
                return false;
            }

            @Override
            public void evictData() {
                used[0] = true;
            }

            @Override
            public void evictData(final Object key) {
                used[0] = true;
            }

            @Override
            public void release() {
                used[0] = true;
            }
        };
        final StorageAccess guarded = new ClusteredCacheRegionFactory.FailClosedStorageAccess(
                delegate, () ->
        {
            if (!healthy[0]) {
                throw new CacheException("receiver failed");
            }
        });

        assertFalse(guarded.contains("key"));
        assertTrue(used[0]);
        used[0] = false;
        healthy[0] = false;
        assertThrows(CacheException.class, () -> guarded.contains("key"));
        assertFalse(used[0], "failed receiver must prevent access to the local timestamps cache");
    }

    @Test
    void resolveSerializationTypesProviderDefaultsWhenUnset() {
        final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

        final SerializationTypesProvider provider = factory.resolveSerializationTypesProvider(null, Map.of());

        assertInstanceOf(SerializationTypesProvider.Default.class, provider);
    }

    @Test
    void resolveSerializationTypesProviderAcceptsPublicConstructor() {
        final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

        final SerializationTypesProvider provider = factory.resolveSerializationTypesProvider(
                null, Map.of(ClusteredConfigurationPropertyNames.SERIALIZATION_TYPES_PROVIDER, PublicTypesProvider.class));

        assertInstanceOf(PublicTypesProvider.class, provider);
    }

    @Test
    void resolveSerializationTypesProviderRejectsWrongClass() {
        final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

        assertThrows(CacheException.class,
                () -> factory.resolveSerializationTypesProvider(null,
                        Map.of(ClusteredConfigurationPropertyNames.SERIALIZATION_TYPES_PROVIDER, String.class)),
                "a configured class with the wrong serialization contract must fail at configuration time");
    }

        /// Types provider with a public no-argument constructor.
    public static final class PublicTypesProvider implements SerializationTypesProvider {
        public PublicTypesProvider() {
        }

        @Override
        public Collection<Class<?>> provideTypes() {
            return List.of();
        }
    }
}
