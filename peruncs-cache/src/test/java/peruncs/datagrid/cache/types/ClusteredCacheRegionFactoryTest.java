package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.hibernate.cache.CacheException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the region factory health gate and configuration translation.
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
    void clusteredCacheConfigurationTranslatesHibernateKeys() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.channel", "aeron:ipc");
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.stream-id", "2001");

        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties);

        assertEquals("aeron:ipc", configuration.channel());
        assertEquals(2001, configuration.streamId());
        assertNull(configuration.nodeId());
    }

        /// The fixed payload schema removed the serializer type provider, so the
    /// old key must fail loudly instead of being silently ignored.
    @Test
    void clusteredCacheConfigurationRejectsRemovedSerializationTypesProviderKey() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.serialization-types-provider", "configured");

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties));
        assertTrue(failure.getMessage().contains("serialization-types-provider"),
                "the failure must name the removed key: " + failure.getMessage());
    }

    @Test
    void clusteredCacheConfigurationNormalizesAndRejectsValues() {
        final UUID nodeId = UUID.randomUUID();
        final Map<String, Object> normalized = new HashMap<>();
        normalized.put("hibernate.cache.eclipsestore.clustered.aeron.node-id", "  %s  ".formatted(nodeId));
        normalized.put("hibernate.cache.eclipsestore.clustered.aeron.embedded-driver", "TRUE");

        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(normalized);

        assertEquals(nodeId, configuration.nodeId());
        assertTrue(configuration.embeddedDriver());

        final Map<String, Object> badBoolean = new HashMap<>();
        badBoolean.put("hibernate.cache.eclipsestore.clustered.aeron.embedded-driver", "maybe");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(badBoolean));

        final Map<String, Object> badNodeId = new HashMap<>();
        badNodeId.put("hibernate.cache.eclipsestore.clustered.aeron.node-id", "not-a-uuid");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(badNodeId),
                "an invalid node id must be rejected");

        final Map<String, Object> badStream = new HashMap<>();
        badStream.put("hibernate.cache.eclipsestore.clustered.aeron.stream-id", "-1");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(badStream));
    }
}
