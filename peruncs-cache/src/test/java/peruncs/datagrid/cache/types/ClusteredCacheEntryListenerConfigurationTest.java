package peruncs.datagrid.cache.types;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageCommunicationProvider;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageSender;
import peruncs.datagrid.cache.aeron.AeronClusteredConfigurationPropertyNames;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static peruncs.datagrid.cache.test.ClusteredCacheTestSupport.publish;
import static peruncs.datagrid.cache.test.ClusteredCacheTestSupport.serializer;

/// Verifies the listener configuration owns its sender and disposes it once.
class ClusteredCacheEntryListenerConfigurationTest {
    private static Map<String, Object> properties() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(AeronClusteredConfigurationPropertyNames.CHANNEL, "aeron:ipc");
        properties.put(AeronClusteredConfigurationPropertyNames.STREAM_ID, "2001");
        properties.put(AeronClusteredConfigurationPropertyNames.EMBEDDED_DRIVER, "true");
        properties.put(AeronClusteredConfigurationPropertyNames.OFFER_TIMEOUT_MILLIS, "10000");
        properties.put(AeronClusteredConfigurationPropertyNames.DRIVER_TIMEOUT_MILLIS, "10000");
        return properties;
    }

    @Test
    void disposeIsIdempotentAndFailsClosed() {
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties(), serializer());
        final ClusteredCacheEntryListenerConfiguration configuration =
                new ClusteredCacheEntryListenerConfiguration(sender);

        configuration.dispose();
        configuration.dispose();

        assertThrows(CacheEntryListenerException.class,
                () -> publish(sender, EventType.CREATED, "cache", "table", 1L),
                "a disposed sender must reject further publishes");
    }

    @Test
    void listenerFactoryReturnsTheSender() {
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties(), serializer());
        final ClusteredCacheEntryListenerConfiguration configuration =
                new ClusteredCacheEntryListenerConfiguration(sender);
        try {
            final CacheEntryListenerConfiguration<Object, Object> listener =
                    configuration.getUpdateTimestampsCacheEntryListenerConfiguration();
            assertSame(sender, listener.getCacheEntryListenerFactory().create());
            assertTrue(listener.isSynchronous(), "clustered invalidations must be synchronous");
            assertFalse(listener.isOldValueRequired());
            assertNotNull(listener.getCacheEntryEventFilterFactory(),
                    "remote timestamp updates must not be rebroadcast");
        } finally {
            configuration.dispose();
        }
    }

    @Test
    void nullSenderIsRejected() {
        assertThrows(NullPointerException.class,
                () -> new ClusteredCacheEntryListenerConfiguration(null));
    }
}
