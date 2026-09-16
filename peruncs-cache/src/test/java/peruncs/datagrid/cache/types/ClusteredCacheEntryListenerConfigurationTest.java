package peruncs.datagrid.cache.types;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheConfiguration;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageCommunicationProvider;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageSender;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;

import static org.junit.jupiter.api.Assertions.*;
import static peruncs.datagrid.cache.test.ClusteredCacheTestSupport.publish;

/// Verifies the listener configuration owns its sender and disposes it once.
class ClusteredCacheEntryListenerConfigurationTest {
    private static AeronClusteredCacheConfiguration configuration() {
        return new AeronClusteredCacheConfiguration(
                "aeron:ipc", 2001, null, null, true, 10_000L, 10_000L, 1 << 20,
                500L, 5_000L, null, null, false, false);
    }

    @Test
    void disposeIsIdempotentAndFailsClosed() {
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(configuration());
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
                provider.provideUpdateTimestampsCacheMessageSender(configuration());
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
