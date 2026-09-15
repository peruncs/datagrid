package peruncs.datagrid.cache.aeron;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cache.test.ClusteredCacheTestSupport;
import peruncs.datagrid.cache.types.ClusteredCacheEntryListenerConfiguration;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import javax.cache.configuration.MutableConfiguration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// A remote timestamp applied through a real cache must never be rebroadcast.
///
/// Listener suppression only sees updates applied on the thread running
/// `cache.invoke`. If upstream ever dispatches listeners on another thread,
/// the suppression binding is lost and every received invalidation is
/// re-broadcast into an infinite cluster loop. This test exercises the real
/// cache, the real listener configuration, and the real (unconnected) sender:
/// any publish attempt fails on the sender's missing resources, so the test
/// fails loudly on that day instead of looping silently in production.
class ClusteredCacheRemoteUpdateSuppressionTest {
    @Test
    void remoteUpdateThroughRealCacheIsNotRebroadcast() {
        final var provider = new org.eclipse.store.cache.types.CachingProvider();
        try {
            final var manager = provider.getCacheManager();
            final var cache = manager.createCache("timestamps",
                    new MutableConfiguration<Object, Object>().setTypes(Object.class, Object.class));
            final var sender = AeronClusteredCacheMessageSender.New(
                    null,
                    new byte[16],
                    AeronClusteredCacheSenderSequence.SequenceLease.local(),
                    new Object(),
                    () -> {
                    },
                    ClusteredCacheTestSupport.serializer(),
                    TimeUnit.MINUTES.toNanos(1L),
                    1024);
            cache.registerCacheEntryListener(new ClusteredCacheEntryListenerConfiguration(sender)
                    .getUpdateTimestampsCacheEntryListenerConfiguration());

            new ClusteredCacheMessageAcceptor(manager).accept(
                    new TimestampsRegionUpdateMessage("timestamps", "table", 42L));

            assertEquals(42L, cache.get("table"));
            assertEquals(0L, sender.published(),
                    "a suppressed remote update must never reach the sender");
        } finally {
            provider.close();
        }
    }
}
