package peruncs.datagrid.cache.aeron;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cache.types.ClusteredCacheEntryListenerConfiguration;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import javax.cache.configuration.MutableConfiguration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// A remote timestamp applied through a real cache must never be rebroadcast.
///
/// Remote updates are applied through a JCache `EntryProcessor`, which
/// dispatches cache events to registered listeners on the invoking thread. The
/// acceptor wraps that invocation in a `ScopedValue` binding, and the listener
/// filter suppresses events while the binding is active. This test exercises the
/// real cache, the real listener configuration, and the real (unconnected)
/// sender: if the filter ever fails to suppress the remote update, the sender is
/// invoked and the test fails instead of the cluster rebroadcasting silently.
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
