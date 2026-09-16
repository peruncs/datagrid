package peruncs.datagrid.cache.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/// Proves recovery never discards a peer update published during its drain window.
///
/// Re-synchronization drains buffered frames before invalidating the caches,
/// then starts consumption without a second discard: the wipe covers every
/// drained frame, while anything published after the drain stays buffered and
/// is consumed as the new baseline. Draining after the invalidation would
/// discard such an update and let a later heartbeat become a false baseline,
/// leaving the receiver healthy on stale query results.
@Timeout(90)
class AeronClusteredCacheReceiverRecoveryTest {
    private static MediaDriver launchDriver(final Path root) {
        return MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(root.resolve("driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED));
    }

    private static AeronClusteredCacheConfiguration configuration(
            final Path driverDirectory, final Path cursorDirectory) {
        return new AeronClusteredCacheConfiguration(
                "aeron:ipc", 2001, null, driverDirectory.toString(), false,
                10_000L, 10_000L, 1 << 20,
                100L, 30_000L, cursorDirectory.toString(),
                null, false, false);
    }

    private static void awaitConnected(final Publication publication) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!publication.isConnected() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(100_000L);
        }
        if (!publication.isConnected()) {
            throw new IllegalStateException("raw publication did not connect to the receiver subscription");
        }
    }

    private static void offer(final Publication raw, final ExpandableArrayBuffer frame,
                              final byte[] sender, final String table, final long timestamp, final long sequence) {
        final byte[] payload = AeronClusteredCachePayloadCodec.encode(
                new TimestampsRegionUpdateMessage("timestamps", table, timestamp));
        final int length = AeronClusteredCacheMessageCodec.encode(frame, sender, sequence, payload);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (raw.offer(frame, 0, length) < 0 && System.nanoTime() < deadline) {
            LockSupport.parkNanos(10_000L);
        }
    }

    private static byte[] senderId(final long high, final long low) {
        return ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                .putLong(high).putLong(low).array();
    }

    @Test
    void updatePublishedDuringRecoveryDrainIsDelivered(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"));
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(16);
            final AeronClusteredCacheMessageCommunicationProvider provider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver = provider.provideMessageReceiver(
                    properties,
                    new ClusteredCacheMessageAcceptor(null) {
                        @Override
                        public void accept(final TimestampsRegionUpdateMessage message) {
                            received.add(message);
                        }
                    });
            try {
                receiver.start();
                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()));
                     Publication raw = aeron.addPublication(properties.channel(), properties.streamId())) {
                    awaitConnected(raw);
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(256);
                    final byte[] sender = senderId(41L, 42L);
                    offer(raw, frame, sender, "baseline", 1L, 0L);
                    assertNotNull(received.poll(10, TimeUnit.SECONDS),
                            "the baseline frame must be delivered");
                    /* Skip a sequence so the receiver fails closed and its
                     * polling thread stops leaving buffered frames behind. */
                    offer(raw, frame, sender, "skipped", 2L, 5L);
                    final long failureDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (receiver.failure() == null && System.nanoTime() < failureDeadline) {
                        LockSupport.parkNanos(100_000L);
                    }
                    assertNotNull(receiver.failure(), "the gap must fail the receiver first");
                    assertTrue(received.isEmpty(), "the gap frame must fail the receiver without delivery");
                    /* A stale frame buffered while no thread polled: the
                     * pre-invalidation drain must discard it because the wipe
                     * below already covers it. */
                    offer(raw, frame, sender, "stale", 3L, 6L);
                    /* A peer update published after the drain, while recovery
                     * is still invalidating: it must survive as the new
                     * baseline instead of being discarded as residue. */
                    receiver.resyncAfterDrainHook =
                            () -> offer(raw, frame, sender, "recovered", 4L, 7L);
                    try {
                        final long resyncDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        boolean resynced = false;
                        while (System.nanoTime() < resyncDeadline) {
                            try {
                                receiver.resynchronize();
                                resynced = true;
                                break;
                            } catch (final IllegalStateException stillStopping) {
                                if (!stillStopping.getMessage().contains("still stopping")) {
                                    throw stillStopping;
                                }
                                LockSupport.parkNanos(1_000_000L);
                            }
                        }
                        assertTrue(resynced, "re-synchronization never ran");
                        final TimestampsRegionUpdateMessage delivered =
                                received.poll(10, TimeUnit.SECONDS);
                        assertNotNull(delivered,
                                "an update published during recovery's drain window must not be discarded");
                        assertEquals("recovered", delivered.tableName(),
                                "recovery must deliver the post-drain update, not stale residue");
                        assertTrue(received.isEmpty(),
                                "the pre-drain buffered frame must stay discarded after the wipe");
                        assertNull(receiver.failure());
                        assertTrue(receiver.isRunning(),
                                "the receiver must report healthy once the new baseline is accepted");
                    } finally {
                        receiver.resyncAfterDrainHook = null;
                    }
                }
            } finally {
                receiver.dispose();
            }
        }
    }
}
