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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/// Proves persisted/rebasing senders keep a silence deadline and join wipes stay unhealthy.
@Timeout(90)
class AeronClusteredCacheReceiverLivenessTest {
    private static MediaDriver launchDriver(final Path root) {
        return MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(root.resolve("driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED));
    }

    private static AeronClusteredCacheConfiguration configuration(
            final Path driverDirectory, final Path cursorDirectory,
            final long heartbeatIntervalMillis, final long freshnessTimeoutMillis) {
        return new AeronClusteredCacheConfiguration(
                "aeron:ipc", 2001, null, driverDirectory.toString(), false,
                10_000L, 10_000L, 1 << 20,
                heartbeatIntervalMillis, freshnessTimeoutMillis, cursorDirectory.toString(),
                null, false, false);
    }

    private static void awaitConnected(final Publication publication) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!publication.isConnected() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(100_000L);
        }
        if (!publication.isConnected()) {
            throw new IllegalStateException("raw publication did not connect");
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

    private static void preloadCursors(final AeronClusteredCacheConfiguration properties,
                                       final Path cursorDirectory,
                                       final AeronClusteredCacheMessageCodec.SenderId first,
                                       final AeronClusteredCacheMessageCodec.SenderId second) {
        final String namespace =
                AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(properties);
        final AeronClusteredCacheCursorStore store =
                new AeronClusteredCacheCursorStore(cursorDirectory, namespace);
        store.store(java.util.Map.of(first, 0L, second, 0L));
    }

    /// A sender persisted across restart must fail the receiver when it never returns.
    @Test
    void absentPersistedSenderFailsClosedWhileOtherSenderKeepsSending(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final Path cursorDirectory = root.resolve("cursors");
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), cursorDirectory, 50L, 800L);
            /* Preload both cursors so the restart validates continuity instead of
             * hitting the single-sender late-join boundary. */
            final AeronClusteredCacheMessageCodec.SenderId senderA = new AeronClusteredCacheMessageCodec.SenderId(11L, 12L);
            final AeronClusteredCacheMessageCodec.SenderId senderB = new AeronClusteredCacheMessageCodec.SenderId(13L, 14L);
            preloadCursors(properties, cursorDirectory, senderA, senderB);
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(64);
            final AeronClusteredCacheMessageCommunicationProvider provider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver restarted = provider.provideMessageReceiver(
                    properties, new ClusteredCacheMessageAcceptor(null) {
                        @Override
                        public void accept(final TimestampsRegionUpdateMessage message) {
                            received.add(message);
                        }
                    });
            try {
                restarted.start();
                assertTrue(restarted.isRunning());
                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()));
                     Publication raw = aeron.addPublication(properties.channel(), properties.streamId())) {
                    awaitConnected(raw);
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(256);
                    /* Only sender A proves liveness (continuing its persisted
                     * cursor); B stays silent past its deadline. */
                    long sequence = 1L;
                    final byte[] senderABytes = senderId(11L, 12L);
                    final long failureDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (restarted.failure() == null && System.nanoTime() < failureDeadline) {
                        offer(raw, frame, senderABytes, "a", 100L + sequence, sequence);
                        sequence++;
                        LockSupport.parkNanos(50_000_000L);
                    }
                    assertNotNull(restarted.failure(), "absent persisted sender must fail the receiver");
                    assertTrue(restarted.failure().getMessage().contains("stale"),
                            "unexpected failure: " + restarted.failure().getMessage());
                    assertFalse(restarted.isRunning());
                }
            } finally {
                restarted.dispose();
            }
        }
    }

    /// Senders rebased by recovery keep a deadline: silence fails closed.
    @Test
    void absentRebasingSenderFailsClosedAfterResynchronize(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final Path cursorDirectory = root.resolve("cursors");
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), cursorDirectory, 50L, 800L);
            /* Both senders are known before the failure (preloaded cursors), so
             * recovery rebases both with a fresh deadline. */
            preloadCursors(properties, cursorDirectory,
                    new AeronClusteredCacheMessageCodec.SenderId(21L, 22L),
                    new AeronClusteredCacheMessageCodec.SenderId(23L, 24L));
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(64);
            final AeronClusteredCacheMessageCommunicationProvider provider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver = provider.provideMessageReceiver(
                    properties, new ClusteredCacheMessageAcceptor(null) {
                        @Override
                        public void accept(final TimestampsRegionUpdateMessage message) {
                            received.add(message);
                        }
                    });
            try {
                receiver.start();
                final byte[] senderA = senderId(21L, 22L);
                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()));
                     Publication raw = aeron.addPublication(properties.channel(), properties.streamId())) {
                    awaitConnected(raw);
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(256);
                    /* A continues its persisted cursor; B never returns. */
                    offer(raw, frame, senderA, "a", 1L, 1L);
                    assertNotNull(received.poll(10, TimeUnit.SECONDS));
                    /* Force a gap on A so recovery moves both known senders into
                     * the rebase set with a fresh deadline. */
                    offer(raw, frame, senderA, "a", 2L, 5L);
                    final long failureDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (receiver.failure() == null && System.nanoTime() < failureDeadline) {
                        LockSupport.parkNanos(100_000L);
                    }
                    assertNotNull(receiver.failure());
                    /* The failed poller can still be exiting; retry until the
                     * transition runs, mirroring the existing health test. */
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
                    assertTrue(resynced, "resynchronization never ran");
                    /* Neither rebased sender proves liveness again: total silence
                     * fails closed, which also covers the absent-sender case. */
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (receiver.failure() == null && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(50_000_000L);
                    }
                    assertNotNull(receiver.failure(), "absent rebasing sender must fail the receiver");
                    assertFalse(receiver.isRunning());
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    /// Reads must be refused while the first-sender join invalidation is still running.
    @Test
    void joinInvalidationKeepsReceiverUnhealthyUntilBaseline(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 50L, 10_000L);
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AeronClusteredCacheMessageCommunicationProvider provider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final ClusteredCacheMessageAcceptor blocking = new ClusteredCacheMessageAcceptor(null) {
                private final java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);

                @Override
                public void invalidateAll() {
                    if (this.first.compareAndSet(true, false)) {
                        super.invalidateAll();
                        return;
                    }
                    entered.countDown();
                    try {
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    super.invalidateAll();
                }

                @Override
                public void accept(final TimestampsRegionUpdateMessage message) {
                    /* Join baseline payloads need no cache; accept without a manager. */
                }
            };
            final AeronClusteredCacheMessageReceiver receiver =
                    provider.provideMessageReceiver(properties, blocking);
            try {
                receiver.start();
                /* Startup already invalidated once; the next (join) invalidation blocks. */
                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()));
                     Publication raw = aeron.addPublication(properties.channel(), properties.streamId())) {
                    awaitConnected(raw);
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(256);
                    final Thread sender = Thread.ofVirtual().start(() ->
                            offer(raw, frame, senderId(31L, 32L), "late", 9L, 7L));
                    try {
                        assertTrue(entered.await(10, TimeUnit.SECONDS), "join invalidation never started");
                        assertFalse(receiver.isRunning(),
                                "reads must be refused while the join wipe is still running");
                    } finally {
                        release.countDown();
                        sender.join(TimeUnit.SECONDS.toMillis(10));
                    }
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (!receiver.isRunning() && receiver.failure() == null && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(100_000L);
                    }
                    assertTrue(receiver.isRunning(), "receiver must turn healthy after the join baseline");
                }
            } finally {
                release.countDown();
                receiver.dispose();
            }
        }
    }
}
