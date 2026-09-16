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

import javax.cache.event.EventType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;
import static peruncs.datagrid.cache.test.ClusteredCacheTestSupport.publish;

/// Proves the receiver health checks that the polling loop alone cannot cover:
/// a failure landing mid-resynchronization must not report healthy, an
/// over-long cache application is suspect even while the poller is stalled
/// inside it, an oversized cursor file fails closed at startup, and cursor
/// namespaces discriminate hosts and channels instead of colliding.
@Timeout(90)
class AeronClusteredCacheReceiverHealthTest {
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
    void resynchronizeRechecksHealthAfterTransitions(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 100L, 30_000L);
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
                    /* Establish a sender baseline, then skip a sequence so the
                     * receiver can distinguish a real gap from a late join.
                     * Unknown senders are intentionally re-based after a full
                     * invalidation rather than rejected on their first frame. */
                    final byte[] sender = senderId(1L, 2L);
                    offer(raw, frame, sender, "baseline", 1L, 0L);
                    assertNotNull(received.poll(10, TimeUnit.SECONDS),
                            "the baseline frame must be delivered");
                    offer(raw, frame, sender, "table", 2L, 5L);
                    final long failureDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (receiver.failure() == null && System.nanoTime() < failureDeadline) {
                        LockSupport.parkNanos(100_000L);
                    }
                    assertNotNull(receiver.failure(), "the gap must fail the receiver first");
                    assertTrue(received.isEmpty(), "the gap frame must fail the receiver without delivery");

                    /* A failure landing after the transitions — after the
                     * failure latch was cleared and the new thread started —
                     * must not leave the receiver reporting healthy. */
                    receiver.resyncTransitionHook = () -> receiver.failClosed(
                            "injected mid-resync failure",
                            new IllegalStateException("injected mid-resync failure"));
                    try {
                        /* The failed poller thread can still be exiting when the
                         * failure becomes visible; resynchronize reports that as
                         * a retryable "still stopping" state, so retry until the
                         * transition runs instead of asserting on that race. */
                        final long resyncDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        IllegalStateException rejected = null;
                        while (System.nanoTime() < resyncDeadline) {
                            try {
                                receiver.resynchronize();
                                fail("a failure during re-synchronization must refuse a healthy report");
                            } catch (final IllegalStateException failure) {
                                if (failure.getMessage().contains("still stopping")) {
                                    LockSupport.parkNanos(1_000_000L);
                                    continue;
                                }
                                rejected = failure;
                                break;
                            }
                        }
                        assertNotNull(rejected, "re-synchronization never reached the injected failure");
                        assertTrue(rejected.getMessage().contains("during re-synchronization"),
                                "unexpected failure: " + rejected.getMessage());
                        assertNotNull(rejected.getCause(), "the racing failure must be the cause");
                        assertTrue(rejected.getCause().getMessage().contains("injected"),
                                "unexpected cause: " + rejected.getCause().getMessage());
                        assertFalse(receiver.isRunning(), "a failed re-synchronization must not report healthy");
                        assertNotNull(receiver.failure(), "the racing failure must stay observable");
                    } finally {
                        receiver.resyncTransitionHook = null;
                    }
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void overLongApplyIsSuspectWhileThePollerIsStalled(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 50L, 400L);
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver = receiverProvider.provideMessageReceiver(
                    properties,
                    new ClusteredCacheMessageAcceptor(null) {
                        @Override
                        public void accept(final TimestampsRegionUpdateMessage message) {
                            entered.countDown();
                            boolean interrupted = false;
                            for (; ; ) {
                                try {
                                    if (release.await(100L, TimeUnit.MILLISECONDS)) {
                                        break;
                                    }
                                } catch (final InterruptedException failure) {
                                    interrupted = true;
                                }
                            }
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    });
            try {
                receiver.start();
                final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageSender sender =
                        senderProvider.provideUpdateTimestampsCacheMessageSender(properties);
                try {
                    publish(sender, EventType.CREATED, "timestamps", "table", 1L);
                    assertTrue(entered.await(10, TimeUnit.SECONDS), "the receiver never entered the apply");

                    /* The polling thread is stuck inside the acceptor, so only
                     * the apply stamp can mark it suspect. */
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (receiver.isRunning() && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(1_000_000L);
                    }
                    assertFalse(receiver.isRunning(),
                            "an apply running past the freshness timeout must report unhealthy");
                    assertNotNull(receiver.failure(), "the suspect apply must record a terminal failure");
                    assertTrue(receiver.failure().getMessage().contains("did not complete"),
                            "unexpected failure: " + receiver.failure().getMessage());
                } finally {
                    release.countDown();
                    sender.dispose();
                }
            } finally {
                release.countDown();
                receiver.dispose();
            }
        }
    }

    @Test
    void oversizedCursorFileFailsClosedAtStartup(@TempDir final Path root) {
        try (MediaDriver driver = launchDriver(root)) {
            final Path cursorDirectory = root.resolve("cursors");
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), cursorDirectory, 100L, 30_000L);
            final String namespace =
                    AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(properties);
            final AeronClusteredCacheCursorStore store =
                    new AeronClusteredCacheCursorStore(cursorDirectory, namespace);
            final Map<AeronClusteredCacheMessageCodec.SenderId, Long> persisted = new HashMap<>();
            for (long index = 0; index < AeronClusteredCacheMessageReceiver.MAX_TRACKED_SENDERS + 6L; index++) {
                persisted.put(new AeronClusteredCacheMessageCodec.SenderId(index + 1L, index + 2L), index);
            }
            store.store(persisted);

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
                final IllegalStateException failure = assertThrows(IllegalStateException.class, receiver::start);
                assertTrue(failure.getMessage().contains("maximum sender identity count"));
                assertTrue(received.isEmpty(), "no frame was offered, so nothing may be delivered");
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void cursorNamespaceHashesTheChannelInsteadOfTruncating() {
        final String prefix = "aeron:udp?endpoint=" + "a".repeat(100);
        final AeronClusteredCacheConfiguration first = withChannel(
                AeronClusteredCacheConfiguration.defaults(), prefix + ":40123|control-mode=dynamic");
        final AeronClusteredCacheConfiguration second = withChannel(
                AeronClusteredCacheConfiguration.defaults(), prefix + ":40124|control-mode=dynamic");

        final String firstNamespace = AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(first);
        final String secondNamespace = AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(second);
        assertNotEquals(firstNamespace, secondNamespace,
                "channels sharing a 64-character prefix must not share a cursor file");
        assertEquals(firstNamespace,
                AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(first),
                "the namespace must be stable");
        assertTrue(firstNamespace.matches("[A-Za-z0-9_]+"),
                "the namespace must stay file-safe: " + firstNamespace);

        final UUID node = UUID.randomUUID();
        final AeronClusteredCacheConfiguration noded = withNodeId(first, node);
        final AeronClusteredCacheConfiguration otherNoded = withNodeId(first, UUID.randomUUID());
        assertNotEquals(firstNamespace,
                AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(noded),
                "a configured node id must discriminate the namespace");
        assertNotEquals(AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(noded),
                AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(otherNoded),
                "different node ids must not share a cursor file");
        assertTrue(AeronClusteredCacheMessageCommunicationProvider.cursorNamespace(noded).contains(node.toString()),
                "the node id must be visible in its namespace");
    }

    @Test
    void defaultCursorDirectoryCarriesTheHostDiscriminator() {
        final Path configured = Path.of("/tmp/cursors");
        assertEquals(configured, AeronClusteredCacheMessageCommunicationProvider.cursorDirectory(
                withCursorDirectory(AeronClusteredCacheConfiguration.defaults(), "/tmp/cursors")));

        final Path fallback = AeronClusteredCacheMessageCommunicationProvider.cursorDirectory(
                AeronClusteredCacheConfiguration.defaults());
        final String directory = fallback.getFileName().toString();
        assertTrue(directory.startsWith("peruncs-datagrid-cache-cursors-"),
                "the shared temporary fallback must carry a discriminator: " + directory);
        assertNotEquals("peruncs-datagrid-cache-cursors", directory,
                "the bare shared directory name must be gone");
    }

    private static AeronClusteredCacheConfiguration withChannel(
            final AeronClusteredCacheConfiguration base, final String channel) {
        return new AeronClusteredCacheConfiguration(
                channel, base.streamId(), base.nodeId(), base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), base.maxPayloadBytes(),
                base.heartbeatIntervalMillis(), base.freshnessTimeoutMillis(), base.cursorDirectory(),
                base.hmacSecret(), base.productionMode(), base.allowUnsignedFrames());
    }

    private static AeronClusteredCacheConfiguration withNodeId(
            final AeronClusteredCacheConfiguration base, final UUID nodeId) {
        return new AeronClusteredCacheConfiguration(
                base.channel(), base.streamId(), nodeId, base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), base.maxPayloadBytes(),
                base.heartbeatIntervalMillis(), base.freshnessTimeoutMillis(), base.cursorDirectory(),
                base.hmacSecret(), base.productionMode(), base.allowUnsignedFrames());
    }

    private static AeronClusteredCacheConfiguration withCursorDirectory(
            final AeronClusteredCacheConfiguration base, final String cursorDirectory) {
        return new AeronClusteredCacheConfiguration(
                base.channel(), base.streamId(), base.nodeId(), base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), base.maxPayloadBytes(),
                base.heartbeatIntervalMillis(), base.freshnessTimeoutMillis(), cursorDirectory,
                base.hmacSecret(), base.productionMode(), base.allowUnsignedFrames());
    }
}
