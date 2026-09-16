package peruncs.datagrid.cache.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ExpandableArrayBuffer;
import org.eclipse.store.cache.types.CacheManager;
import org.eclipse.store.cache.types.CachingProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.EventType;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;
import static peruncs.datagrid.cache.test.ClusteredCacheTestSupport.publish;

/// Proves the cache-durability guarantees over a real embedded MediaDriver.
///
/// Publication acceptance only proves admission, so these tests exercise what
/// happens afterwards: silence past the freshness deadline must mark the
/// receiver stale instead of silently healthy, a restart must validate its
/// first sequence against the persisted cursor, and a re-synchronized
/// receiver must serve reads again on a fresh baseline. Deadlines are short
/// and every wait is bounded.
@Timeout(90)
class AeronClusteredCacheDurabilityTest {
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

    private static org.eclipse.store.cache.types.Cache<Object, Object> timestampsCache(
            final CacheManager manager) {
        return manager.createCache("timestamps",
                new MutableConfiguration<>().setTypes(Object.class, Object.class));
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

    private static void awaitStale(final AeronClusteredCacheMessageReceiver receiver) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((receiver.isRunning() || receiver.failure() == null) && System.nanoTime() < deadline) {
            LockSupport.parkNanos(1_000_000L);
        }
        assertFalse(receiver.isRunning(), "silence past the deadline must mark the receiver unhealthy");
        assertNotNull(receiver.failure(), "staleness must record a terminal failure");
        assertTrue(receiver.failure().getMessage().toLowerCase(Locale.ROOT).contains("stale"),
                "unexpected staleness failure: " + receiver.failure().getMessage());
    }

    private static void awaitTerminalFailure(
            final AeronClusteredCacheMessageReceiver receiver, final String expectedSnippet) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((receiver.isRunning() || receiver.failure() == null) && System.nanoTime() < deadline) {
            LockSupport.parkNanos(1_000_000L);
        }
        assertFalse(receiver.isRunning(), "a terminal failure must stop the receiver");
        assertNotNull(receiver.failure(), "a terminal failure must be recorded");
        assertTrue(receiver.failure().getMessage().toLowerCase(Locale.ROOT).contains(expectedSnippet),
                "unexpected failure: " + receiver.failure().getMessage());
    }

    private static void awaitGap(final AeronClusteredCacheMessageReceiver receiver) {
        awaitTerminalFailure(receiver, "gap");
    }

        /// One persisted sender cursor: the wire identity and applied sequence.
    private record Cursor(byte[] senderId, long sequence) {
    }

        /// Reads the single persisted sender cursor from the cursor directory.
    private static Cursor readCursor(final Path cursorDirectory) throws Exception {
        final Path file;
        try (var stream = Files.list(cursorDirectory)) {
            file = stream.findFirst().orElseThrow(
                    () -> new IllegalStateException("no cursor file in " + cursorDirectory));
        }
        final java.util.Properties stored = new java.util.Properties();
        try (var reader = Files.newBufferedReader(file)) {
            stored.load(reader);
        }
        final String key = stored.stringPropertyNames().iterator().next();
        final int separator = key.indexOf('-');
        final byte[] senderId = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                .putLong(Long.parseUnsignedLong(key.substring(0, separator), 16))
                .putLong(Long.parseUnsignedLong(key.substring(separator + 1), 16))
                .array();
        return new Cursor(senderId, Long.parseLong(stored.getProperty(key)));
    }

        /// Publishes one raw invalidation frame with an explicit sequence.
    private static void offerRaw(final Publication raw, final byte[] senderId,
                                 final String table, final long timestamp, final long sequence) {
        final byte[] payload = AeronClusteredCachePayloadCodec.encode(
                new TimestampsRegionUpdateMessage("timestamps", table, timestamp));
        final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(256);
        final int length = AeronClusteredCacheMessageCodec.encode(frame, senderId, sequence, payload);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (raw.offer(frame, 0, length) < 0 && System.nanoTime() < deadline) {
            LockSupport.parkNanos(10_000L);
        }
    }

    private static void resynchronizeWithRetry(final AeronClusteredCacheMessageReceiver receiver) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (; ; ) {
            try {
                receiver.resynchronize();
                return;
            } catch (final IllegalStateException retryable) {
                if (!retryable.getMessage().contains("still stopping")) {
                    throw retryable;
                }
                if (System.nanoTime() >= deadline) {
                    fail("the stopping receiver thread did not exit for re-synchronization");
                }
                LockSupport.parkNanos(10_000_000L);
            }
        }
    }

    @Test
    void silencePastDeadlineMarksReceiverStale(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 50L, 400L);
            try (var provider = new CachingProvider()) {
                final CacheManager manager =
                        provider.getCacheManager(new URI("eclipsestore-durability-silence"), null);
                timestampsCache(manager);
                final RecordingAcceptor acceptor = new RecordingAcceptor(manager);
                final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageReceiver receiver =
                        receiverProvider.provideMessageReceiver(properties, acceptor);
                receiver.start();
                try {
                    assertEquals(1, acceptor.invalidations.get(),
                            "startup must invalidate before declaring the receiver healthy");

                    final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                            new AeronClusteredCacheMessageCommunicationProvider();
                    final AeronClusteredCacheMessageSender sender =
                            senderProvider.provideUpdateTimestampsCacheMessageSender(properties);
                    try {
                        publish(sender, EventType.CREATED, "timestamps", "table", 1L);
                        assertNotNull(acceptor.received.poll(10, TimeUnit.SECONDS));
                        assertTrue(receiver.isRunning());
                    } finally {
                        /* Stops both data and heartbeats: from here no frame of
                         * any kind can arrive, although nothing fails loudly. */
                        sender.dispose();
                    }

                    awaitStale(receiver);
                    assertTrue(acceptor.received.isEmpty(),
                            "no phantom invalidation may appear out of silence");
                    assertEquals(2, acceptor.invalidations.get(),
                            "going stale must invalidate the caches it can no longer vouch for");
                } finally {
                    receiver.dispose();
                }
            }
        }
    }

    @Test
    void heartbeatsKeepIdleReceiverHealthyUntilTheyStop(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 50L, 600L);
            try (var provider = new CachingProvider()) {
                final CacheManager manager =
                        provider.getCacheManager(new URI("eclipsestore-durability-heartbeat"), null);
                timestampsCache(manager);
                final RecordingAcceptor acceptor = new RecordingAcceptor(manager);
                final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageReceiver receiver =
                        receiverProvider.provideMessageReceiver(properties, acceptor);
                receiver.start();
                try {
                    final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                            new AeronClusteredCacheMessageCommunicationProvider();
                    final AeronClusteredCacheMessageSender sender =
                            senderProvider.provideUpdateTimestampsCacheMessageSender(properties);
                    try {
                        /* No data is ever published: heartbeats alone must
                         * prove liveness across several freshness windows. */
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (receiver.heartbeats() < 3L && System.nanoTime() < deadline) {
                            LockSupport.parkNanos(1_000_000L);
                        }
                        assertTrue(receiver.heartbeats() >= 3L, "idle heartbeats did not arrive");
                        assertTrue(receiver.isRunning(), "heartbeats must keep an idle receiver healthy");
                        assertTrue(acceptor.received.isEmpty(), "heartbeats carry no invalidation");
                    } finally {
                        sender.dispose();
                    }

                    awaitStale(receiver);
                } finally {
                    receiver.dispose();
                }
            }
        }
    }

    @Test
    void aLateSenderFailsTheReceiverUntilItIsResynchronized(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 50L, 400L);
            try (var provider = new CachingProvider()) {
                final CacheManager manager =
                        provider.getCacheManager(new URI("eclipsestore-durability-peer-loss"), null);
                timestampsCache(manager);
                final RecordingAcceptor acceptor = new RecordingAcceptor(manager);
                final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageReceiver receiver =
                        receiverProvider.provideMessageReceiver(properties, acceptor);
                receiver.start();
                try {
                    final AeronClusteredCacheMessageCommunicationProvider quietSenderProvider =
                            new AeronClusteredCacheMessageCommunicationProvider();
                    final AeronClusteredCacheMessageSender quietSender =
                            quietSenderProvider.provideUpdateTimestampsCacheMessageSender(properties);
                    try {
                        /* Sender A publishes one frame and then goes silent. */
                        publish(quietSender, EventType.CREATED, "timestamps", "quiet", 1L);
                        assertNotNull(acceptor.received.poll(10, TimeUnit.SECONDS));
                    } finally {
                        quietSender.dispose();
                    }

                    /* A second sender arriving after the first join boundary
                     * cannot establish a second continuity window. Its first
                     * heartbeat fails closed until an explicit resynchronization
                     * is performed. */
                    final AeronClusteredCacheMessageCommunicationProvider liveSenderProvider =
                            new AeronClusteredCacheMessageCommunicationProvider();
                    final AeronClusteredCacheMessageSender liveSender =
                            liveSenderProvider.provideUpdateTimestampsCacheMessageSender(properties);
                    try {
                        awaitTerminalFailure(receiver, "unknown sender");
                        assertTrue(acceptor.received.isEmpty(),
                                "a late sender must not admit phantom traffic");
                    } finally {
                        liveSender.dispose();
                    }
                } finally {
                    receiver.dispose();
                }
            }
        }
    }

    @Test
    void restartWithPersistedCursorDetectsGapAndInvalidates(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final Path driverDirectory = root.resolve("driver");
            final AeronClusteredCacheConfiguration properties =
                    configuration(driverDirectory, root.resolve("cursors"), 100L, 30_000L);
            try (var provider = new CachingProvider()) {
                final CacheManager firstManager =
                        provider.getCacheManager(new URI("eclipsestore-durability-restart-first"), null);
                timestampsCache(firstManager);
                final RecordingAcceptor firstAcceptor = new RecordingAcceptor(firstManager);
                final AeronClusteredCacheMessageCommunicationProvider firstReceiverProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageReceiver firstReceiver =
                        firstReceiverProvider.provideMessageReceiver(properties, firstAcceptor);
                firstReceiver.start();

                final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageSender sender =
                        senderProvider.provideUpdateTimestampsCacheMessageSender(properties);
                try {
                    publish(sender, EventType.CREATED, "timestamps", "before-restart", 1L);
                    assertNotNull(firstAcceptor.received.poll(10, TimeUnit.SECONDS));

                    /* Keeps the sender connected while no receiver is polling,
                     * so the missed publishes below are really accepted. */
                    try (Aeron dummy = Aeron.connect(new Aeron.Context()
                            .aeronDirectoryName(driverDirectory.toString()));
                         Subscription keepalive = dummy.addSubscription(
                                 properties.channel(), properties.streamId())) {
                        /* Disposal persists the cursor for what was applied. */
                        firstReceiver.dispose();

                        /* The persisted cursor names the sender identity and the
                         * applied sequence. A raw frame carrying that identity
                         * and a sequence several ahead of the cursor forces the
                         * gap deterministically: whether or not the new
                         * subscription can still read the retained term, the
                         * frame is never adjacent to the persisted baseline. */
                        final Cursor cursor = readCursor(root.resolve("cursors"));
                        try (Publication raw = dummy.addPublication(properties.channel(), properties.streamId())) {
                            final long rawDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (!raw.isConnected() && System.nanoTime() < rawDeadline) {
                                LockSupport.parkNanos(1_000_000L);
                            }
                            assertTrue(raw.isConnected(), "the raw publication did not connect");

                            final CacheManager secondManager =
                                    provider.getCacheManager(new URI("eclipsestore-durability-restart-second"), null);
                            final var secondCache = timestampsCache(secondManager);
                            secondCache.put("stale-entry", 999L);
                            final RecordingAcceptor secondAcceptor = new RecordingAcceptor(secondManager);
                            final AeronClusteredCacheMessageCommunicationProvider secondReceiverProvider =
                                    new AeronClusteredCacheMessageCommunicationProvider();
                            final AeronClusteredCacheMessageReceiver secondReceiver =
                                    secondReceiverProvider.provideMessageReceiver(properties, secondAcceptor);
                            try {
                                secondReceiver.start();
                                assertNull(secondCache.get("stale-entry"),
                                        "startup must invalidate before declaring the receiver healthy");
                                assertEquals(1, secondAcceptor.invalidations.get());

                                secondCache.put("stale-entry", 999L);
                                offerRaw(raw, cursor.senderId(), "after-restart", 4L, cursor.sequence() + 5L);
                                awaitGap(secondReceiver);
                                assertTrue(secondAcceptor.received.isEmpty(),
                                        "the post-gap frame must be rejected before cache application");
                                assertNull(secondCache.get("stale-entry"),
                                        "a persisted-cursor gap must trigger a full invalidation");
                                assertEquals(2, secondAcceptor.invalidations.get());
                                assertTrue(secondReceiver.gaps() >= 1L);
                                /* The production health gate performs exactly this
                                 * bounded recovery: invalidate first, then restart
                                 * the polling thread, so a gap never leaves the
                                 * cache permanently refused. */
                                resynchronizeWithRetry(secondReceiver);
                                assertTrue(secondReceiver.isRunning(),
                                        "re-synchronization must restore the receiver");
                                assertNull(secondReceiver.failure());
                                assertEquals(3, secondAcceptor.invalidations.get(),
                                        "re-synchronization must invalidate before serving again");
                            } finally {
                                secondReceiver.dispose();
                            }
                        }
                    }
                } finally {
                    sender.dispose();
                    firstReceiver.dispose();
                }
            }
        }
    }

    @Test
    void freshnessRestoredAfterResync(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 50L, 400L);
            try (var provider = new CachingProvider()) {
                final CacheManager manager =
                        provider.getCacheManager(new URI("eclipsestore-durability-resync"), null);
                timestampsCache(manager);
                final RecordingAcceptor acceptor = new RecordingAcceptor(manager);
                final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageReceiver receiver =
                        receiverProvider.provideMessageReceiver(properties, acceptor);
                receiver.start();
                try {
                    final AeronClusteredCacheMessageCommunicationProvider firstSenderProvider =
                            new AeronClusteredCacheMessageCommunicationProvider();
                    final AeronClusteredCacheMessageSender firstSender =
                            firstSenderProvider.provideUpdateTimestampsCacheMessageSender(properties);
                    try {
                        publish(firstSender, EventType.CREATED, "timestamps", "before-silence", 1L);
                        assertNotNull(acceptor.received.poll(10, TimeUnit.SECONDS));
                    } finally {
                        /* The sender is gone, so nothing can arrive anymore. */
                        firstSender.dispose();
                    }
                    awaitStale(receiver);
                    assertEquals(2, acceptor.invalidations.get());

                    /* A replacement sender has a fresh identity starting at
                     * zero; after re-synchronization its stream is accepted.
                     * Its heartbeat interval is long so no beat can slip in
                     * before the first data frame. */
                    final AeronClusteredCacheConfiguration secondSenderProperties = configuration(
                            root.resolve("driver"), root.resolve("cursors"), 60_000L, 61_000L);
                    final AeronClusteredCacheMessageCommunicationProvider secondSenderProvider =
                            new AeronClusteredCacheMessageCommunicationProvider();
                    final AeronClusteredCacheMessageSender secondSender =
                            secondSenderProvider.provideUpdateTimestampsCacheMessageSender(
                                    secondSenderProperties);
                    try {
                        resynchronizeWithRetry(receiver);
                        assertTrue(receiver.isRunning(), "re-synchronization must restore health");
                        assertNull(receiver.failure(), "re-synchronization must clear the failure");
                        assertEquals(3, acceptor.invalidations.get(),
                                "re-synchronization must invalidate before declaring health");

                        publish(secondSender, EventType.CREATED, "timestamps", "after-resync", 2L);
                        final TimestampsRegionUpdateMessage resumed =
                                acceptor.received.poll(10, TimeUnit.SECONDS);
                        assertNotNull(resumed, "the post-resync stream must be accepted");
                        assertEquals("after-resync", resumed.tableName());
                        assertTrue(receiver.isRunning());
                    } finally {
                        secondSender.dispose();
                    }
                } finally {
                    receiver.dispose();
                }
            }
        }
    }

    @Test
    void resyncRebaselinesKnownSendersAfterGap(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 100L, 30_000L);
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new LinkedBlockingQueue<>();
            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver = receiverProvider.provideMessageReceiver(
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
                        .aeronDirectoryName(root.resolve("driver").toString()))) {
                    final Publication raw = aeron.addPublication(
                            properties.channel(), properties.streamId());
                    awaitConnected(raw);
                    final byte[] otherSender = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                            .putLong(0x0102030405060708L).putLong(0x1112131415161718L).array();
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(256);

                    offer(raw, frame, otherSender, "first", 1L, 0L);
                    assertNotNull(received.poll(10, TimeUnit.SECONDS));
                    /* Skip a sequence: the receiver fails closed and its
                     * polling thread dies with frames still arriving. */
                    offer(raw, frame, otherSender, "skipped", 2L, 2L);
                    awaitGap(receiver);

                    /* Traffic missed while no polling thread runs. */
                    offer(raw, frame, otherSender, "missed-while-down", 3L, 3L);
                    offer(raw, frame, otherSender, "missed-while-down-2", 4L, 4L);

                    /* The invalidation covered the gap, so the buffered frames
                     * are discarded and the next live frame re-baselines. */
                    resynchronizeWithRetry(receiver);
                    assertTrue(receiver.isRunning());
                    offer(raw, frame, otherSender, "after-resync", 5L, 5L);
                    final TimestampsRegionUpdateMessage resumed = received.poll(10, TimeUnit.SECONDS);
                    assertNotNull(resumed, "the post-resync frame must be accepted on the new baseline");
                    assertEquals("after-resync", resumed.tableName());
                    assertTrue(receiver.isRunning(), "the re-baselined stream must stay healthy");
                    assertNull(receiver.failure());
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    private static void offer(
            final Publication raw,
            final ExpandableArrayBuffer frame,
            final byte[] sender,
            final String table,
            final long timestamp,
            final long sequence) {
        final byte[] payload = AeronClusteredCachePayloadCodec.encode(
                new TimestampsRegionUpdateMessage("timestamps", table, timestamp));
        final int length = AeronClusteredCacheMessageCodec.encode(frame, sender, sequence, payload);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (raw.offer(frame, 0, length) < 0 && System.nanoTime() < deadline) {
            LockSupport.parkNanos(10_000L);
        }
    }

    @Test
    void firstSequenceFromUnknownSenderRebasesAfterInvalidation(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties =
                    configuration(root.resolve("driver"), root.resolve("cursors"), 100L, 30_000L);
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new LinkedBlockingQueue<>();
            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver = receiverProvider.provideMessageReceiver(
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
                        .aeronDirectoryName(root.resolve("driver").toString()))) {
                    final Publication raw = aeron.addPublication(
                            properties.channel(), properties.streamId());
                    awaitConnected(raw);

                    /* A receiver may join after this sender has already emitted
                     * frames. It invalidates before accepting the first observed
                     * sequence, then uses that sequence as its new baseline. */
                    final byte[] otherSender = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                            .putLong(0x0102030405060708L).putLong(0x1112131415161718L).array();
                    final byte[] payload = AeronClusteredCachePayloadCodec.encode(
                            new TimestampsRegionUpdateMessage("timestamps", "table", 1L));
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(128);
                    final int length = AeronClusteredCacheMessageCodec.encode(
                            frame, otherSender, 5L, payload);
                    final long offerDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (raw.offer(frame, 0, length) < 0 && System.nanoTime() < offerDeadline) {
                        LockSupport.parkNanos(10_000L);
                    }

                    final TimestampsRegionUpdateMessage applied = received.poll(10, TimeUnit.SECONDS);
                    assertNotNull(applied, "the first observed frame must establish a baseline");
                    assertEquals("table", applied.tableName());
                    assertNull(receiver.failure(), "a late join must not fail the receiver");
                    assertTrue(receiver.isRunning(), "the receiver must remain healthy after re-baselining");
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void cursorStoreRoundTrips(@TempDir final Path root) {
        final AeronClusteredCacheCursorStore store = new AeronClusteredCacheCursorStore(root, "test_2001");
        store.ensureWritable();
        assertTrue(store.load().isEmpty());

        final AeronClusteredCacheMessageCodec.SenderId sender =
                new AeronClusteredCacheMessageCodec.SenderId(1L, 2L);
        store.store(Map.of(sender, 41L));

        assertEquals(Map.of(sender, 41L), store.load());
        assertEquals(41L, new AeronClusteredCacheCursorStore(root, "test_2001").load().get(sender));
    }

    @Test
    void cursorStoreRejectsCorruption(@TempDir final Path root) throws Exception {
        final AeronClusteredCacheCursorStore store = new AeronClusteredCacheCursorStore(root, "test_2001");
        store.ensureWritable();
        Files.writeString(root.resolve("cursors-test_2001.properties"), "bogus-line-without-separator=xx\n");

        assertThrows(IllegalStateException.class, store::load);
    }

    @Test
    void cursorStoreRejectsAnOversizedFileBeforeParsing(@TempDir final Path root) throws Exception {
        final AeronClusteredCacheCursorStore store = new AeronClusteredCacheCursorStore(root, "test_2001");
        store.ensureWritable();
        Files.write(root.resolve("cursors-test_2001.properties"), new byte[128 * 1024 + 1]);

        final IllegalStateException failure = assertThrows(IllegalStateException.class, store::load);
        assertTrue(failure.getMessage().contains("at most"),
                "the bounded reader must reject the file before Properties parsing");
    }

        /// Acceptor that records deliveries and invalidations over a real cache manager.
    private static final class RecordingAcceptor extends ClusteredCacheMessageAcceptor {
        private final BlockingQueue<TimestampsRegionUpdateMessage> received = new LinkedBlockingQueue<>();
        private final AtomicInteger invalidations = new AtomicInteger();

        private RecordingAcceptor(final CacheManager manager) {
            super(manager);
        }

        @Override
        public void accept(final TimestampsRegionUpdateMessage message) {
            super.accept(message);
            this.received.add(message);
        }

        @Override
        public void invalidateAll() {
            this.invalidations.incrementAndGet();
            super.invalidateAll();
        }
    }
}
