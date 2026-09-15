package peruncs.datagrid.cache.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.store.cache.types.CachingProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cache.types.ClusteredCacheEntryListenerConfiguration;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;
import static peruncs.datagrid.cache.test.ClusteredCacheTestSupport.publish;
import static peruncs.datagrid.cache.test.ClusteredCacheTestSupport.serializer;

/// Verifies the Aeron clustered-cache provider over a real embedded MediaDriver.
class AeronClusteredCacheMessageCommunicationProviderTest {
    private static void provideSender(final AeronClusteredCacheConfiguration configuration) {
        new AeronClusteredCacheMessageCommunicationProvider()
                .provideUpdateTimestampsCacheMessageSender(configuration, serializer())
                .dispose();
    }

    private static MediaDriver launchDriver(final Path root) {
        return MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(root.resolve("driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED));
    }

    private static AeronClusteredCacheConfiguration configuration(final Path driverDirectory) {
        return new AeronClusteredCacheConfiguration(
                "aeron:ipc", 2001, null, driverDirectory.toString(), false, 10_000L, 10_000L, 1 << 20);
    }

    private static AeronClusteredCacheConfiguration withStreamId(
            final AeronClusteredCacheConfiguration base, final int streamId) {
        return new AeronClusteredCacheConfiguration(
                base.channel(), streamId, base.nodeId(), base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), base.maxPayloadBytes());
    }

    private static AeronClusteredCacheConfiguration withChannel(
            final AeronClusteredCacheConfiguration base, final String channel) {
        return new AeronClusteredCacheConfiguration(
                channel, base.streamId(), base.nodeId(), base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), base.maxPayloadBytes());
    }

    private static AeronClusteredCacheConfiguration withNodeId(
            final AeronClusteredCacheConfiguration base, final UUID nodeId) {
        return new AeronClusteredCacheConfiguration(
                base.channel(), base.streamId(), nodeId, base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), base.maxPayloadBytes());
    }

    private static AeronClusteredCacheConfiguration withEmbeddedDriver(
            final AeronClusteredCacheConfiguration base) {
        return new AeronClusteredCacheConfiguration(
                base.channel(), base.streamId(), base.nodeId(), base.directory(), true,
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), base.maxPayloadBytes());
    }

    private static AeronClusteredCacheConfiguration withOfferTimeoutMillis(
            final AeronClusteredCacheConfiguration base, final long offerTimeoutMillis) {
        return new AeronClusteredCacheConfiguration(
                base.channel(), base.streamId(), base.nodeId(), base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), offerTimeoutMillis, base.maxPayloadBytes());
    }

    private static AeronClusteredCacheConfiguration withMaxPayloadBytes(
            final AeronClusteredCacheConfiguration base, final int maxPayloadBytes) {
        return new AeronClusteredCacheConfiguration(
                base.channel(), base.streamId(), base.nodeId(), base.directory(), base.embeddedDriver(),
                base.driverTimeoutMillis(), base.offerTimeoutMillis(), maxPayloadBytes);
    }

    private static ClusteredCacheMessageAcceptor acceptor(final BlockingQueue<TimestampsRegionUpdateMessage> received) {
        return acceptor(received, null);
    }

    private static ClusteredCacheMessageAcceptor acceptor(
            final BlockingQueue<TimestampsRegionUpdateMessage> received,
            final CountDownLatch signal
    ) {
        return new ClusteredCacheMessageAcceptor(null) {
            @Override
            public void accept(final TimestampsRegionUpdateMessage message) {
                received.add(message);
                if (signal != null) {
                    signal.countDown();
                }
            }
        };
    }

    private static int drainAll(final BlockingQueue<TimestampsRegionUpdateMessage> received,
                                final int expected, final long timeoutSeconds) {
        int delivered = 0;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (delivered < expected && System.nanoTime() < deadline) {
            delivered += drain(received);
            if (delivered < expected) {
                LockSupport.parkNanos(10_000L);
            }
        }
        return delivered;
    }

    private static int drain(final BlockingQueue<TimestampsRegionUpdateMessage> received) {
        int count = 0;
        while (received.poll() != null) {
            count++;
        }
        return count;
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

    @Test
    void invalidationReachesAnotherNode(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(16);
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));

            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    receiverProvider.provideMessageReceiver(properties, serializer(), acceptor(received));
            try {
                receiver.start();

                final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageSender sender =
                        senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                try {
                    publish(sender, EventType.CREATED, "default-query-results-region", "table-a", 42L);
                    publish(sender, EventType.UPDATED, "default-query-results-region", "table-a", 43L);

                    final TimestampsRegionUpdateMessage first = received.poll(10, TimeUnit.SECONDS);
                    final TimestampsRegionUpdateMessage second = received.poll(10, TimeUnit.SECONDS);
                    assertNotNull(first, "the other node did not receive the invalidation");
                    assertEquals("default-query-results-region", first.cacheName());
                    assertEquals("table-a", first.tableName());
                    assertEquals(42L, first.timestamp());
                    assertNotNull(second, "the other node did not receive the update invalidation");
                    assertEquals(43L, second.timestamp());
                    assertEquals(2L, sender.published(), "both publishes must be counted");
                    assertEquals(0L, sender.offerRetries(), "no retries on a connected publication");
                } finally {
                    sender.dispose();
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void threeNodesSeeEveryInvalidation(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final BlockingQueue<TimestampsRegionUpdateMessage> first = new ArrayBlockingQueue<>(16);
            final BlockingQueue<TimestampsRegionUpdateMessage> second = new ArrayBlockingQueue<>(16);

            final AeronClusteredCacheMessageCommunicationProvider firstProvider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageCommunicationProvider secondProvider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageCommunicationProvider senderProvider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver firstReceiver =
                    firstProvider.provideMessageReceiver(properties, serializer(), acceptor(first));
            final AeronClusteredCacheMessageReceiver secondReceiver =
                    secondProvider.provideMessageReceiver(properties, serializer(), acceptor(second));
            try {
                firstReceiver.start();
                secondReceiver.start();

                final AeronClusteredCacheMessageSender sender =
                        senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                try {
                    publish(sender, EventType.CREATED, "cache", "table", 5L);
                } finally {
                    sender.dispose();
                }

                assertNotNull(first.poll(10, TimeUnit.SECONDS), "the first node did not receive the invalidation");
                assertNotNull(second.poll(10, TimeUnit.SECONDS), "the second node did not receive the invalidation");
            } finally {
                firstReceiver.dispose();
                secondReceiver.dispose();
            }
        }
    }

    @Test
    void streamMismatchIsolatesProviders(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final AeronClusteredCacheConfiguration otherStream = withStreamId(properties, 2002);

            final BlockingQueue<TimestampsRegionUpdateMessage> sameStream = new ArrayBlockingQueue<>(16);
            final BlockingQueue<TimestampsRegionUpdateMessage> other = new ArrayBlockingQueue<>(16);
            final AeronClusteredCacheMessageCommunicationProvider sameProvider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageCommunicationProvider otherProvider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageCommunicationProvider senderProvider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver sameReceiver =
                    sameProvider.provideMessageReceiver(properties, serializer(), acceptor(sameStream));
            final AeronClusteredCacheMessageReceiver otherReceiver =
                    otherProvider.provideMessageReceiver(otherStream, serializer(), acceptor(other));
            try {
                sameReceiver.start();
                otherReceiver.start();

                final AeronClusteredCacheMessageSender sender =
                        senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                try {
                    publish(sender, EventType.CREATED, "cache", "table", 1L);
                } finally {
                    sender.dispose();
                }

                assertNotNull(sameStream.poll(10, TimeUnit.SECONDS), "the same-stream node did not receive the invalidation");
                assertNull(other.poll(1, TimeUnit.SECONDS), "a different stream must not receive the invalidation");
            } finally {
                sameReceiver.dispose();
                otherReceiver.dispose();
            }
        }
    }

    @Test
    void embeddedDriverSkipsItsOwnInvalidation(@TempDir final Path root) throws Exception {
        final CountDownLatch selfReceived = new CountDownLatch(1);
        AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
        properties = withEmbeddedDriver(properties);
        final var serializer = serializer();

        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageReceiver receiver =
                provider.provideMessageReceiver(properties, serializer, acceptor(new ArrayBlockingQueue<>(1), selfReceived));
        try {
            receiver.start();
            final AeronClusteredCacheMessageSender sender =
                    provider.provideUpdateTimestampsCacheMessageSender(properties, serializer);
            try {
                publish(sender, EventType.CREATED, "cache", "table", 7L);
                assertFalse(selfReceived.await(1, TimeUnit.SECONDS),
                        "a node must ignore an invalidation it published itself");
                final AeronClusteredCacheMessageReceiver aeronReceiver =
                        receiver;
                assertTrue(aeronReceiver.selfSkipped() >= 1L, "the self-published frame must be counted");
                assertEquals(0L, aeronReceiver.received(), "no frame may be applied from the same node");
                assertEquals(0L, aeronReceiver.gaps(), "a single frame cannot create a gap");
            } finally {
                sender.dispose();
            }
        } finally {
            receiver.dispose();
        }
    }

    @Test
    void configuredNodeIdSuppressesOtherProvidersOnTheSameNode(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            properties = withNodeId(properties, UUID.randomUUID());

            final CountDownLatch selfReceived = new CountDownLatch(1);
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(1);
            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    receiverProvider.provideMessageReceiver(properties, serializer(), acceptor(received, selfReceived));
            try {
                receiver.start();
                final AeronClusteredCacheMessageSender sender =
                        senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                try {
                    publish(sender, EventType.CREATED, "cache", "table", 7L);
                    assertFalse(selfReceived.await(1, TimeUnit.SECONDS),
                            "providers configured with the same node id must suppress each other's invalidations");
                } finally {
                    sender.dispose();
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void recreatingAProviderKeepsSequenceContinuity(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final String nodeId = UUID.randomUUID().toString();
            AeronClusteredCacheConfiguration senderProperties = configuration(root.resolve("driver"));
            senderProperties = withNodeId(senderProperties, UUID.fromString(nodeId));
            final AeronClusteredCacheConfiguration observerProperties = withNodeId(senderProperties, null);
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(4);
            final AeronClusteredCacheMessageCommunicationProvider observerProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver observer = observerProvider.provideMessageReceiver(
                    observerProperties, serializer(), acceptor(received));
            try {
                observer.start();
                final var firstProvider = new AeronClusteredCacheMessageCommunicationProvider();
                final var first = firstProvider.provideUpdateTimestampsCacheMessageSender(senderProperties, serializer());
                publish(first, EventType.CREATED, "cache", "before-recreate", 1L);
                assertNotNull(received.poll(10, TimeUnit.SECONDS));
                first.dispose();

                final var secondProvider = new AeronClusteredCacheMessageCommunicationProvider();
                final var second = secondProvider.provideUpdateTimestampsCacheMessageSender(senderProperties, serializer());
                try {
                    publish(second, EventType.CREATED, "cache", "after-recreate", 2L);
                    assertNotNull(received.poll(10, TimeUnit.SECONDS));
                    assertEquals(0L, (observer).gaps(),
                            "provider recreation must not reset a live sender incarnation sequence");
                } finally {
                    second.dispose();
                }
            } finally {
                observer.dispose();
            }
        }
    }

    @Test
    void senderFailsWhenNoPeerIsConnected(@TempDir final Path root) {
        AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
        properties = withEmbeddedDriver(properties);
        properties = withOfferTimeoutMillis(properties, 250L);

        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                    assertThrows(CacheEntryListenerException.class,
                            () -> publish(sender, EventType.CREATED, "cache", "table", 1L),
                            "the sender must fail the local write when it cannot publish"));
        } finally {
            sender.dispose();
            assertEquals(0, sender.scratchBufferCount(),
                    "sender disposal must release every callback-thread scratch buffer");
        }
    }

    @Test
    void senderDisposeDoesNotCloseAnInFlightPublication(@TempDir final Path root) throws Exception {
        AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
        properties = withEmbeddedDriver(properties);
        properties = withOfferTimeoutMillis(properties, 6000L);
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
        final AtomicReference<Throwable> publishFailure = new AtomicReference<>();
        final Thread publisher = Thread.ofVirtual().name("blocked-cache-sender-publish-test").unstarted(() ->
        {
            try {
                publish(sender, EventType.CREATED, "cache", "table", 1L);
            } catch (final Throwable failure) {
                publishFailure.set(failure);
            }
        });
        try {
            publisher.start();
            final long offerDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (sender.offerRetries() == 0L && System.nanoTime() < offerDeadline) {
                LockSupport.parkNanos(100_000L);
            }
            assertTrue(sender.offerRetries() > 0L, "publish must be in the retry loop before disposal starts");
            assertThrows(IllegalStateException.class, sender::dispose,
                    "disposal must not close a publication while a listener is publishing");
            publisher.join(8_000L);
            assertFalse(publisher.isAlive(), "the bounded publish timeout must eventually release the sender");
            assertInstanceOf(CacheEntryListenerException.class, publishFailure.get(),
                    "the disconnected publication must fail the cache operation");
        } finally {
            if (publisher.isAlive()) {
                publisher.interrupt();
                publisher.join(1_000L);
            }
            sender.dispose();
        }
    }

    @Test
    void malformedFrameFailsClosedBeforeLaterInvalidations(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(16);
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));

            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    receiverProvider.provideMessageReceiver(properties, serializer(), acceptor(received));
            try {
                receiver.start();

                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()))) {
                    final Publication raw = aeron.addPublication(
                            properties.channel(), properties.streamId());
                    awaitConnected(raw);
                    final UnsafeBuffer truncated =
                            new UnsafeBuffer(new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1]);
                    raw.offer(truncated, 0, truncated.capacity());
                    final AeronClusteredCacheMessageReceiver aeronReceiver =
                            receiver;
                    final long failureDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (aeronReceiver.failure() == null && System.nanoTime() < failureDeadline) {
                        LockSupport.parkNanos(100_000L);
                    }
                    assertNotNull(aeronReceiver.failure(), "malformed input must fail the volatile receiver closed");
                    assertFalse(aeronReceiver.isRunning());

                    final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                            new AeronClusteredCacheMessageCommunicationProvider();
                    final AeronClusteredCacheMessageSender sender =
                            senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                    try {
                        publish(sender, EventType.CREATED, "cache", "table", 9L);
                    } finally {
                        sender.dispose();
                    }

                    assertNull(received.poll(1, TimeUnit.SECONDS),
                            "the receiver must not apply frames after malformed input");
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void timedOutOfferDoesNotConsumeASequence(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            properties = withOfferTimeoutMillis(properties, 1L);
            final var serializer = serializer();
            final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageSender sender =
                    senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer);
            try {
                assertThrows(CacheEntryListenerException.class,
                        () -> publish(sender, EventType.CREATED, "cache", "before-connect", 1L),
                        "an offer with no subscription must time out");

                final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(4);
                final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageReceiver receiver = receiverProvider.provideMessageReceiver(
                        properties, serializer, acceptor(received));
                try {
                    receiver.start();
                    publish(sender, EventType.CREATED, "cache", "after-connect", 2L);
                    final TimestampsRegionUpdateMessage message = received.poll(10, TimeUnit.SECONDS);
                    assertNotNull(message, "the first successful offer must reach the receiver");
                    assertEquals("after-connect", message.tableName());
                    assertEquals(0L, (receiver).gaps(),
                            "a failed offer must not create a sequence gap");
                } finally {
                    receiver.dispose();
                }
            } finally {
                sender.dispose();
            }
        }
    }

    @Test
    void acceptorFailureStopsReceiverAndIsObservable(@TempDir final Path root) {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver = receiverProvider.provideMessageReceiver(
                    properties,
                    serializer(),
                    new ClusteredCacheMessageAcceptor(null) {
                        @Override
                        public void accept(final TimestampsRegionUpdateMessage message) {
                            throw new IllegalStateException("cache apply failed");
                        }
                    });
            receiver.start();
            final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageSender sender =
                    senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
            try {
                publish(sender, EventType.CREATED, "cache", "table", 1L);
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                while (receiver.failure() == null && System.nanoTime() < deadline) {
                    LockSupport.parkNanos(10_000L);
                }
                assertNotNull(receiver.failure(), "a valid-message application failure must be observable");
                assertFalse(receiver.isRunning(), "a receiver with an application failure must stop polling");
            } finally {
                sender.dispose();
                receiver.dispose();
            }
        }
    }

    @Test
    void oversizedPayloadFailsTheSend() {
        final AeronClusteredCacheConfiguration properties =
                withMaxPayloadBytes(AeronClusteredCacheConfiguration.defaults(), 16);
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
        try {
            assertThrows(CacheEntryListenerException.class,
                    () -> publish(sender, EventType.CREATED, "cache", "table-with-a-long-name", 1L),
                    "a payload above the configured limit must fail the local cache operation");
        } finally {
            sender.dispose();
        }
    }

    @Test
    void disposedSenderFailsTheCacheOperation() {
        final AeronClusteredCacheConfiguration properties = AeronClusteredCacheConfiguration.defaults();
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
        sender.dispose();

        assertThrows(CacheEntryListenerException.class,
                () -> publish(sender, EventType.CREATED, "cache", "table", 1L),
                "a disposed sender must fail the local cache operation");
    }

    @Test
    void disposeIsIdempotent(@TempDir final Path root) {
        AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
        properties = withEmbeddedDriver(properties);
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final var serializer = serializer();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties, serializer);
        final AeronClusteredCacheMessageReceiver receiver =
                provider.provideMessageReceiver(properties, serializer, acceptor(new ArrayBlockingQueue<>(1)));
        receiver.start();
        sender.dispose();
        sender.dispose();
        receiver.dispose();
        receiver.dispose();
    }

    @Test
    void disposedReceiverIsSingleUse(@TempDir final Path root) {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    provider.provideMessageReceiver(properties, serializer(), acceptor(new ArrayBlockingQueue<>(1)));
            receiver.start();
            receiver.dispose();

            assertThrows(IllegalStateException.class, receiver::start,
                    "a disposed receiver is single-use and must not restart");
        }
    }

    @Test
    void receiverIsRunningReflectsLifecycle(@TempDir final Path root) {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    provider.provideMessageReceiver(properties, serializer(), acceptor(new ArrayBlockingQueue<>(1)));

            assertFalse(receiver.isRunning(), "a receiver is not running before start");
            receiver.start();
            assertTrue(receiver.isRunning(), "a started receiver is running");
            receiver.dispose();
            assertFalse(receiver.isRunning(), "a disposed receiver is not running");
        }
    }

    @Test
    void blockedReceiverDisposeIsBoundedAndRetryable(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver = receiverProvider.provideMessageReceiver(
                    properties,
                    serializer(),
                    new ClusteredCacheMessageAcceptor(null) {
                        @Override
                        public void accept(final TimestampsRegionUpdateMessage message) {
                            entered.countDown();
                            boolean interrupted = false;
                            for (; ; ) {
                                try {
                                    release.await();
                                    break;
                                } catch (final InterruptedException failure) {
                                    interrupted = true;
                                }
                            }
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    });
            receiver.start();
            final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageSender sender =
                    senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
            try {
                publish(sender, EventType.CREATED, "cache", "table", 1L);
                assertTrue(entered.await(5, TimeUnit.SECONDS), "receiver callback did not start");
                final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
                final Thread closer = Thread.ofVirtual().name("blocked-cache-receiver-close-test").unstarted(() ->
                {
                    try {
                        receiver.dispose();
                    } catch (final Throwable failure) {
                        closeFailure.set(failure);
                    }
                });
                closer.start();
                closer.join(6_000L);
                assertFalse(closer.isAlive(), "dispose must have a bounded wait");
                assertInstanceOf(IllegalStateException.class, closeFailure.get(),
                        "a blocked receiver must report a disposal timeout");
                assertFalse(receiver.isRunning(), "a stopping receiver must not report readiness");
                release.countDown();
                receiver.dispose();
            } finally {
                release.countDown();
                sender.dispose();
                receiver.dispose();
            }
        }
    }

    @Test
    void providerResourcesAreTerminalAfterFullDispose(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(16);
            final var serializer = serializer();
            final ClusteredCacheMessageAcceptor acceptor = acceptor(received);
            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();

            final AeronClusteredCacheMessageReceiver receiver =
                    receiverProvider.provideMessageReceiver(properties, serializer, acceptor);
            receiver.start();
            final AeronClusteredCacheMessageSender sender =
                    senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer);
            publish(sender, EventType.CREATED, "cache", "table", 1L);
            assertNotNull(received.poll(10, TimeUnit.SECONDS));
            sender.dispose();
            receiver.dispose();

            assertThrows(IllegalStateException.class,
                    () -> receiverProvider.provideMessageReceiver(properties, serializer, acceptor),
                    "a provider with closed resources must reject a new receiver lifecycle");
            assertThrows(IllegalStateException.class,
                    () -> senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer),
                    "a provider with closed resources must reject a new sender lifecycle");
        }
    }

    @Test
    void providerReturnsOneOwnedSenderAndReceiverAndRejectsRebind(@TempDir final Path root) {
        AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final var serializer = serializer();
        final ClusteredCacheMessageAcceptor acceptor = acceptor(new ArrayBlockingQueue<>(1));

        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(properties, serializer);
        assertSame(sender, provider.provideUpdateTimestampsCacheMessageSender(properties, serializer));
        final AeronClusteredCacheMessageReceiver receiver = provider.provideMessageReceiver(properties, serializer, acceptor);
        assertSame(receiver, provider.provideMessageReceiver(properties, serializer, acceptor));

        assertThrows(IllegalArgumentException.class,
                () -> provider.provideUpdateTimestampsCacheMessageSender(properties, serializer()),
                "a second serializer cannot share the provider-owned sender");
        assertThrows(IllegalArgumentException.class,
                () -> provider.provideMessageReceiver(properties, serializer(), acceptor(new ArrayBlockingQueue<>(1))),
                "a second acceptor cannot share the provider-owned receiver");

        sender.dispose();
        receiver.dispose();
    }

    @Test
    void disposedHandlesAreNeverReturnedAgain(@TempDir final Path root) {
        final AeronClusteredCacheConfiguration properties =
                withEmbeddedDriver(configuration(root.resolve("driver")));
        final var serializer = serializer();
        final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageReceiver receiver = receiverProvider.provideMessageReceiver(
                properties, serializer, acceptor(new ArrayBlockingQueue<>(4)));
        receiver.start();
        final AeronClusteredCacheMessageSender sender =
                senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer);
        try {
            sender.dispose();
            assertThrows(IllegalStateException.class,
                    () -> senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer));
        } finally {
            receiver.dispose();
        }

        final AeronClusteredCacheMessageCommunicationProvider receiverOnlyProvider =
                new AeronClusteredCacheMessageCommunicationProvider();
        final AeronClusteredCacheMessageReceiver disposedReceiver = receiverOnlyProvider.provideMessageReceiver(
                properties, serializer, acceptor(new ArrayBlockingQueue<>(4)));
        disposedReceiver.start();
        disposedReceiver.dispose();
        assertThrows(IllegalStateException.class,
                () -> receiverOnlyProvider.provideMessageReceiver(properties, serializer,
                        acceptor(new ArrayBlockingQueue<>(4))));
    }

    @Test
    void conflictingConfigurationIsRejected() {
        final AeronClusteredCacheConfiguration properties = AeronClusteredCacheConfiguration.defaults();
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        provider.provideUpdateTimestampsCacheMessageSender(properties, serializer()).dispose();

        final AeronClusteredCacheConfiguration conflicting = withStreamId(properties, 2002);
        assertThrows(IllegalStateException.class,
                () -> provider.provideMessageReceiver(conflicting, serializer(),
                        acceptor(new ArrayBlockingQueue<>(1))),
                "a terminal provider must reject a new receiver lifecycle");
    }

    @Test
    void conflictingNodeIdIsRejected() {
        AeronClusteredCacheConfiguration properties = AeronClusteredCacheConfiguration.defaults();
        properties = withNodeId(properties, UUID.randomUUID());
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        provider.provideUpdateTimestampsCacheMessageSender(properties, serializer()).dispose();

        final AeronClusteredCacheConfiguration conflicting = withNodeId(properties, UUID.randomUUID());
        assertThrows(IllegalStateException.class,
                () -> provider.provideMessageReceiver(conflicting, serializer(),
                        acceptor(new ArrayBlockingQueue<>(1))),
                "a terminal provider must reject a new receiver lifecycle");
    }

    @Test
    void gapInSenderSequenceIsDetected(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(16);
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));

            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    receiverProvider.provideMessageReceiver(properties, serializer(), acceptor(received));
            try {
                receiver.start();

                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()))) {
                    final Publication raw = aeron.addPublication(
                            properties.channel(), properties.streamId());
                    awaitConnected(raw);

                    final byte[] otherSender = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                            .putLong(0x0102030405060708L).putLong(0x1112131415161718L).array();
                    final byte[] payload = AeronClusteredCachePayloadCodec.encode(
                            new TimestampsRegionUpdateMessage("cache", "table", 1L));
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(128);
                    raw.offer(frame, 0, AeronClusteredCacheMessageCodec.encode(frame, otherSender, 1L, payload));
                    raw.offer(frame, 0, AeronClusteredCacheMessageCodec.encode(frame, otherSender, 3L, payload));

                    assertNotNull(received.poll(10, TimeUnit.SECONDS),
                            "the first contiguous invalidation must be applied");
                    assertNull(received.poll(1, TimeUnit.SECONDS),
                            "the invalidation after a sender gap must be rejected before cache application");
                    final AeronClusteredCacheMessageReceiver aeronReceiver =
                            receiver;
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (aeronReceiver.gaps() < 1L && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(10_000L);
                    }
                    assertTrue(aeronReceiver.gaps() >= 1L,
                            "a skipped sequence must be reported as a gap");
                    assertNotNull(aeronReceiver.failure(),
                            "a lost invalidation must fail the volatile receiver closed");
                    assertFalse(aeronReceiver.isRunning(),
                            "a receiver with a sequence gap must stop polling");
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void concurrentPublishersDeliverEveryInvalidation(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(64);
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));

            final AeronClusteredCacheMessageCommunicationProvider receiverProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    receiverProvider.provideMessageReceiver(properties, serializer(), acceptor(received));
            try {
                receiver.start();

                final AeronClusteredCacheMessageCommunicationProvider senderProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageSender sender =
                        senderProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                try {
                    final int publisherCount = 4;
                    final int perThread = 10;
                    final CountDownLatch start = new CountDownLatch(1);
                    final List<Thread> publishers = new ArrayList<>();
                    for (int t = 0; t < publisherCount; t++) {
                        final int publisher = t;
                        final Thread thread = Thread.ofVirtual().name("cache-publisher-%s".formatted(publisher)).unstarted(() ->
                        {
                            try {
                                start.await();
                            } catch (final InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            for (int i = 0; i < perThread; i++) {
                                publish(sender, EventType.CREATED, "cache", "table-%s-%s".formatted(publisher, i),
                                        publisher * 1000L + i);
                            }
                        });
                        publishers.add(thread);
                        thread.start();
                    }
                    start.countDown();
                    for (final Thread thread : publishers) {
                        thread.join(15_000L);
                    }

                    int delivered = 0;
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (delivered < publisherCount * perThread && System.nanoTime() < deadline) {
                        delivered += drain(received);
                    }
                    assertEquals(publisherCount * perThread, delivered,
                            "every concurrently published invalidation must be delivered exactly once");
                } finally {
                    sender.dispose();
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void receiverFailsClosedWhenSenderIdentityCardinalityIsUnbounded(@TempDir final Path root) {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver receiver =
                    provider.provideMessageReceiver(
                            properties, serializer(), acceptor(new ArrayBlockingQueue<>(2048)));
            try {
                receiver.start();
                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()))) {
                    final Publication raw = aeron.addPublication(
                            properties.channel(), properties.streamId());
                    try {
                        awaitConnected(raw);
                        final byte[] payload = AeronClusteredCachePayloadCodec.encode(
                                new TimestampsRegionUpdateMessage("cache", "table", 1L));
                        final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(128);
                        for (int index = 0; index <= 1_024; index++) {
                            final byte[] sender = ByteBuffer.allocate(Long.BYTES * 2)
                                    .order(ByteOrder.BIG_ENDIAN).putLong(index + 1L).putLong(index + 2L).array();
                            final int length = AeronClusteredCacheMessageCodec.encode(frame, sender, 0L, payload);
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (raw.offer(frame, 0, length) < 0 && System.nanoTime() < deadline) {
                                LockSupport.parkNanos(10_000L);
                            }
                        }
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (receiver.failure() == null && System.nanoTime() < deadline) {
                            LockSupport.parkNanos(10_000L);
                        }
                        assertNotNull(receiver.failure(), "sender identity cardinality must be bounded");
                        assertFalse(receiver.isRunning(), "receiver must stop after identity cardinality overflow");
                    } finally {
                        raw.close();
                    }
                }
            } finally {
                receiver.dispose();
            }
        }
    }

    @Test
    void sharedNodeIdDoesNotCreateFalseGaps(@TempDir final Path root) {
        try (MediaDriver driver = launchDriver(root)) {
            AeronClusteredCacheConfiguration properties = configuration(root.resolve("driver"));
            final String nodeId = UUID.randomUUID().toString();
            properties = withNodeId(properties, UUID.fromString(nodeId));
            /* The observer is a different node and must not share the node id,
             * otherwise it would self-suppress every observed frame. */
            final AeronClusteredCacheConfiguration observerProperties = withNodeId(properties, null);

            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(32);
            final AeronClusteredCacheMessageCommunicationProvider observerProvider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver observer =
                    observerProvider.provideMessageReceiver(observerProperties, serializer(), acceptor(received));
            try {
                observer.start();

                final AeronClusteredCacheMessageCommunicationProvider firstProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageCommunicationProvider secondProvider =
                        new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageSender first =
                        firstProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                final AeronClusteredCacheMessageSender second =
                        secondProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer());
                try {
                    /* Interleave both providers so two independent per-instance
                     * counters would interleave (0, 0, 1, 1) and report false gaps. */
                    for (int i = 0; i < 5; i++) {
                        publish(first, EventType.CREATED, "cache", "table-%s".formatted(i), i);
                        publish(second, EventType.CREATED, "cache", "table-%s".formatted((i + 100)), i + 100L);
                    }

                    int delivered = 0;
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (delivered < 10 && System.nanoTime() < deadline) {
                        delivered += drain(received);
                    }
                    assertEquals(10, delivered, "every invalidation from both providers must be delivered");

                    final AeronClusteredCacheMessageReceiver aeronObserver =
                            observer;
                    final long gapDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (aeronObserver.gaps() != 0L && System.nanoTime() < gapDeadline) {
                        LockSupport.parkNanos(10_000L);
                    }
                    assertEquals(0L, aeronObserver.gaps(),
                            "providers sharing one node id must draw from one sequence and never report false gaps");
                } finally {
                    first.dispose();
                    second.dispose();
                }
            } finally {
                observer.dispose();
            }
        }
    }

    @Test
    void sharedNodeIdOnDifferentStreamsDoesNotCreateFalseGaps(@TempDir final Path root) {
        try (MediaDriver driver = launchDriver(root)) {
            final String nodeId = UUID.randomUUID().toString();
            AeronClusteredCacheConfiguration streamA = configuration(root.resolve("driver"));
            streamA = withNodeId(streamA, UUID.fromString(nodeId));
            AeronClusteredCacheConfiguration streamB = streamA;
            streamB = withStreamId(streamB, 2002);
            final AeronClusteredCacheConfiguration observerA = withNodeId(streamA, null);
            final AeronClusteredCacheConfiguration observerB = withNodeId(streamB, null);

            final BlockingQueue<TimestampsRegionUpdateMessage> receivedA = new ArrayBlockingQueue<>(16);
            final BlockingQueue<TimestampsRegionUpdateMessage> receivedB = new ArrayBlockingQueue<>(16);
            final AeronClusteredCacheMessageCommunicationProvider observerProviderA = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageCommunicationProvider observerProviderB = new AeronClusteredCacheMessageCommunicationProvider();
            final AeronClusteredCacheMessageReceiver observerReceiverA = observerProviderA.provideMessageReceiver(
                    observerA, serializer(), acceptor(receivedA));
            final AeronClusteredCacheMessageReceiver observerReceiverB = observerProviderB.provideMessageReceiver(
                    observerB, serializer(), acceptor(receivedB));
            try {
                observerReceiverA.start();
                observerReceiverB.start();

                final AeronClusteredCacheMessageCommunicationProvider senderProviderA = new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageCommunicationProvider senderProviderB = new AeronClusteredCacheMessageCommunicationProvider();
                final AeronClusteredCacheMessageSender senderA =
                        senderProviderA.provideUpdateTimestampsCacheMessageSender(streamA, serializer());
                final AeronClusteredCacheMessageSender senderB =
                        senderProviderB.provideUpdateTimestampsCacheMessageSender(streamB, serializer());
                try {
                    for (int i = 0; i < 5; i++) {
                        publish(senderA, EventType.CREATED, "cache", "a-%s".formatted(i), i);
                        publish(senderB, EventType.CREATED, "cache", "b-%s".formatted(i), i + 100L);
                    }

                    assertEquals(5, drainAll(receivedA, 5, 15));
                    assertEquals(5, drainAll(receivedB, 5, 15));

                    final AeronClusteredCacheMessageReceiver aeronObserverA =
                            observerReceiverA;
                    final AeronClusteredCacheMessageReceiver aeronObserverB =
                            observerReceiverB;
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while ((aeronObserverA.gaps() != 0L || aeronObserverB.gaps() != 0L) &&
                           System.nanoTime() < deadline) {
                        LockSupport.parkNanos(10_000L);
                    }
                    assertEquals(0L, aeronObserverA.gaps(),
                            "one node id on one stream must not report false gaps");
                    assertEquals(0L, aeronObserverB.gaps(),
                            "one node id on another stream must not report false gaps");
                } finally {
                    senderA.dispose();
                    senderB.dispose();
                }
            } finally {
                observerReceiverA.dispose();
                observerReceiverB.dispose();
            }
        }
    }

    @Test
    void invalidConfigurationIsRejected() {
        final AeronClusteredCacheConfiguration valid = AeronClusteredCacheConfiguration.defaults();
        assertThrows(IllegalArgumentException.class,
                () -> withStreamId(valid, -1));
        assertThrows(IllegalArgumentException.class,
                () -> withMaxPayloadBytes(valid, 0));
        assertThrows(IllegalArgumentException.class,
                () -> withMaxPayloadBytes(valid, Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class,
                () -> withOfferTimeoutMillis(valid, Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class,
                () -> withNodeId(valid, new UUID(0L, 0L)));
        assertThrows(IllegalArgumentException.class,
                () -> provideSender(withChannel(valid, "not-a-channel")));
        assertThrows(IllegalArgumentException.class,
                () -> provideSender(withChannel(valid, "aeron:udp?endpoint=localhost:40123")),
                "unicast UDP cannot deliver N-to-N invalidations and must be rejected");
        assertThrows(IllegalArgumentException.class,
                () -> provideSender(withChannel(valid, "aeron:udp?endpoint=0.0.0.0:40123|control-mode=dynamic")),
                "wildcard endpoints must be rejected");
        assertThrows(IllegalArgumentException.class,
                () -> provideSender(withChannel(valid,
                        "aeron:udp?endpoint=10.0.0.1:40123|control=0.0.0.0:40124|control-mode=dynamic")),
                "wildcard control endpoints must be rejected");
        assertThrows(IllegalArgumentException.class,
                () -> provideSender(withChannel(valid,
                        "aeron:udp?endpoint=localhost:40123|control-mode=dynamic")),
                "loopback UDP outside an embedded driver must be rejected");
    }

    @Test
    void receivedInvalidationIsNotRebroadcast(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration base = configuration(root.resolve("driver"));
            final AeronClusteredCacheConfiguration firstNode = withNodeId(base, UUID.randomUUID());
            final AeronClusteredCacheConfiguration secondNode = withNodeId(base, UUID.randomUUID());
            final var provider = new CachingProvider();
            try {
                final var firstManager = provider.getCacheManager(new URI("eclipsestore-aeron-first"), null);
                final var secondManager = provider
                        .getCacheManager(new URI("eclipsestore-aeron-second"), null);
                final var firstCache = firstManager.createCache("timestamps",
                        new MutableConfiguration<>().setTypes(Object.class, Object.class));
                final var secondCache = secondManager.createCache("timestamps",
                        new MutableConfiguration<>().setTypes(Object.class, Object.class));
                final var firstSenderProvider = new AeronClusteredCacheMessageCommunicationProvider();
                final var secondSenderProvider = new AeronClusteredCacheMessageCommunicationProvider();
                /* Each node keeps one serializer for both bindings, as the API requires. */
                final var firstSerializer = serializer();
                final var secondSerializer = serializer();
                final AeronClusteredCacheMessageSender firstSender = firstSenderProvider
                        .provideUpdateTimestampsCacheMessageSender(firstNode, firstSerializer);
                final AeronClusteredCacheMessageSender secondSender = secondSenderProvider
                        .provideUpdateTimestampsCacheMessageSender(secondNode, secondSerializer);
                final AeronClusteredCacheMessageReceiver firstReceiver = firstSenderProvider
                        .provideMessageReceiver(firstNode, firstSerializer,
                                new ClusteredCacheMessageAcceptor(firstManager));
                final AeronClusteredCacheMessageReceiver secondReceiver = secondSenderProvider
                        .provideMessageReceiver(secondNode, secondSerializer,
                                new ClusteredCacheMessageAcceptor(secondManager));
                firstCache.registerCacheEntryListener(new ClusteredCacheEntryListenerConfiguration(
                        firstSender).getUpdateTimestampsCacheEntryListenerConfiguration());
                secondCache.registerCacheEntryListener(new ClusteredCacheEntryListenerConfiguration(
                        secondSender).getUpdateTimestampsCacheEntryListenerConfiguration());
                try {
                    firstReceiver.start();
                    secondReceiver.start();

                    firstCache.put("table", 7L);
                    assertEquals(1, firstSender.published(), "the local write must broadcast once");

                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (!Long.valueOf(7L).equals(secondCache.get("table"))
                            && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(10_000L);
                    }
                    assertEquals(7L, secondCache.get("table"), "the second node did not apply the invalidation");
                    /* A rebroadcasting receiver would have published by now: the
                     * listener fires inline on apply, so settling briefly is enough. */
                    Thread.sleep(1_000L);
                    assertEquals(0, secondSender.published(),
                            "a received invalidation must not be rebroadcast");
                } finally {
                    firstReceiver.dispose();
                    secondReceiver.dispose();
                    firstSender.dispose();
                    secondSender.dispose();
                }
            } finally {
                provider.close();
            }
        }
    }

    @Test
    void sameNodeIdAcrossRepeatedBindingsReturnsTheSender() {
        final UUID nodeId = UUID.randomUUID();
        final AeronClusteredCacheConfiguration first = withNodeId(AeronClusteredCacheConfiguration.defaults(), nodeId);
        final AeronClusteredCacheMessageCommunicationProvider provider = new AeronClusteredCacheMessageCommunicationProvider();
        final var serializer = serializer();
        final AeronClusteredCacheMessageSender sender =
                provider.provideUpdateTimestampsCacheMessageSender(first, serializer);
        assertSame(sender, provider.provideUpdateTimestampsCacheMessageSender(
                withNodeId(AeronClusteredCacheConfiguration.defaults(), nodeId), serializer));
        sender.dispose();
    }
}
