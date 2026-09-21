package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReaderWatermark;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/// Exercises the deployed reader-to-writer progress stream without reflection.
class AeronWatermarkChannelTest {
    private static byte[] watermarkBytes() {
        final byte[] bytes = new byte[92];
        bytes[0] = 1;
        bytes[91] = 4;
        return bytes;
    }

    /// Verifies the endpoint factories reject a missing Aeron instance or receiver with a null check.
    @Test
    void endpointFactoriesRejectMissingAeronOrReceiver() {
        assertThrows(NullPointerException.class,
                () -> AeronWatermarkChannel.reader(null, "aeron:ipc", 1));
        assertThrows(NullPointerException.class,
                () -> AeronWatermarkChannel.writer(null, "aeron:ipc", 1, (buffer, offset, length) -> {
                }));
        assertThrows(NullPointerException.class,
                () -> AeronWatermarkChannel.writer(null, "aeron:ipc", 1, null));
    }

    /// Verifies close waits for an in-progress receiver to return instead of deadlocking.
    @Test
    void closeWaitsForAnInProgressReceiverWithoutDeadlocking(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("blocking-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final CountDownLatch receiverEntered = new CountDownLatch(1);
            final CountDownLatch releaseReceiver = new CountDownLatch(1);
            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            try (AeronWatermarkChannel writer = AeronWatermarkChannel.writer(aeron, "aeron:ipc", 79,
                    (value, offset, length) ->
                    {
                        receiverEntered.countDown();
                        try {
                            releaseReceiver.await();
                        } catch (final InterruptedException failure) {
                            Thread.currentThread().interrupt();
                        }
                    }); AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 79)) {
                reader.publish(watermarkBytes());
                assertTrue(receiverEntered.await(5, TimeUnit.SECONDS));
                final Thread closer = Thread.ofVirtual().name("watermark-close-test").unstarted(() ->
                {
                    try {
                        writer.close();
                    } catch (final Throwable failure) {
                        closeFailure.set(failure);
                    }
                });
                closer.start();
                releaseReceiver.countDown();
                closer.join(5_000L);
                assertFalse(closer.isAlive(), "close must finish after the receiver returns");
                assertNull(closeFailure.get());
            } finally {
                releaseReceiver.countDown();
            }
        }
    }

    /// Verifies publishing after close fails while a repeated close stays idempotent.
    @Test
    void publishAfterCloseFails(@TempDir final Path directory) {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("closed-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 78);
            reader.close();
            assertThrows(IllegalStateException.class, () -> reader.publish(watermarkBytes()));
            reader.close();
        }
    }

    /// Verifies a closed Aeron publication surfaces as a terminal worker failure with an idempotent close.
    @Test
    void closedAeronPublicationBecomesTerminalFailure(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("terminal-publication-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context)) {
            final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
            final AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 81);
            aeron.close();
            assertThrows(IllegalArgumentException.class, () -> reader.publish(new byte[]{1}));
            reader.publish(watermarkBytes());
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (reader.failure() == null && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertNotNull(reader.failure(), "a closed Aeron publication must fail the watermark worker");
            assertThrows(RuntimeException.class, reader::close);
            assertDoesNotThrow(reader::close, "a terminal watermark close must be idempotent");
        }
    }

    /// Verifies an interrupted close fails fast and the subsequent close succeeds.
    @Test
    void interruptedCloseCanBeRetried(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("interrupted-close-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final CountDownLatch receiverEntered = new CountDownLatch(1);
            final CountDownLatch releaseReceiver = new CountDownLatch(1);
            final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            try (AeronWatermarkChannel writer = AeronWatermarkChannel.writer(aeron, "aeron:ipc", 80,
                    (value, offset, length) ->
                    {
                        receiverEntered.countDown();
                        try {
                            releaseReceiver.await();
                        } catch (final InterruptedException failure) {
                            Thread.currentThread().interrupt();
                        }
                    }); AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 80)) {
                reader.publish(watermarkBytes());
                assertTrue(receiverEntered.await(5, TimeUnit.SECONDS));
                final Thread interruptedCloser = Thread.ofVirtual().name("interrupted-watermark-close-test").unstarted(() ->
                {
                    Thread.currentThread().interrupt();
                    try {
                        writer.close();
                    } catch (final Throwable failure) {
                        firstFailure.set(failure);
                    }
                });
                interruptedCloser.start();
                interruptedCloser.join(5_000L);
                assertInstanceOf(IllegalStateException.class, firstFailure.get());
                releaseReceiver.countDown();
            } finally {
                releaseReceiver.countDown();
            }
        }
    }

    /// Verifies published watermark bytes reach the writer intact with no worker failure.
    @Test
    void latestReaderWatermarkReachesWriter(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final CountDownLatch received = new CountDownLatch(1);
            final AtomicReference<byte[]> actual = new AtomicReference<>();
            try (AeronWatermarkChannel writer = AeronWatermarkChannel.writer(aeron, "aeron:ipc", 77,
                    (value, offset, length) ->
                    {
                        final byte[] copy = new byte[length];
                        value.getBytes(offset, copy);
                        actual.set(copy);
                        received.countDown();
                    });
                 AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 77)) {
                final byte[] expected = watermarkBytes();
                reader.publish(expected);
                assertTrue(received.await(5, TimeUnit.SECONDS));
                assertArrayEquals(expected, actual.get());
                assertNull(writer.failure());
                assertNull(reader.failure());
            }
        }
    }

    /// Verifies an unconnected latest-value publication is dropped so close never waits for the flush timeout.
    @Test
    void unconnectedPublicationIsDroppedInsteadOfFailingClose(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("unconnected-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            /* No subscriber on this stream: every offer reports NOT_CONNECTED.
             * A latest-value stream drops what nobody receives — the next
             * cursor advance or restart re-advertises — so close must succeed
             * instead of failing to flush the unreceivable value. */
            try (AeronWatermarkChannel reader = AeronWatermarkChannel.reader(
                    aeron, "aeron:ipc", 84, TimeUnit.SECONDS.toNanos(30))) {
                reader.publishEncoded(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                        1, 2, 3, 4);
                /* Let the worker observe NOT_CONNECTED. Close must then take the
                 * discard path immediately instead of waiting for 30 seconds. */
                Thread.sleep(250L);
                final long started = System.nanoTime();
                reader.close();
                assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2),
                        "an unconnected watermark close must not wait for the flush timeout");
                assertTrue(reader.isClosed());
                assertNull(reader.failure());
            }
        }
    }

    /// Verifies a retained watermark is delivered intact when the subscriber connects after publication.
    @Test
    void publishedWatermarkArrivesWhenSubscriberConnectsLate(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("late-subscriber-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final UUID readerId = UUID.randomUUID();
            final UUID clusterId = UUID.randomUUID();
            final UUID generation = UUID.randomUUID();
            try (AeronWatermarkChannel reader = AeronWatermarkChannel.reader(
                    aeron, "aeron:ipc", 85, TimeUnit.SECONDS.toNanos(30))) {
                reader.publishEncoded(readerId, clusterId, generation, 7L, 8L, 9L, 10L);
                /* Let the worker offer while nobody subscribes, so the value
                 * can only arrive via retry — never via a first offer that
                 * raced the subscriber — with no further transaction and no
                 * reader restart. */
                Thread.sleep(500L);
                final AtomicReference<AeronReaderWatermark> received = new AtomicReference<>();
                try (Subscription subscription = aeron.addSubscription("aeron:ipc", 85)) {
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (received.get() == null && System.nanoTime() < deadline) {
                        subscription.poll((buffer, offset, length, header) ->
                                received.set(AeronReaderWatermark.decode(new CRC32C(), buffer, offset, length)), 16);
                        if (received.get() == null) Thread.sleep(10L);
                    }
                }
                final AeronReaderWatermark watermark = received.get();
                assertNotNull(watermark, "late subscriber never received the retained watermark");
                assertEquals(readerId, watermark.readerId());
                assertEquals(clusterId, watermark.clusterId());
                assertEquals(generation, watermark.storeGeneration());
                assertEquals(7L, watermark.writerEpoch());
                assertEquals(8L, watermark.recordingId());
                assertEquals(9L, watermark.sequence());
                assertEquals(10L, watermark.position());
            }
        }
    }

    /// Verifies caller-owned publish copies its input so fixed buffers stay reusable for encoded publish.
    @Test
    void callerOwnedPublishCanBeMixedWithEncodedPublish(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("mixed-publish-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver _ = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final CountDownLatch firstReceived = new CountDownLatch(1);
            final CountDownLatch secondReceived = new CountDownLatch(1);
            final AtomicInteger deliveries = new AtomicInteger();
            try (AeronWatermarkChannel _ = AeronWatermarkChannel.writer(aeron, "aeron:ipc", 82,
                    (_, _, _) ->
                    {
                        if (deliveries.getAndIncrement() == 0) firstReceived.countDown();
                        else secondReceived.countDown();
                    });
                 AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 82)) {
                final byte[] callerOwned = watermarkBytes();
                reader.publish(callerOwned);
                callerOwned[91] = 99;
                assertTrue(firstReceived.await(5, TimeUnit.SECONDS));
                reader.publishEncoded(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                        1, 2, 3, 4);
                assertTrue(secondReceived.await(5, TimeUnit.SECONDS),
                        "fixed buffers must remain reusable after caller-owned publish");
            }
        }
    }
}
