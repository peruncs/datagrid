package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Exercises the deployed reader-to-writer progress stream without reflection.
class AeronWatermarkChannelTest {
    private static byte[] watermarkBytes() {
        final byte[] bytes = new byte[116];
        bytes[0] = 1;
        bytes[115] = 4;
        return bytes;
    }

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

    @Test
    void closeWaitsForAnInProgressReceiverWithoutDeadlocking(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("blocking-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver driver = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final CountDownLatch receiverEntered = new CountDownLatch(1);
            final CountDownLatch releaseReceiver = new CountDownLatch(1);
            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            final AeronWatermarkChannel writer = AeronWatermarkChannel.writer(aeron, "aeron:ipc", 79,
                    (value, offset, length) ->
                    {
                        receiverEntered.countDown();
                        try {
                            releaseReceiver.await();
                        } catch (final InterruptedException failure) {
                            Thread.currentThread().interrupt();
                        }
                    });
            try (AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 79)) {
                reader.publish(watermarkBytes());
                assertTrue(receiverEntered.await(5, TimeUnit.SECONDS));
                final Thread closer = new Thread(() ->
                {
                    try {
                        writer.close();
                    } catch (final Throwable failure) {
                        closeFailure.set(failure);
                    }
                }, "watermark-close-test");
                closer.start();
                releaseReceiver.countDown();
                closer.join(5_000L);
                assertFalse(closer.isAlive(), "close must finish after the receiver returns");
                assertNull(closeFailure.get());
            } finally {
                releaseReceiver.countDown();
                writer.close();
            }
        }
    }

    @Test
    void publishAfterCloseFails(@TempDir final Path directory) {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("closed-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver driver = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 78);
            reader.close();
            assertThrows(IllegalStateException.class, () -> reader.publish(watermarkBytes()));
            reader.close();
        }
    }

    @Test
    void closedAeronPublicationBecomesTerminalFailure(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("terminal-publication-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver driver = MediaDriver.launch(context)) {
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

    @Test
    void interruptedCloseCanBeRetried(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("interrupted-close-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver driver = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final CountDownLatch receiverEntered = new CountDownLatch(1);
            final CountDownLatch releaseReceiver = new CountDownLatch(1);
            final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            final AeronWatermarkChannel writer = AeronWatermarkChannel.writer(aeron, "aeron:ipc", 80,
                    (value, offset, length) ->
                    {
                        receiverEntered.countDown();
                        try {
                            releaseReceiver.await();
                        } catch (final InterruptedException failure) {
                            Thread.currentThread().interrupt();
                        }
                    });
            try (AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 80)) {
                reader.publish(watermarkBytes());
                assertTrue(receiverEntered.await(5, TimeUnit.SECONDS));
                final Thread interruptedCloser = new Thread(() ->
                {
                    Thread.currentThread().interrupt();
                    try {
                        writer.close();
                    } catch (final Throwable failure) {
                        firstFailure.set(failure);
                    }
                }, "interrupted-watermark-close-test");
                interruptedCloser.start();
                interruptedCloser.join(5_000L);
                assertInstanceOf(IllegalStateException.class, firstFailure.get());
                releaseReceiver.countDown();
                writer.close();
            } finally {
                releaseReceiver.countDown();
                writer.close();
            }
        }
    }

    @Test
    void latestReaderWatermarkReachesWriter(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver driver = MediaDriver.launch(context);
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

    @Test
    void callerOwnedPublishCanBeMixedWithEncodedPublish(@TempDir final Path directory) throws Exception {
        final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(directory.resolve("mixed-publish-driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver driver = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()))) {
            final CountDownLatch firstReceived = new CountDownLatch(1);
            final CountDownLatch secondReceived = new CountDownLatch(1);
            final AtomicInteger deliveries = new AtomicInteger();
            try (AeronWatermarkChannel writer = AeronWatermarkChannel.writer(aeron, "aeron:ipc", 82,
                    (value, offset, length) ->
                    {
                        if (deliveries.getAndIncrement() == 0) firstReceived.countDown();
                        else secondReceived.countDown();
                    });
                 AeronWatermarkChannel reader = AeronWatermarkChannel.reader(aeron, "aeron:ipc", 82)) {
                final byte[] callerOwned = watermarkBytes();
                reader.publish(callerOwned);
                callerOwned[115] = 99;
                assertTrue(firstReceived.await(5, TimeUnit.SECONDS));
                reader.publishEncoded(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                        1, 2, 3, 4, new byte[32]);
                assertTrue(secondReceived.await(5, TimeUnit.SECONDS),
                        "fixed buffers must remain reusable after caller-owned publish");
            }
        }
    }
}
