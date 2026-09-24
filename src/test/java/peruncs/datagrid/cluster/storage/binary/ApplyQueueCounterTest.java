package peruncs.datagrid.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.ReplicationUnavailableException;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Deterministic counter and wake-up test for the merger's byte accounting.
///
/// Holds the materialization handler on a latch so the queue state is
/// observable at each ownership leg: after admission, while the worker owns
/// the batch, after completion, after a submission-failure rollback, and
/// after disposal. Also proves an over-limit admission wakes a worker parked
/// in its coalescing delay immediately instead of after the full timeout.
class ApplyQueueCounterTest {
    private static final long COALESCING_TIMEOUT_MS = 10_000L;
    private static final long SOFT_LIMIT_BYTES = 256L;

    private static Binary binary(final int bufferCount, final int bufferSize) {
        final ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        for (int index = 0; index < bufferCount; index++) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
            for (int fill = 0; fill < bufferSize; fill++) {
                buffer.put((byte) fill);
            }
            /* ChunksWrapper stores the logical length in the position:
             * do not flip, or the payload reads as zero bytes. */
            buffers[index] = buffer;
        }
        return ChunksWrapper.New(buffers);
    }

    /// A handler that parks until released, recording entry.
    private record ParkedHandler(
            CountDownLatch entered,
            CountDownLatch release,
            AtomicReference<Runnable> updaterSeen) implements ObjectGraphUpdateHandler {
        @Override
        public void objectGraphUpdateAvailable(final Runnable updater) {
            this.updaterSeen.set(updater);
            this.entered.countDown();
            try {
                this.release.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// After admission the bytes are queued; once the worker holds the batch
    /// they moved to in-flight exactly once; after completion both are zero.
    @Test
    void countersTrackEnqueueHandoffAndCompletion() throws Exception {
        final CountDownLatch handlerEntered = new CountDownLatch(1);
        final CountDownLatch releaseHandler = new CountDownLatch(1);
        final AtomicReference<Runnable> updaterSeen = new AtomicReference<>();
        final ParkedHandler handler =
                new ParkedHandler(handlerEntered, releaseHandler, updaterSeen);
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(
                StorageBinaryDataMergerTestSupport.configuration(
                        StorageBinaryDataMergerTestSupport.foundation(),
                        StorageBinaryDataMergerTestSupport.connection(),
                        handler,
                        COALESCING_TIMEOUT_MS,
                        SOFT_LIMIT_BYTES,
                        60_000L));
        try {
            /* First delivery: below the soft limit, so the producer never
             * waits and the worker stays parked in its coalescing delay. */
            final long firstBytes = 2L * Long.BYTES;
            merger.receiveData(binary(2, Long.BYTES));
            assertEquals(firstBytes, merger.queuedBytes(), "admitted bytes must be queued");
            assertEquals(0L, merger.inFlightBytes(), "nothing is in flight while the worker coalesces");

            /* Second delivery crosses the soft limit: the admission must
             * wake the parked worker immediately. The delivery thread then
             * parks in the backpressure wait, so it runs on a helper. */
            final Thread delivery = new Thread(() ->
            {
                final int overLimitBuffers = (int) (SOFT_LIMIT_BYTES / Long.BYTES) + 1;
                merger.receiveData(binary(overLimitBuffers, Long.BYTES));
            });
            final long wakeStartNanos = System.nanoTime();
            delivery.start();

            assertTrue(handlerEntered.await(5, TimeUnit.SECONDS),
                    "an over-limit admission must wake the worker far below the "
                            + COALESCING_TIMEOUT_MS + " ms coalescing timeout");
            final long wakeElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wakeStartNanos);
            assertTrue(wakeElapsedMs < COALESCING_TIMEOUT_MS,
                    "worker wake-up took " + wakeElapsedMs + " ms; it must not wait out the coalescing timeout");

            /* The worker holds the batch in the blocked handler: every byte
             * moved from queued to in-flight exactly once. */
            final long totalBytes = firstBytes + (SOFT_LIMIT_BYTES + Long.BYTES);
            assertEquals(0L, merger.queuedBytes(), "the worker drained the whole queue");
            assertEquals(totalBytes, merger.inFlightBytes(), "the batch is accounted in flight exactly once");

            releaseHandler.countDown();
            delivery.join(30_000);
            merger.awaitApplied();
            assertEquals(0L, merger.queuedBytes(), "completion releases the queued bytes");
            assertEquals(0L, merger.inFlightBytes(), "completion releases the in-flight bytes");
        } finally {
            releaseHandler.countDown();
            merger.dispose();
        }
    }

    /// A disposal while the worker holds nothing resets both counters, and a
    /// disposed merger refuses further admissions with zero counters.
    @Test
    void disposalReleasesBothCounters() throws Exception {
        final CountDownLatch handlerEntered = new CountDownLatch(1);
        final CountDownLatch releaseHandler = new CountDownLatch(1);
        final ParkedHandler handler =
                new ParkedHandler(handlerEntered, releaseHandler, new AtomicReference<>());
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(
                StorageBinaryDataMergerTestSupport.configuration(
                        StorageBinaryDataMergerTestSupport.foundation(),
                        StorageBinaryDataMergerTestSupport.connection(),
                        handler,
                        COALESCING_TIMEOUT_MS,
                        SOFT_LIMIT_BYTES,
                        60_000L));
        merger.receiveData(binary(1, Long.BYTES));
        releaseHandler.countDown();
        merger.dispose();
        assertEquals(0L, merger.queuedBytes());
        assertEquals(0L, merger.inFlightBytes());
        assertThrows(ReplicationUnavailableException.class, () -> merger.receiveData(binary(1, Long.BYTES)));
        assertEquals(0L, merger.queuedBytes());
        assertEquals(0L, merger.inFlightBytes());
    }
}
