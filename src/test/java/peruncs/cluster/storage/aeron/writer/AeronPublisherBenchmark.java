package peruncs.cluster.storage.aeron.writer;

import org.agrona.DirectBuffer;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.UUID;

/// Test-side benchmark for the complete publisher staging path.
///
/// It runs the same data-chunk copy, CRC, envelope encoding, and terminal
/// marker path as a writer, using a non-blocking offerer instead of an Archive.
/// It reports bytes copied and allocated per transaction without adding a
/// benchmark dependency or production runtime code.
public final class AeronPublisherBenchmark {
    private AeronPublisherBenchmark() {
    }

    static void main(final String[] arguments) {
        int payloadLength = 1_048_576;
        int chunkSize = 16 * 1024;
        int sourceBuffers = 4;
        int warmup = 100;
        int iterations = 2_000;
        for (final String argument : arguments) {
            if (argument.startsWith("--payload=")) payloadLength = Integer.parseInt(argument.substring(10));
            else if (argument.startsWith("--chunk-size=")) chunkSize = Integer.parseInt(argument.substring(13));
            else if (argument.startsWith("--source-buffers=")) sourceBuffers = Integer.parseInt(argument.substring(17));
            else if (argument.startsWith("--warmup=")) warmup = Integer.parseInt(argument.substring(9));
            else if (argument.startsWith("--iterations=")) iterations = Integer.parseInt(argument.substring(13));
        }
        final Result result = measure(payloadLength, chunkSize, sourceBuffers, warmup, iterations);
        System.out.printf(
                "payload=%d chunks=%d sources=%d iterations=%d ns/tx=%.1f MiB/s=%.1f copiedBytes/tx=%d offeredBytes/tx=%d allocatedBytes/tx=%s%n",
                result.payloadLength(), result.chunkCount(), result.sourceBuffers(), result.iterations(),
                result.nanosecondsPerTransaction(), result.mebibytesPerSecond(), result.copiedBytesPerTransaction(),
                result.offeredBytesPerTransaction(),
                result.allocatedBytesPerTransaction() < 0 ? "unavailable" : Long.toString(result.allocatedBytesPerTransaction()));
    }

        /// Measures publisher copy, CRC, envelope, and terminal-marker work.
    static Result measure(
            final int payloadLength, final int chunkSize, final int sourceBuffers, final int warmup, final int iterations) {
        if (payloadLength <= 0 || chunkSize <= 0 || sourceBuffers <= 0 || warmup < 0 || iterations <= 0)
            throw new IllegalArgumentException("invalid publisher benchmark parameters");
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(chunkSize)
                .maxTransactionBytes(Math.max(payloadLength, chunkSize))
                .offerTimeoutNanos(1_000_000_000L)
                .build();
        final ByteBuffer[] sources = sourceBuffers(payloadLength, sourceBuffers);
        final CountingOfferer offerer = new CountingOfferer();
        try (AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                offerer, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0)) {
            for (int i = 0; i < warmup; i++) publisher.publishTransaction(null, sources);
            offerer.reset();
            final AllocationCounter allocation = AllocationCounter.start();
            final long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) publisher.publishTransaction(null, sources);
            final long elapsed = System.nanoTime() - start;
            final long allocated = allocation.bytesSinceStart();
            final double seconds = elapsed / 1_000_000_000.0;
            return new Result(payloadLength, chunkSize, sourceBuffers, countChunks(payloadLength, chunkSize),
                    iterations, elapsed, (long) payloadLength * iterations, offerer.offeredBytes, allocated,
                    payloadLength * (double) iterations / seconds / (1024.0 * 1024.0));
        }
    }

    private static ByteBuffer[] sourceBuffers(final int payloadLength, final int sourceBuffers) {
        final ByteBuffer[] sources = new ByteBuffer[sourceBuffers];
        int remaining = payloadLength;
        for (int i = 0; i < sourceBuffers; i++) {
            final int length = remaining / (sourceBuffers - i);
            sources[i] = ByteBuffer.allocateDirect(length);
            for (int offset = 0; offset < length; offset++) sources[i].put((byte) (offset + i));
            sources[i].flip();
            remaining -= length;
        }
        return sources;
    }

    private static int countChunks(final int payloadLength, final int chunkSize) {
        return Math.max(1, (payloadLength + chunkSize - 1) / chunkSize);
    }

    record Result(
            int payloadLength,
            int chunkSize,
            int sourceBuffers,
            int chunkCount,
            int iterations,
            long elapsedNanoseconds,
            long copiedBytes,
            long offeredBytes,
            long allocatedBytes,
            double mebibytesPerSecond
    ) {
        long copiedBytesPerTransaction() {
            return copiedBytes / iterations;
        }

        long offeredBytesPerTransaction() {
            return offeredBytes / iterations;
        }

        double nanosecondsPerTransaction() {
            return elapsedNanoseconds / (double) iterations;
        }

        long allocatedBytesPerTransaction() {
            return allocatedBytes < 0 ? -1L : allocatedBytes / iterations;
        }
    }

    private static final class CountingOfferer implements AeronOfferRetryer.Offerer {
        private long offeredBytes;
        private long offers;

        @Override
        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            this.offeredBytes += length;
            return ++this.offers;
        }

        private void reset() {
            this.offeredBytes = 0;
            this.offers = 0;
        }
    }

    private static final class AllocationCounter {
        private final com.sun.management.ThreadMXBean bean;
        private final long threadId;
        private final long start;

        private AllocationCounter(final com.sun.management.ThreadMXBean bean, final long threadId, final long start) {
            this.bean = bean;
            this.threadId = threadId;
            this.start = start;
        }

        private static AllocationCounter start() {
            final Object candidate = ManagementFactory.getThreadMXBean();
            if (!(candidate instanceof com.sun.management.ThreadMXBean bean) ||
                !bean.isThreadAllocatedMemorySupported()) return new AllocationCounter(null, -1, -1);
            try {
                if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
                final long threadId = Thread.currentThread().threadId();
                return new AllocationCounter(bean, threadId, bean.getThreadAllocatedBytes(threadId));
            } catch (final UnsupportedOperationException | SecurityException ignored) {
                return new AllocationCounter(null, -1, -1);
            }
        }

        private long bytesSinceStart() {
            if (this.bean == null) return -1;
            try {
                return this.bean.getThreadAllocatedBytes(this.threadId) - this.start;
            } catch (final UnsupportedOperationException | SecurityException ignored) {
                return -1;
            }
        }
    }
}
