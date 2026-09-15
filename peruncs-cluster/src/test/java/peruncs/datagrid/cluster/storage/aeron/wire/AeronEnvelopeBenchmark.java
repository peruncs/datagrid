package peruncs.datagrid.cluster.storage.aeron.wire;

import org.agrona.concurrent.UnsafeBuffer;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.UUID;

/// Reproducible, dependency-free benchmark for the Aeron envelope staging path.
///
/// Run from the module test class path with
/// `java ... AeronEnvelopeBenchmark --iterations=5000 --sizes=65536,1048576 --chunk-size=16384`. The output reports chunk count, throughput, bytes copied,
/// and (when the JVM exposes it) bytes allocated by the benchmark thread. Archive
/// offers and forced checkpoint writes remain a separate environment benchmark.
public final class AeronEnvelopeBenchmark {
    private AeronEnvelopeBenchmark() {
    }

    public static void main(final String[] arguments) {
        int iterations = 2_000;
        int warmup = 100;
        int chunkSize = 16 * 1024;
        String sizes = "65536,1048576,16777216";
        for (final String argument : arguments) {
            if (argument.startsWith("--iterations=")) iterations = Integer.parseInt(argument.substring(13));
            else if (argument.startsWith("--warmup=")) warmup = Integer.parseInt(argument.substring(9));
            else if (argument.startsWith("--chunk-size=")) chunkSize = Integer.parseInt(argument.substring(13));
            else if (argument.startsWith("--sizes=")) sizes = argument.substring(8);
        }
        if (iterations <= 0 || warmup < 0 || chunkSize <= 0) {
            throw new IllegalArgumentException("iterations and chunk-size must be positive; warmup cannot be negative");
        }
        final UUID clusterId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        for (final String sizeText : sizes.split(",")) {
            final int payloadLength = Integer.parseInt(sizeText.trim());
            if (payloadLength <= 0) throw new IllegalArgumentException("size must be positive");
            final BenchmarkResult result = measure(clusterId, 1, payloadLength, chunkSize, warmup, iterations);
            System.out.printf(
                    "payload=%d chunks=%d chunkSize=%d iterations=%d ns/tx=%.1f MiB/s=%.1f copiedBytes/tx=%d allocatedBytes/tx=%s%n",
                    payloadLength, result.chunkCount(), chunkSize, iterations, result.nanosecondsPerTransaction(),
                    result.mebibytesPerSecond(), result.copiedBytesPerTransaction(),
                    result.allocatedBytesPerTransaction() < 0 ? "unavailable" : Long.toString(result.allocatedBytesPerTransaction()));
        }
    }

        /// Measures one payload size using the same per-chunk framing as the writer.
    static BenchmarkResult measure(
            final UUID clusterId,
            final long epoch,
            final int payloadLength,
            final int chunkSize,
            final int warmup,
            final int iterations
    ) {
        if (clusterId == null || epoch < 0 || payloadLength <= 0 || chunkSize <= 0 || warmup < 0 || iterations <= 0) {
            throw new IllegalArgumentException("invalid benchmark parameters");
        }
        final UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(payloadLength));
        for (int i = 0; i < payloadLength; i++) payload.putByte(i, (byte) i);
        final int chunkCount = Math.toIntExact(((long) payloadLength + chunkSize - 1L) / chunkSize);
        final UnsafeBuffer target = new UnsafeBuffer(ByteBuffer.allocateDirect(
                AeronReplicationEnvelope.HEADER_LENGTH + chunkSize));
        final var checksum = new AeronReplicationEnvelope.ChecksumContext();
        for (int i = 0; i < warmup; i++) {
            final int sequence = i;
            AeronReplicationEnvelope.withChecksumContext(checksum, () -> {
                encodeTransaction(target, payload, clusterId, epoch, sequence,
                        payloadLength, chunkSize, chunkCount);
                return null;
            });
        }
        final AllocationCounter allocation = AllocationCounter.start();
        final long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            final int sequence = i + warmup;
            AeronReplicationEnvelope.withChecksumContext(checksum, () -> {
                encodeTransaction(target, payload, clusterId, epoch, sequence,
                        payloadLength, chunkSize, chunkCount);
                return null;
            });
        }
        final long elapsed = System.nanoTime() - start;
        final long allocated = allocation.bytesSinceStart();
        final long copied = Math.multiplyExact((long) payloadLength, iterations);
        final double seconds = elapsed / 1_000_000_000.0;
        return new BenchmarkResult(payloadLength, chunkSize, chunkCount, iterations, elapsed,
                copied, allocated, payloadLength * (double) iterations / seconds / (1024.0 * 1024.0));
    }

    private static void encodeTransaction(
            final UnsafeBuffer target,
            final UnsafeBuffer payload,
            final UUID clusterId,
            final long epoch,
            final long sequence,
            final int payloadLength,
            final int chunkSize,
            final int chunkCount
    ) {
        for (int chunkIndex = 0, offset = 0; chunkIndex < chunkCount; chunkIndex++) {
            final int length = Math.min(chunkSize, payloadLength - offset);
            AeronReplicationEnvelope.encode(target, 0, clusterId, epoch, sequence,
                    AeronReplicationEnvelope.Kind.STORE_BINARY, payloadLength, chunkIndex, chunkCount, offset, 0,
                    payload, offset, length);
            offset += length;
        }
    }

    record BenchmarkResult(
            int payloadLength,
            int chunkSize,
            int chunkCount,
            int iterations,
            long elapsedNanoseconds,
            long copiedBytes,
            long allocatedBytes,
            double mebibytesPerSecond
    ) {
        double nanosecondsPerTransaction() {
            return elapsedNanoseconds / (double) iterations;
        }

        long copiedBytesPerTransaction() {
            return copiedBytes / iterations;
        }

        long allocatedBytesPerTransaction() {
            return allocatedBytes < 0 ? -1L : allocatedBytes / iterations;
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

        static AllocationCounter start() {
            final Object candidate = ManagementFactory.getThreadMXBean();
            if (!(candidate instanceof com.sun.management.ThreadMXBean bean) ||
                !bean.isThreadAllocatedMemorySupported()) return unavailable();
            try {
                if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
                final long threadId = Thread.currentThread().getId();
                return new AllocationCounter(bean, threadId, bean.getThreadAllocatedBytes(threadId));
            } catch (final UnsupportedOperationException | SecurityException ignored) {
                return unavailable();
            }
        }

        private static AllocationCounter unavailable() {
            return new AllocationCounter(null, -1L, -1L);
        }

        long bytesSinceStart() {
            if (this.bean == null) return -1L;
            try {
                return this.bean.getThreadAllocatedBytes(this.threadId) - this.start;
            } catch (final UnsupportedOperationException | SecurityException ignored) {
                return -1L;
            }
        }
    }
}
