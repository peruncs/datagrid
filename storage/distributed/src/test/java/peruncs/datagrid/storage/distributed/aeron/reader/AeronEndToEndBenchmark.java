package peruncs.datagrid.storage.distributed.aeron.reader;

import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.storage.distributed.types.StorageBinaryDataReceiver;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Test-only end-to-end application-path benchmark. It measures envelope
 * encoding, direct-buffer assembly, CRC validation, and complete-binary handoff
 * without adding a benchmark dependency or production instrumentation.
 */
public final class AeronEndToEndBenchmark {
    private AeronEndToEndBenchmark() {
    }

    public static void main(final String[] arguments) {
        int payload = 1_048_576;
        int chunk = 16 * 1024;
        int warmup = 100;
        int iterations = 1_000;
        for (final String argument : arguments) {
            if (argument.startsWith("--payload=")) payload = Integer.parseInt(argument.substring(10));
            else if (argument.startsWith("--chunk-size=")) chunk = Integer.parseInt(argument.substring(13));
            else if (argument.startsWith("--warmup=")) warmup = Integer.parseInt(argument.substring(9));
            else if (argument.startsWith("--iterations=")) iterations = Integer.parseInt(argument.substring(13));
        }
        final Result result = measure(payload, chunk, warmup, iterations);
        System.out.printf(
                "payload=%d chunks=%d iterations=%d ns/tx=%.1f MiB/s=%.1f " +
                "assembledBytes/tx=%d allocatedBytes/tx=%s%n",
                result.payloadLength(), result.chunkCount(), result.iterations(),
                result.nanosecondsPerTransaction(), result.mebibytesPerSecond(),
                result.assembledBytesPerTransaction(),
                result.allocatedBytesPerTransaction() < 0 ? "unavailable" :
                        Long.toString(result.allocatedBytesPerTransaction()));
    }

    /** Measures the complete application framing and reader assembly path. */
    static Result measure(final int payloadLength, final int chunkSize, final int warmup, final int iterations) {
        if (payloadLength <= 0 || chunkSize <= 0 || warmup < 0 || iterations <= 0)
            throw new IllegalArgumentException("invalid end-to-end benchmark parameters");
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(chunkSize).maxTransactionBytes(payloadLength).build();
        final ByteBuffer payload = ByteBuffer.allocateDirect(payloadLength);
        for (int i = 0; i < payloadLength; i++) payload.put((byte) (i * 31));
        payload.flip();
        final UUID clusterId = UUID.randomUUID();
        final BenchmarkReceiver receiver = new BenchmarkReceiver();
        final TransactionAssembler assembler = new TransactionAssembler(configuration, clusterId, 1, receiver);
        final UnsafeBuffer sourceBuffer = new UnsafeBuffer(payload);
        final UnsafeBuffer frame = new UnsafeBuffer(ByteBuffer.allocateDirect(chunkSize + AeronReplicationEnvelope.HEADER_LENGTH));
        final UnsafeBuffer empty = new UnsafeBuffer(ByteBuffer.allocateDirect(0));
        for (int i = 0; i < warmup; i++) publishOne(assembler, sourceBuffer, frame, empty, chunkSize, clusterId, i);
        receiver.reset();
        final AllocationCounter allocation = AllocationCounter.start();
        final long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) publishOne(assembler, sourceBuffer, frame, empty, chunkSize, clusterId, warmup + i);
        final long elapsed = System.nanoTime() - start;
        final long allocated = allocation.bytesSinceStart();
        if (assembler.failure() != null) throw new IllegalStateException("benchmark assembly failed", assembler.failure());
        return new Result(payloadLength, chunkCount(payloadLength, chunkSize), iterations, elapsed,
                receiver.assembledBytes, allocated, payloadLength * (double) iterations /
                                                    (elapsed / 1_000_000_000.0) / (1024.0 * 1024.0));
    }

    private static void publishOne(final TransactionAssembler assembler, final UnsafeBuffer source,
                                   final UnsafeBuffer frame, final UnsafeBuffer empty, final int chunkSize, final UUID clusterId, final long sequence) {
        final int length = source.capacity();
        final int count = chunkCount(length, chunkSize);
        final int crc = AeronReplicationEnvelope.crc32c(source, 0, length);
        for (int index = 0, offset = 0; offset < length; index++) {
            final int size = Math.min(chunkSize, length - offset);
            final int encoded = AeronReplicationEnvelope.encode(frame, 0, clusterId, 1,
                    sequence, AeronReplicationEnvelope.Kind.STORE_BINARY, length, index, count, offset, 0,
                    source, offset, size);
            assembler.onFragment(frame, 0, encoded, null);
            offset += size;
        }
        final int encoded = AeronReplicationEnvelope.encode(frame, 0, clusterId, 1, sequence,
                AeronReplicationEnvelope.Kind.COMMIT, length, 0, count, 0, crc, empty, 0, 0);
        assembler.onFragment(frame, 0, encoded, null);
    }

    private static int chunkCount(final int length, final int chunkSize) {
        return Math.max(1, (length + chunkSize - 1) / chunkSize);
    }

    record Result(int payloadLength, int chunkCount, int iterations, long elapsedNanoseconds,
                  long assembledBytes, long allocatedBytes, double mebibytesPerSecond) {
        double nanosecondsPerTransaction() {
            return elapsedNanoseconds / (double) iterations;
        }

        long assembledBytesPerTransaction() {
            return assembledBytes / iterations;
        }

        long allocatedBytesPerTransaction() {
            return allocatedBytes < 0 ? -1L : allocatedBytes / iterations;
        }
    }

    private static final class BenchmarkReceiver implements StorageBinaryDataReceiver {
        private long assembledBytes;

        @Override
        public void receiveTypeDictionary(final String value) {
        }

        @Override
        public void receiveData(final Binary value) {
            for (final ByteBuffer chunk : value.buffers()) this.assembledBytes += chunk.position();
        }

        private void reset() {
            this.assembledBytes = 0L;
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
            final Object candidate = java.lang.management.ManagementFactory.getThreadMXBean();
            if (!(candidate instanceof com.sun.management.ThreadMXBean bean) || !bean.isThreadAllocatedMemorySupported())
                return new AllocationCounter(null, -1L, -1L);
            try {
                if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
                final long id = Thread.currentThread().getId();
                return new AllocationCounter(bean, id, bean.getThreadAllocatedBytes(id));
            } catch (final UnsupportedOperationException | SecurityException ignored) {
                return new AllocationCounter(null, -1L, -1L);
            }
        }

        private long bytesSinceStart() {
            return this.bean == null ? -1L : this.bean.getThreadAllocatedBytes(this.threadId) - this.start;
        }
    }
}
