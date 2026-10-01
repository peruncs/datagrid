package peruncs.cluster.storage.aeron.writer;

import io.aeron.DirectBufferVector;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/// Compares the current staging copy with Aeron's equivalent vectored term-buffer copy.
/// Both paths compute transaction and per-chunk CRC32C, then write the identical frame bytes.
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class AeronGatherOfferBenchmark {
    private static final int HEADER_LENGTH = 84;
    private static final int CHUNK_SIZE = 128 * 1024;

    @Param({"1024", "65536", "131072", "262144", "1048576"})
    private int payloadBytes;

    @Param({"1", "4", "8"})
    private int sourceBufferCount;

    private ByteBuffer[] sources;
    private UnsafeBuffer[] sourceViews;
    private UnsafeBuffer header;
    private UnsafeBuffer staging;
    private UnsafeBuffer term;
    private DirectBufferVector[][] vectorsByCount;
    private DirectBufferVector[] vectorScratch;
    private final CRC32C transactionCrc = new CRC32C();
    private final CRC32C chunkCrc = new CRC32C();

    /// Allocates reusable direct buffers and verifies both paths produce identical frame bytes.
    @Setup(Level.Trial)
    public void setUp() {
        this.sources = new ByteBuffer[this.sourceBufferCount];
        this.sourceViews = new UnsafeBuffer[this.sourceBufferCount];
        int remaining = this.payloadBytes;
        for (int i = 0; i < this.sourceBufferCount; i++) {
            final int length = remaining / (this.sourceBufferCount - i);
            this.sources[i] = ByteBuffer.allocateDirect(length);
            for (int offset = 0; offset < length; offset++) this.sources[i].put((byte) (offset + i));
            this.sources[i].flip();
            this.sourceViews[i] = new UnsafeBuffer(this.sources[i]);
            remaining -= length;
        }
        this.header = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_LENGTH));
        for (int i = 0; i < HEADER_LENGTH; i++) this.header.putByte(i, (byte) i);
        this.staging = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_LENGTH + CHUNK_SIZE));
        this.term = new UnsafeBuffer(ByteBuffer.allocateDirect(this.payloadBytes +
                chunks(this.payloadBytes) * HEADER_LENGTH));
        this.vectorScratch = new DirectBufferVector[this.sourceBufferCount + 1];
        for (int i = 0; i < this.vectorScratch.length; i++) this.vectorScratch[i] = new DirectBufferVector();
        this.vectorsByCount = new DirectBufferVector[this.vectorScratch.length + 1][];
        for (int count = 1; count < this.vectorsByCount.length; count++) {
            this.vectorsByCount[count] = Arrays.copyOf(this.vectorScratch, count);
        }

        final int stagedResult = this.staged();
        final int stagedTransactionCrc = (int) this.transactionCrc.getValue();
        final int stagedChunkCrc = (int) this.chunkCrc.getValue();
        final byte[] stagedBytes = bytes(this.term, this.payloadBytes + chunks(this.payloadBytes) * HEADER_LENGTH);
        final int gatheredResult = this.gathered();
        if (stagedResult != gatheredResult || stagedTransactionCrc != (int) this.transactionCrc.getValue() ||
            stagedChunkCrc != (int) this.chunkCrc.getValue() ||
            !Arrays.equals(stagedBytes, bytes(this.term, stagedBytes.length))) {
            throw new IllegalStateException("staged and gathered bytes or CRCs differ");
        }
    }

    /// Measures checksum, staging-copy, and contiguous offer work.
    @Benchmark
    public int staged() {
        this.transactionCrc.reset();
        int sourceIndex = 0;
        int sourceOffset = 0;
        int logicalOffset = 0;
        int termOffset = 0;
        while (logicalOffset < this.payloadBytes) {
            final int chunkLength = Math.min(CHUNK_SIZE, this.payloadBytes - logicalOffset);
            this.chunkCrc.reset();
            int copied = 0;
            while (copied < chunkLength) {
                while (sourceOffset == this.sources[sourceIndex].limit()) {
                    sourceIndex++;
                    sourceOffset = 0;
                }
                final int amount = Math.min(this.sources[sourceIndex].limit() - sourceOffset, chunkLength - copied);
                update(this.transactionCrc, this.sources[sourceIndex], sourceOffset, amount);
                update(this.chunkCrc, this.sources[sourceIndex], sourceOffset, amount);
                this.staging.putBytes(HEADER_LENGTH + copied, this.sourceViews[sourceIndex], sourceOffset, amount);
                sourceOffset += amount;
                copied += amount;
            }
            this.staging.putBytes(0, this.header, 0, HEADER_LENGTH);
            this.term.putBytes(termOffset, this.staging, 0, HEADER_LENGTH + chunkLength);
            termOffset += HEADER_LENGTH + chunkLength;
            logicalOffset += chunkLength;
        }
        return (int) (this.transactionCrc.getValue() ^ this.chunkCrc.getValue() ^ this.term.getByte(termOffset - 1));
    }

    /// Measures the same checksums and a reusable vectored offer without the staging copy.
    @Benchmark
    public int gathered() {
        this.transactionCrc.reset();
        int sourceIndex = 0;
        int sourceOffset = 0;
        int logicalOffset = 0;
        int termOffset = 0;
        while (logicalOffset < this.payloadBytes) {
            final int chunkLength = Math.min(CHUNK_SIZE, this.payloadBytes - logicalOffset);
            this.chunkCrc.reset();
            int copied = 0;
            int vectorCount = 1;
            while (copied < chunkLength) {
                while (sourceOffset == this.sources[sourceIndex].limit()) {
                    sourceIndex++;
                    sourceOffset = 0;
                }
                final int amount = Math.min(this.sources[sourceIndex].limit() - sourceOffset, chunkLength - copied);
                update(this.transactionCrc, this.sources[sourceIndex], sourceOffset, amount);
                update(this.chunkCrc, this.sources[sourceIndex], sourceOffset, amount);
                this.vectorScratch[vectorCount++].reset(this.sourceViews[sourceIndex], sourceOffset, amount);
                sourceOffset += amount;
                copied += amount;
            }
            final DirectBufferVector[] vectors = this.vectorsByCount[vectorCount];
            vectors[0].reset(this.header, 0, HEADER_LENGTH);
            final int offeredLength = DirectBufferVector.validateAndComputeLength(vectors);
            for (final DirectBufferVector vector : vectors) {
                this.term.putBytes(termOffset, vector.buffer(), vector.offset(), vector.length());
                termOffset += vector.length();
            }
            if (offeredLength != HEADER_LENGTH + chunkLength) throw new IllegalStateException("bad vector length");
            logicalOffset += chunkLength;
        }
        return (int) (this.transactionCrc.getValue() ^ this.chunkCrc.getValue() ^ this.term.getByte(termOffset - 1));
    }

    private static int chunks(final int payloadBytes) {
        return (payloadBytes + CHUNK_SIZE - 1) / CHUNK_SIZE;
    }

    private static void update(final CRC32C crc, final ByteBuffer source, final int offset, final int length) {
        final int position = source.position();
        final int limit = source.limit();
        try {
            source.position(offset).limit(offset + length);
            crc.update(source);
        } finally {
            source.limit(limit).position(position);
        }
    }

    private static byte[] bytes(final UnsafeBuffer buffer, final int length) {
        final byte[] result = new byte[length];
        buffer.getBytes(0, result);
        return result;
    }
}
