package peruncs.cluster.storage.aeron.writer;

import io.aeron.DirectBufferVector;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Verifies the gathered and the staged data paths put identical frames on the wire.
class EnvelopeFramerTest {
    private static final UUID CLUSTER = UUID.fromString("0f4e7a52-6b0a-4e16-8c63-0f6f5d3c1b11");
    private static final int CHUNK = 256;

    /// Records every frame offered, flattening vectors into one message, and counts gathered offers.
    private static final class Recorder implements AeronOfferRetryer.Offerer {
        final List<byte[]> frames = new ArrayList<>();
        final AtomicInteger gathered = new AtomicInteger();
        private final boolean vectors;
        private long position;

        Recorder(final boolean vectors) {
            this.vectors = vectors;
        }

        @Override
        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            this.frames.add(copy);
            return this.position += length;
        }

        @Override
        public long offer(final DirectBufferVector[] vectors) {
            if (!this.vectors) return AeronOfferRetryer.Offerer.super.offer(vectors);
            this.gathered.incrementAndGet();
            int length = 0;
            for (final DirectBufferVector vector : vectors) length += vector.length();
            final byte[] copy = new byte[length];
            int at = 0;
            for (final DirectBufferVector vector : vectors) {
                vector.buffer().getBytes(vector.offset(), copy, at, vector.length());
                at += vector.length();
            }
            this.frames.add(copy);
            return this.position += length;
        }
    }

    private record Outcome(List<byte[]> frames, int crc, int gatheredOffers) {
    }

    private static Outcome frame(final boolean vectors, final byte[][] sources) {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(CHUNK).maxTransactionBytes(8 * 1024).build();
        final ByteBuffer storage = ByteBuffer.allocateDirect(CHUNK + 128);
        final Recorder recorder = new Recorder(vectors);
        final ByteBuffer[] buffers = new ByteBuffer[sources.length];
        int length = 0;
        for (int i = 0; i < sources.length; i++) {
            buffers[i] = ByteBuffer.wrap(sources[i]);
            length += sources[i].length;
        }
        try (EnvelopeFramer framer = new EnvelopeFramer(7L,
                new EnvelopeFramer.Configuration(CLUSTER, 1L, 99L, CHUNK, storage, new EnvelopeFramer.GatherScratch()),
                3L, configuration.maxMessageLength(), new AeronOfferRetryer(recorder, configuration))) {
            final int crc = framer.offerDataChunks(buffers, buffers.length, length);
            return new Outcome(recorder.frames, crc, recorder.gathered.get());
        }
    }

    private static byte[][] split(final int total, final int... cuts) {
        final byte[] all = new byte[total];
        new Random(total).nextBytes(all);
        final byte[][] parts = new byte[cuts.length + 1][];
        int from = 0;
        for (int i = 0; i <= cuts.length; i++) {
            final int to = i == cuts.length ? total : cuts[i];
            parts[i] = java.util.Arrays.copyOfRange(all, from, to);
            from = to;
        }
        return parts;
    }

    private static void assertSameOnTheWire(final byte[][] sources, final boolean expectGather) {
        final Outcome staged = frame(false, sources);
        final Outcome gathered = frame(true, sources);
        assertEquals(staged.crc(), gathered.crc(), "transaction checksum");
        assertEquals(staged.frames().size(), gathered.frames().size(), "frame count");
        for (int i = 0; i < staged.frames().size(); i++) {
            assertArrayEquals(staged.frames().get(i), gathered.frames().get(i), "frame " + i);
        }
        assertEquals(0, staged.gatheredOffers());
        if (expectGather) assertTrue(gathered.gatheredOffers() > 0, "the gather branch must have run");
    }

    @Test
    void manySourcesGrowTheVectorScratchAndMatchTheStagedFrames() {
        assertSameOnTheWire(split(1_000, 100, 200, 300, 400, 500, 600, 700, 800, 900), true);
    }

    @Test
    void chunkBoundariesOnAndAcrossSourceBoundariesMatch() {
        assertSameOnTheWire(split(4 * CHUNK, CHUNK, 2 * CHUNK, 3 * CHUNK), true);
        assertSameOnTheWire(split(3 * CHUNK, 10, 20, 3 * CHUNK - 5), true);
    }

    @Test
    void aChunkSpanningThreeSourcesMatches() {
        assertSameOnTheWire(split(2 * CHUNK, 50, 120), true);
    }

    @Test
    void readOnlyHeapSourcesAreAccepted() {
        final byte[][] sources = split(2 * CHUNK + 17, 100);
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(CHUNK).maxTransactionBytes(8 * 1024).build();
        final Recorder recorder = new Recorder(true);
        final ByteBuffer[] buffers = {ByteBuffer.wrap(sources[0]).asReadOnlyBuffer(),
                ByteBuffer.wrap(sources[1]).asReadOnlyBuffer()};
        try (EnvelopeFramer framer = new EnvelopeFramer(1L,
                new EnvelopeFramer.Configuration(CLUSTER, 1L, 99L, CHUNK, ByteBuffer.allocateDirect(CHUNK + 128),
                        new EnvelopeFramer.GatherScratch()),
                1L, configuration.maxMessageLength(), new AeronOfferRetryer(recorder, configuration))) {
            framer.offerDataChunks(buffers, 2, 2 * CHUNK + 17);
        }
        assertEquals(3, recorder.frames.size());
    }

    @Test
    void payloadsSmallerThanOneChunkAreGatheredToo() {
        final Outcome small = frame(true, split(CHUNK - 1, 10));
        assertEquals(1, small.gatheredOffers(), "every data chunk is offered as vectors");
        assertEquals(1, small.frames().size());
        assertSameOnTheWire(split(CHUNK - 1, 10), true);
    }
}
