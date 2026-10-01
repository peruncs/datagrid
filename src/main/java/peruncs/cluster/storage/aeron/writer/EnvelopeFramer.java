package peruncs.cluster.storage.aeron.writer;

import io.aeron.DirectBufferVector;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.io.FaultInjection;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32C;

/// Encodes one transaction's frames in the publisher's reusable frame buffer.
///
/// Full chunks use Aeron's gathering offer; smaller payloads reuse the staging area.
final class EnvelopeFramer implements AutoCloseable {
    /* P1-6 JMH: four-buffer crossover is the default 128 KiB chunk size. */
    private static final int MIN_GATHER_PAYLOAD_BYTES = AeronReplicationConfiguration.DEFAULT_CHUNK_SIZE;
    private static final LazyConstant<UnsafeBuffer> EMPTY_BUFFER = LazyConstant.of(UnsafeBuffer::new);

    /// Publisher-wide framing values and reusable vector scratch.
    record Configuration(UUID clusterId, long epoch, long wireNonce, int chunkSize, ByteBuffer storage,
                         GatherScratch gatherScratch) {
        Configuration {
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(storage, "storage");
            Objects.requireNonNull(gatherScratch, "gatherScratch");
            if (wireNonce == 0L || chunkSize <= 0) throw new IllegalArgumentException("invalid framing configuration");
        }
    }

    /// Reuses source wrappers and exact-length vector arrays for the publisher's serialized writes.
    /// It is not thread-safe; a publisher prepares only one transaction at a time.
    static final class GatherScratch {
        private static final ByteBuffer EMPTY_SOURCE = ByteBuffer.allocate(0).asReadOnlyBuffer();
        private UnsafeBuffer[] sourceBuffers = new UnsafeBuffer[0];
        private DirectBufferVector[] slots = new DirectBufferVector[0];
        private DirectBufferVector[][] vectorsByCount = new DirectBufferVector[0][];

        private void ensureSourceCapacity(final int sourceCount) {
            if (sourceCount <= this.sourceBuffers.length) return;
            final int capacity = Math.max(sourceCount, Math.max(4, this.sourceBuffers.length * 2));
            this.sourceBuffers = Arrays.copyOf(this.sourceBuffers, capacity);
            for (int i = 0; i < capacity; i++) {
                if (this.sourceBuffers[i] == null) this.sourceBuffers[i] = new UnsafeBuffer();
            }
            final int oldLength = this.slots.length;
            this.slots = Arrays.copyOf(this.slots, capacity + 1);
            for (int i = oldLength; i < this.slots.length; i++) this.slots[i] = new DirectBufferVector();
            this.vectorsByCount = Arrays.copyOf(this.vectorsByCount, this.slots.length + 1);
        }

        private UnsafeBuffer source(final int index, final ByteBuffer buffer) {
            this.sourceBuffers[index].wrap(buffer);
            return this.sourceBuffers[index];
        }

        private UnsafeBuffer source(final int index) {
            return this.sourceBuffers[index];
        }

        private void clearSources(final int sourceCount) {
            for (int i = 0; i < sourceCount; i++) this.sourceBuffers[i].wrap(EMPTY_SOURCE);
        }

        private DirectBufferVector slot(final int index) {
            return this.slots[index];
        }

        private DirectBufferVector[] vectors(final int count) {
            DirectBufferVector[] vectors = this.vectorsByCount[count];
            if (vectors == null) this.vectorsByCount[count] = vectors = Arrays.copyOf(this.slots, count);
            return vectors;
        }
    }

    private final long sequence;
    private final UUID clusterId;
    private final long epoch;
    private final long wireNonce;
    private final int chunkSize;
    private final int maxMessageLength;
    private final AeronOfferRetryer offerer;
    /* Captured once so every frame of one transaction carries one token. */
    private final long fencingToken;
    private final ByteBuffer storage;
    private final UnsafeBuffer buffer;
    private final GatherScratch gatherScratch;
    private long lastOfferPosition;
    private final AeronReplicationEnvelope.ChecksumContext checksum =
            new AeronReplicationEnvelope.ChecksumContext();
    private final CRC32C dataCrc = new CRC32C();
    private final CRC32C chunkCrc = new CRC32C();
    private final AtomicBoolean closed = new AtomicBoolean();

    /// Creates a framer over the publisher's reusable direct staging buffer.
    EnvelopeFramer(final long sequence, final Configuration configuration, final long fencingToken,
                   final int maxMessageLength, final AeronOfferRetryer offerer) {
        Objects.requireNonNull(configuration, "configuration");
        this.sequence = sequence;
        this.clusterId = configuration.clusterId();
        this.epoch = configuration.epoch();
        this.wireNonce = configuration.wireNonce();
        this.chunkSize = configuration.chunkSize();
        this.maxMessageLength = maxMessageLength;
        this.offerer = Objects.requireNonNull(offerer, "offerer");
        this.fencingToken = fencingToken;
        this.storage = configuration.storage();
        this.buffer = new UnsafeBuffer(this.storage);
        this.gatherScratch = configuration.gatherScratch();
    }

    /// Encodes the type dictionary into one frame per chunk and offers each.
    void offerDictionaryChunks(final DirectBuffer bytes, final int length) {
        if (length == 0) {
            return;
        }
        final int count = chunkCount(length, this.chunkSize);
        for (int index = 0, offset = 0; offset < length; index++) {
            final int chunkLength = Math.min(this.chunkSize, length - offset);
            this.offerEncoded(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, length, index, count,
                    offset, 0, bytes, offset, chunkLength);
            offset += chunkLength;
        }
    }

    /// Streams Store data directly from the caller's buffer sequence into one
    /// frame per chunk, offers each frame, and returns the CRC32C of the
    /// complete logical Store binary.
    ///
    /// The coordinator uses the returned CRC in the commit marker. The earlier
    /// fence CRC remains a separate pass because it is the recovery evidence
    /// written before local Store acceptance.
    int offerDataChunks(final ByteBuffer[] sources, final int sourceCount, final int length) {
        if (this.closed.get()) throw new IllegalStateException("Aeron envelope framer is closed");
        if (length >= MIN_GATHER_PAYLOAD_BYTES && this.offerer.supportsVectors()) {
            return this.offerGatheredDataChunks(sources, sourceCount, length);
        }
        final CRC32C crc = this.dataCrc;
        crc.reset();
        if (length == 0) {
            this.offerEncoded(AeronReplicationEnvelope.Kind.STORE_BINARY,
                    0, 0, 1, 0, 0, EMPTY_BUFFER.get(), 0, 0);
            return 0;
        }

        final int count = chunkCount(length, this.chunkSize);
        int sourceIndex = 0;
        ByteBuffer source = sources[sourceIndex];
        int sourcePosition = source.position();
        int logicalOffset = 0;
        for (int chunkIndex = 0; chunkIndex < count; chunkIndex++) {
            final int chunkLength = Math.min(this.chunkSize, length - logicalOffset);
            this.chunkCrc.reset();
            int copied = 0;
            while (copied < chunkLength) {
                while (sourcePosition >= source.limit()) {
                    if (++sourceIndex >= sourceCount) throw new IllegalArgumentException("data buffer length changed");
                    source = sources[sourceIndex];
                    sourcePosition = source.position();
                }
                /* CRC and envelope fill read the source segment directly:
                 * no heap staging copy between the caller buffers and the
                 * off-heap envelope. */
                final int amount = Math.min(source.limit() - sourcePosition, chunkLength - copied);
                updateCrc(crc, source, sourcePosition, amount);
                updateCrc(this.chunkCrc, source, sourcePosition, amount);
                this.buffer.putBytes(AeronReplicationEnvelope.HEADER_LENGTH + copied,
                        source, sourcePosition, amount);
                sourcePosition += amount;
                copied += amount;
            }
            this.offerDataChunk(length, chunkIndex, count, logicalOffset,
                    chunkLength, (int) this.chunkCrc.getValue());
            /* Intra-transaction seam for forked crash tests: a chunk budget
             * kills the child between two milestones of one large
             * transaction. Unbound cost is one ScopedValue check. */
            FaultInjection.invoke("DATA_CHUNK", this.sequence);
            logicalOffset += chunkLength;
        }
        return (int) crc.getValue();
    }

    private int offerGatheredDataChunks(final ByteBuffer[] sources, final int sourceCount, final int length) {
        final CRC32C crc = this.dataCrc;
        crc.reset();
        this.gatherScratch.ensureSourceCapacity(sourceCount);
        try {
            for (int i = 0; i < sourceCount; i++) this.gatherScratch.source(i, sources[i]);
            final int count = chunkCount(length, this.chunkSize);
            int sourceIndex = 0;
            int sourcePosition = sources[0].position();
            int logicalOffset = 0;
            for (int chunkIndex = 0; chunkIndex < count; chunkIndex++) {
                final int chunkLength = Math.min(this.chunkSize, length - logicalOffset);
                this.chunkCrc.reset();
                int copied = 0;
                int vectorCount = 1;
                while (copied < chunkLength) {
                    while (sourcePosition >= sources[sourceIndex].limit()) {
                        if (++sourceIndex >= sourceCount) {
                            throw new IllegalArgumentException("data buffer length changed");
                        }
                        sourcePosition = sources[sourceIndex].position();
                    }
                    final ByteBuffer source = sources[sourceIndex];
                    final int amount = Math.min(source.limit() - sourcePosition, chunkLength - copied);
                    updateCrc(crc, source, sourcePosition, amount);
                    updateCrc(this.chunkCrc, source, sourcePosition, amount);
                    this.gatherScratch.slot(vectorCount++).reset(
                            this.gatherScratch.source(sourceIndex), sourcePosition, amount);
                    sourcePosition += amount;
                    copied += amount;
                }
                this.offerGatheredDataChunk(length, chunkIndex, count, logicalOffset, chunkLength,
                        (int) this.chunkCrc.getValue(), this.gatherScratch.vectors(vectorCount));
                FaultInjection.invoke("DATA_CHUNK", this.sequence);
                logicalOffset += chunkLength;
            }
            return (int) crc.getValue();
        } finally {
            this.gatherScratch.clearSources(sourceCount);
        }
    }

    /// Encodes and offers a terminal commit or abort marker.
    ///
    /// @return Aeron publication position of the offered marker
    long offerMarker(final AeronReplicationEnvelope.Kind kind,
                     final int payloadLength, final int chunkCount, final int commitCrc32c) {
        return this.offerEncoded(kind, payloadLength, 0, Math.max(1, chunkCount), 0,
                commitCrc32c, EMPTY_BUFFER.get(), 0, 0);
    }

    /// Returns the last successfully offered frame position.
    long lastOfferPosition() {
        return this.lastOfferPosition;
    }

    /// Computes the CRC32C of the populated prefix of a reusable buffer array
    /// without offering anything. Used for fence metadata and failure evidence.
    static int computeDataCrc(final ByteBuffer[] sources, final int sourceCount, final int length,
                              final CRC32C crc) {
        crc.reset();
        int remaining = length;
        for (int sourceIndex = 0; sourceIndex < sourceCount; sourceIndex++) {
            final ByteBuffer sourceBuffer = sources[sourceIndex];
            final int amount = Math.min(remaining, sourceBuffer.remaining());
            if (amount > 0) {
                updateCrc(crc, sourceBuffer, sourceBuffer.position(), amount);
                remaining -= amount;
            }
            if (remaining == 0) break;
        }
        if (remaining != 0) throw new IllegalArgumentException("data buffer length changed");
        return (int) crc.getValue();
    }

    /// Returns the number of chunks a payload of `length` bytes occupies.
    static int chunkCount(final int length, final int chunkSize) {
        return Math.max(1, (int) ((length + (long) chunkSize - 1L) / chunkSize));
    }

    /// Updates CRC32C from a source range without copying or allocating a view.
    ///
    /// The writer is the only reader of these buffers for the duration of a
    /// write, so the position and limit are moved to the requested range and
    /// restored before returning. Both checksum passes cover the same
    /// segments, so this replaces two `ByteBuffer.duplicate()` views per
    /// segment per chunk with no view at all; on return the caller's buffer
    /// state is byte-for-byte unchanged.
    private static void updateCrc(final CRC32C crc, final ByteBuffer source,
                                  final int offset, final int length) {
        final int position = source.position();
        final int limit = source.limit();
        try {
            source.position(offset).limit(offset + length);
            crc.update(source);
        } finally {
            source.limit(limit).position(position);
        }
    }

    private long offerEncoded(final AeronReplicationEnvelope.Kind kind,
                              final int payloadLength, final int chunkIndex, final int chunkCount,
                              final int chunkOffset, final int commitCrc32c,
                              final DirectBuffer payload, final int payloadOffset,
                              final int payloadChunkLength) {
        if (this.closed.get()) throw new IllegalStateException("Aeron envelope framer is closed");
        final int encodedLength = AeronReplicationEnvelope.encode(this.buffer, 0, this.clusterId,
                this.epoch, this.fencingToken, this.wireNonce, this.sequence, kind,
                payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c,
                payload == null ? EMPTY_BUFFER.get() : payload, payloadOffset, payloadChunkLength,
                this.checksum);
        if (encodedLength > this.maxMessageLength) {
            throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
        }
        return this.lastOfferPosition = this.offerer.offer(this.buffer, encodedLength);
    }

    private void offerDataChunk(final int payloadLength, final int chunkIndex,
                                final int chunkCount, final int chunkOffset,
                                final int payloadChunkLength, final int payloadCrc32c) {
        final int encodedLength = AeronReplicationEnvelope.encodeWithPayloadCrc(this.buffer, 0,
                this.clusterId, this.epoch, this.fencingToken, this.wireNonce, this.sequence,
                AeronReplicationEnvelope.Kind.STORE_BINARY, payloadLength,
                chunkIndex, chunkCount, chunkOffset, 0, this.buffer,
                AeronReplicationEnvelope.HEADER_LENGTH, payloadChunkLength,
                payloadCrc32c, this.checksum);
        if (encodedLength > this.maxMessageLength) {
            throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
        }
        this.lastOfferPosition = this.offerer.offer(this.buffer, encodedLength);
    }

    private void offerGatheredDataChunk(final int payloadLength, final int chunkIndex,
                                        final int chunkCount, final int chunkOffset,
                                        final int payloadChunkLength, final int payloadCrc32c,
                                        final DirectBufferVector[] vectors) {
        /* Encode only the header; Aeron reads its payload from the remaining vectors. */
        final int encodedLength = AeronReplicationEnvelope.encodeWithPayloadCrc(this.buffer, 0,
                this.clusterId, this.epoch, this.fencingToken, this.wireNonce, this.sequence,
                AeronReplicationEnvelope.Kind.STORE_BINARY, payloadLength,
                chunkIndex, chunkCount, chunkOffset, 0, this.buffer,
                AeronReplicationEnvelope.HEADER_LENGTH, payloadChunkLength,
                payloadCrc32c, this.checksum);
        if (encodedLength > this.maxMessageLength) {
            throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
        }
        vectors[0].reset(this.buffer, 0, AeronReplicationEnvelope.HEADER_LENGTH);
        this.lastOfferPosition = this.offerer.offer(vectors);
    }

    /// Marks this transaction framer complete; the publisher owns and reuses the buffer.
    @Override
    public void close() {
        this.closed.set(true);
    }
}
