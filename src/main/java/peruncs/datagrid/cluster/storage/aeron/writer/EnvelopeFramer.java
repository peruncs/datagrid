package peruncs.datagrid.cluster.storage.aeron.writer;

import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/// Encodes one transaction's replication frames into its own staging buffer
/// and offers each frame once through the publisher's retryer.
///
/// A framer is created when its transaction is prepared and freed when the
/// transaction becomes terminal. No buffer is shared across transactions, so
/// a prepared transaction keeps its frame bytes until commit or abort without
/// blocking the next prepare. Retry, deadline, and lease-gate policy stays in
/// the retryer and the gate; this type only encodes, accumulates CRCs, and
/// performs the single non-blocking offer attempt per frame.
final class EnvelopeFramer implements AutoCloseable {
    private static final LazyConstant<UnsafeBuffer> EMPTY_BUFFER = LazyConstant.of(UnsafeBuffer::new);

    private final long sequence;
    private final UUID clusterId;
    private final long epoch;
    private final long wireNonce;
    private final int chunkSize;
    private final int maxMessageLength;
    private final long offerTimeoutNanos;
    private final AeronOfferRetryer offerer;
    /* Read at encode time on every frame so a late lease claim is never masked
     * by a stale snapshot. Both suppliers are captured once by the publisher,
     * so this adds no per-transaction allocation. */
    private final LongSupplier fencingToken;
    private final Supplier<WriterLeaseGate> leaseGate;
    private final ByteBuffer storage;
    private final UnsafeBuffer buffer;
    private final AeronReplicationEnvelope.ChecksumContext checksum =
            new AeronReplicationEnvelope.ChecksumContext();
    private final CRC32C dataCrc = new CRC32C();
    private final CRC32C chunkCrc = new CRC32C();
    private final AtomicBoolean freed = new AtomicBoolean();

    /// Creates a framer owning one direct staging buffer for one transaction.
    EnvelopeFramer(final long sequence, final UUID clusterId, final long epoch, final long wireNonce,
                   final int chunkSize, final int maxMessageLength, final long offerTimeoutNanos,
                   final AeronOfferRetryer offerer, final LongSupplier fencingToken,
                   final Supplier<WriterLeaseGate> leaseGate) {
        this.sequence = sequence;
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.epoch = epoch;
        this.wireNonce = wireNonce;
        this.chunkSize = chunkSize;
        this.maxMessageLength = maxMessageLength;
        this.offerTimeoutNanos = offerTimeoutNanos;
        this.offerer = Objects.requireNonNull(offerer, "offerer");
        this.fencingToken = Objects.requireNonNull(fencingToken, "fencingToken");
        this.leaseGate = Objects.requireNonNull(leaseGate, "leaseGate");
        this.storage = ByteBuffer.allocateDirect(chunkSize + AeronReplicationEnvelope.HEADER_LENGTH);
        this.buffer = new UnsafeBuffer(this.storage);
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
            CrashHook.invoke("DATA_CHUNK", this.sequence);
            logicalOffset += chunkLength;
        }
        return (int) crc.getValue();
    }

    /// Encodes and offers a terminal commit or abort marker.
    ///
    /// @return Aeron publication position of the offered marker
    long offerMarker(final AeronReplicationEnvelope.Kind kind,
                     final int payloadLength, final int chunkCount, final int commitCrc32c) {
        return this.offerEncoded(kind, payloadLength, 0, Math.max(1, chunkCount), 0,
                commitCrc32c, EMPTY_BUFFER.get(), 0, 0);
    }

    /// Computes the CRC32C of the populated prefix of a reusable buffer array
    /// without offering anything. Used for fence metadata and failure evidence.
    static int computeDataCrc(final ByteBuffer[] sources, final int sourceCount, final int length) {
        final CRC32C crc = new CRC32C();
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
        final int encodedLength = AeronReplicationEnvelope.encode(this.buffer, 0, this.clusterId,
                this.epoch, this.fencingToken.getAsLong(), this.wireNonce, this.sequence, kind,
                payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c,
                payload == null ? EMPTY_BUFFER.get() : payload, payloadOffset, payloadChunkLength,
                this.checksum);
        if (encodedLength > this.maxMessageLength) {
            throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
        }
        final WriterLeaseGate gate = this.leaseGate.get();
        final long budget = kind == AeronReplicationEnvelope.Kind.COMMIT ||
                kind == AeronReplicationEnvelope.Kind.ABORT
                ? gate.terminalOfferBudgetNanos() : this.offerTimeoutNanos;
        return this.offerer.offerGated(this.buffer, encodedLength, gate, budget);
    }

    private void offerDataChunk(final int payloadLength, final int chunkIndex,
                                final int chunkCount, final int chunkOffset,
                                final int payloadChunkLength, final int payloadCrc32c) {
        final int encodedLength = AeronReplicationEnvelope.encodeWithPayloadCrc(this.buffer, 0,
                this.clusterId, this.epoch, this.fencingToken.getAsLong(), this.wireNonce, this.sequence,
                AeronReplicationEnvelope.Kind.STORE_BINARY, payloadLength,
                chunkIndex, chunkCount, chunkOffset, 0, this.buffer,
                AeronReplicationEnvelope.HEADER_LENGTH, payloadChunkLength,
                payloadCrc32c, this.checksum);
        if (encodedLength > this.maxMessageLength) {
            throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
        }
        this.offerer.offerGated(this.buffer, encodedLength, this.leaseGate.get(), this.offerTimeoutNanos);
    }

    /// Frees the direct staging buffer; the framer is single-use after close.
    @Override
    public void close() {
        if (this.freed.compareAndSet(false, true)) BufferUtil.free(this.storage);
    }
}
