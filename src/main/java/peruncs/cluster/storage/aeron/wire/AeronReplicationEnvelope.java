package peruncs.cluster.storage.aeron.wire;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import peruncs.cluster.errors.CorruptReplicationDataException;
import peruncs.cluster.storage.Crc32C;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/// The small envelope around one piece of a replicated Store transaction.
///
/// Eclipse Serializer still owns the Store binary format. This type adds
/// only the identity, order, chunk location, and checksum needed to move that
/// binary through Aeron. A transaction is visible only after its commit marker
/// and full-binary checksum pass validation.
///
/// The header and payload CRC32C checksums detect accidental corruption. The
/// cluster wire nonce rejects accidental cross-wiring between otherwise valid
/// replication streams; it is not authentication and must not replace network
/// isolation or Archive control-session authentication.
public final class AeronReplicationEnvelope {
    public static final int MAGIC = 0x44474152; // DGAR
        /// Wire version with a checksum covering every decision-bearing header field.
    public static final short VERSION = 5;
        /// Header bytes, including the wire nonce and final header CRC32C.
    public static final int HEADER_LENGTH = 84;
        /// Largest logical transaction payload accepted on the wire.
    public static final int MAX_TRANSACTION_PAYLOAD_BYTES = 64 * 1024 * 1024;
        /// Largest chunk count accepted for one logical payload.
    public static final int MAX_PACKET_COUNT = 1_000_000;
    private static final int WIRE_NONCE_OFFSET = 72;
    private static final int HEADER_CRC_OFFSET = 80;

    private AeronReplicationEnvelope() {
    }

    /// Derives the stable nonce for standalone codec fixtures only.
    ///
    /// Production nodes pass the configured nonce explicitly to both the writer
    /// and the reader. Deriving a nonce from the cluster id is a fixture
    /// convenience, never a deployment default: mixing a derived nonce with an
    /// explicitly configured one cross-wires the streams.
    ///
    /// @param clusterId fixture cluster identity
    /// @return non-zero derived nonce
    public static long defaultWireNonce(final UUID clusterId) {
        Objects.requireNonNull(clusterId, "clusterId");
        return clusterId.getLeastSignificantBits() | 1L;
    }

        /// Reusable checksum state owned by one codec operation.
    ///
    /// The owner passes this instance explicitly to
    /// [#encode(MutableDirectBuffer, int, UUID, long, long, long, long, Kind, int, int, int, int, int, DirectBuffer, int, int, ChecksumContext)],
    /// [#encodeWithPayloadCrc(MutableDirectBuffer, int, UUID, long, long, long, long, Kind, int, int, int, int, int, DirectBuffer, int, int, int, ChecksumContext)],
    /// or [#crc32c(DirectBuffer, int, int, ChecksumContext)]. An instance is
    /// never shared across threads and never retained by a platform thread.
    public static final class ChecksumContext {
        private final Crc32C.Context crc = new Crc32C.Context();

        private int compute(final DirectBuffer payload, final int offset, final int length) {
            return this.crc.compute(payload, offset, length);
        }
    }

        /// Returns whether a wire kind carries transaction data rather than a terminal marker.
    public static boolean isPayloadKindCode(final int code) {
        return code == Kind.TYPE_DICTIONARY.code || code == Kind.STORE_BINARY.code;
    }

        /// Encodes directly into a caller-owned Agrona buffer. The writer uses this
    /// form so each chunk can be offered without first creating a heap frame.
    ///
    /// The checksum of the copied payload is computed with the caller-owned
    /// context in the same pass that stages the bytes. Passing the context
    /// explicitly keeps the hot path free of thread-local or scoped state and
    /// lets one context serve a whole burst of frames.
    ///
    /// @param target          destination buffer
    /// @param targetOffset    destination offset
    /// @param clusterId       replication cluster identity
    /// @param epoch           writer epoch
    /// @param fencingToken    writer fencing token; readers reject stale tokens
    /// @param wireNonce       deployment nonce shared with the reader; must not be zero
    /// @param sequence        transaction sequence
    /// @param kind            envelope kind
    /// @param payloadLength   logical, unchunked payload length
    /// @param chunkIndex      zero-based chunk index
    /// @param chunkCount      total chunk count
    /// @param chunkOffset     offset within the logical payload
    /// @param commitCrc32c    complete Store-binary CRC carried by terminal markers
    /// @param payload         source payload buffer
    /// @param payloadOffset   source offset
    /// @param chunkLength     bytes copied into the envelope
    /// @param checksumContext caller-owned checksum state; never shared across codec operations
    /// @return number of bytes written
    /// @throws IllegalArgumentException when bounds or protocol fields are invalid
    public static int encode(
            final MutableDirectBuffer target,
            final int targetOffset,
            final UUID clusterId,
            final long epoch,
            final long fencingToken,
            final long wireNonce,
            final long sequence,
            final Kind kind,
            final int payloadLength,
            final int chunkIndex,
            final int chunkCount,
            final int chunkOffset,
            final int commitCrc32c,
            final DirectBuffer payload,
            final int payloadOffset,
            final int chunkLength,
            final ChecksumContext checksumContext
    ) {
        Objects.requireNonNull(checksumContext, "checksumContext");
        return encodeWithPayloadCrc(target, targetOffset, clusterId, epoch, fencingToken, wireNonce, sequence,
                kind, payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c, payload, payloadOffset,
                chunkLength, crc32c(payload, payloadOffset, chunkLength, checksumContext), checksumContext);
    }

        /// Encodes an envelope when the caller already computed the payload checksum
    /// while staging the bytes. This avoids a second full pass over every data
    /// chunk on the writer hot path.
    ///
    /// @param target          destination buffer
    /// @param targetOffset    destination offset
    /// @param clusterId       replication cluster identity
    /// @param epoch           writer epoch
    /// @param fencingToken    writer fencing token; readers reject stale tokens
    /// @param wireNonce       deployment nonce shared with the reader; must not be zero
    /// @param sequence        transaction sequence
    /// @param kind            envelope kind
    /// @param payloadLength   logical payload length
    /// @param chunkIndex      zero-based chunk index
    /// @param chunkCount      total chunk count
    /// @param chunkOffset     logical payload offset
    /// @param commitCrc32c    complete transaction checksum for terminal markers
    /// @param payload         source payload
    /// @param payloadOffset   source offset
    /// @param chunkLength     bytes in this frame
    /// @param payloadCrc32c   checksum of the staged payload bytes
    /// @param checksumContext caller-owned checksum state used for the header CRC
    /// @return encoded frame length
    /// @throws IllegalArgumentException when bounds or protocol fields are invalid
    public static int encodeWithPayloadCrc(
            final MutableDirectBuffer target,
            final int targetOffset,
            final UUID clusterId,
            final long epoch,
            final long fencingToken,
            final long wireNonce,
            final long sequence,
            final Kind kind,
            final int payloadLength,
            final int chunkIndex,
            final int chunkCount,
            final int chunkOffset,
            final int commitCrc32c,
            final DirectBuffer payload,
            final int payloadOffset,
            final int chunkLength,
            final int payloadCrc32c,
            final ChecksumContext checksumContext
    ) {
        Objects.requireNonNull(checksumContext, "checksumContext");
        validate(clusterId, epoch, fencingToken, wireNonce, sequence, kind, payloadLength, chunkIndex, chunkCount,
                chunkOffset, commitCrc32c, payload, payloadOffset, chunkLength);
        final int encodedLength = Math.addExact(HEADER_LENGTH, chunkLength);
        if (target == null || targetOffset < 0 || targetOffset > target.capacity() - encodedLength) {
            throw new IllegalArgumentException("target buffer is too small");
        }
        target.putInt(targetOffset, MAGIC, ByteOrder.BIG_ENDIAN);
        target.putShort(targetOffset + 4, VERSION, ByteOrder.BIG_ENDIAN);
        target.putByte(targetOffset + 6, (byte) kind.code);
        target.putByte(targetOffset + 7, (byte) 0);
        target.putLong(targetOffset + 8, epoch, ByteOrder.BIG_ENDIAN);
        target.putLong(targetOffset + 16, sequence, ByteOrder.BIG_ENDIAN);
        target.putInt(targetOffset + 24, payloadLength, ByteOrder.BIG_ENDIAN);
        target.putInt(targetOffset + 28, chunkIndex, ByteOrder.BIG_ENDIAN);
        target.putInt(targetOffset + 32, chunkCount, ByteOrder.BIG_ENDIAN);
        target.putInt(targetOffset + 36, chunkOffset, ByteOrder.BIG_ENDIAN);
        target.putInt(targetOffset + 40, payloadCrc32c, ByteOrder.BIG_ENDIAN);
        target.putInt(targetOffset + 44, commitCrc32c, ByteOrder.BIG_ENDIAN);
        target.putLong(targetOffset + 48, clusterId.getMostSignificantBits(), ByteOrder.BIG_ENDIAN);
        target.putLong(targetOffset + 56, clusterId.getLeastSignificantBits(), ByteOrder.BIG_ENDIAN);
        target.putLong(targetOffset + 64, fencingToken, ByteOrder.BIG_ENDIAN);
        target.putLong(targetOffset + WIRE_NONCE_OFFSET, wireNonce, ByteOrder.BIG_ENDIAN);
        target.putInt(targetOffset + HEADER_CRC_OFFSET,
                checksumContext.compute(target, targetOffset, HEADER_CRC_OFFSET), ByteOrder.BIG_ENDIAN);
        // The publisher can stage a multi-buffer chunk directly in the destination
        // payload area. Avoid copying that already-staged range a second time.
        if (payload != target || payloadOffset != targetOffset + HEADER_LENGTH) {
            target.putBytes(targetOffset + HEADER_LENGTH, payload, payloadOffset, chunkLength);
        }
        return encodedLength;
    }

    private static void validate(
            final UUID clusterId,
            final long epoch,
            final long fencingToken,
            final long wireNonce,
            final long sequence,
            final Kind kind,
            final int payloadLength,
            final int chunkIndex,
            final int chunkCount,
            final int chunkOffset,
            final int commitCrc32c,
            final DirectBuffer payload,
            final int payloadOffset,
            final int chunkLength
    ) {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(payload, "payload");
        if (wireNonce == 0L) {
            throw new IllegalArgumentException("wireNonce must not be zero");
        }
        if (epoch < 0 || fencingToken <= 0 || sequence < 0 || sequence == Long.MAX_VALUE || payloadLength < 0 ||
            payloadLength > MAX_TRANSACTION_PAYLOAD_BYTES || chunkIndex < 0 ||
            chunkCount <= 0 || chunkCount > MAX_PACKET_COUNT ||
            chunkIndex >= chunkCount || chunkOffset < 0 || payloadOffset < 0 || chunkLength < 0 ||
            payloadOffset > payload.capacity() - chunkLength) {
            throw new IllegalArgumentException("invalid envelope field");
        }
        if (chunkLength > payloadLength ||
            (kind == Kind.COMMIT || kind == Kind.ABORT) && chunkLength != 0) {
            throw new IllegalArgumentException("invalid payload length");
        }
        if ((kind == Kind.COMMIT || kind == Kind.ABORT) &&
            (chunkIndex != 0 || chunkOffset != 0 || kind == Kind.ABORT && commitCrc32c != 0)) {
            throw new IllegalArgumentException("non-canonical terminal marker");
        }
        if (kind != Kind.COMMIT && kind != Kind.ABORT) {
            /* A zero-length data payload is represented by exactly one empty chunk.
             * Accepting a multi-chunk empty transaction would let a sender reserve a
             * sequence that can never reach a valid terminal state and would leave the
             * assembler waiting forever for bytes that do not exist. */
            if (payloadLength == 0 && (chunkIndex != 0 || chunkCount != 1 || chunkOffset != 0)) {
                throw new IllegalArgumentException("empty payload must use one canonical chunk");
            }
            if (payloadLength > 0 && chunkLength == 0) {
                throw new IllegalArgumentException("non-empty payload chunks must carry bytes");
            }
            final long end = (long) chunkOffset + chunkLength;
            if (end > payloadLength || chunkIndex == chunkCount - 1 && end != payloadLength) {
                throw new IllegalArgumentException("chunk does not match logical payload length");
            }
        }
    }

        /// Decodes one complete envelope and copies its payload.
    ///
    /// Use the reusable view overload in a reader. This method is kept for
    /// tests and callers that need an owned byte array.
    public static Envelope decode(final DirectBuffer source, final int offset, final int length) {
        return copyToOwned(source, decodeView(source, offset, length));
    }

        /// Copies a decoded view's payload into an owned envelope.
    private static Envelope copyToOwned(final DirectBuffer source, final EnvelopeView view) {
        final byte[] payload = new byte[view.payloadLengthOnWire];
        source.getBytes(view.payloadOffset, payload);
        return new Envelope(view.clusterId(), view.wireNonce, view.epoch, view.fencingToken, view.sequence, view.kind,
                view.payloadLength, view.chunkIndex, view.chunkCount, view.chunkOffset, view.commitCrc32c, payload);
    }

        /// Convenience form for codec tests; production readers use the reusable view.
    static EnvelopeView decodeView(final DirectBuffer source, final int offset, final int length) {
        return decodeView(source, offset, length, new EnvelopeView());
    }

        /// Decodes into a caller-provided view using the view's own checksum state.
    /// The view is valid until the next call that reuses it, so a reader must
    /// copy any payload it keeps.
    ///
    /// Every structural claim is checked before use: magic, version, header
    /// CRC, flags, non-negative epoch and sequence, payload and chunk bounds
    /// against the replication limits, exact fit of the last chunk, and
    /// canonical marker form (no payload, zeroed chunk fields). Anything
    /// else fails as a wire error rather than a corrupt read.
    ///
    /// @param source encoded frame
    /// @param offset first byte of the frame
    /// @param length frame length including the fixed header
    /// @param view reusable decode target that owns its checksum state
    /// @return the supplied view, populated from the frame
    /// @throws CorruptReplicationDataException when the frame is malformed or corrupt
    public static EnvelopeView decodeView(
            final DirectBuffer source,
            final int offset,
            final int length,
            final EnvelopeView view
    ) {
        Objects.requireNonNull(view, "view");
        return decodeViewInternal(source, offset, length, view);
    }

    private static EnvelopeView decodeViewInternal(
            final DirectBuffer source,
            final int offset,
            final int length,
            final EnvelopeView view
    ) {
        /* Invalidate a reused view before reading any new bytes. If parsing
         * fails halfway through, callers cannot accidentally observe the
         * previous frame through a stale view. */
        view.clear();
        if (source == null || offset < 0 || length < HEADER_LENGTH || length > source.capacity() ||
            offset > source.capacity() - length) {
            throw new CorruptReplicationDataException("truncated envelope");
        }
        if (source.getInt(offset, ByteOrder.BIG_ENDIAN) != MAGIC ||
            source.getShort(offset + 4, ByteOrder.BIG_ENDIAN) != VERSION) {
            throw new CorruptReplicationDataException("unknown DataGrid envelope");
        }
        if (source.getInt(offset + HEADER_CRC_OFFSET, ByteOrder.BIG_ENDIAN) !=
            crc32c(source, offset, HEADER_CRC_OFFSET, view.checksumContext)) {
            throw new CorruptReplicationDataException("envelope header CRC32C mismatch");
        }
        final int kindCode = source.getByte(offset + 6) & 0xff;
        if (source.getByte(offset + 7) != 0) {
            throw new CorruptReplicationDataException("unknown envelope flags");
        }
        final Kind kind = Kind.fromCode(kindCode);
        final long epoch = source.getLong(offset + 8, ByteOrder.BIG_ENDIAN);
        final long fencingToken = source.getLong(offset + 64, ByteOrder.BIG_ENDIAN);
        final long wireNonce = source.getLong(offset + WIRE_NONCE_OFFSET, ByteOrder.BIG_ENDIAN);
        if (fencingToken <= 0) {
            throw new CorruptReplicationDataException("envelope carries no writer fencing token");
        }
        if (wireNonce == 0L) {
            throw new CorruptReplicationDataException("envelope carries no wire nonce");
        }
        final long sequence = source.getLong(offset + 16, ByteOrder.BIG_ENDIAN);
        final int payloadLength = source.getInt(offset + 24, ByteOrder.BIG_ENDIAN);
        final int chunkIndex = source.getInt(offset + 28, ByteOrder.BIG_ENDIAN);
        final int chunkCount = source.getInt(offset + 32, ByteOrder.BIG_ENDIAN);
        final int chunkOffset = source.getInt(offset + 36, ByteOrder.BIG_ENDIAN);
        final int payloadOnWire = length - HEADER_LENGTH;
        if (epoch < 0 || sequence < 0 || sequence == Long.MAX_VALUE || payloadLength < 0 ||
            payloadLength > MAX_TRANSACTION_PAYLOAD_BYTES || chunkIndex < 0 ||
            chunkCount <= 0 || chunkCount > MAX_PACKET_COUNT ||
            chunkIndex >= chunkCount || chunkOffset < 0 ||
            (kind != Kind.COMMIT && kind != Kind.ABORT &&
             (payloadOnWire > payloadLength ||
              (chunkIndex == chunkCount - 1 && (long) chunkOffset + payloadOnWire != payloadLength)))) {
            throw new CorruptReplicationDataException("invalid envelope bounds");
        }
        if ((kind == Kind.COMMIT || kind == Kind.ABORT) && length != HEADER_LENGTH) {
            throw new CorruptReplicationDataException("marker carries a payload");
        }
        if ((kind == Kind.COMMIT || kind == Kind.ABORT) &&
            (chunkIndex != 0 || chunkOffset != 0 || (kind == Kind.ABORT && source.getInt(offset + 44,
                    ByteOrder.BIG_ENDIAN) != 0))) {
            throw new CorruptReplicationDataException("non-canonical terminal marker");
        }
        if (kind != Kind.COMMIT && kind != Kind.ABORT &&
            ((long) chunkOffset + payloadOnWire > payloadLength)) {
            throw new CorruptReplicationDataException("chunk exceeds logical payload length");
        }
        if (kind != Kind.COMMIT && kind != Kind.ABORT && payloadLength == 0 &&
            (chunkIndex != 0 || chunkCount != 1 || chunkOffset != 0)) {
            /* payloadOnWire is already known to be 0 here: a positive wire
             * length with a zero logical length fails the bounds check above. */
            throw new CorruptReplicationDataException("empty payload must use one canonical chunk");
        }
        if (kind != Kind.COMMIT && kind != Kind.ABORT && payloadLength > 0 && payloadOnWire == 0) {
            throw new CorruptReplicationDataException("non-empty payload chunks must carry bytes");
        }
        if (source.getInt(offset + 40, ByteOrder.BIG_ENDIAN) !=
            crc32c(source, offset + HEADER_LENGTH, payloadOnWire, view.checksumContext)) {
            throw new CorruptReplicationDataException("payload CRC32C mismatch");
        }
        view.set(source, offset + HEADER_LENGTH, payloadOnWire,
                source.getLong(offset + 48, ByteOrder.BIG_ENDIAN),
                source.getLong(offset + 56, ByteOrder.BIG_ENDIAN), wireNonce, epoch, fencingToken, sequence, kind,
                payloadLength, chunkIndex, chunkCount, chunkOffset, source.getInt(offset + 44, ByteOrder.BIG_ENDIAN));
        return view;
    }

        /// Computes the checksum used to detect damaged chunks and commits.
    public static int crc32c(final byte[] payload) {
        return Crc32C.compute(payload);
    }

        /// Computes the same checksum directly from an Agrona buffer range using
    /// caller-owned state, keeping the direct-buffer path allocation-free.
    ///
    /// @param payload         source buffer
    /// @param offset          first byte to include
    /// @param length          number of bytes to include
    /// @param checksumContext caller-owned checksum state
    /// @return CRC32C of the range
    /// @throws IllegalArgumentException when the range is invalid
    public static int crc32c(final DirectBuffer payload, final int offset, final int length,
                             final ChecksumContext checksumContext) {
        Objects.requireNonNull(checksumContext, "checksumContext");
        if (payload == null || offset < 0 || length < 0 || offset > payload.capacity() - length) {
            throw new IllegalArgumentException("invalid CRC32C range");
        }
        return checksumContext.compute(payload, offset, length);
    }

        /// Identifies the data or terminal marker carried by an envelope.
    public enum Kind {
                /// A chunk of the type dictionary that precedes Store data.
        TYPE_DICTIONARY(1),
                /// A chunk of one Store binary.
        STORE_BINARY(2),
                /// The terminal witness for a published transaction. Its payload length
        /// and chunk count repeat the assembled binary metadata; its commit CRC
        /// validates the complete binary.
        COMMIT(3),
                /// The terminal witness for a rejected transaction. Its payload length
        /// and chunk count repeat the prepared metadata, while its CRC is zero.
        ABORT(4);

        private final int code;

        Kind(final int code) {
            this.code = code;
        }

        private static Kind fromCode(final int code) {
            return switch (code) {
                case 1 -> TYPE_DICTIONARY;
                case 2 -> STORE_BINARY;
                case 3 -> COMMIT;
                case 4 -> ABORT;
                default -> throw new CorruptReplicationDataException("unknown envelope kind=%s".formatted(code));
            };
        }
    }

        /// Reusable view over one decoded envelope; it does not own the payload.
    ///
    /// The view owns one [ChecksumContext] for the lifetime of the decode state,
    /// so a reader decodes repeatedly without allocating and without scoped or
    /// thread-local checksum state. A view belongs to one thread.
    public static final class EnvelopeView {
        private final ChecksumContext checksumContext = new ChecksumContext();
        private DirectBuffer source;
        private int payloadOffset;
        private int payloadLengthOnWire;
        private long clusterMostSignificantBits;
        private long clusterLeastSignificantBits;
        private long wireNonce;
        private long epoch;
        private long fencingToken;
        private long sequence;
        private Kind kind;
        private int payloadLength;
        private int chunkIndex;
        private int chunkCount;
        private int chunkOffset;
        private int commitCrc32c;

        public EnvelopeView() {
        }

        private void clear() {
            this.source = null;
            this.payloadOffset = 0;
            this.payloadLengthOnWire = 0;
            this.clusterMostSignificantBits = 0L;
            this.clusterLeastSignificantBits = 0L;
            this.wireNonce = 0L;
            this.epoch = 0L;
            this.fencingToken = 0L;
            this.sequence = 0L;
            this.kind = null;
            this.payloadLength = 0;
            this.chunkIndex = 0;
            this.chunkCount = 0;
            this.chunkOffset = 0;
            this.commitCrc32c = 0;
        }

        void set(final DirectBuffer source, final int payloadOffset, final int payloadLengthOnWire,
                 final long clusterMostSignificantBits, final long clusterLeastSignificantBits,
                 final long wireNonce,
                 final long epoch, final long fencingToken, final long sequence, final Kind kind,
                 final int payloadLength, final int chunkIndex, final int chunkCount, final int chunkOffset,
                 final int commitCrc32c) {
            this.source = source;
            this.payloadOffset = payloadOffset;
            this.payloadLengthOnWire = payloadLengthOnWire;
            this.clusterMostSignificantBits = clusterMostSignificantBits;
            this.clusterLeastSignificantBits = clusterLeastSignificantBits;
            this.wireNonce = wireNonce;
            this.epoch = epoch;
            this.fencingToken = fencingToken;
            this.sequence = sequence;
            this.kind = kind;
            this.payloadLength = payloadLength;
            this.chunkIndex = chunkIndex;
            this.chunkCount = chunkCount;
            this.chunkOffset = chunkOffset;
            this.commitCrc32c = commitCrc32c;
        }

        public UUID clusterId() {
            return new UUID(this.clusterMostSignificantBits, this.clusterLeastSignificantBits);
        }

        public boolean matches(final UUID clusterId) {
            return clusterId.getMostSignificantBits() == this.clusterMostSignificantBits &&
                   clusterId.getLeastSignificantBits() == this.clusterLeastSignificantBits;
        }

        /// Returns whether this frame belongs to the expected cluster and nonce.
        public boolean matches(final UUID clusterId, final long wireNonce) {
            return matches(clusterId) && this.wireNonce == wireNonce;
        }

        public long wireNonce() {
            return this.wireNonce;
        }

        public long epoch() {
            return this.epoch;
        }

        public long fencingToken() {
            return this.fencingToken;
        }

        public long sequence() {
            return this.sequence;
        }

        public Kind kind() {
            return this.kind;
        }

        public int payloadLength() {
            return this.payloadLength;
        }

        public int chunkIndex() {
            return this.chunkIndex;
        }

        public int chunkCount() {
            return this.chunkCount;
        }

        public int chunkOffset() {
            return this.chunkOffset;
        }

        public int commitCrc32c() {
            return this.commitCrc32c;
        }

                /// Returns the borrowed source buffer; valid until the next decode into this view.
        public DirectBuffer source() {
            return this.source;
        }

                /// Returns the payload offset in [#source()].
        public int payloadOffset() {
            return this.payloadOffset;
        }

                /// Returns the number of payload bytes present in this envelope.
        public int payloadLengthOnWire() {
            return this.payloadLengthOnWire;
        }
    }

        /// Owned decoded envelope returned by the allocation-friendly codec path.
    ///
    /// @param clusterId expected cluster identity
    /// @param epoch expected writer epoch
    /// @param fencingToken writer fencing token
    /// @param sequence transaction sequence
    /// @param kind frame kind
    /// @param payloadLength logical payload length in bytes
    /// @param chunkIndex zero-based index of this chunk
    /// @param chunkCount total chunks of the message
    /// @param chunkOffset byte offset of this chunk in the payload
    /// @param commitCrc32c payload checksum
    /// @param payload owned payload bytes
    public record Envelope(
            UUID clusterId,
            long wireNonce,
            long epoch,
            long fencingToken,
            long sequence,
            Kind kind,
            int payloadLength,
            int chunkIndex,
            int chunkCount,
            int chunkOffset,
            int commitCrc32c,
            byte[] payload
    ) {
        public Envelope {
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(payload, "payload");
            payload = payload.clone();
            if (wireNonce == 0L) {
                throw new CorruptReplicationDataException("owned envelope carries no wire nonce");
            }
            if (payloadLength < 0 || payloadLength > MAX_TRANSACTION_PAYLOAD_BYTES ||
                chunkIndex < 0 || chunkCount <= 0 || chunkCount > MAX_PACKET_COUNT ||
                chunkIndex >= chunkCount || chunkOffset < 0) {
                throw new CorruptReplicationDataException("invalid owned envelope bounds");
            }
            if (kind == Kind.COMMIT || kind == Kind.ABORT) {
                if (chunkIndex != 0 || chunkOffset != 0 || payload.length != 0 ||
                    (kind == Kind.ABORT && commitCrc32c != 0)) {
                    throw new CorruptReplicationDataException("invalid owned terminal envelope");
                }
            } else if (payloadLength == 0) {
                if (chunkIndex != 0 || chunkCount != 1 || chunkOffset != 0 || payload.length != 0) {
                    throw new CorruptReplicationDataException("invalid owned empty payload envelope");
                }
            } else if (payload.length == 0 || (long) chunkOffset + payload.length > payloadLength) {
                throw new CorruptReplicationDataException("owned envelope payload exceeds logical bounds");
            }
        }

        /// Returns a defensive copy so callers cannot mutate the decoded envelope.
        @Override
        public byte[] payload() {
            return this.payload.clone();
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) return true;
            if (!(other instanceof Envelope envelope)) return false;
            return this.epoch == envelope.epoch
                    && this.fencingToken == envelope.fencingToken
                    && this.sequence == envelope.sequence
                    && this.wireNonce == envelope.wireNonce
                    && this.payloadLength == envelope.payloadLength
                    && this.chunkIndex == envelope.chunkIndex
                    && this.chunkCount == envelope.chunkCount
                    && this.chunkOffset == envelope.chunkOffset
                    && this.commitCrc32c == envelope.commitCrc32c
                    && Objects.equals(this.clusterId, envelope.clusterId)
                    && this.kind == envelope.kind
                    && Arrays.equals(this.payload, envelope.payload);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(this.clusterId, this.wireNonce, this.epoch, this.fencingToken, this.sequence, this.kind,
                    this.payloadLength, this.chunkIndex, this.chunkCount, this.chunkOffset,
                    this.commitCrc32c);
            return 31 * result + Arrays.hashCode(this.payload);
        }
    }

}
