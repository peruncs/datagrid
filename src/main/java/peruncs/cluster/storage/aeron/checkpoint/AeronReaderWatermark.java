package peruncs.cluster.storage.aeron.checkpoint;

import org.agrona.DirectBuffer;
import org.eclipse.serializer.concurrency.LockedExecutor;
import peruncs.cluster.storage.Crc32C;

import java.nio.ByteOrder;
import java.util.*;

import static peruncs.cluster.storage.aeron.checkpoint.AeronCheckpointCodec.*;

/// Monotonic reader watermark used by the Archive-retention controller.
///
/// The seven identity and progress fields name one reader's durable boundary:
/// a token copied from a different reader, cluster, generation, recording, or
/// writer epoch cannot authorize deletion. Integrity comes from the CRC32C
/// trailer, which detects corruption; trust comes from the deployment
/// boundary, since watermarks travel the isolated replication network and only
/// configured reader identities are accepted.
///
/// @param readerId        reader that produced the watermark
/// @param clusterId       replication cluster identity
/// @param storeGeneration Store image identity
/// @param writerEpoch     writer epoch associated with the recording
/// @param recordingId     Aeron Archive recording identity
/// @param sequence        transaction sequence acknowledged by the reader
/// @param position        Archive position acknowledged by the reader
public record AeronReaderWatermark(
        UUID readerId,
        UUID clusterId,
        UUID storeGeneration,
        long writerEpoch,
        long recordingId,
        long sequence,
        long position) {

    /// Frame magic (`DGWM`).
    public static final int MAGIC = 0x4447574D;
    private static final short VERSION = 1;
    private static final int CRC_OFFSET = 88;
        /// Serialized watermark length in bytes.
    public static final int ENCODED_LENGTH = CRC_OFFSET + Integer.BYTES;
    private static final UUID UUID_ZERO = new UUID(0L, 0L);

        /// Validates a watermark token, which is immutable after construction.
    public AeronReaderWatermark {
        Objects.requireNonNull(readerId, "readerId");
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(storeGeneration, "storeGeneration");
        if (writerEpoch < 0 || recordingId < 0 || sequence < -1 || sequence == Long.MAX_VALUE || position < -1) {
            throw new IllegalArgumentException("invalid Aeron watermark");
        }
    }

        /// Creates a watermark for one reader boundary.
    ///
    /// @param readerId        reader that produced the watermark
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store image identity
    /// @param writerEpoch     writer epoch associated with the recording
    /// @param recordingId     Aeron Archive recording identity
    /// @param sequence        transaction sequence acknowledged by the reader
    /// @param position        Archive position acknowledged by the reader
    /// @return a watermark naming the boundary
    public static AeronReaderWatermark of(
            final UUID readerId,
            final UUID clusterId,
            final UUID storeGeneration,
            final long writerEpoch,
            final long recordingId,
            final long sequence,
            final long position
    ) {
        return new AeronReaderWatermark(readerId, clusterId, storeGeneration, writerEpoch, recordingId,
                sequence, position);
    }

        /// Writes a watermark into a caller-provided fixed-size array. This is
    /// used by the latest-value control channel to reuse its two hand-off buffers.
    ///
    /// @param target          destination with exactly [#ENCODED_LENGTH] bytes
    /// @param readerId        reader that produced the watermark
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store image identity
    /// @param writerEpoch     writer epoch associated with the recording
    /// @param recordingId     Aeron Archive recording identity
    /// @param sequence        transaction sequence acknowledged by the reader
    /// @param position        Archive position acknowledged by the reader
    public static void encodeInto(
            final byte[] target,
            final UUID readerId,
            final UUID clusterId,
            final UUID storeGeneration,
            final long writerEpoch,
            final long recordingId,
            final long sequence,
            final long position
    ) {
        validateFields(readerId, clusterId, storeGeneration, writerEpoch, recordingId, sequence, position);
        if (target == null || target.length != ENCODED_LENGTH)
            throw new IllegalArgumentException("watermark target must contain exactly %s bytes".formatted(ENCODED_LENGTH));
        int cursor = putHeader(target, 0, MAGIC, VERSION);
        cursor = putUuid(target, cursor, readerId);
        cursor = putUuid(target, cursor, clusterId);
        cursor = putUuid(target, cursor, storeGeneration);
        cursor = putLong(target, cursor, writerEpoch);
        cursor = putLong(target, cursor, recordingId);
        cursor = putLong(target, cursor, sequence);
        putLong(target, cursor, position);
        putInt(target, CRC_OFFSET, Crc32C.compute(target, 0, CRC_OFFSET));
    }

        /// Decodes a token.
    ///
    /// @param encoded serialized watermark bytes
    /// @return decoded watermark
    public static AeronReaderWatermark decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length != ENCODED_LENGTH) {
            throw new IllegalArgumentException("invalid Aeron watermark encoding length");
        }
        final var reader = new FrameReader(encoded, 0);
        final int magic = reader.readInt();
        final short type = reader.readShort();
        final short flags = reader.readShort();
        final SerializedNodeIdentity identity = reader.readNodeIdentity();
        return decodeFrame(
                magic, type, flags,
                identity.clusterId(), identity.nodeId(), identity.storeGeneration(),
                reader.readLong(), reader.readLong(), reader.readLong(), reader.readLong(),
                reader.readInt(), Crc32C.compute(encoded, 0, CRC_OFFSET));
    }

        /// Decodes directly from an Aeron/Agrona frame without copying the identity bytes.
    ///
    /// @param crcReuse caller-owned checksum state used for the CRC32C check;
    ///                  the watermark worker owns one for its lifetime
    /// @param encoded source frame containing one serialized watermark
    /// @param offset  first byte of the serialized watermark
    /// @param length  serialized watermark length; must be [#ENCODED_LENGTH]
    /// @return decoded watermark
    public static AeronReaderWatermark decode(final Crc32C.Context crcReuse, final DirectBuffer encoded,
                                              final int offset, final int length) {
        Objects.requireNonNull(encoded, "encoded");
        Objects.requireNonNull(crcReuse, "crcReuse");
        if (offset < 0 || length != ENCODED_LENGTH ||
            offset > encoded.capacity() - length) {
            throw new IllegalArgumentException("invalid Aeron watermark encoding length");
        }
        final int actualCrc = crcReuse.compute(encoded, offset, CRC_OFFSET);
        return decodeFrame(
                encoded.getInt(offset, ByteOrder.BIG_ENDIAN),
                encoded.getShort(offset + VERSION_OFFSET, ByteOrder.BIG_ENDIAN),
                encoded.getShort(offset + FLAGS_OFFSET, ByteOrder.BIG_ENDIAN),
                new UUID(encoded.getLong(offset + 8, ByteOrder.BIG_ENDIAN),
                        encoded.getLong(offset + 16, ByteOrder.BIG_ENDIAN)),
                new UUID(encoded.getLong(offset + 24, ByteOrder.BIG_ENDIAN),
                        encoded.getLong(offset + 32, ByteOrder.BIG_ENDIAN)),
                new UUID(encoded.getLong(offset + 40, ByteOrder.BIG_ENDIAN),
                        encoded.getLong(offset + 48, ByteOrder.BIG_ENDIAN)),
                encoded.getLong(offset + 56, ByteOrder.BIG_ENDIAN),
                encoded.getLong(offset + 64, ByteOrder.BIG_ENDIAN),
                encoded.getLong(offset + 72, ByteOrder.BIG_ENDIAN),
                encoded.getLong(offset + 80, ByteOrder.BIG_ENDIAN),
                encoded.getInt(offset + CRC_OFFSET, ByteOrder.BIG_ENDIAN),
                actualCrc);
    }

    private static AeronReaderWatermark decodeFrame(
            final int magic, final short version, final short flags,
            final UUID readerId, final UUID clusterId, final UUID storeGeneration,
            final long epoch, final long recordingId, final long sequence, final long position,
            final int expectedCrc, final int actualCrc) {
        if (magic != MAGIC) throw new IllegalArgumentException("unknown Aeron watermark magic");
        if (version != VERSION) throw new IllegalArgumentException("unsupported Aeron watermark version");
        if (flags != 0) throw new IllegalArgumentException("unsupported Aeron watermark flags");
        if (expectedCrc != actualCrc) throw new IllegalArgumentException("Aeron watermark CRC32C mismatch");
        return new AeronReaderWatermark(
                readerId, clusterId, storeGeneration, epoch, recordingId, sequence, position);
    }

        /// Creates an aggregate at the least advanced boundary. The
    /// aggregate uses the zero UUID as its reader id and is accepted only by a
    /// caller that has already collected every configured reader token.
    ///
    /// @param watermarks reader watermarks from one writer
    /// @return a watermark at the least advanced boundary
    public static AeronReaderWatermark aggregate(final Collection<AeronReaderWatermark> watermarks) {
        if (watermarks == null || watermarks.isEmpty()) throw new IllegalArgumentException("watermarks are empty");
        final Set<UUID> readers = new HashSet<>();
        AeronReaderWatermark least = null;
        for (final AeronReaderWatermark watermark : watermarks) {
            if (watermark == null) throw new IllegalArgumentException("invalid Aeron watermark");
            if (!readers.add(watermark.readerId()))
                throw new IllegalArgumentException("duplicate Aeron watermark reader identity");
            if (least == null) {
                least = watermark;
            } else {
                if (differsFromWriterIdentity(least, watermark))
                    throw new IllegalArgumentException("watermarks do not belong to one Aeron writer");
                if (compareProgress(watermark, least) < 0) least = watermark;
            }
        }
        return of(UUID_ZERO, least.clusterId(), least.storeGeneration(), least.writerEpoch(),
                least.recordingId(), least.sequence(), least.position());
    }

    private static void validateFields(final UUID readerId, final UUID clusterId, final UUID storeGeneration,
                                       final long writerEpoch, final long recordingId, final long sequence, final long position) {
        Objects.requireNonNull(readerId, "readerId");
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(storeGeneration, "storeGeneration");
        if (writerEpoch < 0 || recordingId < 0 || sequence < -1 || sequence == Long.MAX_VALUE || position < -1)
            throw new IllegalArgumentException("invalid Aeron watermark progress");
    }

    private static boolean differsFromWriterIdentity(
            final AeronReaderWatermark left, final AeronReaderWatermark right) {
        return !left.clusterId().equals(right.clusterId()) ||
               !left.storeGeneration().equals(right.storeGeneration()) ||
               left.writerEpoch() != right.writerEpoch() || left.recordingId() != right.recordingId();
    }

    private static int compareProgress(
            final AeronReaderWatermark left, final AeronReaderWatermark right) {
        final int sequence = Long.compare(left.sequence(), right.sequence());
        return sequence == 0 ? Long.compare(left.position(), right.position()) : sequence;
    }

    private static boolean monotonicProgress(
            final AeronReaderWatermark newer, final AeronReaderWatermark previous) {
        /* Recording positions are monotonic independently of the logical sequence.
         * Comparing sequence first alone would accept (N+1, position before N),
         * producing a cursor that claims to have advanced while pointing backwards
         * into the recording. */
        if (newer.sequence() == previous.sequence()) {
            /* A resolved sequence has exactly one terminal Archive position. Accept
             * byte-for-byte progress duplicates, but reject a cursor assembled from
             * fields observed at different transaction boundaries. */
            return newer.position() == previous.position();
        }
        return newer.sequence() > previous.sequence() && newer.position() > previous.position();
    }

        /// Encodes this token for a cursor or a control message.
    ///
    /// @return serialized watermark bytes
    public byte[] encode() {
        final byte[] encoded = new byte[ENCODED_LENGTH];
        encodeInto(encoded, this.readerId, this.clusterId, this.storeGeneration,
                this.writerEpoch, this.recordingId, this.sequence, this.position);
        return encoded;
    }

        /// Tracks the greatest accepted watermark and rejects replay, rollback, or a
    /// conflicting position for an already acknowledged sequence.
    public static final class Validator implements AutoCloseable {
        private final LockedExecutor state = LockedExecutor.New();
        private final Map<UUID, AeronReaderWatermark> latest = new HashMap<>();
        private boolean closed;

                /// Creates a validator for one writer.
        public Validator() {
        }

                /// Accepts a monotonically advancing reader watermark.
        ///
        /// @param watermark watermark to accept
        public void accept(final AeronReaderWatermark watermark) {
            this.state.write(() ->
            {
                this.ensureOpen();
                if (watermark == null)
                    throw new IllegalArgumentException("invalid Aeron watermark");
                final AeronReaderWatermark previous = this.latest.get(watermark.readerId());
                if (previous != null && (differsFromWriterIdentity(previous, watermark) ||
                                         !monotonicProgress(watermark, previous))) {
                    throw new IllegalStateException("Aeron reader watermark is not monotonic");
                }
                this.latest.put(watermark.readerId(), watermark);
            });
        }

                /// Returns the latest accepted watermark for one reader, or `null`.
        ///
        /// @param readerId reader identity
        /// @return latest accepted watermark, or `null`
        public AeronReaderWatermark latest(final UUID readerId) {
            return this.state.read(() ->
            {
                this.ensureOpen();
                return this.latest.get(readerId);
            });
        }

                /// Restores one reader entry after a failed durable state write.
        ///
        /// @param readerId  reader identity
        /// @param watermark prior watermark, or `null` to remove the entry
        public void restore(final UUID readerId, final AeronReaderWatermark watermark) {
            this.state.write(() ->
            {
                this.ensureOpen();
                Objects.requireNonNull(readerId, "readerId");
                if (watermark == null) {
                    this.latest.remove(readerId);
                    return;
                }
                if (!readerId.equals(watermark.readerId()))
                    throw new IllegalArgumentException("cannot restore an Aeron watermark for another reader");
                this.latest.put(readerId, watermark);
            });
        }

                /// Returns a stable snapshot for aggregation or diagnostics.
        ///
        /// @return immutable reader-to-watermark snapshot
        public Map<UUID, AeronReaderWatermark> snapshot() {
            return this.state.read(() ->
            {
                this.ensureOpen();
                return Map.copyOf(this.latest);
            });
        }

                /// Returns whether every supplied reader has a validated watermark.
        boolean containsAll(final Set<UUID> readers) {
            return this.state.read(() ->
            {
                this.ensureOpen();
                return this.latest.keySet().containsAll(readers);
            });
        }

                /// Removes a reader's validated watermark when its retirement is persisted.
        void remove(final UUID readerId) {
            this.state.write(() ->
            {
                this.ensureOpen();
                this.latest.remove(readerId);
            });
        }

                /// Returns whether this validator has a watermark for the supplied reader.
        boolean contains(final UUID readerId) {
            return this.state.read(() ->
            {
                this.ensureOpen();
                return this.latest.containsKey(readerId);
            });
        }

                /// Rejects operations after close.
        void ensureOpen() {
            this.state.read(() ->
            {
                if (this.closed) throw new IllegalStateException("Aeron watermark validator is closed");
            });
        }

                /// Releases the validator; further operations fail.
        @Override
        public void close() {
            this.state.write(() ->
            {
                this.closed = true;
            });
        }
    }

        /// Verifies and aggregates a configured set of reader acknowledgements.
    public static final class Quorum implements AutoCloseable {
        private final LockedExecutor state = LockedExecutor.New();
        private final Set<UUID> expectedReaders;
        private final Set<UUID> activeReaders;
        private final Set<UUID> retiredReaders = new HashSet<>();
        private final Validator validator = new Validator();

                /// Creates a quorum for the configured readers.
        ///
        /// @param expectedReaders reader identities that must acknowledge
        public Quorum(final Collection<UUID> expectedReaders) {
            if (expectedReaders == null || expectedReaders.isEmpty())
                throw new IllegalArgumentException("at least one Aeron reader is required");
            final HashSet<UUID> readers = new HashSet<>(expectedReaders);
            if (readers.size() != expectedReaders.size() || readers.contains(null) || readers.contains(UUID_ZERO))
                throw new IllegalArgumentException("invalid Aeron reader identity");
            this.expectedReaders = readers;
            this.activeReaders = new HashSet<>(readers);
        }

                /// Accepts one acknowledgement from a configured reader.
        ///
        /// @param watermark acknowledgement to accept
        public void accept(final AeronReaderWatermark watermark) {
            this.state.write(() ->
            {
                this.validator.ensureOpen();
                if (watermark == null || watermark.sequence() < 0 || watermark.position() < 0 ||
                    !this.activeReaders.contains(watermark.readerId()))
                    throw new IllegalArgumentException("Aeron watermark reader is not part of the configured quorum");
                this.validator.accept(watermark);
            });
        }

                /// Returns the latest accepted watermark for one reader, or `null`.
        ///
        /// @param readerId reader identity
        /// @return latest accepted watermark, or `null`
        public AeronReaderWatermark latest(final UUID readerId) {
            return this.state.read(() ->
            {
                this.validator.ensureOpen();
                return this.validator.latest(readerId);
            });
        }

                /// Restores one reader entry after a failed durable state write.
        ///
        /// @param readerId  reader identity
        /// @param watermark prior watermark, or `null` to remove the entry
        public void restore(final UUID readerId, final AeronReaderWatermark watermark) {
            this.state.write(() ->
            {
                this.validator.ensureOpen();
                if (!this.expectedReaders.contains(readerId))
                    throw new IllegalArgumentException("cannot restore an Aeron watermark for an unknown reader");
                if (!this.activeReaders.contains(readerId) && watermark != null)
                    throw new IllegalStateException("cannot restore an Aeron watermark for a retired reader");
                this.validator.restore(readerId, watermark);
            });
        }

                /// Returns the least advanced acknowledgement once every reader has reported.
        ///
        /// @return least advanced acknowledgement
        public AeronReaderWatermark aggregate() {
            return this.state.read(() ->
            {
                this.validator.ensureOpen();
                if (this.activeReaders.isEmpty())
                    throw new IllegalStateException("Aeron reader quorum has no active readers");
                AeronReaderWatermark least = null;
                for (final UUID readerId : this.activeReaders) {
                    final AeronReaderWatermark watermark = this.validator.latest(readerId);
                    if (watermark == null) {
                        throw new IllegalStateException(
                                "Aeron reader quorum has not acknowledged the requested boundary; missing=%s".formatted(this.missingReaders()));
                    }
                    if (least == null) {
                        least = watermark;
                    } else {
                        if (differsFromWriterIdentity(least, watermark))
                            throw new IllegalStateException("Aeron reader quorum contains mixed writer identities");
                        if (compareProgress(watermark, least) < 0) least = watermark;
                    }
                }
                return of(UUID_ZERO, least.clusterId(), least.storeGeneration(), least.writerEpoch(),
                        least.recordingId(), least.sequence(), least.position());
            });
        }

                /// Returns whether every configured reader has supplied a watermark.
        ///
        /// @return `true` when every configured reader has reported
        public boolean isComplete() {
            return this.state.read(() ->
            {
                this.validator.ensureOpen();
                return !this.activeReaders.isEmpty() &&
                       this.validator.containsAll(this.activeReaders);
            });
        }

                /// Returns the active readers that have not supplied a durable watermark.
        /// This is intended for health and operator diagnostics; it is not a
        /// retention authorization by itself.
        ///
        /// @return immutable set of readers still missing from the quorum
        public Set<UUID> missingReaders() {
            return this.state.read(() ->
            {
                this.validator.ensureOpen();
                final HashSet<UUID> missing = new HashSet<>();
                for (final UUID readerId : this.activeReaders) {
                    if (!this.validator.contains(readerId)) missing.add(readerId);
                }
                return Set.copyOf(missing);
            });
        }

        /// Returns whether a reader identity is rejected by this writer's retention quorum.
        ///
        /// @param readerId reader identity
        /// @return {@code true} when the reader is not an active quorum member
        public boolean rejectsReader(final UUID readerId) {
            return this.state.read(() ->
            {
                this.validator.ensureOpen();
                return !this.activeReaders.contains(readerId);
            });
        }

                /// Permanently retires a configured reader from subsequent quorum decisions.
        ///
        /// @param readerId configured reader identity
        /// @return `true` when the reader was newly retired
        public boolean retire(final UUID readerId) {
            return this.state.write(() ->
            {
                this.validator.ensureOpen();
                if (!this.expectedReaders.contains(readerId))
                    throw new IllegalArgumentException("reader is not configured for this Aeron quorum: %s".formatted(readerId));
                if (!this.retiredReaders.add(readerId)) return false;
                this.activeReaders.remove(readerId);
                this.validator.remove(readerId);
                return true;
            });
        }

                /// Restores a reader when durable retirement persistence fails.
        ///
        /// @param readerId configured reader identity
        public void reinstate(final UUID readerId) {
            this.state.write(() ->
            {
                this.validator.ensureOpen();
                if (this.retiredReaders.remove(readerId)) this.activeReaders.add(readerId);
            });
        }

                /// Returns the durable retirement tombstones.
        ///
        /// @return immutable set of retired reader identities
        public Set<UUID> retiredReaders() {
            return this.state.read(() ->
            {
                this.validator.ensureOpen();
                return Set.copyOf(this.retiredReaders);
            });
        }

                /// Returns the per-reader state for durable persistence.
        ///
        /// @return immutable reader-to-watermark snapshot
        public Map<UUID, AeronReaderWatermark> snapshot() {
            return this.state.read(() ->
            {
                this.validator.ensureOpen();
                return this.validator.snapshot();
            });
        }

                /// Releases the quorum validator.
        @Override
        public void close() {
            this.validator.close();
        }
    }
}
