package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import org.agrona.DirectBuffer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.*;

import static peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronCheckpointCodec.*;

/// Authenticated, monotonic reader watermark used by the Archive-retention
/// controller.
///
/// The signed bytes include the reader, cluster, Store generation, writer
/// epoch, recording identity, sequence, and recording position. A token copied
/// from a different reader, cluster, generation, recording, or writer epoch
/// therefore cannot authorize deletion.
///
/// @param readerId        reader that produced the watermark
/// @param clusterId       replication cluster identity
/// @param storeGeneration Store image identity
/// @param writerEpoch     writer epoch associated with the recording
/// @param recordingId     Aeron Archive recording identity
/// @param sequence        transaction sequence acknowledged by the reader
/// @param position        Archive position acknowledged by the reader
/// @param authentication  HMAC over the identity and progress fields
public record AeronAuthenticatedWatermark(
        UUID readerId,
        UUID clusterId,
        UUID storeGeneration,
        long writerEpoch,
        long recordingId,
        long sequence,
        long position,
        byte[] authentication) {

    private static final int VERSION = 1;
    private static final int IDENTITY_LENGTH = Integer.BYTES + UUID_BYTES * 3 + Long.BYTES * 4;
    private static final int AUTHENTICATION_LENGTH = 32;
        /// Serialized watermark length in bytes.
    public static final int ENCODED_LENGTH = IDENTITY_LENGTH + AUTHENTICATION_LENGTH;
    private static final String ALGORITHM = "HmacSHA256";
    private static final UUID UUID_ZERO = new UUID(0L, 0L);
    private static final ScopedValue<Mac> MAC = ScopedValue.newInstance();

        /// Creates a watermark and copies the authentication bytes so the token is
    /// immutable after construction.
    public AeronAuthenticatedWatermark {
        if (readerId == null || clusterId == null || storeGeneration == null || authentication == null) {
            throw new NullPointerException("watermark identity and authentication are required");
        }
        if (writerEpoch < 0 || recordingId < 0 || sequence < -1 || sequence == Long.MAX_VALUE || position < -1 ||
            authentication.length != AUTHENTICATION_LENGTH) {
            throw new IllegalArgumentException("invalid Aeron watermark");
        }
        authentication = authentication.clone();
    }

        /// Creates a signed watermark using a caller-owned secret key.
    ///
    /// @param readerId        reader that produced the watermark
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store image identity
    /// @param writerEpoch     writer epoch associated with the recording
    /// @param recordingId     Aeron Archive recording identity
    /// @param sequence        transaction sequence acknowledged by the reader
    /// @param position        Archive position acknowledged by the reader
    /// @param secret          HMAC secret
    /// @return a signed watermark
    public static AeronAuthenticatedWatermark sign(
            final UUID readerId,
            final UUID clusterId,
            final UUID storeGeneration,
            final long writerEpoch,
            final long recordingId,
            final long sequence,
            final long position,
            final byte[] secret
    ) {
        validateFields(readerId, clusterId, storeGeneration, writerEpoch, recordingId, sequence, position);
        final byte[] authentication = authenticate(canonical(readerId, clusterId,
                        storeGeneration, writerEpoch, recordingId, sequence, position),
                secret);
        return new AeronAuthenticatedWatermark(readerId, clusterId,
                storeGeneration, writerEpoch, recordingId, sequence, position,
                authentication);
    }

        /// Signs directly into the fixed wire representation used by the watermark publication.
    ///
    /// @param readerId        reader that produced the watermark
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store image identity
    /// @param writerEpoch     writer epoch associated with the recording
    /// @param recordingId     Aeron Archive recording identity
    /// @param sequence        transaction sequence acknowledged by the reader
    /// @param position        Archive position acknowledged by the reader
    /// @param secret          HMAC secret
    /// @return serialized signed watermark
    public static byte[] signEncoded(
            final UUID readerId,
            final UUID clusterId,
            final UUID storeGeneration,
            final long writerEpoch,
            final long recordingId,
            final long sequence,
            final long position,
            final byte[] secret
    ) {
        final byte[] encoded = new byte[ENCODED_LENGTH];
        signEncodedInto(encoded, readerId, clusterId, storeGeneration, writerEpoch, recordingId, sequence, position,
                secret);
        return encoded;
    }

        /// Writes a signed watermark into a caller-provided fixed-size array. This is
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
    /// @param secret          HMAC secret
    public static void signEncodedInto(
            final byte[] target,
            final UUID readerId,
            final UUID clusterId,
            final UUID storeGeneration,
            final long writerEpoch,
            final long recordingId,
            final long sequence,
            final long position,
            final byte[] secret
    ) {
        validateFields(readerId, clusterId, storeGeneration, writerEpoch, recordingId, sequence, position);
        if (target == null || target.length != ENCODED_LENGTH)
            throw new IllegalArgumentException("watermark target must contain exactly %s bytes".formatted(ENCODED_LENGTH));
        putCanonical(target, readerId, clusterId, storeGeneration, writerEpoch, recordingId, sequence, position);
        withMac(() ->
        {
            authenticateInto(target, IDENTITY_LENGTH, secret, target, IDENTITY_LENGTH);
            return null;
        });
    }

        /// Decodes a token; authentication is checked separately with
    /// [#verify(byte\[\])].
    ///
    /// @param encoded serialized watermark bytes
    /// @return decoded watermark
    public static AeronAuthenticatedWatermark decode(final byte[] encoded) {
        if (encoded == null) throw new NullPointerException("encoded");
        if (encoded.length != ENCODED_LENGTH) {
            throw new IllegalArgumentException("invalid Aeron watermark encoding length");
        }
        if (getInt(encoded, 0) != VERSION)
            throw new IllegalArgumentException("unsupported Aeron watermark version");
        int offset = Integer.BYTES;
        final UUID readerId = getUuid(encoded, offset);
        offset += UUID_BYTES;
        final UUID clusterId = getUuid(encoded, offset);
        offset += UUID_BYTES;
        final UUID storeGeneration = getUuid(encoded, offset);
        offset += UUID_BYTES;
        final long epoch = getLong(encoded, offset);
        offset += Long.BYTES;
        final long recordingId = getLong(encoded, offset);
        offset += Long.BYTES;
        final long sequence = getLong(encoded, offset);
        offset += Long.BYTES;
        final long position = getLong(encoded, offset);
        offset += Long.BYTES;
        final byte[] authentication = new byte[AUTHENTICATION_LENGTH];
        System.arraycopy(encoded, offset, authentication, 0, AUTHENTICATION_LENGTH);
        return new AeronAuthenticatedWatermark(
                readerId, clusterId, storeGeneration, epoch, recordingId, sequence, position, authentication);
    }

        /// Decodes directly from an Aeron/Agrona frame without copying the identity bytes.
    ///
    /// @param encoded source frame containing one serialized watermark
    /// @param offset  first byte of the serialized watermark
    /// @param length  serialized watermark length; must be [#ENCODED_LENGTH]
    /// @return decoded watermark
    public static AeronAuthenticatedWatermark decode(final DirectBuffer encoded, final int offset, final int length) {
        if (encoded == null) throw new NullPointerException("encoded");
        if (offset < 0 || length != ENCODED_LENGTH ||
            offset > encoded.capacity() - length) {
            throw new IllegalArgumentException("invalid Aeron watermark encoding length");
        }
        int cursor = offset;
        if (encoded.getInt(cursor, ByteOrder.BIG_ENDIAN) != VERSION)
            throw new IllegalArgumentException("unsupported Aeron watermark version");
        cursor += Integer.BYTES;
        final UUID readerId = new UUID(encoded.getLong(cursor, ByteOrder.BIG_ENDIAN),
                encoded.getLong(cursor + Long.BYTES, ByteOrder.BIG_ENDIAN));
        cursor += 16;
        final UUID clusterId = new UUID(encoded.getLong(cursor, ByteOrder.BIG_ENDIAN),
                encoded.getLong(cursor + Long.BYTES, ByteOrder.BIG_ENDIAN));
        cursor += 16;
        final UUID storeGeneration = new UUID(encoded.getLong(cursor, ByteOrder.BIG_ENDIAN),
                encoded.getLong(cursor + Long.BYTES, ByteOrder.BIG_ENDIAN));
        cursor += 16;
        final long epoch = encoded.getLong(cursor, ByteOrder.BIG_ENDIAN);
        final long recordingId = encoded.getLong(cursor += Long.BYTES, ByteOrder.BIG_ENDIAN);
        final long sequence = encoded.getLong(cursor += Long.BYTES, ByteOrder.BIG_ENDIAN);
        final long position = encoded.getLong(cursor += Long.BYTES, ByteOrder.BIG_ENDIAN);
        cursor += Long.BYTES;
        final byte[] authentication = new byte[AUTHENTICATION_LENGTH];
        encoded.getBytes(cursor, authentication);
        return new AeronAuthenticatedWatermark(
                readerId, clusterId, storeGeneration, epoch, recordingId, sequence, position, authentication);
    }

        /// Creates an authenticated aggregate at the least advanced boundary. The
    /// aggregate uses the zero UUID as its reader id and is accepted only by a
    /// caller that has already verified every configured reader token.
    ///
    /// @param watermarks authenticated reader watermarks from one writer
    /// @param secret     HMAC secret
    /// @return a signed watermark at the least advanced boundary
    public static AeronAuthenticatedWatermark aggregate(
            final Collection<AeronAuthenticatedWatermark> watermarks, final byte[] secret) {
        if (watermarks == null || watermarks.isEmpty()) throw new IllegalArgumentException("watermarks are empty");
        final Set<UUID> readers = new HashSet<>();
        AeronAuthenticatedWatermark least = null;
        for (final AeronAuthenticatedWatermark watermark : watermarks) {
            if (watermark == null || !watermark.verify(secret))
                throw new SecurityException("invalid Aeron watermark authentication");
            if (!readers.add(watermark.readerId()))
                throw new IllegalArgumentException("duplicate Aeron watermark reader identity");
            if (least == null) {
                least = watermark;
            } else {
                if (!sameWriterIdentity(least, watermark))
                    throw new IllegalArgumentException("watermarks do not belong to one Aeron writer");
                if (compareProgress(watermark, least) < 0) least = watermark;
            }
        }
        return sign(UUID_ZERO, least.clusterId(), least.storeGeneration(), least.writerEpoch(),
                least.recordingId(), least.sequence(), least.position(), secret);
    }

    private static byte[] canonical(
            final UUID readerId, final UUID clusterId, final UUID storeGeneration, final long writerEpoch,
            final long recordingId, final long sequence, final long position) {
        if (readerId == null || clusterId == null || storeGeneration == null)
            throw new NullPointerException("watermark identity");
        final byte[] canonical = new byte[IDENTITY_LENGTH];
        putCanonical(canonical, readerId, clusterId, storeGeneration, writerEpoch, recordingId, sequence, position);
        return canonical;
    }

    private static void putCanonical(final byte[] target,
                                     final UUID readerId, final UUID clusterId, final UUID storeGeneration, final long writerEpoch,
                                     final long recordingId, final long sequence, final long position) {
        int cursor = 0;
        cursor = putInt(target, cursor, VERSION);
        cursor = putUuid(target, cursor, readerId);
        cursor = putUuid(target, cursor, clusterId);
        cursor = putUuid(target, cursor, storeGeneration);
        cursor = putLong(target, cursor, writerEpoch);
        cursor = putLong(target, cursor, recordingId);
        cursor = putLong(target, cursor, sequence);
        putLong(target, cursor, position);
    }

    private static void validateFields(final UUID readerId, final UUID clusterId, final UUID storeGeneration,
                                       final long writerEpoch, final long recordingId, final long sequence, final long position) {
        if (readerId == null || clusterId == null || storeGeneration == null)
            throw new NullPointerException("watermark identity");
        if (writerEpoch < 0 || recordingId < 0 || sequence < -1 || sequence == Long.MAX_VALUE || position < -1)
            throw new IllegalArgumentException("invalid Aeron watermark progress");
    }

    private static boolean sameWriterIdentity(
            final AeronAuthenticatedWatermark left, final AeronAuthenticatedWatermark right) {
        return left.clusterId().equals(right.clusterId()) &&
               left.storeGeneration().equals(right.storeGeneration()) &&
               left.writerEpoch() == right.writerEpoch() && left.recordingId() == right.recordingId();
    }

    private static int compareProgress(
            final AeronAuthenticatedWatermark left, final AeronAuthenticatedWatermark right) {
        final int sequence = Long.compare(left.sequence(), right.sequence());
        return sequence == 0 ? Long.compare(left.position(), right.position()) : sequence;
    }

    private static boolean monotonicProgress(
            final AeronAuthenticatedWatermark newer, final AeronAuthenticatedWatermark previous) {
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

    private static byte[] authenticate(final byte[] value, final byte[] secret) {
        return withMac(() ->
        {
            final byte[] authentication = new byte[AUTHENTICATION_LENGTH];
            authenticateInto(value, value.length, secret, authentication, 0);
            return authentication;
        });
    }

    private static void authenticateInto(final byte[] value, final int length,
                                         final byte[] secret, final byte[] target, final int targetOffset) {
        if (secret == null || secret.length < 16)
            throw new IllegalArgumentException("watermark secret must contain at least 16 bytes");
        try {
            final Mac mac = MAC.get();
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            mac.update(value, 0, length);
            mac.doFinal(target, targetOffset);
        } catch (final GeneralSecurityException failure) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", failure);
        }
    }

    private static <T> T withMac(final java.util.function.Supplier<T> operation) {
        if (MAC.isBound()) return operation.get();
        try {
            return ScopedValue.where(MAC, Mac.getInstance(ALGORITHM)).call(operation::get);
        } catch (final GeneralSecurityException failure) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", failure);
        }
    }

        /// Returns a defensive copy so callers cannot mutate the signed token.
    ///
    /// @return authentication tag copy
    @Override
    public byte[] authentication() {
        return this.authentication.clone();
    }

    @Override
    public boolean equals(final Object candidate) {
        if (this == candidate) return true;
        if (!(candidate instanceof AeronAuthenticatedWatermark other)) return false;
        return this.writerEpoch == other.writerEpoch && this.recordingId == other.recordingId &&
               this.sequence == other.sequence && this.position == other.position &&
               this.readerId.equals(other.readerId) && this.clusterId.equals(other.clusterId) &&
               this.storeGeneration.equals(other.storeGeneration) &&
               Arrays.equals(this.authentication, other.authentication);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(this.readerId, this.clusterId, this.storeGeneration,
                this.writerEpoch, this.recordingId, this.sequence, this.position);
        return 31 * result + Arrays.hashCode(this.authentication);
    }

        /// Verifies the HMAC without accepting a token with a different identity.
    ///
    /// @param secret HMAC secret
    /// @return `true` when the token was signed with the secret
    public boolean verify(final byte[] secret) {
        return withMac(() ->
        {
            final byte[] canonical = new byte[IDENTITY_LENGTH];
            final byte[] expected = new byte[AUTHENTICATION_LENGTH];
            try {
                putCanonical(canonical, this.readerId, this.clusterId, this.storeGeneration, this.writerEpoch,
                        this.recordingId, this.sequence, this.position);
                authenticateInto(canonical, IDENTITY_LENGTH, secret, expected, 0);
                return MessageDigest.isEqual(this.authentication, expected);
            } finally {
                Arrays.fill(canonical, (byte) 0);
                Arrays.fill(expected, (byte) 0);
            }
        });
    }

        /// Encodes the signed token for a cursor or a control message.
    ///
    /// @return serialized watermark bytes
    public byte[] encode() {
        final byte[] encoded = new byte[ENCODED_LENGTH];
        putCanonical(encoded, this.readerId, this.clusterId, this.storeGeneration,
                this.writerEpoch, this.recordingId, this.sequence, this.position);
        System.arraycopy(this.authentication, 0, encoded, IDENTITY_LENGTH, AUTHENTICATION_LENGTH);
        return encoded;
    }

        /// Tracks the greatest accepted watermark and rejects replay, rollback, or a
    /// conflicting position for an already acknowledged sequence.
    public static final class Validator implements AutoCloseable {
        private final byte[] secret;
        private final Map<UUID, AeronAuthenticatedWatermark> latest = new HashMap<>();
        private boolean erased;

                /// Creates a validator for one HMAC secret.
        ///
        /// @param secret HMAC secret
        public Validator(final byte[] secret) {
            if (secret == null || secret.length < 16)
                throw new IllegalArgumentException("watermark secret must contain at least 16 bytes");
            this.secret = secret.clone();
        }

                /// Accepts a verified, monotonically advancing reader watermark.
        ///
        /// @param watermark watermark to accept
        public synchronized void accept(final AeronAuthenticatedWatermark watermark) {
            this.ensureOpen();
            if (watermark == null || !watermark.verify(this.secret))
                throw new SecurityException("invalid Aeron watermark authentication");
            final AeronAuthenticatedWatermark previous = this.latest.get(watermark.readerId());
            if (previous != null && (!sameWriterIdentity(previous, watermark) ||
                                     !monotonicProgress(watermark, previous))) {
                throw new IllegalStateException("Aeron reader watermark is not monotonic");
            }
            this.latest.put(watermark.readerId(), watermark);
        }

                /// Returns the latest accepted watermark for one reader, or `null`.
        ///
        /// @param readerId reader identity
        /// @return latest accepted watermark, or `null`
        public synchronized AeronAuthenticatedWatermark latest(final UUID readerId) {
            this.ensureOpen();
            return this.latest.get(readerId);
        }

                /// Restores one reader entry after a failed durable state write.
        ///
        /// @param readerId  reader identity
        /// @param watermark prior watermark, or `null` to remove the entry
        public synchronized void restore(final UUID readerId, final AeronAuthenticatedWatermark watermark) {
            this.ensureOpen();
            if (readerId == null) throw new NullPointerException("readerId");
            if (watermark == null) {
                this.latest.remove(readerId);
                return;
            }
            if (!readerId.equals(watermark.readerId()) || !watermark.verify(this.secret))
                throw new SecurityException("cannot restore an unauthenticated Aeron watermark");
            this.latest.put(readerId, watermark);
        }

                /// Returns a stable snapshot for aggregation or diagnostics.
        ///
        /// @return immutable reader-to-watermark snapshot
        public synchronized Map<UUID, AeronAuthenticatedWatermark> snapshot() {
            this.ensureOpen();
            return Map.copyOf(this.latest);
        }

                /// Returns whether every supplied reader has a validated watermark.
        synchronized boolean containsAll(final Set<UUID> readers) {
            this.ensureOpen();
            return this.latest.keySet().containsAll(readers);
        }

                /// Removes a reader's validated watermark when its retirement is persisted.
        synchronized void remove(final UUID readerId) {
            this.ensureOpen();
            this.latest.remove(readerId);
        }

                /// Returns whether this validator has a watermark for the supplied reader.
        synchronized boolean contains(final UUID readerId) {
            this.ensureOpen();
            return this.latest.containsKey(readerId);
        }

                /// Rejects operations after the secret has been erased.
        synchronized void ensureOpen() {
            if (this.erased) throw new IllegalStateException("Aeron watermark validator is closed");
        }

                /// Erases the HMAC key when the owning retention controller is closed.
        synchronized void clearSecret() {
            if (!this.erased) {
                Arrays.fill(this.secret, (byte) 0);
                this.erased = true;
            }
        }

                /// Erases the retained HMAC key.
        @Override
        public void close() {
            this.clearSecret();
        }
    }

        /// Verifies and aggregates a configured set of reader acknowledgements.
    public static final class Quorum implements AutoCloseable {
        private final Set<UUID> expectedReaders;
        private final Set<UUID> activeReaders;
        private final Set<UUID> retiredReaders = new HashSet<>();
        private final Validator validator;

                /// Creates a quorum for the configured readers.
        ///
        /// @param expectedReaders reader identities that must acknowledge
        /// @param secret          HMAC secret
        public Quorum(final Collection<UUID> expectedReaders, final byte[] secret) {
            if (expectedReaders == null || expectedReaders.isEmpty())
                throw new IllegalArgumentException("at least one Aeron reader is required");
            final HashSet<UUID> readers = new HashSet<>(expectedReaders);
            if (readers.size() != expectedReaders.size() || readers.contains(null) || readers.contains(UUID_ZERO))
                throw new IllegalArgumentException("invalid Aeron reader identity");
            this.expectedReaders = readers;
            this.activeReaders = new HashSet<>(readers);
            this.validator = new Validator(secret);
        }

                /// Accepts one authenticated acknowledgement from a configured reader.
        ///
        /// @param watermark acknowledgement to accept
        public synchronized void accept(final AeronAuthenticatedWatermark watermark) {
            this.validator.ensureOpen();
            if (watermark == null || watermark.sequence() < 0 || watermark.position() < 0 ||
                !this.activeReaders.contains(watermark.readerId()))
                throw new SecurityException("Aeron watermark reader is not part of the configured quorum");
            this.validator.accept(watermark);
        }

                /// Returns the least advanced acknowledgement once every reader has reported.
        ///
        /// @return least advanced authenticated acknowledgement
        public synchronized AeronAuthenticatedWatermark aggregate() {
            this.validator.ensureOpen();
            if (this.activeReaders.isEmpty())
                throw new IllegalStateException("Aeron reader quorum has no active readers");
            AeronAuthenticatedWatermark least = null;
            for (final UUID readerId : this.activeReaders) {
                final AeronAuthenticatedWatermark watermark = this.validator.latest(readerId);
                if (watermark == null) {
                    throw new IllegalStateException(
                            "Aeron reader quorum has not acknowledged the requested boundary; missing=%s".formatted(this.missingReaders()));
                }
                /* Entries in Validator have already passed HMAC and monotonic checks.
                 * Compare them in place instead of copying every token into a temporary
                 * list and authenticating the same bytes a second time. */
                if (least == null) {
                    least = watermark;
                } else {
                    if (!sameWriterIdentity(least, watermark))
                        throw new IllegalStateException("Aeron reader quorum contains mixed writer identities");
                    if (compareProgress(watermark, least) < 0) least = watermark;
                }
            }
            return sign(UUID_ZERO, least.clusterId(), least.storeGeneration(), least.writerEpoch(),
                    least.recordingId(), least.sequence(), least.position(), this.validator.secret);
        }

                /// Returns whether every configured reader has supplied a watermark.
        ///
        /// @return `true` when every configured reader has reported
        public synchronized boolean isComplete() {
            this.validator.ensureOpen();
            return !this.activeReaders.isEmpty() &&
                   this.validator.containsAll(this.activeReaders);
        }

                /// Returns the active readers that have not supplied a durable watermark.
        /// This is intended for health and operator diagnostics; it is not a
        /// retention authorization by itself.
        ///
        /// @return immutable set of readers still missing from the quorum
        public synchronized Set<UUID> missingReaders() {
            this.validator.ensureOpen();
            final HashSet<UUID> missing = new HashSet<>();
            for (final UUID readerId : this.activeReaders) {
                if (!this.validator.contains(readerId)) missing.add(readerId);
            }
            return Set.copyOf(missing);
        }

                /// Returns whether a reader identity is part of this writer's retention quorum.
        ///
        /// @param readerId reader identity
        /// @return `true` when the reader belongs to the quorum
        public synchronized boolean acceptsReader(final UUID readerId) {
            this.validator.ensureOpen();
            return this.activeReaders.contains(readerId);
        }

                /// Permanently retires a configured reader from subsequent quorum decisions.
        ///
        /// @param readerId configured reader identity
        /// @return `true` when the reader was newly retired
        public synchronized boolean retire(final UUID readerId) {
            this.validator.ensureOpen();
            if (!this.expectedReaders.contains(readerId))
                throw new IllegalArgumentException("reader is not configured for this Aeron quorum: %s".formatted(readerId));
            if (!this.retiredReaders.add(readerId)) return false;
            this.activeReaders.remove(readerId);
            this.validator.remove(readerId);
            return true;
        }

                /// Restores a reader when durable retirement persistence fails.
        ///
        /// @param readerId configured reader identity
        public synchronized void reinstate(final UUID readerId) {
            this.validator.ensureOpen();
            if (this.retiredReaders.remove(readerId)) this.activeReaders.add(readerId);
        }

                /// Returns the durable retirement tombstones.
        ///
        /// @return immutable set of retired reader identities
        public synchronized Set<UUID> retiredReaders() {
            this.validator.ensureOpen();
            return Set.copyOf(this.retiredReaders);
        }

                /// Returns the authenticated per-reader state for durable persistence.
        ///
        /// @return immutable reader-to-watermark snapshot
        public synchronized Map<UUID, AeronAuthenticatedWatermark> snapshot() {
            this.validator.ensureOpen();
            return this.validator.snapshot();
        }

                /// Returns one reader's current acknowledgement for transactional updates.
        ///
        /// @param readerId reader identity
        /// @return current acknowledgement, or `null`
        public synchronized AeronAuthenticatedWatermark latest(final UUID readerId) {
            this.validator.ensureOpen();
            return this.validator.latest(readerId);
        }

                /// Restores one reader's prior acknowledgement after persistence failure.
        ///
        /// @param readerId  reader identity
        /// @param watermark prior watermark, or `null` to remove the entry
        public synchronized void restore(final UUID readerId, final AeronAuthenticatedWatermark watermark) {
            this.validator.ensureOpen();
            if (readerId == null || !this.expectedReaders.contains(readerId))
                throw new IllegalArgumentException("reader is not configured for this Aeron quorum: %s".formatted(readerId));
            if (watermark != null && !this.activeReaders.contains(readerId))
                throw new IllegalStateException("cannot restore a watermark for a retired Aeron reader: %s".formatted(readerId));
            this.validator.restore(readerId, watermark);
        }

                /// Erases the validator key after the retention controller releases it.
        public synchronized void clearSecret() {
            this.validator.clearSecret();
        }

                /// Erases the retained HMAC key.
        @Override
        public void close() {
            this.clearSecret();
        }

    }
}
