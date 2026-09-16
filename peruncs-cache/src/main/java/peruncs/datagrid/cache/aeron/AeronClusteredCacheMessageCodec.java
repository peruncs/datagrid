package peruncs.datagrid.cache.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;

/// Fixed framing for clustered-cache invalidation and heartbeat frames.
///
/// A frame is a big-endian magic, a version, a frame type, the sender identity
/// (a 16-byte UUID), a per-sender monotonic sequence, the serialized payload
/// length, and the serialized payload. The sender identity lets a node ignore
/// frames it published itself; comparing two 64-bit words is allocation-free
/// and constant-time, so a hostile sender id cannot trigger a per-byte
/// comparison loop. The sequence lets a receiver detect a missed burst of
/// invalidations or heartbeats.
///
/// Every frame ends with a CRC32C over the header and payload. The checksum
/// detects accidental corruption but never forgery. When an HMAC secret is
/// configured, a 32-byte HMAC-SHA256 over the same bytes follows the checksum;
/// the receiver rejects unsigned frames outright, and compares the tag with
/// [MessageDigest#isEqual] so a forged frame must carry the secret. Without a
/// secret the channel must sit on an isolated network.
///
/// The payload length is validated before any allocation, so a hostile or
/// corrupt length cannot trigger an unbounded allocation. Checksums and tags
/// stream over the frame in fixed 8 KiB chunks: the hot path never copies a
/// whole frame into a retained scratch array.
final class AeronClusteredCacheMessageCodec {
        /// Wire version; bump when the framing changes.
    static final int VERSION = 3;
        /// Frame carrying one timestamp invalidation.
    static final int TYPE_INVALIDATION = 0;
        /// Frame carrying no payload; proves the sender is still publishing.
    static final int TYPE_HEARTBEAT = 1;
        /// Checksum bytes appended after the serialized payload.
    static final int CRC_LENGTH = Integer.BYTES;
        /// Authentication tag bytes appended after the checksum when a secret is configured.
    static final int HMAC_LENGTH = 32;
        /// Header bytes: magic, version, type, sender id, sequence, payload length.
    static final int HEADER_LENGTH = Integer.BYTES * 4 + Long.BYTES * 3;
        /// Magic "DGCC" identifying a DataGrid clustered-cache frame.
    private static final int MAGIC = 0x44474343;
    private static final String HMAC_ALGORITHM = "HmacSHA256";
        /// Allocation-free big-endian long view over sender-id bytes.
    private static final VarHandle LONG_BE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final int MAGIC_OFFSET = 0;
    private static final int VERSION_OFFSET = Integer.BYTES;
    private static final int TYPE_OFFSET = Integer.BYTES * 2;
    private static final int SENDER_ID_OFFSET = Integer.BYTES * 3;
    private static final int SEQUENCE_OFFSET = SENDER_ID_OFFSET + Long.BYTES * 2;
    private static final int PAYLOAD_LENGTH_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    private static final int PAYLOAD_OFFSET = HEADER_LENGTH;
        /// Shared empty payload so heartbeats never allocate a throwaway array.
    private static final byte[] EMPTY_PAYLOAD = new byte[0];
    private static final SecretKeySpec DUMMY_KEY = new SecretKeySpec(new byte[16], HMAC_ALGORITHM);

    private AeronClusteredCacheMessageCodec() {
    }

        /// Encodes one unsigned invalidation frame into the supplied buffer at offset zero.
    ///
    /// @param buffer   target buffer with room for the frame
    /// @param senderId sender identity bytes
    /// @param sequence per-sender monotonic sequence of this frame
    /// @param payload  serialized invalidation payload
    /// @return encoded frame length
    static int encode(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence,
                      final byte[] payload) {
        return encode(buffer, senderId, sequence, TYPE_INVALIDATION, payload, null);
    }

        /// Encodes one invalidation frame, signed when a secret is supplied.
    ///
    /// @param buffer   target buffer with room for the frame
    /// @param senderId sender identity bytes
    /// @param sequence per-sender monotonic sequence of this frame
    /// @param payload  serialized invalidation payload
    /// @param secret   HMAC secret, or `null` for an unsigned frame
    /// @return encoded frame length
    static int encode(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence,
                      final byte[] payload, final byte[] secret) {
        return encode(buffer, senderId, sequence, TYPE_INVALIDATION, payload, secret);
    }

        /// Encodes one unsigned heartbeat frame into the supplied buffer at offset zero.
    /// A heartbeat carries no payload but consumes the next per-sender
    /// sequence, so receivers can tell a quiet sender from a lost one.
    ///
    /// @param buffer   target buffer with room for the frame
    /// @param senderId sender identity bytes
    /// @param sequence per-sender monotonic sequence of this frame
    /// @return encoded frame length
    static int encodeHeartbeat(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence) {
        return encode(buffer, senderId, sequence, TYPE_HEARTBEAT, EMPTY_PAYLOAD, null);
    }

        /// Encodes one heartbeat frame, signed when a secret is supplied.
    ///
    /// @param buffer   target buffer with room for the frame
    /// @param senderId sender identity bytes
    /// @param sequence per-sender monotonic sequence of this frame
    /// @param secret   HMAC secret, or `null` for an unsigned frame
    /// @return encoded frame length
    static int encodeHeartbeat(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence,
                               final byte[] secret) {
        return encode(buffer, senderId, sequence, TYPE_HEARTBEAT, EMPTY_PAYLOAD, secret);
    }

        /// Encodes one frame around a payload already placed at [#payloadOffset].
    ///
    /// The sender serializes straight into its scratch buffer and calls this to
    /// finish the frame in place, so no intermediate payload array is allocated.
    ///
    /// @param buffer        scratch holding the payload at the payload offset
    /// @param senderId      sender identity bytes
    /// @param sequence      per-sender monotonic sequence of this frame
    /// @param type          frame type
    /// @param payloadLength payload bytes already placed at the payload offset
    /// @param secret        HMAC secret, or `null` for an unsigned frame
    /// @return encoded frame length
    static int encodeFrame(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence,
                           final int type, final int payloadLength, final byte[] secret) {
        checkFrameArguments(senderId, sequence, type, payloadLength);
        if (secret != null && secret.length < AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException("HMAC secret must contain at least %s bytes".formatted(
                    AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES));
        }
        try {
            writeHeader(buffer, 0, senderId, sequence, type, payloadLength);
            return finishFrame(buffer, 0, payloadLength, secret);
        } catch (final IndexOutOfBoundsException failure) {
            throw new IllegalArgumentException("buffer is too small for Aeron clustered-cache frame", failure);
        }
    }

    private static int encode(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence,
                              final int type, final byte[] payload, final byte[] secret) {
        Objects.requireNonNull(buffer, "buffer");
        Objects.requireNonNull(payload, "payload");
        checkFrameArguments(senderId, sequence, type, payload.length);
        if (secret != null && secret.length < AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException("HMAC secret must contain at least %s bytes".formatted(
                    AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES));
        }
        try {
            /* ExpandableArrayBuffer grows on demand; fixed buffers report an
             * insufficient destination through IndexOutOfBoundsException. Keep both
             * behaviours while exposing one deterministic codec exception. */
            writeHeader(buffer, 0, senderId, sequence, type, payload.length);
            buffer.putBytes(PAYLOAD_OFFSET, payload, 0, payload.length);
            return finishFrame(buffer, 0, payload.length, secret);
        } catch (final IndexOutOfBoundsException failure) {
            throw new IllegalArgumentException("buffer is too small for Aeron clustered-cache frame", failure);
        }
    }

    private static void checkFrameArguments(final byte[] senderId, final long sequence, final int type,
                                            final int payloadLength) {
        if (senderId == null || senderId.length != Long.BYTES * 2) {
            throw new IllegalArgumentException("sender id must be exactly 16 bytes");
        }
        if (sequence < 0 || sequence == Long.MAX_VALUE) {
            throw new IllegalArgumentException("sequence must be in [0, Long.MAX_VALUE)");
        }
        if (type != TYPE_INVALIDATION && type != TYPE_HEARTBEAT) {
            throw new IllegalArgumentException("unknown Aeron clustered-cache frame type: %s".formatted(type));
        }
        if (type == TYPE_HEARTBEAT && payloadLength != 0) {
            throw new IllegalArgumentException("Aeron clustered-cache heartbeat frames carry no payload");
        }
        if (payloadLength < 0) {
            throw new IllegalArgumentException("payload length must not be negative: %s".formatted(payloadLength));
        }
    }

    private static void writeHeader(final MutableDirectBuffer buffer, final int offset, final byte[] senderId,
                                    final long sequence, final int type, final int payloadLength) {
        buffer.putInt(offset + MAGIC_OFFSET, MAGIC, ByteOrder.BIG_ENDIAN);
        buffer.putInt(offset + VERSION_OFFSET, VERSION, ByteOrder.BIG_ENDIAN);
        buffer.putInt(offset + TYPE_OFFSET, type, ByteOrder.BIG_ENDIAN);
        buffer.putBytes(offset + SENDER_ID_OFFSET, senderId, 0, Long.BYTES);
        buffer.putBytes(offset + SENDER_ID_OFFSET + Long.BYTES, senderId, Long.BYTES, Long.BYTES);
        buffer.putLong(offset + SEQUENCE_OFFSET, sequence, ByteOrder.BIG_ENDIAN);
        buffer.putInt(offset + PAYLOAD_LENGTH_OFFSET, payloadLength, ByteOrder.BIG_ENDIAN);
    }

    private static int finishFrame(final MutableDirectBuffer buffer, final int offset, final int payloadLength,
                                   final byte[] secret) {
        final int covered = HEADER_LENGTH + payloadLength;
        buffer.putInt(offset + covered, checksum(buffer, offset, covered), ByteOrder.BIG_ENDIAN);
        if (secret == null) {
            return covered + CRC_LENGTH;
        }
        /* Reuse the worker-local tag scratch. A signed heartbeat or invalidation
         * must not allocate a short-lived array on the listener/heartbeat hot path. */
        final byte[] tag = TAG_SCRATCH.get();
        try {
            authenticateInto(buffer, offset, covered, secret, tag, 0);
            buffer.putBytes(offset + covered + CRC_LENGTH, tag, 0, HMAC_LENGTH);
            return covered + CRC_LENGTH + HMAC_LENGTH;
        } finally {
            Arrays.fill(tag, (byte) 0);
        }
    }

        /// Returns the identity of a 16-byte sender id byte array.
    ///
    /// @param senderId sender identity bytes
    /// @return sender identity
    static SenderId senderIdOf(final byte[] senderId) {
        if (senderId == null || senderId.length != Long.BYTES * 2) {
            throw new IllegalArgumentException("sender id must be exactly 16 bytes");
        }
        return new SenderId(readLong(senderId, 0), readLong(senderId, Long.BYTES));
    }

        /// Clears authentication material retained by the current worker thread.
    /// Transport owners call this when their sender or receiver thread exits.
    static void clearThreadLocalAuthenticationState() {
        final CachedKey cached = AUTHENTICATION_KEY.get();
        if (cached != null && cached.secret() != null) Arrays.fill(cached.secret(), (byte) 0);
        AUTHENTICATION_KEY.remove();
        AUTHENTICATION.remove();
        TAG_SCRATCH.remove();
        TAG_RECEIVED.remove();
        STREAM_CHUNK.remove();
        CHECKSUM.remove();
    }

        /// Returns the payload offset of a frame at the given frame offset, so the
    /// receiver can decode the payload straight from the receive buffer.
    ///
    /// @param offset frame offset
    /// @return payload offset
    static int payloadOffset(final int offset) {
        return offset + PAYLOAD_OFFSET;
    }

        /// Validates a frame once and returns everything the receiver needs.
    ///
    /// The polling hot path must not recompute the checksum or tag per accessor:
    /// one validation covers the header, the sender identity, the sequence, the
    /// payload bounds, the CRC32C, and — when a secret is configured — the HMAC.
    /// An oversized self frame fails instead of skipping: the local sender
    /// can never emit one, so it is corrupt or spoofed either way. An unsigned
    /// frame is rejected outright when a secret is configured.
    ///
    /// @param buffer           source buffer
    /// @param offset           frame offset
    /// @param length           frame length
    /// @param maxPayloadBytes  maximum accepted payload size
    /// @param expectedSenderId local sender identity for self-suppression
    /// @param secret           HMAC secret, or `null` to accept unsigned frames
    /// @return validated frame fields
    /// @throws IllegalArgumentException when the frame is malformed, oversized, or unauthenticated
    static ValidatedFrame validate(final DirectBuffer buffer, final int offset, final int length,
                                   final int maxPayloadBytes, final byte[] expectedSenderId,
                                   final byte[] secret) {
        return validate(buffer, offset, length, maxPayloadBytes, expectedSenderId, secret, null);
    }

        /// Validates a frame, accepting the retiring key during rotation overlap.
    ///
    /// Verification tries the primary key first and falls back to the
    /// previous key. A `null` previous key behaves exactly like
    /// [#validate(DirectBuffer,int,int,int,byte[],byte[])].
    ///
    /// @param buffer           source buffer
    /// @param offset           frame offset
    /// @param length           frame length
    /// @param maxPayloadBytes  maximum accepted payload size
    /// @param expectedSenderId local sender identity for self-suppression
    /// @param primarySecret    HMAC secret, or `null` to accept unsigned frames
    /// @param previousSecret   retiring HMAC secret, or `null`
    /// @return validated frame fields
    /// @throws IllegalArgumentException when the frame is malformed, oversized, or unauthenticated
    static ValidatedFrame validate(final DirectBuffer buffer, final int offset, final int length,
                                   final int maxPayloadBytes, final byte[] expectedSenderId,
                                   final byte[] primarySecret, final byte[] previousSecret) {
        final Header header = validateHeader(buffer, offset, length, maxPayloadBytes, primarySecret, previousSecret);
        final boolean self = expectedSenderId != null && expectedSenderId.length == Long.BYTES * 2 &&
                             buffer.getLong(offset + SENDER_ID_OFFSET, ByteOrder.BIG_ENDIAN) ==
                             readLong(expectedSenderId, 0) &&
                             buffer.getLong(offset + SENDER_ID_OFFSET + Long.BYTES, ByteOrder.BIG_ENDIAN) ==
                             readLong(expectedSenderId, Long.BYTES);
        final SenderId sender = new SenderId(
                buffer.getLong(offset + SENDER_ID_OFFSET, ByteOrder.BIG_ENDIAN),
                buffer.getLong(offset + SENDER_ID_OFFSET + Long.BYTES, ByteOrder.BIG_ENDIAN));
        return new ValidatedFrame(sender, header.sequence, header.payloadLength, header.heartbeat, self);
    }

        /// Validates all fixed framing fields once and returns the decoded lengths.
    private static Header validateHeader(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final int maxPayloadBytes,
            final byte[] primarySecret,
            final byte[] previousSecret
    ) {
        if (!validRange(buffer, offset, length)) {
            throw new IllegalArgumentException(
                    "Aeron clustered-cache frame range is outside the buffer or shorter than its header");
        }
        if (buffer.getInt(offset + MAGIC_OFFSET, ByteOrder.BIG_ENDIAN) != MAGIC) {
            throw new IllegalArgumentException("unknown Aeron clustered-cache frame magic");
        }
        if (buffer.getInt(offset + VERSION_OFFSET, ByteOrder.BIG_ENDIAN) != VERSION) {
            throw new IllegalArgumentException("unsupported Aeron clustered-cache frame version");
        }
        final int type = buffer.getInt(offset + TYPE_OFFSET, ByteOrder.BIG_ENDIAN);
        final boolean heartbeat;
        if (type == TYPE_HEARTBEAT) {
            heartbeat = true;
        } else if (type == TYPE_INVALIDATION) {
            heartbeat = false;
        } else {
            throw new IllegalArgumentException(
                    "unknown Aeron clustered-cache frame type: %s".formatted(type));
        }
        final long sequence = buffer.getLong(offset + SEQUENCE_OFFSET, ByteOrder.BIG_ENDIAN);
        if (!validSequence(sequence)) {
            throw new IllegalArgumentException("invalid Aeron clustered-cache frame sequence: %s".formatted(sequence));
        }
        final int payloadLength = buffer.getInt(offset + PAYLOAD_LENGTH_OFFSET, ByteOrder.BIG_ENDIAN);
        if (payloadLength < 0 || (maxPayloadBytes >= 0 && payloadLength > maxPayloadBytes)) {
            throw new IllegalArgumentException(
                    "Invalid Aeron clustered-cache payload length: %s".formatted(payloadLength));
        }
        if (heartbeat && payloadLength != 0) {
            throw new IllegalArgumentException("Aeron clustered-cache heartbeat frames carry no payload");
        }
        final int covered = HEADER_LENGTH + payloadLength;
        if (primarySecret == null) {
            if (covered + CRC_LENGTH != length) {
                throw new IllegalArgumentException(
                        "Aeron clustered-cache frame length does not match its payload");
            }
        } else {
            if (covered + CRC_LENGTH == length) {
                throw new IllegalArgumentException(
                        "Aeron clustered-cache unsigned frame is rejected: an HMAC secret is configured");
            }
            if (covered + CRC_LENGTH + HMAC_LENGTH != length) {
                throw new IllegalArgumentException(
                        "Aeron clustered-cache frame length does not match its payload");
            }
        }
        final int checksumOffset = offset + covered;
        final int expectedChecksum = buffer.getInt(checksumOffset, ByteOrder.BIG_ENDIAN);
        if (expectedChecksum != checksum(buffer, offset, covered)) {
            throw new IllegalArgumentException("Aeron clustered-cache frame CRC32C mismatch");
        }
        if (primarySecret != null && !verifyAuthentication(buffer, offset, covered, primarySecret) &&
            (previousSecret == null || !verifyAuthentication(buffer, offset, covered, previousSecret))) {
            throw new IllegalArgumentException("Aeron clustered-cache frame HMAC mismatch");
        }
        return new Header(sequence, payloadLength, heartbeat);
    }

        /// Returns the worker-local key spec for the secret, re-created only
    /// when the secret bytes change.
    private static SecretKeySpec cachedKey(final byte[] secret) {
        final CachedKey cached = AUTHENTICATION_KEY.get();
        if (cached == null || !Arrays.equals(cached.secret(), secret)) {
            final byte[] copy = secret.clone();
            final SecretKeySpec key = new SecretKeySpec(copy, HMAC_ALGORITHM);
            if (cached != null && cached.secret() != null) Arrays.fill(cached.secret(), (byte) 0);
            AUTHENTICATION_KEY.set(new CachedKey(copy, key));
            return key;
        }
        return cached.spec();
    }

    private record CachedKey(byte[] secret, SecretKeySpec spec) {
    }

        /// Checks a frame range without allowing integer overflow or a DirectBuffer
    /// bounds exception to escape the polling callback. Aeron can deliver a
    /// truncated fragment when a publication is interrupted; the receiver treats
    /// that input as a terminal stream failure rather than applying later frames.
    private static boolean validRange(final DirectBuffer buffer, final int offset, final int length) {
        return buffer != null && offset >= 0 && length >= HEADER_LENGTH + CRC_LENGTH &&
               offset <= buffer.capacity() - length;
    }

    private static final ThreadLocal<CRC32C> CHECKSUM = ThreadLocal.withInitial(CRC32C::new);
    private static final ThreadLocal<Mac> AUTHENTICATION = ThreadLocal.withInitial(() ->
    {
        try {
            return Mac.getInstance(HMAC_ALGORITHM);
        } catch (final GeneralSecurityException failure) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", failure);
        }
    });
    /* Worker-local key spec, re-created only when the configured secret
     * changes: building one SecretKeySpec per frame allocated the same few
     * dozen bytes on every send and receive. The cached spec keeps the secret
     * bytes; the codec zeroes nothing here because the owner (configuration)
     * retains the original array and scrubs it on close. */
    private static final ThreadLocal<CachedKey> AUTHENTICATION_KEY = ThreadLocal.withInitial(() -> new CachedKey(null, null));
        /// Reused 64-byte tag scratch: two disjoint 32-byte HMAC regions.
    private static final ThreadLocal<byte[]> TAG_SCRATCH = ThreadLocal.withInitial(() -> new byte[HMAC_LENGTH]);
    private static final ThreadLocal<byte[]> TAG_RECEIVED = ThreadLocal.withInitial(() -> new byte[HMAC_LENGTH]);
        /// Fixed streaming chunk: checksums and tags never retain a frame-sized copy.
    private static final ThreadLocal<byte[]> STREAM_CHUNK = ThreadLocal.withInitial(() -> new byte[8_192]);

    private static int checksum(final DirectBuffer buffer, final int offset, final int length) {
        final CRC32C crc = CHECKSUM.get();
        crc.reset();
        streamBytes(buffer, offset, length, crc::update);
        return (int) crc.getValue();
    }

        /// Feeds one buffer range to a chunked sink without retaining a frame-sized copy.
    ///
    /// Checksums and tags share this loop; only the per-chunk sink differs.
    ///
    /// @param buffer   source buffer
    /// @param offset   first byte to feed
    /// @param length   bytes to feed
    /// @param consumer per-chunk sink
    private static void streamBytes(final DirectBuffer buffer, final int offset, final int length,
                                    final ChunkConsumer consumer) {
        final byte[] chunk = STREAM_CHUNK.get();
        int remaining = length;
        int cursor = offset;
        while (remaining > 0) {
            final int part = Math.min(remaining, chunk.length);
            buffer.getBytes(cursor, chunk, 0, part);
            consumer.accept(chunk, 0, part);
            cursor += part;
            remaining -= part;
        }
    }

        /// Per-chunk sink for [#streamBytes].
    @FunctionalInterface
    private interface ChunkConsumer {
            /// Accepts one chunk.
        ///
        /// @param chunk  scratch array holding the chunk
        /// @param offset first chunk byte to consume
        /// @param length chunk bytes to consume
        void accept(byte[] chunk, int offset, int length);
    }

        /// Computes the HMAC-SHA256 tag of a buffer range into the target array.
    private static void authenticateInto(final DirectBuffer buffer, final int offset, final int length,
                                         final byte[] secret, final byte[] target, final int targetOffset) {
        if (secret == null || secret.length < AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException("HMAC secret must contain at least %s bytes".formatted(
                    AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES));
        }
        final Mac mac = AUTHENTICATION.get();
        final SecretKeySpec key = cachedKey(secret);
        try {
            mac.init(key);
            streamBytes(buffer, offset, length, mac::update);
            mac.doFinal(target, targetOffset);
        } catch (final GeneralSecurityException failure) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", failure);
        } finally {
            /* The JDK exposes no wipe for a Mac's internal schedule, so replace
             * it with a dummy key: a retained Mac never carries the real
             * schedule beyond one call. */
            try {
                mac.init(DUMMY_KEY);
            } catch (final GeneralSecurityException ignored) {
                /* A scrub with a valid fixed key cannot fail; its failure must
                 * never mask the authentication result. */
            }
        }
    }

        /// Verifies the trailing tag against the covered bytes in constant time.
    private static boolean verifyAuthentication(final DirectBuffer buffer, final int offset, final int covered,
                                               final byte[] secret) {
        /* Reused tag buffers, each 32 bytes: the computed expectation and the
         * received tag. MessageDigest.isEqual keeps the comparison constant
         * time. */
        final byte[] expected = TAG_SCRATCH.get();
        final byte[] received = TAG_RECEIVED.get();
        try {
            authenticateInto(buffer, offset, covered, secret, expected, 0);
            buffer.getBytes(offset + covered + CRC_LENGTH, received, 0, HMAC_LENGTH);
            return MessageDigest.isEqual(received, expected);
        } finally {
            /* Wipe both tags: a retained scratch never carries a tag. */
            Arrays.fill(expected, (byte) 0);
            Arrays.fill(received, (byte) 0);
        }
    }

    private static boolean validSequence(final long sequence) {
        return sequence >= 0 && sequence < Long.MAX_VALUE;
    }

        /// Reads a big-endian long without allocating.
    private static long readLong(final byte[] bytes, final int offset) {
        return (long) LONG_BE.get(bytes, offset);
    }

        /// Identity of one frame sender, used as a gap-tracking key.
    record SenderId(long mostSignificantBits, long leastSignificantBits) {
    }

        /// Decoded fixed header fields.
    private record Header(long sequence, int payloadLength, boolean heartbeat) {
    }

        /// One validation's worth of receiver inputs.
    ///
    /// @param sender        frame sender identity
    /// @param sequence      per-sender monotonic sequence
    /// @param payloadLength validated payload length
    /// @param heartbeat     whether the frame is a payload-less heartbeat
    /// @param self          whether the frame carries the local sender identity
    record ValidatedFrame(SenderId sender, long sequence, int payloadLength, boolean heartbeat, boolean self) {
    }
}
