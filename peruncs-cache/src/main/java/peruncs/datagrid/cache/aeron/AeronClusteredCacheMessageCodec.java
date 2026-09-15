package peruncs.datagrid.cache.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

/// Fixed framing for one clustered-cache invalidation frame.
///
/// A frame is a big-endian magic, a version, the sender identity (a 16-byte
/// UUID), a per-sender monotonic sequence, the serialized payload length, and
/// the serialized payload. The sender identity lets a node ignore frames it
/// published itself; comparing two 64-bit words is allocation-free and
/// constant-time, so a hostile sender id cannot trigger a per-byte comparison
/// loop. The sequence lets a receiver detect a missed burst of invalidations.
///
/// The payload length is validated before any allocation, so a hostile or
/// corrupt length cannot trigger an unbounded allocation. CRC32C detects
/// accidental corruption but does not authenticate an untrusted peer.
final class AeronClusteredCacheMessageCodec {
        /// Wire version; bump when the framing changes.
    static final int VERSION = 2;
        /// Checksum bytes appended after the serialized payload.
    static final int CRC_LENGTH = Integer.BYTES;
        /// Header bytes: magic, version, sender id, sequence, payload length.
    static final int HEADER_LENGTH = Integer.BYTES * 3 + Long.BYTES * 3;
        /// Magic "DGCC" identifying a DataGrid clustered-cache frame.
    private static final int MAGIC = 0x44474343;
        /// Allocation-free big-endian long view over sender-id bytes.
    private static final VarHandle LONG_BE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final int MAGIC_OFFSET = 0;
    private static final int VERSION_OFFSET = Integer.BYTES;
    private static final int SENDER_ID_OFFSET = Integer.BYTES * 2;
    private static final int SEQUENCE_OFFSET = SENDER_ID_OFFSET + Long.BYTES * 2;
    private static final int PAYLOAD_LENGTH_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    private static final int PAYLOAD_OFFSET = HEADER_LENGTH;

    private AeronClusteredCacheMessageCodec() {
    }

        /// Encodes one frame into the supplied buffer at offset zero.
    ///
    /// @param buffer   target buffer with room for the frame
    /// @param senderId sender identity bytes
    /// @param sequence per-sender monotonic sequence of this frame
    /// @param payload  serialized invalidation payload
    /// @return encoded frame length
    static int encode(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence,
                      final byte[] payload) {
        if (buffer == null || payload == null) {
            throw new NullPointerException("buffer and payload");
        }
        if (senderId == null || senderId.length != Long.BYTES * 2) {
            throw new IllegalArgumentException("sender id must be exactly 16 bytes");
        }
        if (sequence < 0 || sequence == Long.MAX_VALUE) {
            throw new IllegalArgumentException("sequence must be in [0, Long.MAX_VALUE)");
        }
        try {
            /* ExpandableArrayBuffer grows on demand; fixed buffers report an
             * insufficient destination through IndexOutOfBoundsException. Keep both
             * behaviours while exposing one deterministic codec exception. */
            buffer.putInt(MAGIC_OFFSET, MAGIC, ByteOrder.BIG_ENDIAN);
            buffer.putInt(VERSION_OFFSET, VERSION, ByteOrder.BIG_ENDIAN);
            buffer.putBytes(SENDER_ID_OFFSET, senderId, 0, Long.BYTES);
            buffer.putBytes(SENDER_ID_OFFSET + Long.BYTES, senderId, Long.BYTES, Long.BYTES);
            buffer.putLong(SEQUENCE_OFFSET, sequence, ByteOrder.BIG_ENDIAN);
            buffer.putInt(PAYLOAD_LENGTH_OFFSET, payload.length, ByteOrder.BIG_ENDIAN);
            buffer.putBytes(PAYLOAD_OFFSET, payload, 0, payload.length);
            buffer.putInt(PAYLOAD_OFFSET + payload.length, checksum(buffer, 0, HEADER_LENGTH + payload.length),
                    ByteOrder.BIG_ENDIAN);
        } catch (final IndexOutOfBoundsException failure) {
            throw new IllegalArgumentException("buffer is too small for Aeron clustered-cache frame", failure);
        }
        return HEADER_LENGTH + payload.length + CRC_LENGTH;
    }

        /// Validates the frame header and reports whether it carries the expected
    /// sender identity without copying the sender-id bytes or looping per byte.
    /// A malformed or truncated frame simply does not match.
    ///
    /// @param buffer           source buffer
    /// @param offset           frame offset
    /// @param length           frame length
    /// @param expectedSenderId sender identity to compare against
    /// @return `true` when the frame is well formed and carries the expected identity
    static boolean senderIdMatches(final DirectBuffer buffer, final int offset, final int length,
                                   final byte[] expectedSenderId) {
        if (expectedSenderId == null || expectedSenderId.length != Long.BYTES * 2) {
            return false;
        }
        try {
            validateHeader(buffer, offset, length, -1);
        } catch (final IllegalArgumentException malformed) {
            return false;
        }
        return buffer.getLong(offset + SENDER_ID_OFFSET, ByteOrder.BIG_ENDIAN) == readLong(expectedSenderId, 0) &&
               buffer.getLong(offset + SENDER_ID_OFFSET + Long.BYTES, ByteOrder.BIG_ENDIAN) ==
               readLong(expectedSenderId, Long.BYTES);
    }

        /// Returns the sender identity of a frame whose header has already been
    /// validated by [#senderIdMatches].
    ///
    /// @param buffer source buffer
    /// @param offset frame offset
    /// @return sender identity
    static SenderId senderIdOf(final DirectBuffer buffer, final int offset) {
        if (!validHeaderRange(buffer, offset)) {
            throw new IllegalArgumentException("Aeron clustered-cache frame header is outside the buffer");
        }
        return new SenderId(
                buffer.getLong(offset + SENDER_ID_OFFSET, ByteOrder.BIG_ENDIAN),
                buffer.getLong(offset + SENDER_ID_OFFSET + Long.BYTES, ByteOrder.BIG_ENDIAN));
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

        /// Validates the header length and returns the per-sender sequence of a
    /// frame. The magic and version must already have been validated by
    /// [#senderIdMatches].
    ///
    /// @param buffer source buffer
    /// @param offset frame offset
    /// @param length frame length
    /// @return per-sender sequence
    /// @throws IllegalArgumentException when the frame is shorter than its header
    static long sequenceOf(final DirectBuffer buffer, final int offset, final int length) {
        return validateHeader(buffer, offset, length, -1).sequence;
    }

        /// Validates the complete frame and returns a copy of its payload.
    ///
    /// @param buffer          source buffer
    /// @param offset          frame offset
    /// @param length          frame length
    /// @param maxPayloadBytes maximum accepted payload size
    /// @return copied payload bytes
    /// @throws IllegalArgumentException when the frame is malformed or oversized
    static byte[] decodePayload(final DirectBuffer buffer, final int offset, final int length,
                                final int maxPayloadBytes) {
        final Header header = validateHeader(buffer, offset, length, maxPayloadBytes);

        final byte[] payload = new byte[header.payloadLength];
        buffer.getBytes(offset + PAYLOAD_OFFSET, payload, 0, header.payloadLength);
        return payload;
    }

        /// Validates a frame once and returns everything the receiver needs.
    ///
    /// The polling hot path must not recompute the CRC per accessor: one
    /// validation covers the header, the sender identity, the sequence, and
    /// the payload bounds, and the payload is sliced without revalidating.
    /// An oversized self frame fails instead of skipping: the local sender
    /// can never emit one, so it is corrupt or spoofed either way.
    ///
    /// @param buffer           source buffer
    /// @param offset           frame offset
    /// @param length           frame length
    /// @param maxPayloadBytes  maximum accepted payload size
    /// @param expectedSenderId local sender identity for self-suppression
    /// @return validated frame fields
    /// @throws IllegalArgumentException when the frame is malformed or oversized
    static ValidatedFrame validate(final DirectBuffer buffer, final int offset, final int length,
                                   final int maxPayloadBytes, final byte[] expectedSenderId) {
        final Header header = validateHeader(buffer, offset, length, maxPayloadBytes);
        final SenderId sender = senderIdOf(buffer, offset);
        final boolean self = expectedSenderId != null && expectedSenderId.length == Long.BYTES * 2 &&
                             buffer.getLong(offset + SENDER_ID_OFFSET, ByteOrder.BIG_ENDIAN) ==
                             readLong(expectedSenderId, 0) &&
                             buffer.getLong(offset + SENDER_ID_OFFSET + Long.BYTES, ByteOrder.BIG_ENDIAN) ==
                             readLong(expectedSenderId, Long.BYTES);
        return new ValidatedFrame(sender, header.sequence, header.payloadLength, self);
    }

        /// Copies the payload of a frame validated by [#validate].
    ///
    /// @param buffer        source buffer
    /// @param offset        frame offset
    /// @param payloadLength payload length from the validated frame
    /// @return copied payload bytes
    static byte[] payloadOf(final DirectBuffer buffer, final int offset, final int payloadLength) {
        final byte[] payload = new byte[payloadLength];
        buffer.getBytes(offset + PAYLOAD_OFFSET, payload, 0, payloadLength);
        return payload;
    }

        /// Validates all fixed framing fields once and returns the decoded lengths.
    private static Header validateHeader(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final int maxPayloadBytes
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
        final long sequence = buffer.getLong(offset + SEQUENCE_OFFSET, ByteOrder.BIG_ENDIAN);
        if (!validSequence(sequence)) {
            throw new IllegalArgumentException("invalid Aeron clustered-cache frame sequence: %s".formatted(sequence));
        }
        final int payloadLength = buffer.getInt(offset + PAYLOAD_LENGTH_OFFSET, ByteOrder.BIG_ENDIAN);
        if (payloadLength < 0 || (maxPayloadBytes >= 0 && payloadLength > maxPayloadBytes)) {
            throw new IllegalArgumentException(
                    "Invalid Aeron clustered-cache payload length: %s".formatted(payloadLength));
        }
        if (HEADER_LENGTH + payloadLength + CRC_LENGTH != length) {
            throw new IllegalArgumentException(
                    "Aeron clustered-cache frame length does not match its payload");
        }
        final int checksumOffset = offset + HEADER_LENGTH + payloadLength;
        final int expectedChecksum = buffer.getInt(checksumOffset, ByteOrder.BIG_ENDIAN);
        final int actualChecksum = checksum(buffer, offset, HEADER_LENGTH + payloadLength);
        if (expectedChecksum != actualChecksum) {
            throw new IllegalArgumentException("Aeron clustered-cache frame CRC32C mismatch");
        }
        return new Header(sequence, payloadLength);
    }

        /// Checks a frame range without allowing integer overflow or a DirectBuffer
    /// bounds exception to escape the polling callback. Aeron can deliver a
    /// truncated fragment when a publication is interrupted; the receiver treats
    /// that input as a terminal stream failure rather than applying later frames.
    private static boolean validRange(final DirectBuffer buffer, final int offset, final int length) {
        return buffer != null && offset >= 0 && length >= HEADER_LENGTH + CRC_LENGTH &&
               offset <= buffer.capacity() - length;
    }

    private static boolean validHeaderRange(final DirectBuffer buffer, final int offset) {
        return buffer != null && offset >= 0 && offset <= buffer.capacity() - HEADER_LENGTH;
    }

    private static final ThreadLocal<CRC32C> CHECKSUM = ThreadLocal.withInitial(CRC32C::new);
    /* Bulk CRC needs one contiguous copy; the polling thread reuses this array
     * instead of calling getByte once per byte (about a million virtual calls
     * for a one-megabyte frame). */
    private static final ThreadLocal<byte[]> CHECKSUM_SCRATCH = ThreadLocal.withInitial(() -> new byte[0]);

    private static int checksum(final DirectBuffer buffer, final int offset, final int length) {
        final CRC32C crc = CHECKSUM.get();
        crc.reset();
        byte[] scratch = CHECKSUM_SCRATCH.get();
        if (scratch.length < length) {
            scratch = new byte[length];
            CHECKSUM_SCRATCH.set(scratch);
        }
        buffer.getBytes(offset, scratch, 0, length);
        crc.update(scratch, 0, length);
        return (int) crc.getValue();
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
    private record Header(long sequence, int payloadLength) {
    }

        /// One validation's worth of receiver inputs.
    ///
    /// @param sender        frame sender identity
    /// @param sequence      per-sender monotonic sequence
    /// @param payloadLength validated payload length
    /// @param self          whether the frame carries the local sender identity
    record ValidatedFrame(SenderId sender, long sequence, int payloadLength, boolean self) {
    }
}
