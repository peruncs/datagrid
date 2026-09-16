package peruncs.datagrid.cache.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/// Encodes the only message permitted on the clustered-cache wire.
///
/// This small fixed-schema codec keeps network input out of Eclipse Serializer's
/// dynamic type resolution. A remote peer can therefore produce only one
/// timestamp update shape, never an arbitrary registered Java object.
final class AeronClusteredCachePayloadCodec {
    private static final int FIXED_LENGTH = Integer.BYTES * 2 + Long.BYTES;
    private static final int MAX_NAME_BYTES = 1 << 20;

    private AeronClusteredCachePayloadCodec() {
    }

    static byte[] encode(final TimestampsRegionUpdateMessage message) {
        Objects.requireNonNull(message, "message");
        final byte[] encoded = new byte[encodedLength(message)];
        encodeInto(message, new UnsafeBuffer(encoded), 0);
        return encoded;
    }

        /// Returns the encoded length without allocating: UTF-8 byte counts are
    /// derived straight from the characters, so the sender can size its scratch
    /// buffer exactly before serializing into it.
    ///
    /// @param message message to measure
    /// @return encoded payload length
    static int encodedLength(final TimestampsRegionUpdateMessage message) {
        Objects.requireNonNull(message, "message");
        final int cacheLength = utf8Length(message.cacheName());
        final int tableLength = utf8Length(message.tableName());
        if (cacheLength < 1 || cacheLength > MAX_NAME_BYTES || tableLength < 1 || tableLength > MAX_NAME_BYTES) {
            throw new IllegalArgumentException("cache or table name is too large");
        }
        return Math.addExact(FIXED_LENGTH, Math.addExact(cacheLength, tableLength));
    }

        /// Counts UTF-8 bytes without allocating the encoded array.
    private static int utf8Length(final String value) {
        if (value == null) {
            return 0;
        }
        int length = 0;
        for (int index = 0, end = value.length(); index < end; index++) {
            final char current = value.charAt(index);
            if (current < 0x80) {
                length++;
            } else if (current < 0x800) {
                length += 2;
            } else if (Character.isHighSurrogate(current) && index + 1 < end &&
                       Character.isLowSurrogate(value.charAt(index + 1))) {
                length += 4;
                index++;
            } else {
                length += 3;
            }
        }
        return length;
    }

        /// Encodes one message straight into a caller-owned buffer.
    ///
    /// The sender serializes directly into its reusable scratch at the frame's
    /// payload offset, so publishing never allocates a per-message payload
    /// array or `ByteBuffer`. Only the two small name arrays are transient.
    ///
    /// @param message message to encode
    /// @param target  destination buffer with room for [#encodedLength] bytes at the offset
    /// @param offset  payload offset within the destination
    /// @return encoded payload length
    static int encodeInto(final TimestampsRegionUpdateMessage message, final MutableDirectBuffer target,
                          final int offset) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(target, "target");
        final byte[] cacheName = message.cacheName().getBytes(StandardCharsets.UTF_8);
        final byte[] tableName = message.tableName().getBytes(StandardCharsets.UTF_8);
        if (cacheName.length < 1 || cacheName.length > MAX_NAME_BYTES ||
            tableName.length < 1 || tableName.length > MAX_NAME_BYTES) {
            throw new IllegalArgumentException("cache or table name is too large");
        }
        final int length = Math.addExact(FIXED_LENGTH, Math.addExact(cacheName.length, tableName.length));
        try {
            target.putInt(offset, cacheName.length, ByteOrder.BIG_ENDIAN);
            target.putInt(offset + Integer.BYTES, tableName.length, ByteOrder.BIG_ENDIAN);
            target.putLong(offset + Integer.BYTES * 2, message.timestamp(), ByteOrder.BIG_ENDIAN);
            target.putBytes(offset + FIXED_LENGTH, cacheName, 0, cacheName.length);
            target.putBytes(offset + FIXED_LENGTH + cacheName.length, tableName, 0, tableName.length);
        } catch (final IndexOutOfBoundsException failure) {
            throw new IllegalArgumentException("target is too small for the clustered-cache payload", failure);
        }
        return length;
    }

        /// Decodes one invalidation straight from the receive buffer.
    ///
    /// The polling hot path must not copy the whole payload into a new
    /// `byte[]` first: only the two name fields are copied, in one bulk
    /// transfer each, and the fixed header is read directly off the buffer.
    ///
    /// @param buffer receive buffer holding the frame
    /// @param offset offset of the payload within the buffer
    /// @param length validated payload length
    /// @return decoded invalidation
    /// @throws IllegalArgumentException when the payload is malformed
    static TimestampsRegionUpdateMessage decode(final DirectBuffer buffer, final int offset, final int length) {
        if (buffer == null || offset < 0 || length < FIXED_LENGTH || offset > buffer.capacity() - length) {
            throw new IllegalArgumentException("clustered-cache payload is truncated");
        }
        final int cacheLength = buffer.getInt(offset, ByteOrder.BIG_ENDIAN);
        final int tableLength = buffer.getInt(offset + Integer.BYTES, ByteOrder.BIG_ENDIAN);
        final long timestamp = buffer.getLong(offset + Integer.BYTES * 2, ByteOrder.BIG_ENDIAN);
        if (cacheLength < 1 || cacheLength > MAX_NAME_BYTES ||
            tableLength < 1 || tableLength > MAX_NAME_BYTES ||
            (long) cacheLength + tableLength != (long) length - FIXED_LENGTH) {
            throw new IllegalArgumentException("clustered-cache payload has invalid name lengths");
        }
        try {
            final String cacheName = decodeName(buffer, offset + FIXED_LENGTH, cacheLength);
            final String tableName = decodeName(buffer, offset + FIXED_LENGTH + cacheLength, tableLength);
            return new TimestampsRegionUpdateMessage(cacheName, tableName, timestamp);
        } catch (final CharacterCodingException failure) {
            throw new IllegalArgumentException("clustered-cache payload is not valid UTF-8", failure);
        }
    }

    private static String decodeName(final DirectBuffer buffer, final int offset, final int length)
            throws CharacterCodingException {
        final byte[] name = new byte[length];
        buffer.getBytes(offset, name, 0, length);
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(name))
                .toString();
    }

}
