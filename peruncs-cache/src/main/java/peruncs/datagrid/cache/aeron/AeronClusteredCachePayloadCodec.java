package peruncs.datagrid.cache.aeron;

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
        final byte[] cacheName = message.cacheName().getBytes(StandardCharsets.UTF_8);
        final byte[] tableName = message.tableName().getBytes(StandardCharsets.UTF_8);
        if (cacheName.length > MAX_NAME_BYTES || tableName.length > MAX_NAME_BYTES) {
            throw new IllegalArgumentException("cache or table name is too large");
        }
        final int length = Math.addExact(FIXED_LENGTH, Math.addExact(cacheName.length, tableName.length));
        return ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN)
                .putInt(cacheName.length)
                .putInt(tableName.length)
                .putLong(message.timestamp())
                .put(cacheName)
                .put(tableName)
                .array();
    }

    static TimestampsRegionUpdateMessage decode(final byte[] payload) {
        if (payload == null || payload.length < FIXED_LENGTH) {
            throw new IllegalArgumentException("clustered-cache payload is truncated");
        }
        final ByteBuffer encoded = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        final int cacheLength = encoded.getInt();
        final int tableLength = encoded.getInt();
        final long timestamp = encoded.getLong();
        if (cacheLength < 1 || cacheLength > MAX_NAME_BYTES ||
            tableLength < 1 || tableLength > MAX_NAME_BYTES ||
            (long) cacheLength + tableLength != encoded.remaining()) {
            throw new IllegalArgumentException("clustered-cache payload has invalid name lengths");
        }
        try {
            final String cacheName = decodeName(encoded, cacheLength);
            final String tableName = decodeName(encoded, tableLength);
            return new TimestampsRegionUpdateMessage(cacheName, tableName, timestamp);
        } catch (final CharacterCodingException failure) {
            throw new IllegalArgumentException("clustered-cache payload is not valid UTF-8", failure);
        }
    }

    private static String decodeName(final ByteBuffer encoded, final int length)
            throws CharacterCodingException {
        final ByteBuffer name = encoded.slice(encoded.position(), length);
        encoded.position(encoded.position() + length);
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(name)
                .toString();
    }
}
