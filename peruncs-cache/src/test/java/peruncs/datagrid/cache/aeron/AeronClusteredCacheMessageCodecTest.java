package peruncs.datagrid.cache.aeron;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the fixed clustered-cache frame format and its bounds checks.
class AeronClusteredCacheMessageCodecTest {
    private static final byte[] SENDER_ID = uuidBytes(UUID.fromString("12345678-1234-1234-1234-123456789abc"));

    private static byte[] uuidBytes(final UUID uuid) {
        return ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private static long readLong(final byte[] bytes, final int offset) {
        return ByteBuffer.wrap(bytes, offset, Long.BYTES).getLong();
    }

    @Test
    void roundTripsPayload() {
        final byte[] payload = {1, 2, 3, 4, 5};
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);

        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);
        assertEquals(AeronClusteredCacheMessageCodec.HEADER_LENGTH +
                AeronClusteredCacheMessageCodec.CRC_LENGTH + payload.length, length);
        assertTrue(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 0, length, SENDER_ID));
        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 0, length, uuidBytes(UUID.randomUUID())));
        assertEquals(7L, AeronClusteredCacheMessageCodec.sequenceOf(buffer, 0, length));
        final AeronClusteredCacheMessageCodec.SenderId sender =
                AeronClusteredCacheMessageCodec.senderIdOf(buffer, 0);
        assertEquals(readLong(SENDER_ID, 0), sender.mostSignificantBits());
        assertEquals(readLong(SENDER_ID, Long.BYTES), sender.leastSignificantBits());
        assertArrayEquals(payload, AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, 1024));
    }

    @Test
    void rejectsPayloadCorruptionEvenWhenFrameLengthIsValid() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1, 2, 3});
        buffer.putByte(AeronClusteredCacheMessageCodec.HEADER_LENGTH, (byte) 9);

        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, 1024));
    }

    @Test
    void rejectsWrongLengthSenderId() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final byte[] shortSenderId = {1, 2, 3, 4};
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.encode(buffer, shortSenderId, 0L, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.senderIdOf(shortSenderId));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.senderIdOf(null));
    }

    @Test
    void rejectsExhaustedSequences() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, -1L, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, Long.MAX_VALUE, new byte[0]));
    }

    @Test
    void sequenceOfRejectsTruncatedFrame() {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1]);
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.sequenceOf(
                        buffer, 0, AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1));
    }

    @Test
    void decodesAtNonZeroOffset() {
        final byte[] payload = {9, 8, 7};
        final ExpandableArrayBuffer encoded = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(encoded, SENDER_ID, 7L, payload);

        final int offset = 7;
        final UnsafeBuffer positioned = new UnsafeBuffer(new byte[offset + length + 4]);
        positioned.putBytes(offset, encoded, 0, length);

        assertTrue(AeronClusteredCacheMessageCodec.senderIdMatches(positioned, offset, length, SENDER_ID));
        assertArrayEquals(payload, AeronClusteredCacheMessageCodec.decodePayload(positioned, offset, length, 1024));
    }

    @Test
    void roundTripsEmptyPayload() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 0L, new byte[0]);
        assertEquals(AeronClusteredCacheMessageCodec.HEADER_LENGTH +
                AeronClusteredCacheMessageCodec.CRC_LENGTH, length);
        assertArrayEquals(new byte[0], AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, 1024));
    }

    @Test
    void truncatedFrameDoesNotMatchAndIsRejected() {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1]);
        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(
                buffer, 0, AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(
                        buffer, 0, AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1, 1024));
    }

    @Test
    void rejectsUnknownMagic() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1});
        buffer.putInt(0, 0x12345678, ByteOrder.BIG_ENDIAN);

        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 0, length, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, 1024));
    }

    @Test
    void rejectsUnsupportedVersion() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1});
        buffer.putInt(Integer.BYTES, AeronClusteredCacheMessageCodec.VERSION + 1, ByteOrder.BIG_ENDIAN);

        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 0, length, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, 1024));
    }

    @Test
    void rejectsOversizedPayload() {
        final byte[] payload = new byte[64];
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);

        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, payload.length - 1));
    }

    @Test
    void acceptsExactMaxPayload() {
        final byte[] payload = new byte[64];
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);

        assertArrayEquals(payload, AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, payload.length));
    }

    @Test
    void rejectsTrailingBytes() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 2L, new byte[]{9});
        buffer.putInt(length, 0, ByteOrder.BIG_ENDIAN);

        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length + Integer.BYTES, 16));
    }

    @Test
    void selfSuppressionDoesNotHideMalformedPayloadLength() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 2L, new byte[]{9});
        buffer.putInt(AeronClusteredCacheMessageCodec.HEADER_LENGTH - Integer.BYTES, 999,
                ByteOrder.BIG_ENDIAN);

        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 0, length, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(buffer, 0, length, 1024));
    }

    @Test
    void selfSuppressionDoesNotHideInvalidSequence() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 2L, new byte[]{9});
        buffer.putLong(Integer.BYTES * 2 + Long.BYTES * 2, -1L, ByteOrder.BIG_ENDIAN);

        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 0, length, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.sequenceOf(buffer, 0, length));
    }

    @Test
    void malformedRangesNeverEscapeSenderIdentityProbe() {
        final UnsafeBuffer buffer = new UnsafeBuffer(
                new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH]);
        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, -1,
                AeronClusteredCacheMessageCodec.HEADER_LENGTH, SENDER_ID));
        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 1,
                AeronClusteredCacheMessageCodec.HEADER_LENGTH, SENDER_ID));
        assertFalse(AeronClusteredCacheMessageCodec.senderIdMatches(buffer, 0,
                AeronClusteredCacheMessageCodec.HEADER_LENGTH, null));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.senderIdOf(buffer, 1));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.decodePayload(buffer, -1,
                        AeronClusteredCacheMessageCodec.HEADER_LENGTH, 1));
    }

    @Test
    void encodeRejectsInsufficientDestination() {
        final UnsafeBuffer buffer = new UnsafeBuffer(
                new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1]);
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[0]));
    }
}
