package peruncs.datagrid.cache.aeron;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the fixed clustered-cache frame format, its bounds checks, and its
/// optional HMAC authentication through the single [AeronClusteredCacheMessageCodec#validate] entry point.
class AeronClusteredCacheMessageCodecTest {
    private static final byte[] SENDER_ID = uuidBytes(UUID.fromString("12345678-1234-1234-1234-123456789abc"));
    private static final byte[] SECRET =
            "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private static final byte[] WRONG_SECRET =
            "abcdef0123456789abcdef0123456789".getBytes(StandardCharsets.UTF_8);

    private static byte[] uuidBytes(final UUID uuid) {
        return ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private static long readLong(final byte[] bytes, final int offset) {
        return ByteBuffer.wrap(bytes, offset, Long.BYTES).getLong();
    }

    private static byte[] payloadBytes(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] payload = new byte[length];
        buffer.getBytes(offset, payload, 0, length);
        return payload;
    }

    private static AeronClusteredCacheMessageCodec.ValidatedFrame validateUnsigned(
            final DirectBuffer buffer, final int offset, final int length, final byte[] expectedSenderId) {
        return AeronClusteredCacheMessageCodec.validate(buffer, offset, length, 1024, expectedSenderId, null);
    }

    private static AeronClusteredCacheMessageCodec.ValidatedFrame validateSigned(
            final DirectBuffer buffer, final int offset, final int length, final byte[] expectedSenderId) {
        return AeronClusteredCacheMessageCodec.validate(
                buffer, offset, length, 1024, expectedSenderId, SECRET);
    }

    @Test
    void roundTripsPayload() {
        final byte[] payload = {1, 2, 3, 4, 5};
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);

        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);
        assertEquals(AeronClusteredCacheMessageCodec.HEADER_LENGTH +
                AeronClusteredCacheMessageCodec.CRC_LENGTH + payload.length, length);
        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateUnsigned(buffer, 0, length, uuidBytes(UUID.randomUUID()));
        assertFalse(frame.self());
        assertFalse(frame.heartbeat());
        assertEquals(7L, frame.sequence());
        assertEquals(readLong(SENDER_ID, 0), frame.sender().mostSignificantBits());
        assertEquals(readLong(SENDER_ID, Long.BYTES), frame.sender().leastSignificantBits());
        assertEquals(payload.length, frame.payloadLength());
        assertArrayEquals(payload, payloadBytes(
                buffer, AeronClusteredCacheMessageCodec.payloadOffset(0), frame.payloadLength()));
    }

    @Test
    void rejectsPayloadCorruptionEvenWhenFrameLengthIsValid() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1, 2, 3});
        buffer.putByte(AeronClusteredCacheMessageCodec.HEADER_LENGTH, (byte) 9);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, SENDER_ID));
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
    void validationRejectsTruncatedFrame() {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1]);
        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(
                        buffer, 0, AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1, SENDER_ID));
    }

    @Test
    void validatesAtNonZeroOffset() {
        final byte[] payload = {9, 8, 7};
        final ExpandableArrayBuffer encoded = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(encoded, SENDER_ID, 7L, payload);

        final int offset = 7;
        final UnsafeBuffer positioned = new UnsafeBuffer(new byte[offset + length + 4]);
        positioned.putBytes(offset, encoded, 0, length);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateUnsigned(positioned, offset, length, SENDER_ID);
        assertTrue(frame.self());
        assertEquals(7L, frame.sequence());
        assertArrayEquals(payload, payloadBytes(
                positioned, AeronClusteredCacheMessageCodec.payloadOffset(offset), frame.payloadLength()));
    }

    @Test
    void roundTripsEmptyPayload() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 0L, new byte[0]);
        assertEquals(AeronClusteredCacheMessageCodec.HEADER_LENGTH +
                AeronClusteredCacheMessageCodec.CRC_LENGTH, length);
        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateUnsigned(buffer, 0, length, SENDER_ID);
        assertEquals(0, frame.payloadLength());
        assertFalse(frame.heartbeat());
    }

    @Test
    void rejectsUnknownMagic() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1});
        buffer.putInt(0, 0x12345678, ByteOrder.BIG_ENDIAN);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, SENDER_ID));
    }

    @Test
    void rejectsUnsupportedVersion() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1});
        buffer.putInt(Integer.BYTES, AeronClusteredCacheMessageCodec.VERSION + 1, ByteOrder.BIG_ENDIAN);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, SENDER_ID));
    }

    @Test
    void rejectsOversizedPayload() {
        final byte[] payload = new byte[64];
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);

        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.validate(
                        buffer, 0, length, payload.length - 1, SENDER_ID, null));
    }

    @Test
    void acceptsExactMaxPayload() {
        final byte[] payload = new byte[64];
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                AeronClusteredCacheMessageCodec.validate(buffer, 0, length, payload.length, SENDER_ID, null);
        assertEquals(payload.length, frame.payloadLength());
    }

    @Test
    void rejectsTrailingBytes() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 2L, new byte[]{9});
        buffer.putInt(length, 0, ByteOrder.BIG_ENDIAN);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length + Integer.BYTES, SENDER_ID));
    }

    @Test
    void validationDoesNotHideMalformedPayloadLength() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 2L, new byte[]{9});
        buffer.putInt(AeronClusteredCacheMessageCodec.HEADER_LENGTH - Integer.BYTES, 999,
                ByteOrder.BIG_ENDIAN);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, SENDER_ID));
    }

    @Test
    void validationDoesNotHideInvalidSequence() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 2L, new byte[]{9});
        buffer.putLong(Integer.BYTES * 3 + Long.BYTES * 2, -1L, ByteOrder.BIG_ENDIAN);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, SENDER_ID));
    }

    @Test
    void malformedRangesAreRejected() {
        final UnsafeBuffer buffer = new UnsafeBuffer(
                new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH]);
        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, -1, AeronClusteredCacheMessageCodec.HEADER_LENGTH, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 1, AeronClusteredCacheMessageCodec.HEADER_LENGTH, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, -1, AeronClusteredCacheMessageCodec.HEADER_LENGTH, SENDER_ID));
    }

    @Test
    void encodeRejectsInsufficientDestination() {
        final UnsafeBuffer buffer = new UnsafeBuffer(
                new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1]);
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[0]));
    }

    @Test
    void singleValidationCoversForeignFrame() {
        final byte[] payload = {1, 2, 3, 4, 5};
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                AeronClusteredCacheMessageCodec.validate(
                        buffer, 0, length, 1024, uuidBytes(UUID.randomUUID()), null);
        assertFalse(frame.self());
        assertFalse(frame.heartbeat());
        assertEquals(7L, frame.sequence());
        assertEquals(readLong(SENDER_ID, 0), frame.sender().mostSignificantBits());
        assertEquals(readLong(SENDER_ID, Long.BYTES), frame.sender().leastSignificantBits());
        assertEquals(payload.length, frame.payloadLength());
    }

    @Test
    void singleValidationFlagsSelfFrameWithoutDecoding() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, new byte[]{9});

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateUnsigned(buffer, 0, length, SENDER_ID);
        assertTrue(frame.self());
        assertFalse(frame.heartbeat());
        assertEquals(7L, frame.sequence());
    }

    @Test
    void heartbeatRoundTripsWithoutPayload() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encodeHeartbeat(buffer, SENDER_ID, 11L);

        assertEquals(AeronClusteredCacheMessageCodec.HEADER_LENGTH +
                AeronClusteredCacheMessageCodec.CRC_LENGTH, length);
        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateUnsigned(buffer, 0, length, uuidBytes(UUID.randomUUID()));
        assertTrue(frame.heartbeat());
        assertFalse(frame.self());
        assertEquals(11L, frame.sequence());
        assertEquals(0, frame.payloadLength());
    }

    @Test
    void heartbeatIsSubjectToSelfSuppression() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encodeHeartbeat(buffer, SENDER_ID, 3L);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateUnsigned(buffer, 0, length, SENDER_ID);
        assertTrue(frame.heartbeat());
        assertTrue(frame.self());
    }

    @Test
    void unknownFrameTypeIsRejected() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{9});
        buffer.putInt(Integer.BYTES * 2, 99, ByteOrder.BIG_ENDIAN);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, SENDER_ID));
    }

    @Test
    void heartbeatWithPayloadIsRejected() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{9});
        /* Forge a heartbeat type over an invalidation body: the CRC still
         * matches the header, so only the type/payload rule can reject it. */
        buffer.putInt(Integer.BYTES * 2, AeronClusteredCacheMessageCodec.TYPE_HEARTBEAT, ByteOrder.BIG_ENDIAN);
        final int payloadLengthOffset = AeronClusteredCacheMessageCodec.HEADER_LENGTH - Integer.BYTES;
        final int payloadLength = buffer.getInt(payloadLengthOffset, ByteOrder.BIG_ENDIAN);
        final CRC32C prototype = new CRC32C();
        final byte[] header = new byte[AeronClusteredCacheMessageCodec.HEADER_LENGTH + payloadLength];
        buffer.getBytes(0, header, 0, header.length);
        prototype.update(header, 0, header.length);
        buffer.putInt(AeronClusteredCacheMessageCodec.HEADER_LENGTH + payloadLength, (int) prototype.getValue(),
                ByteOrder.BIG_ENDIAN);
        final int forgedLength = length;

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, forgedLength, SENDER_ID));
    }

    @Test
    void payloadDecodesDirectlyFromTheReceiveBuffer() {
        final TimestampsRegionUpdateMessage message =
                new TimestampsRegionUpdateMessage("cache", "table", 42L);
        final byte[] payload = AeronClusteredCachePayloadCodec.encode(message);
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                AeronClusteredCacheMessageCodec.validate(
                        buffer, 0, length, 1 << 20, uuidBytes(UUID.randomUUID()), null);
        assertEquals(message, AeronClusteredCachePayloadCodec.decode(
                buffer, AeronClusteredCacheMessageCodec.payloadOffset(0), frame.payloadLength()));
    }

    @Test
    void directBufferDecodeRejectsTruncation() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCachePayloadCodec.decode(buffer, 0, 3));
    }

    @Test
    void singleValidationRejectsCorruptionAndTruncation() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1, 2, 3});
        buffer.putByte(AeronClusteredCacheMessageCodec.HEADER_LENGTH, (byte) 9);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(
                        buffer, 0, AeronClusteredCacheMessageCodec.HEADER_LENGTH - 1, SENDER_ID));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.validate(buffer, 0, length, 2, SENDER_ID, null));
    }

    @Test
    void signedFrameRoundTrips() {
        final byte[] payload = {1, 2, 3, 4, 5};
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);

        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 7L, payload, SECRET);
        assertEquals(AeronClusteredCacheMessageCodec.HEADER_LENGTH +
                AeronClusteredCacheMessageCodec.CRC_LENGTH +
                AeronClusteredCacheMessageCodec.HMAC_LENGTH + payload.length, length);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateSigned(buffer, 0, length, uuidBytes(UUID.randomUUID()));
        assertFalse(frame.self());
        assertFalse(frame.heartbeat());
        assertEquals(7L, frame.sequence());
        assertArrayEquals(payload, payloadBytes(
                buffer, AeronClusteredCacheMessageCodec.payloadOffset(0), frame.payloadLength()));
    }

    @Test
    void tamperedPayloadIsRejected() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(
                buffer, SENDER_ID, 1L, new byte[]{1, 2, 3}, SECRET);
        buffer.putByte(AeronClusteredCacheMessageCodec.HEADER_LENGTH, (byte) 9);

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> validateSigned(buffer, 0, length, SENDER_ID));
        assertTrue(failure.getMessage().contains("CRC32C"), "corruption must fail on the checksum first");
    }

    @Test
    void forgedTagIsRejectedEvenWhenTheChecksumMatches() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(
                buffer, SENDER_ID, 1L, new byte[]{1, 2, 3}, SECRET);
        /* Forge the payload and repair the CRC, so only the HMAC can reject it. */
        buffer.putByte(AeronClusteredCacheMessageCodec.HEADER_LENGTH, (byte) 9);
        final int covered = AeronClusteredCacheMessageCodec.HEADER_LENGTH + 3;
        final CRC32C repaired = new CRC32C();
        final byte[] signed = new byte[covered];
        buffer.getBytes(0, signed, 0, covered);
        repaired.update(signed, 0, covered);
        buffer.putInt(covered, (int) repaired.getValue(), ByteOrder.BIG_ENDIAN);

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> validateSigned(buffer, 0, length, SENDER_ID));
        assertTrue(failure.getMessage().contains("HMAC"),
                "a CRC-repaired forgery must fail on the tag: " + failure.getMessage());
    }

    @Test
    void wrongSecretIsRejected() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(
                buffer, SENDER_ID, 1L, new byte[]{1, 2, 3}, SECRET);

        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.validate(
                        buffer, 0, length, 1024, uuidBytes(UUID.randomUUID()), WRONG_SECRET));
    }

    @Test
    void retiringSecretIsAcceptedDuringRotationOverlap() {
        final byte[] previous =
                "fedcba9876543210fedcba9876543210".getBytes(StandardCharsets.UTF_8);
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(
                buffer, SENDER_ID, 1L, new byte[]{1, 2, 3}, previous);

        AeronClusteredCacheMessageCodec.validate(buffer, 0, length, 1024, SENDER_ID, SECRET, previous);
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.validate(buffer, 0, length, 1024, SENDER_ID, SECRET));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.validate(
                        buffer, 0, length, 1024, SENDER_ID, SECRET, WRONG_SECRET));
    }

    @Test
    void unsignedFrameIsRejectedWhenASecretIsConfigured() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[]{1, 2, 3});

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> validateSigned(buffer, 0, length, SENDER_ID));
        assertTrue(failure.getMessage().contains("unsigned"),
                "an unsigned frame must be rejected explicitly: " + failure.getMessage());
    }

    @Test
    void signedFrameIsRejectedWithoutASecret() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encode(
                buffer, SENDER_ID, 1L, new byte[]{1, 2, 3}, SECRET);

        assertThrows(IllegalArgumentException.class,
                () -> validateUnsigned(buffer, 0, length, uuidBytes(UUID.randomUUID())));
    }

    @Test
    void signedHeartbeatRoundTrips() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final int length = AeronClusteredCacheMessageCodec.encodeHeartbeat(buffer, SENDER_ID, 11L, SECRET);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                validateSigned(buffer, 0, length, uuidBytes(UUID.randomUUID()));
        assertTrue(frame.heartbeat());
        assertFalse(frame.self());
        assertEquals(0, frame.payloadLength());
    }

    @Test
    void unsignedHeartbeatIsRejectedWhenASecretIsConfigured() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = AeronClusteredCacheMessageCodec.encodeHeartbeat(buffer, SENDER_ID, 3L);

        assertThrows(IllegalArgumentException.class,
                () -> validateSigned(buffer, 0, length, SENDER_ID));
    }

    @Test
    void shortSecretIsRejected() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 1L, new byte[0], new byte[8]));
        assertThrows(IllegalArgumentException.class,
                () -> AeronClusteredCacheMessageCodec.encodeHeartbeat(buffer, SENDER_ID, 1L, new byte[15]));
    }

    @Test
    void encodeIntoMatchesEncode() {
        final TimestampsRegionUpdateMessage message =
                new TimestampsRegionUpdateMessage("cache", "tablé-🗄", 42L);
        final byte[] expected = AeronClusteredCachePayloadCodec.encode(message);
        assertEquals(expected.length, AeronClusteredCachePayloadCodec.encodedLength(message));

        final UnsafeBuffer scratch = new UnsafeBuffer(new byte[expected.length]);
        final int length = AeronClusteredCachePayloadCodec.encodeInto(
                message, scratch, 0);
        assertEquals(expected.length, length);
        final byte[] actual = new byte[length];
        scratch.getBytes(0, actual, 0, length);
        assertArrayEquals(expected, actual);
    }

    @Test
    void encodedLengthCountsMultibyteNames() {
        assertEquals(
                AeronClusteredCachePayloadCodec.encode(
                        new TimestampsRegionUpdateMessage("cache", "table", 1L)).length,
                AeronClusteredCachePayloadCodec.encodedLength(
                        new TimestampsRegionUpdateMessage("cache", "table", 1L)));
        assertEquals(
                AeronClusteredCachePayloadCodec.encode(
                        new TimestampsRegionUpdateMessage("cäché", "tabelle-🗄", 1L)).length,
                AeronClusteredCachePayloadCodec.encodedLength(
                        new TimestampsRegionUpdateMessage("cäché", "tabelle-🗄", 1L)));
    }

    @Test
    void largeFrameStreamsItsChecksum() {
        /* Larger than the 8 KiB streaming chunk, so a whole-frame copy would
         * show up as a retained scratch array instead of chunked reads. */
        final byte[] payload = new byte[128 * 1024];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) (index * 31);
        }
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
        final int length = AeronClusteredCacheMessageCodec.encode(buffer, SENDER_ID, 9L, payload, SECRET);

        final AeronClusteredCacheMessageCodec.ValidatedFrame frame =
                AeronClusteredCacheMessageCodec.validate(
                        buffer, 0, length, payload.length, uuidBytes(UUID.randomUUID()), SECRET);
        assertEquals(9L, frame.sequence());
        assertEquals(payload.length, frame.payloadLength());
        assertArrayEquals(payload, payloadBytes(
                buffer, AeronClusteredCacheMessageCodec.payloadOffset(0), frame.payloadLength()));
    }
}
