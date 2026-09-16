package peruncs.datagrid.cluster.storage.aeron.wire;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies envelope framing, bounds, and corruption detection.
class AeronReplicationEnvelopeTest {
    private static final UUID CLUSTER = UUID.randomUUID();

        /// Verifies round trip preserves opaque serializer bytes.
    @Test
    void roundTripPreservesOpaqueSerializerBytes() {
        final byte[] payload = new byte[]{0, 1, 2, 127, -1};
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 9, 7, 42, AeronReplicationEnvelope.Kind.STORE_BINARY,
                100, 2, 3, 95, 0, payload
        );

        final AeronReplicationEnvelope.Envelope decoded = AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length
        );

        assertEquals(CLUSTER, decoded.clusterId());
        assertEquals(9, decoded.epoch());
        assertEquals(7, decoded.fencingToken());
        assertEquals(42, decoded.sequence());
        assertEquals(AeronReplicationEnvelope.Kind.STORE_BINARY, decoded.kind());
        assertEquals(100, decoded.payloadLength());
        assertEquals(2, decoded.chunkIndex());
        assertEquals(3, decoded.chunkCount());
        assertEquals(95, decoded.chunkOffset());
        assertArrayEquals(payload, decoded.payload());
    }

        /// Verifies rejection of corrupt payload before delivery.
    @Test
    void rejectsCorruptPayloadBeforeDelivery() {
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                1, 0, 1, 0, 0, new byte[]{7}
        );
        encoded[AeronReplicationEnvelope.HEADER_LENGTH] = 8;

        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length
        ));
    }

        /// Decision-bearing terminal fields are protected even when the payload is empty.
    @Test
    void rejectsTerminalKindMutationWithHeaderChecksum() {
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.COMMIT,
                1, 0, 1, 0, AeronReplicationEnvelope.crc32c(new byte[]{7}), new byte[0]);
        encoded[6] = 4;
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length));
    }

        /// Verifies rejection of truncated and unknown version.
    @Test
    void rejectsTruncatedAndUnknownVersion() {
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(new byte[AeronReplicationEnvelope.HEADER_LENGTH - 1]),
                0,
                AeronReplicationEnvelope.HEADER_LENGTH - 1
        ));

        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.COMMIT,
                0, 0, 1, 0, 0, new byte[0]
        );
        encoded[5] = 4;
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length
        ));
    }

        /// Verifies rejection of invalid chunk metadata and source bounds.
    @Test
    void rejectsInvalidChunkMetadataAndSourceBounds() {
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                1, 1, 1, 0, 0, new byte[]{7}
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                0, 0, 2, 0, 0, new byte[0]
        ));

        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.COMMIT,
                0, 0, 1, 0, 0, new byte[0]
        );
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 1, encoded.length
        ));
        encoded[7] = 1;
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length
        ));
    }

        /// Verifies rejection of nulls negative fields and marker payloads.
    @Test
    void rejectsNullsNegativeFieldsAndMarkerPayloads() {
        assertThrows(NullPointerException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                null, 1, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY, 1, 0, 1, 0, 0, new byte[]{1}
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY, -1, 0, 1, 0, 0, new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.COMMIT, 0, 0, 1, 0, 0, new byte[]{1}
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, -1, 1, 0, AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0, 0, new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 1, -1, AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0, 0, new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 1, Long.MAX_VALUE, AeronReplicationEnvelope.Kind.STORE_BINARY,
                0, 0, 1, 0, 0, new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.ABORT, 0, 1, 2, 0, 0, new byte[0]
        ));
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.ABORT, 0, 0, 1, 0, 7, new byte[0]
        ));
    }

        /// Verifies rejection of logical payload bounds and reserved header byte.
    @Test
    void rejectsLogicalPayloadBoundsAndReservedHeaderByte() {
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                1, 0, 1, 0, 0, new byte[]{7}
        );
        java.nio.ByteBuffer.wrap(encoded).putInt(24, 0);
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length
        ));

        final byte[] reserved = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.COMMIT, 0, 0, 1, 0, 0, new byte[0]
        );
        reserved[7] = 1;
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(reserved), 0, reserved.length
        ));
    }

        /// Verifies rejection of truncated data payload declared by header.
    @Test
    void rejectsTruncatedDataPayloadDeclaredByHeader() {
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                1, 0, 1, 0, 0, new byte[]{7}
        );
        java.nio.ByteBuffer.wrap(encoded).putInt(24, 10);
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length - 1
        ));
    }

        /// Verifies decodes at non zero offset without reading outside source.
    @Test
    void decodesAtNonZeroOffsetWithoutReadingOutsideSource() {
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 3, 5, 8, AeronReplicationEnvelope.Kind.STORE_BINARY,
                3, 0, 1, 0, 0, new byte[]{3, 4, 5}
        );
        final byte[] framed = new byte[encoded.length + 6];
        System.arraycopy(encoded, 0, framed, 3, encoded.length);
        final AeronReplicationEnvelope.Envelope result = assertDoesNotThrow(() ->
                AeronReplicationEnvelope.decode(new UnsafeBuffer(framed), 3, encoded.length));
        assertEquals(8, result.sequence());
        assertArrayEquals(new byte[]{3, 4, 5}, result.payload());
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(framed), 3, encoded.length + 1
        ));
    }

        /// Verifies rejection of length larger than source without integer underflow.
    @Test
    void rejectsLengthLargerThanSourceWithoutIntegerUnderflow() {
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.COMMIT,
                0, 0, 1, 0, 0, new byte[0]);
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, Integer.MAX_VALUE));
    }

        /// Verifies envelope payload accessor is defensive.
    @Test
    void envelopePayloadAccessorIsDefensive() {
        final byte[] encoded = AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                1, 0, 1, 0, 0, new byte[]{7});
        final AeronReplicationEnvelope.Envelope envelope = AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, encoded.length);
        final byte[] payload = envelope.payload();
        payload[0] = 9;
        assertArrayEquals(new byte[]{7}, envelope.payload());
    }

        /// Verifies the owned envelope also copies the constructor input.
    @Test
    void envelopeConstructorOwnsPayload() {
        final byte[] payload = new byte[]{7};
        final AeronReplicationEnvelope.Envelope envelope = new AeronReplicationEnvelope.Envelope(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                1, 0, 1, 0, 0, payload);
        payload[0] = 9;
        assertArrayEquals(new byte[]{7}, envelope.payload());
    }

        /// Verifies CRC calculation across heap, sliced, and direct Agrona buffers.
    @Test
    void crcSupportsAllByteBufferRepresentations() {
        final byte[] expected = {4, 8, 15, 16, 23, 42};
        final int expectedCrc = AeronReplicationEnvelope.crc32c(expected);
        AeronReplicationEnvelope.withChecksumContext(new AeronReplicationEnvelope.ChecksumContext(), () -> {
            final byte[] heapBytes = new byte[expected.length + 4];
            System.arraycopy(expected, 0, heapBytes, 2, expected.length);
            assertEquals(expectedCrc,
                    AeronReplicationEnvelope.crc32c(new UnsafeBuffer(ByteBuffer.wrap(heapBytes)), 2, expected.length));

            final ByteBuffer slicedBytes = ByteBuffer.wrap(new byte[]{99, 4, 8, 15, 16, 23, 42, 100}).slice();
            assertEquals(expectedCrc,
                    AeronReplicationEnvelope.crc32c(new UnsafeBuffer(slicedBytes), 1, expected.length));

            final ByteBuffer directBytes = ByteBuffer.allocateDirect(expected.length + 2);
            directBytes.position(1);
            directBytes.put(expected).flip();
            assertEquals(expectedCrc,
                    AeronReplicationEnvelope.crc32c(new UnsafeBuffer(directBytes), 1, expected.length));
            return null;
        });
    }

        /// Verifies the owned form rejects impossible public field combinations.
    @Test
    void ownedEnvelopeValidatesItsPublicFields() {
        assertThrows(ReplicationWireException.class, () -> new AeronReplicationEnvelope.Envelope(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.COMMIT,
                1, 0, 1, 0, 0, new byte[]{7}));
        assertThrows(ReplicationWireException.class, () -> new AeronReplicationEnvelope.Envelope(
                CLUSTER, 1, 9, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                1, 0, 1, 1, 0, new byte[]{7, 8}));
    }

        /// Verifies authenticated frames reject unsigned and forged payloads.
    @Test
    void authenticatedFramesRequireTheConfiguredSecret() {
        final byte[] secret = "replication-secret".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final byte[] payload = new byte[]{1, 2, 3};
        final byte[] encoded = new byte[AeronReplicationEnvelope.HEADER_LENGTH + payload.length
                + AeronReplicationEnvelope.HMAC_LENGTH];
        final int length = AeronReplicationEnvelope.withChecksumContext(
                new AeronReplicationEnvelope.ChecksumContext(), () -> AeronReplicationEnvelope.encode(
                        new UnsafeBuffer(encoded), 0, CLUSTER, 1, 1, 7,
                        AeronReplicationEnvelope.Kind.STORE_BINARY, payload.length, 0, 1, 0, 0,
                        new UnsafeBuffer(payload), 0, payload.length, secret));
        assertEquals(encoded.length, length);
        assertArrayEquals(payload, AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, length, secret).payload());
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, length));
        encoded[AeronReplicationEnvelope.HEADER_LENGTH] ^= 1;
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, length, secret));
    }

        /// Verifies rotation overlap: a frame signed with the retiring key still
    /// decodes while the primary has moved on, and a forged frame fails both keys.
    @Test
    void rotationOverlapAcceptsTheRetiringKey() {
        final byte[] previous = "previous-replication-secret".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final byte[] primary = "primary-replication-secret".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final byte[] payload = new byte[]{1, 2, 3};
        final byte[] encoded = new byte[AeronReplicationEnvelope.HEADER_LENGTH + payload.length
                + AeronReplicationEnvelope.HMAC_LENGTH];
        final int length = AeronReplicationEnvelope.withChecksumContext(
                new AeronReplicationEnvelope.ChecksumContext(), () -> AeronReplicationEnvelope.encode(
                        new UnsafeBuffer(encoded), 0, CLUSTER, 1, 1, 7,
                        AeronReplicationEnvelope.Kind.STORE_BINARY, payload.length, 0, 1, 0, 0,
                        new UnsafeBuffer(payload), 0, payload.length, previous));
        assertEquals(encoded.length, length);
        assertArrayEquals(payload, AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, length, primary, previous).payload());
        assertArrayEquals(payload, AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, length, previous, null).payload());
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, length, primary, null));
        assertThrows(ReplicationWireException.class, () -> AeronReplicationEnvelope.decode(
                new UnsafeBuffer(encoded), 0, length, primary,
                "unrelated-replication-secret".getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
    }

    @Test
    void signedEncodingChecksTagCapacityBeforeWriting() {
        final byte[] secret = "replication-secret".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final byte[] target = new byte[AeronReplicationEnvelope.HEADER_LENGTH + 3];
        java.util.Arrays.fill(target, (byte) 0x5a);
        final byte[] before = target.clone();

        assertThrows(IllegalArgumentException.class, () ->
                AeronReplicationEnvelope.withChecksumContext(
                        new AeronReplicationEnvelope.ChecksumContext(), () -> AeronReplicationEnvelope.encode(
                                new UnsafeBuffer(target), 0, CLUSTER, 1, 1, 7,
                                AeronReplicationEnvelope.Kind.STORE_BINARY, 3, 0, 1, 0, 0,
                                new UnsafeBuffer(new byte[]{1, 2, 3}), 0, 3, secret)));
        assertArrayEquals(before, target, "a rejected signed frame must not partially overwrite its destination");
    }
}
