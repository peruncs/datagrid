package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Tests encoding, integrity, and monotonicity of reader watermarks.
class AeronReaderWatermarkTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final UUID GENERATION = UUID.randomUUID();
    private static final UUID READER_ONE = UUID.randomUUID();
    private static final UUID READER_TWO = UUID.randomUUID();

    /// Verifies a watermark always encodes to one fixed 92-byte frame.
    @Test
    void watermarkUsesOneFixed92ByteFrame() {
        assertEquals(92, AeronReaderWatermark.ENCODED_LENGTH);
        final AeronReaderWatermark watermark = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096);
        assertEquals(92, watermark.encode().length);
    }

    /// Verifies an encoded watermark round-trips and that tampering with any byte is rejected.
    @Test
    void encodedWatermarkRoundTripsAndRejectsTampering() {
        final AeronReaderWatermark watermark = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096);
        assertEquals(watermark, AeronReaderWatermark.decode(watermark.encode()));

        final byte[] encoded = watermark.encode();
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.decode(encoded));
        encoded[encoded.length - 1] ^= 1;
        encoded[10] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.decode(encoded));
    }

    /// Verifies direct encoding into a reusable buffer matches allocated encoding and rejects an undersized target.
    @Test
    void directEncodingIntoReusableBufferMatchesAllocatedEncoding() {
        final byte[] expected = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096).encode();
        final byte[] actual = new byte[AeronReaderWatermark.ENCODED_LENGTH];
        AeronReaderWatermark.encodeInto(actual, READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096);
        assertArrayEquals(expected, actual);
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.encodeInto(
                new byte[91], READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096));
    }

    /// Verifies buffer decoding at an offset preserves every identity field.
    @Test
    void directBufferDecodePreservesEveryIdentityField() {
        final byte[] encoded = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 9, 31, 42, 4_096).encode();
        final byte[] framed = new byte[encoded.length + 7];
        System.arraycopy(encoded, 0, framed, 7, encoded.length);
        final AeronReaderWatermark decoded = AeronReaderWatermark.decode(
                new UnsafeBuffer(framed), 7, encoded.length);
        assertEquals(READER_ONE, decoded.readerId());
        assertEquals(CLUSTER, decoded.clusterId());
        assertEquals(GENERATION, decoded.storeGeneration());
        assertEquals(9, decoded.writerEpoch());
        assertEquals(31, decoded.recordingId());
        assertEquals(42, decoded.sequence());
        assertEquals(4_096, decoded.position());
        assertEquals(AeronReaderWatermark.decode(encoded), decoded);
    }

    /// Verifies decoding rejects truncated frames and frames with the wrong magic or version.
    @Test
    void decodeRejectsTruncatedFramesAndWrongMagicOrVersion() {
        final byte[] encoded = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096).encode();
        assertThrows(IllegalArgumentException.class,
                () -> AeronReaderWatermark.decode(new byte[91]));
        final byte[] badMagic = encoded.clone();
        badMagic[0] ^= (byte) 0xFF;
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.decode(badMagic));
        final byte[] badVersion = encoded.clone();
        badVersion[7] ^= (byte) 0xFF;
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.decode(badVersion));
    }

    /// Verifies the validator accepts forward progress while rejecting rollbacks and identity confusion.
    @Test
    void validatorRejectsRollbackAndIdentityConfusion() {
        try (final AeronReaderWatermark.Validator validator = new AeronReaderWatermark.Validator()) {
            validator.accept(AeronReaderWatermark.of(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100));
            validator.accept(AeronReaderWatermark.of(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100));
            assertThrows(IllegalStateException.class, () -> validator.accept(
                    AeronReaderWatermark.of(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 101)));
            validator.accept(AeronReaderWatermark.of(READER_ONE, CLUSTER, GENERATION, 3, 17, 2, 200));
            assertThrows(IllegalStateException.class, () -> validator.accept(
                    AeronReaderWatermark.of(READER_ONE, CLUSTER, GENERATION, 3, 17, 3, 199)));
            assertThrows(IllegalStateException.class, () -> validator.accept(
                    AeronReaderWatermark.of(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 99)));
            assertThrows(IllegalStateException.class, () -> validator.accept(
                    AeronReaderWatermark.of(READER_ONE, UUID.randomUUID(), GENERATION, 3, 17, 3, 300)));
        }
    }

        /// Sequence values must remain incrementable by the writer and validator.
    @Test
    void rejectsSequenceThatWouldOverflowNextReservation() {
        assertThrows(IllegalArgumentException.class, () -> new AeronReaderWatermark(
                READER_ONE, CLUSTER, GENERATION, 3, 17, Long.MAX_VALUE, 100));
    }

    /// Verifies construction and direct encoding reject negative epoch, recording, sequence, and position values.
    @Test
    void rejectsInvalidProgressBeforeEncoding() {
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, -1, 17, 1, 100));
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, -1, 1, 100));
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, -2, 100));
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, -2));
        assertThrows(IllegalArgumentException.class, () -> AeronReaderWatermark.encodeInto(
                new byte[AeronReaderWatermark.ENCODED_LENGTH], READER_ONE, CLUSTER, GENERATION, 3, 17, -2, 100));
    }

    /// Verifies a quorum requires every reader and aggregates to the least advanced sequence and position.
    @Test
    void quorumRequiresEveryReaderAndAggregatesLeastProgress() {
        try (final AeronReaderWatermark.Quorum quorum =
                new AeronReaderWatermark.Quorum(java.util.Set.of(READER_ONE, READER_TWO))) {
            quorum.accept(AeronReaderWatermark.of(READER_ONE, CLUSTER, GENERATION, 3, 17, 8, 800));
            assertThrows(IllegalStateException.class, quorum::aggregate);
            assertEquals(java.util.Set.of(READER_TWO), quorum.missingReaders());
            quorum.accept(AeronReaderWatermark.of(READER_TWO, CLUSTER, GENERATION, 3, 17, 7, 700));
            final AeronReaderWatermark aggregate = quorum.aggregate();
            assertEquals(7, aggregate.sequence());
            assertEquals(700, aggregate.position());
            assertTrue(quorum.missingReaders().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> quorum.accept(
                    AeronReaderWatermark.of(UUID.randomUUID(), CLUSTER, GENERATION, 3, 17, 9, 900)));
        }
    }

    /// Verifies static aggregation rejects duplicate reader identities.
    @Test
    void aggregateRejectsDuplicateReaderIdentity() {
        final AeronReaderWatermark first = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100);
        final AeronReaderWatermark second = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 2, 200);
        assertThrows(IllegalArgumentException.class,
                () -> AeronReaderWatermark.aggregate(java.util.List.of(first, second)));
    }

    /// Verifies static aggregation reports the least advanced sequence and position boundary.
    @Test
    void aggregateReportsLeastAdvancedBoundary() {
        final AeronReaderWatermark advanced = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 2, 200);
        final AeronReaderWatermark lagging = AeronReaderWatermark.of(
                READER_TWO, CLUSTER, GENERATION, 3, 17, 1, 100);
        final AeronReaderWatermark aggregate = AeronReaderWatermark.aggregate(
                java.util.List.of(advanced, lagging));
        assertEquals(new UUID(0L, 0L), aggregate.readerId());
        assertEquals(1, aggregate.sequence());
        assertEquals(100, aggregate.position());
        assertEquals(aggregate, AeronReaderWatermark.decode(aggregate.encode()));
    }

    /// Verifies the validator rejects restoring a token that belongs to a different reader.
    @Test
    void validatorRejectsRestoringAnIdentityMismatchedToken() {
        try (final AeronReaderWatermark.Validator validator = new AeronReaderWatermark.Validator()) {
            final AeronReaderWatermark token = AeronReaderWatermark.of(
                    READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100);
            assertThrows(IllegalArgumentException.class, () -> validator.restore(READER_TWO, token));
        }
    }

    /// Verifies the quorum rejects restoring unknown or retired readers while allowing a null clear.
    @Test
    void quorumRejectsRestoringUnknownOrRetiredReaders() {
        try (final AeronReaderWatermark.Quorum quorum =
                new AeronReaderWatermark.Quorum(java.util.Set.of(READER_ONE, READER_TWO))) {
            final AeronReaderWatermark token = AeronReaderWatermark.of(
                    READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100);
            assertThrows(IllegalArgumentException.class,
                    () -> quorum.restore(UUID.randomUUID(), token));
            quorum.retire(READER_ONE);
            assertThrows(IllegalStateException.class, () -> quorum.restore(READER_ONE, token));
            assertDoesNotThrow(() -> quorum.restore(READER_ONE, null));
        }
    }

    /// Verifies closed validators and quorums reject further operations.
    @Test
    void closedValidatorAndQuorumRejectFurtherOperations() {
        final AeronReaderWatermark.Validator validator = new AeronReaderWatermark.Validator();
        final AeronReaderWatermark watermark = AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100);
        validator.accept(watermark);
        validator.close();
        assertThrows(IllegalStateException.class, () -> validator.accept(watermark));

        final AeronReaderWatermark.Quorum quorum =
                new AeronReaderWatermark.Quorum(java.util.Set.of(READER_ONE));
        quorum.accept(AeronReaderWatermark.of(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100));
        quorum.close();

        assertThrows(IllegalStateException.class, quorum::aggregate);
        assertThrows(IllegalStateException.class, quorum::missingReaders);
    }
}
