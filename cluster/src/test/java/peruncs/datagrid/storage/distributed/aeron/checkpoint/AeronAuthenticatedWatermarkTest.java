package peruncs.datagrid.storage.distributed.aeron.checkpoint;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Tests authentication, encoding, and monotonicity of retention watermarks. */
class AeronAuthenticatedWatermarkTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final UUID GENERATION = UUID.randomUUID();
    private static final UUID READER_ONE = UUID.randomUUID();
    private static final UUID READER_TWO = UUID.randomUUID();
    private static final byte[] SECRET = "test-only-retention-secret".getBytes(StandardCharsets.UTF_8);

    @Test
    void signedWatermarkRoundTripsAndRejectsTampering() {
        final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096, SECRET);
        final AeronAuthenticatedWatermark decoded = AeronAuthenticatedWatermark.decode(watermark.encode());
        assertTrue(decoded.verify(SECRET));
        assertArrayEquals(watermark.authentication(), decoded.authentication());

        final byte[] encoded = watermark.encode();
        encoded[encoded.length - 1] ^= 1;
        assertFalse(AeronAuthenticatedWatermark.decode(encoded).verify(SECRET));
    }

    @Test
    void directlyEncodedSignatureRoundTripsAndVerifies() {
        final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.decode(
                AeronAuthenticatedWatermark.signEncoded(
                        READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096, SECRET));
        assertTrue(watermark.verify(SECRET));
        assertEquals(42, watermark.sequence());
        assertEquals(4_096, watermark.position());
    }

    @Test
    void directBufferDecodePreservesEveryIdentityField() {
        final byte[] encoded = AeronAuthenticatedWatermark.signEncoded(
                READER_ONE, CLUSTER, GENERATION, 9, 31, 42, 4_096, SECRET);
        final byte[] framed = new byte[encoded.length + 7];
        System.arraycopy(encoded, 0, framed, 7, encoded.length);
        final AeronAuthenticatedWatermark decoded = AeronAuthenticatedWatermark.decode(
                new UnsafeBuffer(framed), 7, encoded.length);
        assertEquals(READER_ONE, decoded.readerId());
        assertEquals(CLUSTER, decoded.clusterId());
        assertEquals(GENERATION, decoded.storeGeneration());
        assertEquals(9, decoded.writerEpoch());
        assertEquals(31, decoded.recordingId());
        assertEquals(42, decoded.sequence());
        assertEquals(4_096, decoded.position());
        assertTrue(decoded.verify(SECRET));
    }

    @Test
    void directEncodingIntoReusableBufferMatchesAllocatedEncoding() {
        final byte[] expected = AeronAuthenticatedWatermark.signEncoded(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096, SECRET);
        final byte[] actual = new byte[expected.length];
        AeronAuthenticatedWatermark.signEncodedInto(actual, READER_ONE, CLUSTER, GENERATION,
                3, 17, 42, 4_096, SECRET);
        assertArrayEquals(expected, actual);
    }

    @Test
    void authenticationAccessorCannotMutateSignedWatermark() {
        final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096, SECRET);
        final byte[] leaked = watermark.authentication();
        leaked[0] ^= 1;
        assertTrue(watermark.verify(SECRET));
    }

    @Test
    void validatorRejectsRollbackAndIdentityConfusion() {
        final AeronAuthenticatedWatermark.Validator validator = new AeronAuthenticatedWatermark.Validator(SECRET);
        validator.accept(AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET));
        validator.accept(AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET));
        assertThrows(IllegalStateException.class, () -> validator.accept(
                AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 101, SECRET)));
        validator.accept(AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 2, 200, SECRET));
        assertThrows(IllegalStateException.class, () -> validator.accept(
                AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 3, 199, SECRET)));
        assertThrows(IllegalStateException.class, () -> validator.accept(
                AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 99, SECRET)));
        assertThrows(IllegalStateException.class, () -> validator.accept(
                AeronAuthenticatedWatermark.sign(READER_ONE, UUID.randomUUID(), GENERATION, 3, 17, 3, 300, SECRET)));
    }

    /** Sequence values must remain incrementable by the writer and validator. */
    @Test
    void rejectsSequenceThatWouldOverflowNextReservation() {
        assertThrows(IllegalArgumentException.class, () -> new AeronAuthenticatedWatermark(
                READER_ONE, CLUSTER, GENERATION, 3, 17, Long.MAX_VALUE, 100, new byte[32]));
    }

    @Test
    void rejectsInvalidSignedProgressBeforeAllocatingAuthentication() {
        assertThrows(IllegalArgumentException.class, () -> AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, -1, 17, 1, 100, SECRET));
        assertThrows(IllegalArgumentException.class, () -> AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, -1, 1, 100, SECRET));
        assertThrows(IllegalArgumentException.class, () -> AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, -2, 100, SECRET));
        assertThrows(IllegalArgumentException.class, () -> AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, -2, SECRET));
    }

    @Test
    void rejectsWeakValidatorSecret() {
        assertThrows(IllegalArgumentException.class,
                () -> new AeronAuthenticatedWatermark.Validator(new byte[15]));
    }

    @Test
    void quorumRequiresEveryReaderAndAggregatesLeastProgress() {
        final AeronAuthenticatedWatermark.Quorum quorum =
                new AeronAuthenticatedWatermark.Quorum(java.util.Set.of(READER_ONE, READER_TWO), SECRET);
        quorum.accept(AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 8, 800, SECRET));
        assertThrows(IllegalStateException.class, quorum::aggregate);
        assertEquals(java.util.Set.of(READER_TWO), quorum.missingReaders());
        quorum.accept(AeronAuthenticatedWatermark.sign(READER_TWO, CLUSTER, GENERATION, 3, 17, 7, 700, SECRET));
        final AeronAuthenticatedWatermark aggregate = quorum.aggregate();
        assertEquals(7, aggregate.sequence());
        assertEquals(700, aggregate.position());
        assertTrue(quorum.missingReaders().isEmpty());
        assertThrows(SecurityException.class, () -> quorum.accept(
                AeronAuthenticatedWatermark.sign(UUID.randomUUID(), CLUSTER, GENERATION, 3, 17, 9, 900, SECRET)));
    }

    @Test
    void aggregateRejectsDuplicateReaderIdentity() {
        final AeronAuthenticatedWatermark first = AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET);
        final AeronAuthenticatedWatermark second = AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 2, 200, SECRET);
        assertThrows(IllegalArgumentException.class,
                () -> AeronAuthenticatedWatermark.aggregate(java.util.List.of(first, second), SECRET));
    }

    @Test
    void validatorRejectsRestoringAnIdentityMismatchedToken() {
        final AeronAuthenticatedWatermark.Validator validator =
                new AeronAuthenticatedWatermark.Validator(SECRET);
        final AeronAuthenticatedWatermark token = AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET);
        assertThrows(SecurityException.class, () -> validator.restore(READER_TWO, token));
    }

    @Test
    void quorumRejectsRestoringUnknownOrRetiredReaders() {
        final AeronAuthenticatedWatermark.Quorum quorum =
                new AeronAuthenticatedWatermark.Quorum(java.util.Set.of(READER_ONE, READER_TWO), SECRET);
        final AeronAuthenticatedWatermark token = AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET);
        assertThrows(IllegalArgumentException.class,
                () -> quorum.restore(UUID.randomUUID(), token));
        quorum.retire(READER_ONE);
        assertThrows(IllegalStateException.class, () -> quorum.restore(READER_ONE, token));
        assertDoesNotThrow(() -> quorum.restore(READER_ONE, null));
    }

    @Test
    void validatorSecretIsErasedWhenItsOwnerCloses() {
        final AeronAuthenticatedWatermark.Validator validator =
                new AeronAuthenticatedWatermark.Validator(SECRET);
        final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET);
        validator.accept(watermark);
        validator.clearSecret();
        assertThrows(IllegalStateException.class, () -> validator.accept(watermark),
                "a closed validator must no longer retain the HMAC key");
    }

    @Test
    void quorumCannotAggregateAfterItsSecretIsErased() {
        final AeronAuthenticatedWatermark.Quorum quorum =
                new AeronAuthenticatedWatermark.Quorum(java.util.Set.of(READER_ONE), SECRET);
        quorum.accept(AeronAuthenticatedWatermark.sign(
                READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET));
        quorum.close();

        assertThrows(IllegalStateException.class, quorum::aggregate);
        assertThrows(IllegalStateException.class, quorum::missingReaders);
    }
}
