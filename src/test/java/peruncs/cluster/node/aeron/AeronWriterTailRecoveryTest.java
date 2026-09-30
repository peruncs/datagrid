package peruncs.cluster.node.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope.EnvelopeView;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Covers the writer's bounded tail decisions independently of Archive replay.
class AeronWriterTailRecoveryTest {
    private static final UUID CLUSTER = UUID.fromString("47bd9301-a38e-450c-8414-4730d9928d48");
    private static final UUID OTHER_CLUSTER = UUID.fromString("824cd053-b098-4bec-83f7-e977a0ce3093");
    private static final long EPOCH = 3L;
    private static final long FENCING_TOKEN = 5L;
    private static final long WIRE_NONCE = AeronReplicationEnvelope.defaultWireNonce(CLUSTER);
    private static final byte[] PAYLOAD = {4, 8, 15, 16, 23, 42};

    @Test
    void recoveryTailMustStayInsideTheRecordingAndTheComputedWindow() {
        assertThrows(ReseedRequiredException.class, () ->
                AeronWriterTailRecovery.validateRecoveryBounds(7L, 99L, 100L, 100L, 1_024, 512));

        final long windowEnd = AeronWriterTailRecovery.validateRecoveryBounds(
                7L, 100L, 100L, 100L, 1_024, 512);
        assertThrows(ReseedRequiredException.class, () ->
                AeronWriterTailRecovery.validateRecoveryBounds(
                        7L, 100L, 100L, windowEnd + 1L, 1_024, 512));
        assertThrows(ReseedRequiredException.class, () ->
                AeronWriterTailRecovery.validateRecoveryBounds(
                        7L, Long.MAX_VALUE - 1L, 0L, Long.MAX_VALUE - 1L, 1_024, 512));
    }

    @Test
    void missingMarkedCommitIsPlannedBeforeAdmissionResumes() {
        final AeronWriterTailRecovery.Scan scan = scan(7L);
        prepare(scan, 7L, 150L);

        final AeronWriterTailRecovery.Result result = scan.result(100L);

        assertEquals(8L, result.nextSequence());
        assertEquals(7L, result.markedCommit().sequence());
        assertNull(result.nextAbort());
        assertEquals(-1L, result.boundaryPosition());
    }

    @Test
    void completeMarkedCommitProvidesTheExistingBoundary() {
        final AeronWriterTailRecovery.Scan scan = scan(7L);
        prepare(scan, 7L, 150L);
        commit(scan, 7L, 200L);

        final AeronWriterTailRecovery.Result result = scan.result(100L);

        assertEquals(8L, result.nextSequence());
        assertNull(result.markedCommit());
        assertEquals(200L, result.boundaryPosition());
    }

    @Test
    void incompleteNextPrepareGetsAnAbortAndConsumesItsSequence() {
        final AeronWriterTailRecovery.Scan scan = scan(7L);
        prepare(scan, 7L, 150L);
        commit(scan, 7L, 200L);
        accept(scan, CLUSTER, EPOCH, FENCING_TOKEN, 8L,
                AeronReplicationEnvelope.Kind.STORE_BINARY, PAYLOAD.length, 0, 2, 0, 0,
                new byte[]{PAYLOAD[0]}, 250L);

        final AeronWriterTailRecovery.Result result = scan.result(100L);

        assertEquals(9L, result.nextSequence());
        assertEquals(8L, result.nextAbort().sequence());
        assertEquals(AeronReplicationEnvelope.Kind.ABORT, result.nextAbort().kind());
    }

    @Test
    void existingAbortForNextSequenceNeedsNoNewMarker() {
        final AeronWriterTailRecovery.Scan scan = scan(7L);
        prepare(scan, 7L, 150L);
        commit(scan, 7L, 200L);
        prepare(scan, 8L, 250L);
        abort(scan, 8L, 300L);

        final AeronWriterTailRecovery.Result result = scan.result(100L);

        assertEquals(9L, result.nextSequence());
        assertNull(result.nextAbort());
        assertEquals(8L, result.boundarySequence());
        assertEquals(300L, result.boundaryPosition());
    }

    @Test
    void markedPrepareMustBeCompleteAndCannotBeAborted() {
        final AeronWriterTailRecovery.Scan missingData = scan(7L);
        assertThrows(ReseedRequiredException.class, () -> missingData.result(100L));

        final AeronWriterTailRecovery.Scan abortedMark = scan(7L);
        prepare(abortedMark, 7L, 150L);
        abort(abortedMark, 7L, 200L);
        assertThrows(ReseedRequiredException.class, () -> abortedMark.result(100L));
    }

    @Test
    void aLaterPrepareCannotFollowAnUncommittedMarkedPrepare() {
        final AeronWriterTailRecovery.Scan scan = scan(7L);
        prepare(scan, 7L, 150L);
        prepare(scan, 8L, 250L);

        assertThrows(ReseedRequiredException.class, () -> scan.result(100L));
    }

    @Test
    void commitForAnUncommittedNextSequenceRequiresReseed() {
        final AeronWriterTailRecovery.Scan scan = scan(7L);
        prepare(scan, 7L, 150L);
        commit(scan, 7L, 200L);
        prepare(scan, 8L, 250L);
        commit(scan, 8L, 300L);

        assertThrows(ReseedRequiredException.class, () -> scan.result(100L));
    }

    @Test
    void laterSequencesAndMismatchedIdentityRequireReseed() {
        final AeronWriterTailRecovery.Scan laterSequence = scan(7L);
        assertThrows(ReseedRequiredException.class,
                () -> prepare(laterSequence, 9L, 150L));

        final AeronWriterTailRecovery.Scan wrongCluster = scan(7L);
        assertThrows(ReseedRequiredException.class, () -> accept(wrongCluster, OTHER_CLUSTER, EPOCH,
                FENCING_TOKEN, 7L, AeronReplicationEnvelope.Kind.STORE_BINARY,
                PAYLOAD.length, 0, 1, 0, 0, PAYLOAD, 150L));

        final AeronWriterTailRecovery.Scan newerFence = scan(7L);
        assertThrows(ReseedRequiredException.class, () -> accept(newerFence, CLUSTER, EPOCH,
                FENCING_TOKEN + 1, 7L, AeronReplicationEnvelope.Kind.STORE_BINARY,
                PAYLOAD.length, 0, 1, 0, 0, PAYLOAD, 150L));

        final AeronWriterTailRecovery.Scan wrongEpoch = scan(7L);
        assertThrows(ReseedRequiredException.class, () -> accept(wrongEpoch, CLUSTER, EPOCH + 1,
                FENCING_TOKEN, 7L, AeronReplicationEnvelope.Kind.STORE_BINARY,
                PAYLOAD.length, 0, 1, 0, 0, PAYLOAD, 150L));
    }

    private static AeronWriterTailRecovery.Scan scan(final long markSequence) {
        final ReplicationMark mark = new ReplicationMark(CLUSTER, UUID.randomUUID(), EPOCH, 12L);
        mark.sequence = markSequence;
        mark.prepareStartPosition = 100L;
        return new AeronWriterTailRecovery.Scan(mark, CLUSTER, EPOCH, WIRE_NONCE, FENCING_TOKEN, 1_024);
    }

    private static void prepare(final AeronWriterTailRecovery.Scan scan, final long sequence, final long position) {
        accept(scan, CLUSTER, EPOCH, FENCING_TOKEN, sequence, AeronReplicationEnvelope.Kind.STORE_BINARY,
                PAYLOAD.length, 0, 1, 0, 0, PAYLOAD, position);
    }

    private static void commit(final AeronWriterTailRecovery.Scan scan, final long sequence, final long position) {
        accept(scan, CLUSTER, EPOCH, FENCING_TOKEN, sequence, AeronReplicationEnvelope.Kind.COMMIT,
                PAYLOAD.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(PAYLOAD), new byte[0], position);
    }

    private static void abort(final AeronWriterTailRecovery.Scan scan, final long sequence, final long position) {
        accept(scan, CLUSTER, EPOCH, FENCING_TOKEN, sequence, AeronReplicationEnvelope.Kind.ABORT,
                PAYLOAD.length, 0, 1, 0, 0, new byte[0], position);
    }

    private static void accept(final AeronWriterTailRecovery.Scan scan, final UUID clusterId, final long epoch,
                               final long fencingToken, final long sequence,
                               final AeronReplicationEnvelope.Kind kind, final int payloadLength,
                               final int chunkIndex, final int chunkCount, final int chunkOffset,
                               final int commitCrc32c, final byte[] payload, final long position) {
        final UnsafeBuffer source = new UnsafeBuffer(payload);
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[AeronReplicationEnvelope.HEADER_LENGTH + payload.length]);
        final int length = AeronReplicationEnvelope.encode(frame, 0, clusterId, epoch, fencingToken,
                AeronReplicationEnvelope.defaultWireNonce(clusterId), sequence, kind, payloadLength,
                chunkIndex, chunkCount, chunkOffset, commitCrc32c, source, 0, payload.length,
                new AeronReplicationEnvelope.ChecksumContext());
        final EnvelopeView decoded = AeronReplicationEnvelope.decodeView(frame, 0, length, new EnvelopeView());
        scan.accept(frame, decoded, position);
    }
}
