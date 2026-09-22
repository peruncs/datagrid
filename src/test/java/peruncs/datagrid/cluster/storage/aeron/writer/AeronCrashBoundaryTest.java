package peruncs.datagrid.cluster.storage.aeron.writer;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.crashtest.CrashBarrier;
import peruncs.datagrid.cluster.storage.aeron.crashtest.CrashPoint;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the writer's terminal-state rules at injected crash boundaries.
class AeronCrashBoundaryTest {
    private final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
            .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).offerTimeoutNanos(5_000_000L).build();

    /// Verifies prepared tail failure always publishes abort and fails closed.
    @Test
    void preparedTailFailureAlwaysPublishesAbortAndFailsClosed() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationPublisher publisher = publisher(kinds);
        try (CrashBarrier barrier = new CrashBarrier(CrashPoint.AFTER_DATA_CHUNKS, true, 1_000_000_000L)) {
            CrashHook.runWithHook(barrier::reached, () -> assertThrows(CrashBarrier.SimulatedCrash.class,
                    () -> publisher.prepareTransaction(
                            null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1, 2, 3})})));
        }
        assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.ABORT), kinds);
        assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
                null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{4})}));
        publisher.close();
    }

    /// Verifies ambiguous commit fails closed without publishing a second terminal marker.
    @Test
    void ambiguousCommitFailsClosedWithoutPublishingASecondTerminalMarker() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationPublisher publisher = publisher(kinds);
        try (CrashBarrier barrier = new CrashBarrier(CrashPoint.AFTER_COMMIT_OFFER, true, 1_000_000_000L)) {
            CrashHook.runWithHook(barrier::reached, () -> assertThrows(CrashBarrier.SimulatedCrash.class,
                    () -> publisher.publishTransaction(
                            null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{9})})));
        }
        assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.COMMIT), kinds);
        assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
                null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{8})}));
        publisher.close();
    }

    /// Verifies a recorded commit without a terminal checkpoint retains the refusal fence.
    @Test
    void recordedCommitBeforeCheckpointIsConvertedToUncertainty() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AeronReplicationPublisher publisher = publisher(new ArrayList<>());
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
        try {
            CrashHook.runWithHook((name, ignored) -> {
                if ("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT".equals(name)) {
                    throw new CrashBarrier.SimulatedCrash(
                            CrashPoint.AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT, ignored);
                }
            }, () -> assertThrows(CrashBarrier.SimulatedCrash.class, () -> coordinator.distributeData(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{5})))));
            assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
                    AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
        } finally {
            coordinator.dispose();
        }
    }

    private AeronReplicationPublisher publisher(final List<AeronReplicationEnvelope.Kind> kinds) {
        return AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
    }
}
