package peruncs.cluster.storage.aeron.writer;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.crashtest.CrashBarrier;
import peruncs.cluster.storage.aeron.crashtest.CrashPoint;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.io.FaultInjection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


/// Verifies the writer's terminal-state rules at injected crash boundaries.
class AeronCrashBoundaryTest {
    private final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
            .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).offerTimeoutNanos(5_000_000L).build();

    /// Verifies a recorded abort leaves a low-level publisher usable.
    @Test
    void recordedAbortLeavesPublisherUsable() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationPublisher publisher = publisher(kinds);
        try (CrashBarrier barrier = new CrashBarrier(CrashPoint.AFTER_DATA_CHUNKS, true, 1_000_000_000L)) {
            FaultInjection.runWithHook(barrier::reached, () -> assertThrows(WriteRejectedException.class,
                    () -> publisher.prepareTransaction(
                            null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1, 2, 3})})));
        }
        assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.ABORT), kinds);
        assertDoesNotThrow(() -> publisher.publishTransaction(
                null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{4})}));
        assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.ABORT,
                AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.COMMIT), kinds);
        publisher.close();
    }

    /// Verifies ambiguous commit fails closed without publishing a second terminal marker.
    @Test
    void ambiguousCommitFailsClosedWithoutPublishingASecondTerminalMarker() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationPublisher publisher = publisher(kinds);
        try (CrashBarrier barrier = new CrashBarrier(CrashPoint.AFTER_COMMIT_OFFER, true, 1_000_000_000L)) {
            FaultInjection.runWithHook(barrier::reached, () -> assertThrows(CrashBarrier.SimulatedCrash.class,
                    () -> publisher.publishTransaction(
                            null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{9})})));
        }
        assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.COMMIT), kinds);
        assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
                null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{8})}));
        publisher.close();
    }
    private AeronReplicationPublisher publisher(final List<AeronReplicationEnvelope.Kind> kinds) {
        return AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
    }
}
