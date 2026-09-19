package peruncs.datagrid.cluster.storage.aeron.writer;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Proves the `ENQUEUE_THEN_ARCHIVE` failure contract and its operational recovery evidence.
///
/// When Aeron preparation fails after the local Store enqueue, the writer must
/// not resume in place: the local write is durable but has no Archive copy,
/// so readers can never replay it. The write call throws a reseed-required
/// failure, the sequence stays consumed, and the checkpoint fence records a
/// non-terminal `COMMITTING_UNCERTAIN` state — the durable evidence restart
/// recovery uses to refuse the sequence until the node is reseeded from a
/// healthy peer or a backup.
class EnqueueThenArchiveRecoveryTest {
    /// Verifies preparation failure after local acceptance leaves uncertain recovery evidence and requires a reseed.
    @Test
    void preparationFailureAfterLocalAcceptanceRequiresReseed() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final List<String> localWrites = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                .build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(),
                (state, sequence, length, chunks, crc, position) -> states.add(state));
        final AeronStorageBinaryReplicationTarget target = AeronStorageBinaryReplicationTarget.New(
                recordingTarget(localWrites), coordinator);
        try {
            final Binary data = ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}));
            /* Fail preparation after the local enqueue completed: this is the
             * transient preparation failure the contract calls out. */
            CrashHook.runWithHook((name, sequence) ->
            {
                if ("AFTER_ENQUEUE_BEFORE_PREPARE".equals(name)) publisher.failClosed();
            }, () ->
            {
                final IllegalStateException failure = assertThrows(IllegalStateException.class,
                        () -> target.write(data),
                        "a local write without an Archive copy must fail reseed-required");
                assertTrue(failure.getMessage().contains("reseed is required"),
                        "unexpected failure: " + failure.getMessage());
            });
            assertEquals(List.of("local"), localWrites,
                    "the local Store write completed before preparation failed");
            assertEquals(
                    List.of(AeronReplicationCheckpoint.State.ENQUEUED,
                            AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN),
                    states,
                    "recovery evidence must be durable and non-terminal so restart refuses the sequence");
            assertEquals(1L, coordinator.nextSequence(),
                    "the accepted sequence must stay consumed, never reused");
            assertFalse(target.isWritable(),
                    "the failed writer must stop instead of resuming in place over a diverged Store");
        } finally {
            coordinator.dispose();
        }
    }

    private static PersistenceTarget<Binary> recordingTarget(final List<String> localWrites) {
        return new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                localWrites.add("local");
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
    }
}
