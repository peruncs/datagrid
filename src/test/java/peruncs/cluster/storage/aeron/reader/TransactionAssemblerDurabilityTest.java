package peruncs.cluster.storage.aeron.reader;

import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelopeTestSupport;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the live durability gate: a terminal marker observed on the live
/// publication runs ahead of the Archive recording, so the assembler must
/// withhold it — never stage, never advance the cursor — until the recorded
/// position covers the marker, and must fail closed when the recording stalls
/// beyond the reader stop budget.
class TransactionAssemblerDurabilityTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final long EPOCH = 17;

    private static final class RecordingReceiver implements StorageBinaryDataReceiver {
        private Binary last;
        private String dictionary;

        @Override
        public void receiveData(final Binary value) {
            this.last = value;
        }

        @Override
        public void receiveTypeDictionary(final String value) {
            this.dictionary = value;
        }

        @Override
        public void awaitApplied() {
        }
    }

    private static TransactionAssembler assembler(final AtomicLong recordedPosition) {
        return assembler(recordedPosition, 5_000_000_000L);
    }

    private static TransactionAssembler assembler(final AtomicLong recordedPosition,
                                                  final long readerStopTimeoutNanos) {
        return TransactionAssemblerTestSupport.New(
                AeronReplicationConfiguration.builder()
                        .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024)
                        .readerBarrierMaxTransactions(1)
                        .readerStopTimeoutNanos(readerStopTimeoutNanos)
                        .build(),
                CLUSTER, EPOCH, -1L, new RecordingReceiver(), () -> {
                }, null, required -> recordedPosition.get() >= required);
    }

    /// Builds a live-image header whose position (end of its frame) is
    /// `position`; the value must be frame-aligned.
    private static Header liveHeader(final long position) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, 0, DataHeaderFlyweight.HEADER_LENGTH);
        flyweight.frameLength((int) position);
        final Header header = new Header(0, 16);
        header.buffer(buffer);
        header.offset(0);
        assertEquals(position, header.position(), "test header position wiring");
        return header;
    }

    private static byte[] data(final long sequence, final int length) {
        final byte[] data = new byte[length];
        java.util.Arrays.fill(data, (byte) sequence);
        return data;
    }

    private static byte[] dataFrame(final long sequence, final int length) {
        return AeronReplicationEnvelopeTestSupport.encode(CLUSTER, EPOCH, 1L, sequence,
                AeronReplicationEnvelope.Kind.STORE_BINARY, length, 0, 1, 0, 0, data(sequence, length));
    }

    private static byte[] commitFrame(final long sequence, final int dataLength) {
        return AeronReplicationEnvelopeTestSupport.encode(CLUSTER, EPOCH, 1L, sequence,
                AeronReplicationEnvelope.Kind.COMMIT, dataLength, 0, 1, 0,
                AeronReplicationEnvelope.crc32c(data(sequence, dataLength)), new byte[0]);
    }

    /// A live COMMIT whose recording position is still behind the publication
    /// must not be delivered: the data stays buffered, the cursor stays put,
    /// and a later redelivery applies once the Archive confirms coverage.
    @Test
    void liveCommitIsWithheldUntilRecorded() {
        final AtomicLong recorded = new AtomicLong(60L);
        final TransactionAssembler assembler = assembler(recorded);
        try {
            final long dataEnd = 64L;
            final long commitEnd = 128L;
            /* Data chunks are never gated: they buffer without a terminal
             * marker and mutate nothing outside the incomplete transaction. */
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(0L, 4)), 0,
                    dataFrame(0L, 4).length, liveHeader(dataEnd), true),
                    "data frames must pass the live gate");
            /* The recording stops short of the COMMIT: live delivery of the
             * marker would apply a transaction absent from durable history. */
            assertTrue(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(commitEnd), true),
                    "an unrecorded live COMMIT must be withheld");
            assertEquals(-1L, assembler.lastResolvedSequence(), "withheld for redelivery, not resolved");
            assertTrue(assembler.cursorSnapshot().position() < commitEnd);
            assertEquals(0, assembler.unflushedDeliveryCount(), "nothing may be staged yet");

            /* The Archive catches up; the redelivered COMMIT now applies. */
            recorded.set(commitEnd);
            assertFalse(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(commitEnd), true),
                    "once recorded, the redelivered COMMIT applies");
            assertTrue(assembler.unflushedDeliveryCount() > 0, "the transaction must be staged");
            assembler.flushDeliveries();
            assertEquals(0L, assembler.lastResolvedSequence());
            assertEquals(commitEnd, assembler.cursorSnapshot().position(),
                    "the resolved cursor names the position after the commit frame");
        } finally {
            assembler.dispose();
        }
    }

    /// A recording that never covers the live COMMIT fails the assembler
    /// closed within the reader stop budget instead of applying or parking.
    @Test
    void stalledRecordingFailsClosedInsteadOfApplying() throws Exception {
        final AtomicLong recorded = new AtomicLong(60L);
        final TransactionAssembler assembler = assembler(recorded, 40_000_000L);
        try {
            final long dataEnd = 64L;
            final long commitEnd = 128L;
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(0L, 4)), 0,
                    dataFrame(0L, 4).length, liveHeader(dataEnd), true));
            assertTrue(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(commitEnd), true));
            assertNull(assembler.failure(), "a short stall is withheld, not failed");
            Thread.sleep(80L);
            assertThrows(ReplicationUnavailableException.class,
                    () -> assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                            commitFrame(0L, 4).length, liveHeader(commitEnd), true),
                    "a recording stall beyond the budget must fail closed");
            assertNotNull(assembler.failure());
            assertEquals(-1L, assembler.lastResolvedSequence(),
                    "the failed assembler never carried the transaction");
        } finally {
            assembler.dispose();
        }
    }

    /// Replay-sourced frames are recorded by definition: the gate is not
    /// consulted even when the supplier could not confirm coverage.
    @Test
    void replayBypassesTheDurabilityGate() {
        final AtomicLong recorded = new AtomicLong(0L);
        final TransactionAssembler assembler = assembler(recorded);
        try {
            final long dataEnd = 64L;
            final long commitEnd = 128L;
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(0L, 4)), 0,
                    dataFrame(0L, 4).length, liveHeader(dataEnd), false),
                    "replayed data needs no recording proof");
            assertFalse(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(commitEnd), false),
                    "replayed commit needs no recording proof");
            assembler.flushDeliveries();
            assertEquals(0L, assembler.lastResolvedSequence());
        } finally {
            assembler.dispose();
        }
    }

    /// A lost Archive control channel is not a durability verdict: the
    /// marker stays withheld on the stall budget instead of latching a
    /// terminal reader failure, and delivery resumes once queries succeed.
    @Test
    void archiveControlFailureWithholdsWithoutLatching() {
        final AtomicLong recorded = new AtomicLong(60L);
        final AtomicLong outages = new AtomicLong(3L);
        final TransactionAssembler assembler = TransactionAssemblerTestSupport.New(
                AeronReplicationConfiguration.builder()
                        .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024)
                        .readerBarrierMaxTransactions(1)
                        .readerStopTimeoutNanos(5_000_000_000L)
                        .build(),
                CLUSTER, EPOCH, -1L, new RecordingReceiver(), () -> {
                }, null, required ->
                {
                    if (outages.getAndDecrement() > 0L) {
                        throw new io.aeron.archive.client.ArchiveException("control channel lost");
                    }
                    return recorded.get() >= required;
                });
        try {
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(0L, 4)), 0,
                    dataFrame(0L, 4).length, liveHeader(64L), true));
            /* Three control-channel outages withhold the marker without
             * latching any failure; the fourth poll confirms coverage and
             * applies it. */
            for (int attempt = 0; attempt < 3; attempt++) {
                assertTrue(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                        commitFrame(0L, 4).length, liveHeader(128L), true),
                        "control failure must withhold, not fail");
                assertNull(assembler.failure(),
                        "a transient control-channel loss must not latch a terminal failure");
            }
            recorded.set(128L);
            assertFalse(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(128L), true),
                    "once queries succeed again, the covered marker applies");
            assembler.flushDeliveries();
            assertEquals(0L, assembler.lastResolvedSequence());
            assertNull(assembler.failure());
        } finally {
            assembler.dispose();
        }
    }

    /// A live terminal marker with no header cannot prove its durability
    /// position: fail closed instead of silently bypassing the gate.
    @Test
    void liveTerminalMarkerWithoutHeaderFailsClosed() {
        final AtomicLong recorded = new AtomicLong(Long.MAX_VALUE);
        final TransactionAssembler assembler = assembler(recorded);
        try {
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(0L, 4)), 0,
                    dataFrame(0L, 4).length, liveHeader(64L), true));
            assertThrows(IllegalStateException.class,
                    () -> assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                            commitFrame(0L, 4).length, null, true),
                    "a headerless live COMMIT must not slip past the gate");
            assertNotNull(assembler.failure(), "the bypass attempt must latch a failure");
        } finally {
            assembler.dispose();
        }
    }

    /// A genuinely durable commit at max recorded position applies on a
    /// directly answerable max-recorded query even when nothing is active.
    @Test
    void maxRecordedPositionCoversStoppedRecording() {
        /* A stopped recording reports its recorded maximum, not -1: the gate
         * treats durably recorded markers as covered. */
        final AtomicLong recorded = new AtomicLong(128L);
        final TransactionAssembler assembler = assembler(recorded);
        try {
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(0L, 4)), 0,
                    dataFrame(0L, 4).length, liveHeader(64L), true));
            assertFalse(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(128L), true));
            assembler.flushDeliveries();
            assertEquals(0L, assembler.lastResolvedSequence());
        } finally {
            assembler.dispose();
        }
    }

    /// A fresh stall window opens per withheld marker: an admitted commit
    /// resets the budget so one slow recording cannot poison later commits.
    @Test
    void admittedCommitResetsTheStallBudget() {
        final AtomicLong recorded = new AtomicLong(60L);
        final TransactionAssembler assembler = assembler(recorded);
        try {
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(0L, 4)), 0,
                    dataFrame(0L, 4).length, liveHeader(64L), true));
            assertTrue(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(128L), true));
            recorded.set(128L);
            assertFalse(assembler.onFragment(new UnsafeBuffer(commitFrame(0L, 4)), 0,
                    commitFrame(0L, 4).length, liveHeader(128L), true));
            assembler.flushDeliveries();

            /* The next transaction stalls again from a fresh budget. */
            assertFalse(assembler.onFragment(new UnsafeBuffer(dataFrame(1L, 4)), 0,
                    dataFrame(1L, 4).length, liveHeader(192L), true));
            assertTrue(assembler.onFragment(new UnsafeBuffer(commitFrame(1L, 4)), 0,
                    commitFrame(1L, 4).length, liveHeader(256L), true),
                    "a new stall must be withheld from a fresh budget");
            assertEquals(0L, assembler.lastResolvedSequence(),
                    "the resolved boundary stays at the last covered commit");
            recorded.set(256L);
            assertFalse(assembler.onFragment(new UnsafeBuffer(commitFrame(1L, 4)), 0,
                    commitFrame(1L, 4).length, liveHeader(256L), true));
            assembler.flushDeliveries();
            assertEquals(1L, assembler.lastResolvedSequence());
        } finally {
            assembler.dispose();
        }
    }
}
