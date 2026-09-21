package peruncs.datagrid.cluster.node.aeron;

import io.aeron.archive.client.ArchiveException;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.replication.ReplicationLogRetention;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReaderWatermark;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the retention quorum before any Archive operation is attempted.
class AeronArchiveRetentionTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final UUID GENERATION = UUID.randomUUID();
    private static final UUID READER = UUID.randomUUID();

    private static AeronArchiveRetention retention(final Runnable ensureWriter) {
        return retention(ensureWriter, null, true);
    }

    private static AeronArchiveRetention retention(final Runnable ensureWriter, final Path state) {
        return retention(ensureWriter, state, true);
    }

    private static AeronArchiveRetention retention(final Runnable ensureWriter, final boolean watermarkDeliveryAvailable) {
        return retention(ensureWriter, null, watermarkDeliveryAvailable);
    }

    private static AeronArchiveRetention retention(final Runnable ensureWriter, final Path state,
                                                   final boolean watermarkDeliveryAvailable) {
        return new AeronArchiveRetention(Set.of(READER), ensureWriter, unavailableRecording(), () -> 17,
                () -> new AeronWriterBoundary(4, 17, 8_192), ignored -> 0L,
                CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608,
                () -> watermarkDeliveryAvailable, state, AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }

    private static AeronArchiveRetention retention(final Set<UUID> readers, final Path state) {
        return retention(readers, state, () -> {
        });
    }

    private static AeronArchiveRetention retention(final Set<UUID> readers, final Path state,
                                                   final Runnable ensureWriter) {
        return new AeronArchiveRetention(readers, ensureWriter, unavailableRecording(), () -> 17,
                () -> new AeronWriterBoundary(4, 17, 8_192), ignored -> 0L,
                CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608,
                () -> true, state, AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }

    private static AeronArchiveRetention.RecordingPositions unavailableRecording() {
        return new AeronArchiveRetention.RecordingPositions(
                ignored -> {
                    throw new IllegalStateException("Aeron Archive is not running");
                },
                ignored -> {
                    throw new IllegalStateException("Aeron Archive is not running");
                },
                ignored -> {
                    throw new IllegalStateException("Aeron Archive is not running");
                });
    }

    private static AeronArchiveRetention retentionWithPurger(final java.util.function.LongUnaryOperator purger) {
        return new AeronArchiveRetention(Set.of(READER), () -> {
        },
                new AeronArchiveRetention.RecordingPositions(
                        ignored -> 0L, ignored -> 16L * 1_024 * 1_024, ignored -> -1L), () -> 17,
                () -> new AeronWriterBoundary(4, 17, 16L * 1_024 * 1_024), purger,
                CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, null, AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }

    /* A live writer behind its terminal checkpoint: the Archive has durably
     * recorded up to `recordedPosition` while the checkpoint file still names
     * sequence 4 at position 8192, as happens between the commit's Archive
     * acknowledgement and the checkpoint fsync on a continuously appending
     * writer. */
    private static AeronArchiveRetention retentionWithRecordedPosition(final long recordedPosition) {
        return retentionWithRecordedPosition(recordedPosition, ignored -> 0L);
    }

    private static AeronArchiveRetention retentionWithRecordedPosition(
            final long recordedPosition, final java.util.function.LongUnaryOperator purger) {
        return new AeronArchiveRetention(Set.of(READER), () -> {
        },
                new AeronArchiveRetention.RecordingPositions(
                        ignored -> 0L, ignored -> -1L, ignored -> recordedPosition), () -> 17,
                () -> new AeronWriterBoundary(4, 17, 8_192), purger,
                CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, null,
                AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }

    private static ReplicationCursor deletionCursor(final long position) {
        return ReplicationCursor.of("aeron", GENERATION, 4, new AeronReplicationCursor(
                CLUSTER, UUID.randomUUID(), GENERATION, 1, 3, 17, position, 4).encode());
    }

    private static ReplicationCursor cursor(final UUID reader) {
        return ReplicationCursor.of("aeron", GENERATION, 4, AeronReaderWatermark.of(
                reader, CLUSTER, GENERATION, 1, 17, 4, 4_096).encode());
    }

    /// Verifies retention without watermark delivery stays unsupported and rejects retirement without starting the writer.
    @Test
    void retentionIsUnsupportedWhenWatermarkDeliveryIsNotAvailable() {
        final AeronArchiveRetention retention = retention(
                () -> {
                    throw new AssertionError("writer must not start");
                }, false);
        assertFalse(retention.isSupported());
        assertThrows(UnsupportedOperationException.class, () -> retention.retireReader(READER));
        retention.close();
    }

    /// Verifies disabled retention ignores dormant state on disk and rejects watermarks without starting the writer.
    @Test
    void disabledRetentionDoesNotReadDormantState() {
        final Path state = Path.of(System.getProperty("java.io.tmpdir"),
                "aeron-retention-disabled-%s.state".formatted(UUID.randomUUID()));
        try {
            assertDoesNotThrow(() -> Files.write(state, new byte[]{0x01, 0x02, 0x03}));
            final AeronArchiveRetention retention = retention(
                    () -> {
                        throw new AssertionError("disabled retention must not start the writer");
                    }, state, false);
            assertFalse(retention.isSupported());
            assertThrows(UnsupportedOperationException.class,
                    () -> retention.recordReaderWatermark(cursor(READER)));
            retention.close();
        } finally {
            assertDoesNotThrow(() -> Files.deleteIfExists(state));
        }
    }

    /// Verifies a malformed watermark is rejected before lazy writer startup runs.
    @Test
    void malformedWatermarkIsRejectedBeforeStartingTheWriter() {
        final AtomicBoolean started = new AtomicBoolean();
        final AeronArchiveRetention retention = retention(() -> started.set(true));
        assertThrows(IllegalArgumentException.class, () -> retention.recordReaderWatermark(
                new ReplicationCursor("aeron", GENERATION, 1, "010203")));
        assertFalse(started.get(), "identity checks must precede lazy writer startup");
        retention.close();
    }

    /// Verifies an unresolved watermark is rejected before lazy writer recovery starts.
    @Test
    void unresolvedWatermarkIsRejectedBeforeStartingTheWriter() {
        final AtomicBoolean started = new AtomicBoolean();
        final AeronArchiveRetention retention = retention(() -> started.set(true));
        final AeronReaderWatermark unresolved = AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, -1, -1);
        assertThrows(IllegalArgumentException.class, () -> retention.recordReaderWatermark(unresolved));
        assertFalse(started.get(), "an unresolved watermark must not start lazy writer recovery");
        retention.close();
    }

    /// Verifies a configured reader watermark completes the retention quorum.
    @Test
    void configuredWatermarkCompletesTheQuorum() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        final AeronReaderWatermark watermark = AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 4, 4_096);
        retention.recordReaderWatermark(ReplicationCursor.of("aeron", GENERATION, 4, watermark.encode()));
        assertTrue(retention.isSupported());
        retention.close();
    }

    /// Verifies a decoded control watermark completes the quorum without cursor re-encoding.
    @Test
    void decodedControlWatermarkCompletesTheQuorumWithoutCursorReencoding() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        retention.recordReaderWatermark(AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 4, 4_096));
        assertTrue(retention.isSupported());
        retention.close();
    }

    /// Verifies a writer startup failure propagates and leaves the quorum uncompleted.
    @Test
    void writerStartupFailureDoesNotAdvanceTheQuorum() {
        final AeronArchiveRetention retention = retention(
                () -> {
                    throw new IllegalStateException("writer unavailable");
                });
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> retention.recordReaderWatermark(cursor(READER)));
        assertEquals("writer unavailable", failure.getMessage());
        assertFalse(retention.isSupported(),
                "a watermark rejected before writer validation must not complete the quorum");
        retention.close();
    }

    /// Verifies a deletion request after quorum reaches the Archive and surfaces its unavailability.
    @Test
    void ordinaryBackupCursorCanRequestDeletionAfterQuorum() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        final AeronReaderWatermark watermark = AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 4, 4_096);
        retention.recordReaderWatermark(ReplicationCursor.of("aeron", GENERATION, 4, watermark.encode()));
        final byte[] ordinaryPosition = new AeronReplicationCursor(
                CLUSTER, UUID.randomUUID(), GENERATION, 1, 3, 17, 4_096, 4).encode();
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> retention.deleteThrough(ReplicationCursor.of("aeron", GENERATION, 4, ordinaryPosition)));
        assertEquals("Aeron Archive is not running", failure.getMessage());
        retention.close();
    }

    /// Verifies an active replay defers segment deletion while preserving the assembled quorum.
    @Test
    void activeReplayDefersSegmentDeletionWithoutLosingTheQuorum() {
        final long acknowledgedPosition = 9L * 1_024 * 1_024;
        final AeronArchiveRetention retention = retentionWithPurger(ignored -> {
            throw new ArchiveException(
                    "invalid detach: replay in progress - state=ACTIVE", ArchiveException.GENERIC);
        });
        retention.recordReaderWatermark(AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 4, acknowledgedPosition));
        final ReplicationLogRetention.MaintenanceResult result =
                retention.deleteThrough(deletionCursor(acknowledgedPosition));
        assertEquals(ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, result.status());
        assertTrue(retention.isSupported());
        retention.close();
    }

    /// Verifies an unrelated Archive failure propagates with its cause instead of deferring as an active replay.
    @Test
    void unrelatedArchiveFailureIsNotMisclassifiedAsAnActiveReplay() {
        final long acknowledgedPosition = 9L * 1_024 * 1_024;
        final AeronArchiveRetention retention = retentionWithPurger(ignored -> {
            throw new ArchiveException(
                    "unrelated Archive failure", ArchiveException.GENERIC);
        });
        retention.recordReaderWatermark(AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 4, acknowledgedPosition));
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> retention.deleteThrough(deletionCursor(acknowledgedPosition)));
        assertInstanceOf(ArchiveException.class, failure.getCause());
        retention.close();
    }

    /// Reproduces the soak finding: a truthful watermark that resolves a commit
    /// from the live stream while the writer is still between the commit's
    /// Archive acknowledgement and the checkpoint fsync names a sequence and
    /// position the terminal checkpoint has not reached yet. It is admissible
    /// because every byte it claims is already durably recorded in the Archive.
    @Test
    void watermarkWithinRecordedPositionAheadOfTerminalCheckpointIsAccepted() {
        final AeronArchiveRetention retention = retentionWithRecordedPosition(16L * 1_024 * 1_024);
        retention.recordReaderWatermark(AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 5, 12_288));
        assertTrue(retention.isSupported(),
                "a watermark covered by the recorded Archive position must assemble the quorum");
        retention.close();
    }

    /// Verifies retention through a boundary beyond the terminal checkpoint is
    /// authorized by the recorded Archive position and purges full segments.
    @Test
    void deletionAheadOfTerminalCheckpointUsesTheRecordedPosition() {
        final long purged = 8L * 1_024 * 1_024;
        final AtomicReference<Long> purgedAt = new AtomicReference<>();
        final AeronArchiveRetention retention = retentionWithRecordedPosition(16L * 1_024 * 1_024, boundary -> {
            purgedAt.set(boundary);
            return 0L;
        });
        retention.recordReaderWatermark(AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 5, 9L * 1_024 * 1_024));
        final byte[] requestedPosition = new AeronReplicationCursor(
                CLUSTER, UUID.randomUUID(), GENERATION, 1, 3, 17, 9L * 1_024 * 1_024, 5).encode();
        final ReplicationLogRetention.MaintenanceResult result =
                retention.deleteThrough(ReplicationCursor.of("aeron", GENERATION, 5, requestedPosition));
        assertEquals(ReplicationLogRetention.MaintenanceResult.Status.DELETED, result.status(),
                "the quorum minimum position 9MiB crosses exactly one complete 8MiB segment");
        assertEquals(purged, purgedAt.get(),
                "the purge must run through the segment boundary, not the raw watermark position");
        retention.close();
    }

    /// Verifies a fabricated future sequence without new Archive bytes stays rejected:
    /// progress past the terminal checkpoint must occupy bytes beyond it.
    @Test
    void fabricatedFutureSequenceWithoutNewBytesIsRejected() {
        final AeronArchiveRetention retention = retentionWithRecordedPosition(16L * 1_024 * 1_024);
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> retention.recordReaderWatermark(AeronReaderWatermark.of(
                        READER, CLUSTER, GENERATION, 1, 17, 5, 4_096)));
        assertEquals("reader watermark is ahead of the durable writer boundary", failure.getMessage());
        assertFalse(retention.isSupported());
        retention.close();
    }

    /// Verifies a watermark beyond the durably recorded Archive position stays rejected:
    /// no commit marker can be durable at a position the Archive has not recorded.
    @Test
    void watermarkBeyondRecordedPositionIsRejected() {
        final AeronArchiveRetention retention = retentionWithRecordedPosition(9_728);
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> retention.recordReaderWatermark(AeronReaderWatermark.of(
                        READER, CLUSTER, GENERATION, 1, 17, 5, 16L * 1_024 * 1_024)));
        assertEquals("reader watermark is ahead of the durable writer boundary", failure.getMessage());
        assertFalse(retention.isSupported());
        retention.close();
    }

    /// Verifies a watermark ahead of the durable writer boundary is rejected and leaves the quorum incomplete.
    @Test
    void watermarkAheadOfDurableWriterBoundaryIsRejected() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        final AeronReaderWatermark watermark = AeronReaderWatermark.of(
                READER, CLUSTER, GENERATION, 1, 17, 5, 4_096);
        assertThrows(IllegalStateException.class, () -> retention.recordReaderWatermark(
                ReplicationCursor.of("aeron", GENERATION, 5, watermark.encode())));
        assertFalse(retention.isSupported(), "an acknowledgement beyond the writer boundary must not complete quorum");
        retention.close();
    }

    /// Verifies recorded reader progress survives a controller restart through persisted state.
    @Test
    void readerProgressSurvivesControllerRestart() throws Exception {
        final Path state = Files.createTempFile("aeron-retention-", ".state");
        try {
            Files.deleteIfExists(state);
            final AeronReaderWatermark watermark = AeronReaderWatermark.of(
                    READER, CLUSTER, GENERATION, 1, 17, 4, 4_096);
            final ReplicationCursor cursor = ReplicationCursor.of("aeron", GENERATION, 4, watermark.encode());
            final AeronArchiveRetention first = retention(() -> {
            }, state);
            first.recordReaderWatermark(cursor);
            first.close();
            final AeronArchiveRetention restarted = retention(() -> {
            }, state);
            assertTrue(restarted.isSupported());
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

    /// Verifies an obsolete development retention state version is rejected with an informative cause.
    @Test
    void rejectsDevelopmentRetentionStateFormats() throws Exception {
        final Path state = Files.createTempFile("aeron-retention-obsolete-", ".state");
        try {
            Files.write(state, ByteBuffer.allocate(Integer.BYTES * 3)
                    .putInt(2).putInt(0).putInt(0).array());
            final AeronArchiveRetention retention = retention(() -> {
            }, state);
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    retention::isSupported);
            assertInstanceOf(java.io.IOException.class, failure.getCause());
            assertEquals("unsupported retention state version", failure.getCause().getMessage());
            retention.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

    /// Verifies a symlinked retention state file is rejected instead of being followed.
    @Test
    void rejectsSymbolicLinkRetentionState() throws Exception {
        final Path directory = Files.createTempDirectory("aeron-retention-symlink-");
        final Path target = directory.resolve("outside.state");
        final Path state = directory.resolve("state");
        try {
            final AeronArchiveRetention first = retention(() -> {
            }, target);
            first.recordReaderWatermark(cursor(READER));
            first.close();
            try {
                Files.createSymbolicLink(state, target.getFileName());
            } catch (final UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
                return; // Symbolic links are unavailable on some supported filesystems.
            }
            final AeronArchiveRetention restarted = retention(() -> {
            }, state);
            assertThrows(IllegalStateException.class, restarted::isSupported);
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
            Files.deleteIfExists(target);
            Files.deleteIfExists(directory);
        }
    }

    /// Verifies a retired reader stays removed from the quorum across a restart and its watermarks are rejected.
    @Test
    void retiredReaderIsPersistentlyRemovedFromTheQuorum() throws Exception {
        final UUID secondReader = UUID.randomUUID();
        final Path state = Files.createTempFile("aeron-retention-retired-", ".state");
        try {
            Files.deleteIfExists(state);
            final AeronArchiveRetention first = new AeronArchiveRetention(Set.of(READER, secondReader),
                    () -> {
                    }, unavailableRecording(), () -> 17, () -> new AeronWriterBoundary(4, 17, 8_192),
                    ignored -> 0L,
                    CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, state, AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
            first.retireReader(secondReader);
            first.recordReaderWatermark(ReplicationCursor.of("aeron", GENERATION, 4,
                    AeronReaderWatermark.of(READER, CLUSTER, GENERATION, 1, 17, 4, 4_096).encode()));
            assertTrue(first.isSupported());
            first.close();

            final AeronArchiveRetention restarted = new AeronArchiveRetention(Set.of(READER, secondReader),
                    () -> {
                    }, unavailableRecording(), () -> 17, () -> new AeronWriterBoundary(4, 17, 8_192),
                    ignored -> 0L,
                    CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, state, AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
            assertTrue(restarted.isSupported());
            assertThrows(IllegalArgumentException.class, () -> restarted.recordReaderWatermark(ReplicationCursor.of(
                    "aeron", GENERATION, 4, AeronReaderWatermark.of(
                    secondReader, CLUSTER, GENERATION, 1, 17, 4, 4_096).encode())));
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

    /// Verifies a reported reader can be retired and the retirement survives a state reload.
    @Test
    void reportedReaderCanBeRetiredAndStateReloaded() throws Exception {
        final UUID retiredReader = UUID.randomUUID();
        final Path state = Files.createTempFile("aeron-retention-reported-retired-", ".state");
        try {
            Files.deleteIfExists(state);
            final Set<UUID> readers = Set.of(READER, retiredReader);
            final AeronArchiveRetention first = retention(readers, state);
            first.recordReaderWatermark(cursor(retiredReader));
            first.retireReader(retiredReader);
            first.recordReaderWatermark(cursor(READER));
            assertTrue(first.isSupported());
            first.close();

            final AeronArchiveRetention restarted = retention(readers, state);
            assertTrue(restarted.isSupported());
            assertThrows(IllegalArgumentException.class,
                    () -> restarted.recordReaderWatermark(cursor(retiredReader)));
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

    /// Verifies a persisted watermark for a reader outside the current configuration fails reload with an actionable error.
    @Test
    void rejectsPersistedWatermarkForReaderOutsideCurrentConfiguration() throws Exception {
        final Path state = Files.createTempFile("aeron-retention-unconfigured-", ".state");
        final UUID replacementReader = UUID.randomUUID();
        try {
            Files.deleteIfExists(state);
            final AeronArchiveRetention first = retention(Set.of(READER), state);
            first.recordReaderWatermark(cursor(READER));
            first.close();

            final AeronArchiveRetention restarted = retention(Set.of(replacementReader), state);
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    restarted::isSupported);
            assertTrue(failure.getMessage().contains("cannot load Aeron retention state"));
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

    /// Verifies a malformed retention state load is atomic and leaves the in-memory quorum unmutated for retry.
    @Test
    void malformedRetentionStateDoesNotPartiallyMutateQuorum() throws Exception {
        final UUID secondReader = UUID.randomUUID();
        final Path state = Files.createTempFile("aeron-retention-partial-", ".state");
        try {
            /* The first retirement is valid, but the duplicate second entry makes the
             * file malformed. Loading must be atomic: a retry after the file is fixed
             * must still be able to acknowledge both configured readers. */
            final ByteBuffer encoded = ByteBuffer.allocate(Integer.BYTES * 3 + 32)
                    .putInt(4).putInt(0).putInt(2)
                    .putLong(secondReader.getMostSignificantBits()).putLong(secondReader.getLeastSignificantBits())
                    .putLong(secondReader.getMostSignificantBits()).putLong(secondReader.getLeastSignificantBits());
            Files.write(state, encoded.array());
            final Set<UUID> readers = Set.of(READER, secondReader);
            final AeronArchiveRetention retention = retention(readers, state);
            assertThrows(IllegalStateException.class, retention::isSupported);
            Files.delete(state);
            retention.recordReaderWatermark(cursor(READER));
            retention.recordReaderWatermark(cursor(secondReader));
            assertTrue(retention.isSupported(), "failed restore must not retire a configured reader in memory");
            retention.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

    /// Verifies a watermark persistence failure rolls back the quorum until the retry succeeds.
    @Test
    void watermarkPersistenceFailureRollsBackTheQuorum() throws Exception {
        final Path directory = Files.createTempDirectory("aeron-retention-rollback-");
        final Path state = directory.resolve("state");
        try (final AeronArchiveRetention retention = retention(() -> {
        }, state)) {
            Files.createDirectory(state);
            assertThrows(IllegalStateException.class,
                    () -> retention.recordReaderWatermark(cursor(READER)));
            Files.delete(state);
            assertFalse(retention.isSupported(),
                    "an acknowledgement that was not persisted must not complete the quorum");
            retention.recordReaderWatermark(cursor(READER));
            assertTrue(retention.isSupported());
        } finally {
            Files.deleteIfExists(state);
            Files.deleteIfExists(directory);
        }
    }

    /// Verifies a queued command wait stays bounded with a timeout when the agent thread is stuck.
    @Test
    void queuedCommandWaitIsBoundedWhenTheAgentIsStuck() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try (final AeronArchiveRetention retention = new AeronArchiveRetention(Set.of(READER), () -> {
            entered.countDown();
            /* Hold the single agent thread until released, ignoring the
             * interrupt from the timed-out caller's cancel: if this command
             * died on cancel, the agent would go free and the bounded wait
             * under test could return instead of timing out, flipping the
             * test on scheduling luck. The overall 30-second bound still
             * fails a genuinely stuck release. */
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            boolean released = false;
            while (!released && System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
                try {
                    released = release.await(100, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            assertTrue(released, "ensureWriter was never released");
        },
                unavailableRecording(), () -> 17, () -> new AeronWriterBoundary(4, 17, 8_192),
                ignored -> 0L, CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608,
                () -> true, null, 60_000L)) {
            final AtomicReference<Throwable> background = new AtomicReference<>();
            final Thread stuck = Thread.ofVirtual().start(() -> {
                try {
                    retention.recordReaderWatermark(cursor(READER));
                } catch (final Throwable failure) {
                    background.set(failure);
                }
            });
            /* The stuck command must outlive the assertion below: sharing the
             * retention operation timeout between the stuck command and the
             * bounded wait races the two identical deadlines against each
             * other, so the wait is bounded on its own calling thread while
             * the retention timeout comfortably exceeds the whole test. */
            final java.util.concurrent.ExecutorService caller =
                    java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                assertTrue(entered.await(30, TimeUnit.SECONDS), "agent did not start the blocking command");
                final java.util.concurrent.Future<Boolean> waiting = caller.submit(retention::isSupported);
                final long start = System.nanoTime();
                assertThrows(TimeoutException.class, () -> waiting.get(300, TimeUnit.MILLISECONDS));
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertTrue(elapsedMillis < 10_000L,
                        "retention wait was not bounded: %s ms".formatted(elapsedMillis));
            } finally {
                release.countDown();
                stuck.join(30_000L);
                assertFalse(stuck.isAlive(), "background command did not finish after release");
                caller.shutdownNow();
            }
            assertNull(background.get(), "unexpected background failure " + background.get());
        }
    }

    /// Verifies an interrupted close stays retryable until agent termination and quorum cleanup complete.
    @Test
    void interruptedCloseStaysRetryableUntilQuorumCleanup() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AeronArchiveRetention retention = retention(() -> {
            entered.countDown();
            /* Ignore close-path shutdownNow interrupts like an uninterruptible
             * Archive call would: this command must outlive the first close
             * attempt and finish only on release. */
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            boolean released = false;
            while (!released && System.nanoTime() < deadline) {
                try {
                    released = release.await(100, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.interrupted();
                }
            }
        });
        final AtomicReference<Throwable> background = new AtomicReference<>();
        final Thread stuck = Thread.ofVirtual().start(() -> {
            try {
                retention.recordReaderWatermark(cursor(READER));
            } catch (final Throwable failure) {
                background.set(failure);
            }
        });
        try {
            assertTrue(entered.await(30, TimeUnit.SECONDS), "agent did not start the blocking command");
            /* An agent command that outlives the first close attempt: interrupt
             * the closer mid-termination so quorum cleanup is skipped. */
            final Thread closing = Thread.ofVirtual().start(retention::close);
            Thread.sleep(500);
            closing.interrupt();
            closing.join(30_000L);
            assertFalse(closing.isAlive(), "interrupted close did not return");
            assertFalse(retentionAgent(retention).isTerminated(),
                    "interrupted close must not have finished termination");
            release.countDown();
            stuck.join(30_000L);
            assertFalse(stuck.isAlive(), "agent command did not finish after release");
            /* The retry must finish termination and quorum cleanup; a close
             * that returns early here leaves the agent executor running. */
            retention.close();
            assertTrue(retentionAgent(retention).isTerminated(),
                    "retryable close did not terminate the agent");
            assertThrows(IllegalStateException.class,
                    () -> retention.recordReaderWatermark(cursor(READER)),
                    "closed retention kept accepting commands");
            retention.close();
            assertTrue(retentionAgent(retention).isTerminated(),
                    "completed close was not idempotent");
        } finally {
            release.countDown();
            retention.close();
        }
        assertNull(background.get(), "unexpected background failure " + background.get());
    }

    private static java.util.concurrent.ExecutorService retentionAgent(
            final AeronArchiveRetention retention) throws Exception {
        /* White-box lifecycle probe: termination has no public observable —
         * post-shutdown submissions are rejected whether the executor is idle
         * or terminated — so the test reads the agent directly. */
        final java.lang.reflect.Field agent = AeronArchiveRetention.class.getDeclaredField("agent");
        agent.setAccessible(true);
        return (java.util.concurrent.ExecutorService) agent.get(retention);
    }

    /// Verifies a retirement persistence failure reinstates the reader so the quorum still requires it.
    @Test
    void retirementPersistenceFailureReinstatesTheReader() throws Exception {
        final UUID secondReader = UUID.randomUUID();
        final Path directory = Files.createTempDirectory("aeron-retirement-rollback-");
        final Path state = directory.resolve("state");
        try (final AeronArchiveRetention retention = retention(Set.of(READER, secondReader), state)) {
            Files.createDirectory(state);
            assertThrows(IllegalStateException.class, () -> retention.retireReader(secondReader));
            Files.delete(state);
            retention.recordReaderWatermark(cursor(READER));
            assertFalse(retention.isSupported(),
                    "failed retirement persistence must leave the reader in the quorum");
            retention.recordReaderWatermark(cursor(secondReader));
            assertTrue(retention.isSupported());
        } finally {
            Files.deleteIfExists(state);
            Files.deleteIfExists(directory);
        }
    }
}
