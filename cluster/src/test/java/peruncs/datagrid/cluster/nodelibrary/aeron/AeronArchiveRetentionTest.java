package peruncs.datagrid.cluster.nodelibrary.aeron;

import io.aeron.archive.client.ArchiveException;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursor;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationLogRetention;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronAuthenticatedWatermark;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies retention authentication before any Archive operation is attempted. */
class AeronArchiveRetentionTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final UUID GENERATION = UUID.randomUUID();
    private static final UUID READER = UUID.randomUUID();
    private static final byte[] SECRET = "retention-test-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8);

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
        return new AeronArchiveRetention(SECRET, Set.of(READER), ensureWriter, unavailableRecording(), () -> 17,
                () -> new AeronWriterBoundary(4, 17, 8_192), ignored -> 0L,
                CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608,
                () -> watermarkDeliveryAvailable, state);
    }

    private static AeronArchiveRetention retention(final Set<UUID> readers, final Path state) {
        return retention(readers, state, () -> {
        });
    }

    private static AeronArchiveRetention retention(final Set<UUID> readers, final Path state,
                                                   final Runnable ensureWriter) {
        return new AeronArchiveRetention(SECRET, readers, ensureWriter, unavailableRecording(), () -> 17,
                () -> new AeronWriterBoundary(4, 17, 8_192), ignored -> 0L,
                CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608,
                () -> true, state);
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
        return new AeronArchiveRetention(SECRET, Set.of(READER), () -> {
        },
                new AeronArchiveRetention.RecordingPositions(
                        ignored -> 0L, ignored -> 16L * 1_024 * 1_024, ignored -> -1L), () -> 17,
                () -> new AeronWriterBoundary(4, 17, 16L * 1_024 * 1_024), purger,
                CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, null);
    }

    private static ReplicationCursor deletionCursor(final long position) {
        return new ReplicationCursor("aeron", GENERATION, 4, new AeronReplicationCursor(
                CLUSTER, UUID.randomUUID(), GENERATION, 1, 17, position, 4).encode());
    }

    private static ReplicationCursor cursor(final UUID reader) {
        return new ReplicationCursor("aeron", GENERATION, 4, AeronAuthenticatedWatermark.sign(
                reader, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET).encode());
    }

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

    @Test
    void disabledRetentionDoesNotReadDormantState() {
        final Path state = Path.of(System.getProperty("java.io.tmpdir"),
                "aeron-retention-disabled-" + UUID.randomUUID() + ".state");
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

    @Test
    void malformedWatermarkIsRejectedBeforeStartingTheWriter() {
        final AtomicBoolean started = new AtomicBoolean();
        final AeronArchiveRetention retention = retention(() -> started.set(true));
        assertThrows(IllegalArgumentException.class, () -> retention.recordReaderWatermark(
                new ReplicationCursor("aeron", GENERATION, 1, new byte[]{1, 2, 3})));
        assertFalse(started.get(), "authentication must precede lazy writer startup");
        retention.close();
    }

    @Test
    void unresolvedWatermarkIsRejectedBeforeStartingTheWriter() {
        final AtomicBoolean started = new AtomicBoolean();
        final AeronArchiveRetention retention = retention(() -> started.set(true));
        final AeronAuthenticatedWatermark unresolved = AeronAuthenticatedWatermark.sign(
                READER, CLUSTER, GENERATION, 1, 17, -1, -1, SECRET);
        assertThrows(SecurityException.class, () -> retention.recordReaderWatermark(unresolved));
        assertFalse(started.get(), "an unresolved watermark must not start lazy writer recovery");
        retention.close();
    }

    @Test
    void authenticatedConfiguredWatermarkCompletesTheQuorum() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
                READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET);
        retention.recordReaderWatermark(new ReplicationCursor("aeron", GENERATION, 4, watermark.encode()));
        assertTrue(retention.isSupported());
        retention.close();
    }

    @Test
    void decodedControlWatermarkCompletesTheQuorumWithoutCursorReencoding() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        retention.recordReaderWatermark(AeronAuthenticatedWatermark.sign(
                READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET));
        assertTrue(retention.isSupported());
        retention.close();
    }

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

    @Test
    void ordinaryBackupCursorCanRequestDeletionAfterAuthenticatedQuorum() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
                READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET);
        retention.recordReaderWatermark(new ReplicationCursor("aeron", GENERATION, 4, watermark.encode()));
        final byte[] ordinaryPosition = new AeronReplicationCursor(
                CLUSTER, UUID.randomUUID(), GENERATION, 1, 17, 4_096, 4).encode();
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> retention.deleteThrough(new ReplicationCursor("aeron", GENERATION, 4, ordinaryPosition)));
        assertEquals("Aeron Archive is not running", failure.getMessage());
        retention.close();
    }

    @Test
    void activeReplayDefersSegmentDeletionWithoutLosingTheQuorum() {
        final long acknowledgedPosition = 9L * 1_024 * 1_024;
        final AeronArchiveRetention retention = retentionWithPurger(ignored -> {
            throw new ArchiveException(
                    "invalid detach: replay in progress - state=ACTIVE", ArchiveException.GENERIC);
        });
        retention.recordReaderWatermark(AeronAuthenticatedWatermark.sign(
                READER, CLUSTER, GENERATION, 1, 17, 4, acknowledgedPosition, SECRET));
        final ReplicationLogRetention.MaintenanceResult result =
                retention.deleteThrough(deletionCursor(acknowledgedPosition));
        assertEquals(ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, result.status());
        assertTrue(retention.isSupported());
        retention.close();
    }

    @Test
    void unrelatedArchiveFailureIsNotMisclassifiedAsAnActiveReplay() {
        final long acknowledgedPosition = 9L * 1_024 * 1_024;
        final AeronArchiveRetention retention = retentionWithPurger(ignored -> {
            throw new ArchiveException(
                    "unrelated Archive failure", ArchiveException.GENERIC);
        });
        retention.recordReaderWatermark(AeronAuthenticatedWatermark.sign(
                READER, CLUSTER, GENERATION, 1, 17, 4, acknowledgedPosition, SECRET));
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> retention.deleteThrough(deletionCursor(acknowledgedPosition)));
        assertInstanceOf(ArchiveException.class, failure.getCause());
        retention.close();
    }

    @Test
    void watermarkAheadOfDurableWriterBoundaryIsRejected() {
        final AeronArchiveRetention retention = retention(() -> {
        });
        final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
                READER, CLUSTER, GENERATION, 1, 17, 5, 4_096, SECRET);
        assertThrows(IllegalStateException.class, () -> retention.recordReaderWatermark(
                new ReplicationCursor("aeron", GENERATION, 5, watermark.encode())));
        assertFalse(retention.isSupported(), "an acknowledgement beyond the writer boundary must not complete quorum");
        retention.close();
    }

    @Test
    void authenticatedReaderProgressSurvivesControllerRestart() throws Exception {
        final Path state = Files.createTempFile("aeron-retention-", ".state");
        try {
            Files.deleteIfExists(state);
            final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
                    READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET);
            final ReplicationCursor cursor = new ReplicationCursor("aeron", GENERATION, 4, watermark.encode());
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

    @Test
    void retiredReaderIsPersistentlyRemovedFromTheQuorum() throws Exception {
        final UUID secondReader = UUID.randomUUID();
        final Path state = Files.createTempFile("aeron-retention-retired-", ".state");
        try {
            Files.deleteIfExists(state);
            final AeronArchiveRetention first = new AeronArchiveRetention(SECRET, Set.of(READER, secondReader),
                    () -> {
                    }, unavailableRecording(), () -> 17, () -> new AeronWriterBoundary(4, 17, 8_192),
                    ignored -> 0L,
                    CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, state);
            first.retireReader(secondReader);
            first.recordReaderWatermark(new ReplicationCursor("aeron", GENERATION, 4,
                    AeronAuthenticatedWatermark.sign(READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET).encode()));
            assertTrue(first.isSupported());
            first.close();

            final AeronArchiveRetention restarted = new AeronArchiveRetention(SECRET, Set.of(READER, secondReader),
                    () -> {
                    }, unavailableRecording(), () -> 17, () -> new AeronWriterBoundary(4, 17, 8_192),
                    ignored -> 0L,
                    CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, state);
            assertTrue(restarted.isSupported());
            assertThrows(SecurityException.class, () -> restarted.recordReaderWatermark(new ReplicationCursor(
                    "aeron", GENERATION, 4, AeronAuthenticatedWatermark.sign(
                    secondReader, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET).encode())));
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

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
            assertThrows(SecurityException.class,
                    () -> restarted.recordReaderWatermark(cursor(retiredReader)));
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

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
            assertTrue(failure.getMessage().contains("cannot load authenticated Aeron retention state"));
            restarted.close();
        } finally {
            Files.deleteIfExists(state);
        }
    }

    @Test
    void malformedRetentionStateDoesNotPartiallyMutateQuorum() throws Exception {
        final UUID secondReader = UUID.randomUUID();
        final Path state = Files.createTempFile("aeron-retention-partial-", ".state");
        try {
            /* The first retirement is valid, but the duplicate second entry makes the
             * file malformed. Loading must be atomic: a retry after the file is fixed
             * must still be able to acknowledge both configured readers. */
            final ByteBuffer encoded = ByteBuffer.allocate(Integer.BYTES * 3 + 32)
                    .putInt(3).putInt(0).putInt(2)
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
