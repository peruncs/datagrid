package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.NodeException;
import peruncs.datagrid.cluster.node.replication.ReplicationLogRetention;
import peruncs.datagrid.cluster.storage.ReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.binary.ReplicationApplier;

import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the backup manager's stop, durability, retention, and resume protocol.
class StorageBackupManagerTest {
    private static final ReplicationCursor CURSOR =
            new ReplicationCursor("test", null, 7L, "010203");

    private static StorageBackupManager manager(
            final FakeBackend backend,
            final FakeClient client,
            final FakeRetention retention,
            final int maxBackupCount
    ) {
        return manager(backend, client, retention, maxBackupCount, () -> CURSOR);
    }

    private static StorageBackupManager manager(
            final FakeBackend backend,
            final FakeClient client,
            final FakeRetention retention,
            final int maxBackupCount,
            final Supplier<ReplicationCursor> cursor
    ) {
        return StorageBackupManager.create(
                storageConnection(), maxBackupCount, backend, cursor, client, retention);
    }

    private static BackupMetadata backup(final long timestamp, final boolean manualSlot) {
        return new BackupMetadata(timestamp, manualSlot, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN,
                null, UUID.randomUUID(), BackupMetadata.UNKNOWN);
    }

    private static ReplicationCursor aeronCursor(
            final UUID clusterId,
            final UUID generation,
            final long epoch,
            final long recordingId,
            final long sequence
    ) {
        return ReplicationCursor.of("aeron", generation, sequence,
                new AeronReplicationCursor(
                        clusterId, UUID.randomUUID(), generation, epoch, 1L, recordingId, 0L, sequence).encode());
    }

    private static BackupMetadata generationBackup(
            final long timestamp,
            final boolean manualSlot,
            final UUID clusterId,
            final UUID generation,
            final long epoch,
            final long recordingId
    ) {
        return new BackupMetadata(timestamp, manualSlot, clusterId, generation,
                epoch, recordingId, BackupMetadata.UNKNOWN, null, UUID.randomUUID(), BackupMetadata.UNKNOWN);
    }

    /// A typed stub keeps this orchestration test independent of Store implementation details.
    private static StorageConnection storageConnection() {
        return new TestStorageConnection();
    }

    /// Verifies a resolved reader stop creates one backup, prunes to the kept count, runs retention once, and resumes the reader.
    @Test
    void createsPrunesRetainsAndResumesAfterAResolvedStop() {
        final FakeClient client = new FakeClient();
        client.running = true;
        final FakeBackend backend = new FakeBackend();
        backend.backups.addAll(List.of(
                backup(1L, false),
                backup(2L, false),
                backup(3L, false)));
        backend.previousCursor = CURSOR;
        final FakeRetention retention = new FakeRetention();
        retention.results.add(new ReplicationLogRetention.MaintenanceResult(
                ReplicationLogRetention.MaintenanceResult.Status.DELETED, 100L, "deleted"));

        final StorageBackupManager manager = manager(backend, client, retention, 2);

        manager.createStorageBackup(false);

        assertEquals(1, client.stopCalls);
        assertEquals(1, client.resumeCalls);
        assertEquals(1, backend.created.size());
        assertEquals(List.of(1L, 2L), backend.deleted.stream().map(BackupMetadata::timestamp).toList());
        assertEquals(List.of(CURSOR), retention.cursors);
        assertEquals(1, retention.calls);
    }

    /// Verifies deferred log retention retries without repeating the Store backup until deletion succeeds.
    @Test
    void retriesDeferredRetentionWithoutRepeatingTheBackup() {
        final FakeBackend backend = new FakeBackend();
        backend.backups.add(backup(1L, false));
        backend.previousCursor = CURSOR;
        final FakeRetention retention = new FakeRetention();
        retention.results.addAll(List.of(
                new ReplicationLogRetention.MaintenanceResult(
                        ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, 10L, "active"),
                new ReplicationLogRetention.MaintenanceResult(
                        ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, 10L, "active"),
                new ReplicationLogRetention.MaintenanceResult(
                        ReplicationLogRetention.MaintenanceResult.Status.DELETED, 10L, "deleted")));

        final StorageBackupManager manager = manager(backend, new FakeClient(), retention, 1);

        manager.createStorageBackup(false);

        assertEquals(1, backend.created.size());
        assertEquals(3, retention.calls);
    }

    /// Verifies an unresolved reader stop creates no backup and leaves the reader stopped for diagnosis.
    @Test
    void doesNotCreateOrResumeWhenTheReaderStopIsUnresolved() {
        final FakeClient client = new FakeClient();
        client.running = true;
        client.stopOutcome = ReplicationApplier.StopOutcome.TIMED_OUT;
        final FakeBackend backend = new FakeBackend();

        final StorageBackupManager manager = manager(backend, client, new FakeRetention(), 1);

        assertThrows(NodeException.class, () -> manager.createStorageBackup(false));
        assertEquals(1, client.stopCalls);
        assertEquals(0, client.resumeCalls);
        assertTrue(backend.created.isEmpty());
    }

    /// Verifies a failed reader blocks backup creation and surfaces its failure as the cause.
    @Test
    void refusesToCreateAfterAReaderFailure() {
        final FakeClient client = new FakeClient();
        client.failure = new IllegalStateException("reader failed");
        final FakeBackend backend = new FakeBackend();

        final StorageBackupManager manager = manager(backend, client, new FakeRetention(), 1);

        final NodeException failure = assertThrows(
                NodeException.class, () -> manager.createStorageBackup(false));
        assertSame(client.failure, failure.getCause());
        assertEquals(0, client.stopCalls);
        assertTrue(backend.created.isEmpty());
    }

    /// Verifies a backup failure is preserved as the primary error when resuming the reader also fails.
    @Test
    void preservesBackupFailureWhenResumeAlsoFails() {
        final FakeClient client = new FakeClient();
        client.running = true;
        client.resumeFailure = new NodeException("resume failed");
        final FakeBackend backend = new FakeBackend();
        backend.createFailure = new IllegalStateException("backup failed");

        final StorageBackupManager manager = manager(backend, client, new FakeRetention(), 1);

        final IllegalStateException failure = assertThrows(
                IllegalStateException.class, () -> manager.createStorageBackup(false));
        assertEquals("backup failed", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertSame(client.resumeFailure, failure.getSuppressed()[0]);
    }

    /// Verifies a manual backup deletes only the previous manual slot and skips log retention.
    @Test
    void manualBackupDeletesOnlyThePreviousManualSlotAndSkipsRetention() {
        final FakeBackend backend = new FakeBackend();
        backend.backups.addAll(List.of(
                backup(1L, false),
                backup(2L, true)));
        final FakeRetention retention = new FakeRetention();

        final StorageBackupManager manager = manager(backend, new FakeClient(), retention, 1);

        manager.createStorageBackup(true);

        assertEquals(List.of(2L), backend.deleted.stream().map(BackupMetadata::timestamp).toList());
        assertEquals(0, retention.calls);
    }

    /// Verifies automatic pruning spares foreign generations sharing the volume and deletes only the oldest compatible backup.
    @Test
    void pruningSparesForeignGenerations() {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final ReplicationCursor local = aeronCursor(cluster, generation, 5L, 42L, 7L);
        final FakeClient client = new FakeClient();
        final FakeBackend backend = new FakeBackend();
        /* The foreign backup is the oldest, so unfiltered pruning would delete
         * it first; compatible pruning must leave it alone. */
        backend.backups.addAll(List.of(
                generationBackup(0L, false, UUID.randomUUID(), UUID.randomUUID(), 9L, 77L),
                generationBackup(1L, false, cluster, generation, 5L, 42L),
                generationBackup(2L, false, cluster, generation, 5L, 42L)));
        backend.previousCursor = local;
        final FakeRetention retention = new FakeRetention();

        manager(backend, client, retention, 2, () -> local).createStorageBackup(false);

        assertEquals(List.of(1L), backend.deleted.stream().map(BackupMetadata::timestamp).toList());
        assertTrue(backend.backups.stream().anyMatch(b -> b.timestamp() == 0L),
                "a foreign generation sharing the volume must never be pruned");
    }

    /// Verifies manual pruning spares foreign manual slots and deletes only the previous local manual backup.
    @Test
    void manualPruningSparesForeignManualSlots() {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final ReplicationCursor local = aeronCursor(cluster, generation, 5L, 42L, 7L);
        final FakeBackend backend = new FakeBackend();
        backend.backups.addAll(List.of(
                generationBackup(5L, true, UUID.randomUUID(), UUID.randomUUID(), 9L, 77L),
                generationBackup(6L, true, cluster, generation, 5L, 42L),
                generationBackup(7L, false, cluster, generation, 5L, 42L)));

        manager(backend, new FakeClient(), new FakeRetention(), 1, () -> local).createStorageBackup(true);

        assertEquals(List.of(6L), backend.deleted.stream().map(BackupMetadata::timestamp).toList());
        assertTrue(backend.backups.stream().anyMatch(b -> b.timestamp() == 5L),
                "a foreign manual backup must never be pruned");
    }

    /// Verifies log retention uses the cursor of the newest compatible backup rather than the newest backup overall.
    @Test
    void retentionUsesTheNewestCompatibleCursor() {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final ReplicationCursor local = aeronCursor(cluster, generation, 5L, 42L, 7L);
        final ReplicationCursor compatibleCursor = aeronCursor(cluster, generation, 5L, 42L, 6L);
        final ReplicationCursor foreignCursor = aeronCursor(UUID.randomUUID(), UUID.randomUUID(), 9L, 77L, 11L);
        final FakeBackend backend = new FakeBackend();
        final BackupMetadata compatible = generationBackup(1L, false, cluster, generation, 5L, 42L);
        final BackupMetadata foreign = generationBackup(9L, false, UUID.randomUUID(), UUID.randomUUID(), 9L, 77L);
        backend.backups.addAll(List.of(compatible, foreign));
        /* The legacy newest-overall path would resolve the foreign cursor. */
        backend.previousCursor = foreignCursor;
        backend.cursorForBackup = candidate ->
                candidate.backupId().equals(compatible.backupId()) ? compatibleCursor : foreignCursor;
        final FakeRetention retention = new FakeRetention();

        manager(backend, new FakeClient(), retention, 10, () -> local).createStorageBackup(false);

        /* The retention cursor must come from the compatibility-selected
         * backup, never from the newest backup overall: exactly one
         * getCursorForBackup call for the compatible candidate. */
        assertEquals(1, backend.previousCalls, "retention reads the cursor of its selected backup exactly once");
        assertEquals(List.of(compatibleCursor), retention.cursors);
    }

    /// Verifies the stored manifest captures the cursor read after the reader-stop boundary, not the pre-stop position.
    @Test
    void manifestCursorIsCapturedAfterTheStopBoundary() {
        final ReplicationCursor before = new ReplicationCursor("test", null, 7L, "010203");
        final ReplicationCursor stopped = new ReplicationCursor("test", null, 99L, "040506");
        final FakeClient client = new FakeClient();
        client.running = true;
        final FakeBackend backend = new FakeBackend();
        final FakeRetention retention = new FakeRetention();

        manager(backend, client, retention, 1, () -> client.stopCalls == 0 ? before : stopped)
                .createStorageBackup(false);

        assertEquals(List.of(stopped), backend.createdCursors,
                "the stored manifest must describe the stopped boundary, not the pre-stop position");
    }

    /// Verifies the maintenance path reports unreadable archives without failing the backup.
    @Test
    void reportsUnreadableArchivesDuringMaintenance() {
        final FakeBackend backend = new FakeBackend();
        backend.unreadable = List.of("broken.identity.zip");
        final StorageBackupManager manager = manager(backend, new FakeClient(), new FakeRetention(), 1);

        assertDoesNotThrow(() -> manager.createStorageBackup(false));
        assertEquals(1, backend.unreadableScans,
                "the prune path must sweep archives that can never be selected");
        assertNull(manager.maintenanceFailure());
    }

    /// Verifies a prune failure after a durable publication does not fail the backup,
    /// is exposed as maintenance failure, and does not skip retention.
    @Test
    void pruneFailureDoesNotFailTheBackupAndRetentionStillRuns() {
        final FakeClient client = new FakeClient();
        client.running = true;
        final FakeBackend backend = new FakeBackend();
        backend.backups.addAll(List.of(backup(1L, false), backup(2L, false)));
        backend.previousCursor = CURSOR;
        backend.deleteFailure = new NodeException("prune failed");
        final FakeRetention retention = new FakeRetention();

        final StorageBackupManager manager = manager(backend, client, retention, 1);

        assertDoesNotThrow(() -> manager.createStorageBackup(false));
        assertEquals(1, backend.created.size(), "the durable backup must be reported as successful");
        assertSame(backend.deleteFailure, manager.maintenanceFailure());
        assertEquals(1, retention.calls, "a prune failure must not skip retention");
        assertEquals(1, client.resumeCalls);
    }

    /// Verifies a retention failure after a durable publication does not fail the
    /// backup and is exposed as a distinct maintenance failure.
    @Test
    void retentionFailureDoesNotFailTheBackup() {
        final FakeBackend backend = new FakeBackend();
        backend.backups.add(backup(1L, false));
        backend.previousCursor = CURSOR;
        final FakeRetention retention = new FakeRetention();
        retention.failure = new NodeException("retention failed");

        final StorageBackupManager manager = manager(backend, new FakeClient(), retention, 2);

        assertDoesNotThrow(() -> manager.createStorageBackup(false));
        assertSame(retention.failure, manager.maintenanceFailure());
    }

    /// Verifies a later clean maintenance phase clears the exposed failure.
    @Test
    void successfulMaintenanceClearsTheObservable() {
        final FakeBackend backend = new FakeBackend();
        backend.backups.add(backup(1L, false));
        backend.previousCursor = CURSOR;
        final FakeRetention retention = new FakeRetention();
        retention.failure = new NodeException("retention failed");
        final StorageBackupManager manager = manager(backend, new FakeClient(), retention, 2);

        assertDoesNotThrow(() -> manager.createStorageBackup(false));
        assertNotNull(manager.maintenanceFailure());

        retention.failure = null;
        assertDoesNotThrow(() -> manager.createStorageBackup(false));
        assertNull(manager.maintenanceFailure());
    }

    /// Verifies an interrupted reader-stop poll raises a domain failure instead of
    /// leaking an interrupted-sleep wrapper.
    @Test
    void interruptedReaderStopRaisesADomainFailure() {
        final FakeClient client = new FakeClient();
        client.running = true;
        client.stopOutcome = ReplicationApplier.StopOutcome.STOPPING;
        final FakeBackend backend = new FakeBackend();
        final StorageBackupManager manager = manager(backend, client, new FakeRetention(), 1);

        try {
            Thread.currentThread().interrupt();
            final NodeException failure = assertThrows(
                    NodeException.class, () -> manager.createStorageBackup(false));
            assertTrue(failure.getMessage().contains("Interrupted"), failure.getMessage());
        } finally {
            Thread.interrupted();
        }
        assertTrue(backend.created.isEmpty());
        assertEquals(0, client.resumeCalls);
    }

    private static final class FakeClient implements ReplicationApplier {
        private boolean running;
        private RuntimeException failure;
        private RuntimeException resumeFailure;
        private ReplicationApplier.StopOutcome stopOutcome =
                ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY;
        private int stopCalls;
        private int resumeCalls;

        @Override
        public void start() {
            this.running = true;
        }

        @Override
        public void stopAtLatestMessage() {
            this.stopCalls++;
            this.running = false;
        }

        @Override
        public ReplicationCursor cursor() {
            return CURSOR;
        }

        @Override
        public boolean isRunning() {
            return this.running;
        }

        @Override
        public RuntimeException failure() {
            return this.failure;
        }

        @Override
        public StopResult stopResult() {
            return new StopResult(this.stopOutcome, CURSOR.logicalSequence(), 42L);
        }

        @Override
        public void resume() {
            this.resumeCalls++;
            if (this.resumeFailure != null) throw this.resumeFailure;
            this.running = true;
        }

        @Override
        public void dispose() {
            this.running = false;
        }
    }

    private static final class FakeBackend implements StorageBackupBackend {
        private final List<BackupMetadata> backups = new ArrayList<>();
        private final List<BackupMetadata> created = new ArrayList<>();
        private final List<ReplicationCursor> createdCursors = new ArrayList<>();
        private final List<BackupMetadata> deleted = new ArrayList<>();
        private ReplicationCursor previousCursor;
        private Function<BackupMetadata, ReplicationCursor> cursorForBackup;
        private int previousCalls;
        private RuntimeException createFailure;
        private RuntimeException deleteFailure;
        private List<String> unreadable = List.of();
        private int unreadableScans;

        @Override
        public List<BackupMetadata> listBackups() {
            return List.copyOf(this.backups);
        }

        @Override
        public List<String> listUnreadableArchives() {
            this.unreadableScans++;
            return this.unreadable;
        }

        @Override
        public ReplicationCursor getCursorForBackup(final BackupMetadata backup) {
            this.previousCalls++;
            return this.cursorForBackup == null ? this.previousCursor : this.cursorForBackup.apply(backup);
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) {
            if (this.deleteFailure != null) throw this.deleteFailure;
            this.deleted.add(backup);
            this.backups.remove(backup);
        }

        @Override
        public void createBackup(
                final StorageConnection connection,
                final ReplicationCursor cursor,
                final BackupMetadata backup
        ) {
            if (this.createFailure != null) throw this.createFailure;
            this.created.add(backup);
            this.createdCursors.add(cursor);
            this.backups.add(backup);
        }

        @Override
        public void restoreBackup(final Path destination, final BackupMetadata backup) {
        }

        @Override
        public boolean hasUserUploadedStorage() {
            return false;
        }

        @Override
        public void restoreUserUploadedStorage(final Path destination) {
        }

        @Override
        public void deleteUserUploadedStorage() {
        }
    }

    private static final class FakeRetention implements ReplicationLogRetention {
        private final Queue<MaintenanceResult> results = new ArrayDeque<>();
        private final List<ReplicationCursor> cursors = new ArrayList<>();
        private RuntimeException failure;
        private int calls;

        @Override
        public MaintenanceResult deleteThrough(final ReplicationCursor cursor) {
            if (this.failure != null) throw this.failure;
            this.calls++;
            this.cursors.add(cursor);
            return this.results.isEmpty()
                    ? new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, -1L, "none")
                    : this.results.remove();
        }

        @Override
        public void close() {
        }
    }
}
