package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.node.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.ReplicationCursorStore;
import peruncs.datagrid.cluster.node.store.StorageFileOperations;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FilesystemVolumeBackupBackendTest {
    private static void createBackupShape(
            final Path root,
            final boolean storage,
            final boolean manifest,
            final boolean ready
    ) throws Exception {
        if (storage) Files.createDirectories(root.resolve("storage"));
        if (manifest) Files.write(root.resolve("manifest"), ReplicationCursorStore.encode(
                new ReplicationCursor("none", null, 0L, new byte[0])));
        if (ready) Files.writeString(root.resolve("ready"), "");
    }

        /// The manager passes the connection through; this test only verifies marker ordering.
    private static StorageConnection noOpStorageConnection() {
        return (StorageConnection) Proxy.newProxyInstance(
                StorageConnection.class.getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, arguments) -> null);
    }

    @Test
    void listsOnlyBackupsWithDurableReadyMarker(@TempDir final Path backupVolume) throws Exception {
        final Path complete = backupVolume.resolve("100");
        createBackupShape(complete, true, true, true);

        final Path incomplete = backupVolume.resolve("200");
        createBackupShape(incomplete, true, true, false);

        final Path manual = backupVolume.resolve("300.manual");
        createBackupShape(manual, true, true, true);

        /* A crashed copy and an operator upload must not be parsed as generated
         * backups.  The latter is intentionally handled by its separate API. */
        createBackupShape(backupVolume.resolve("400"), true, false, true);
        Files.createDirectories(backupVolume.resolve("user-uploaded-storage"));
        Files.createDirectories(backupVolume.resolve("not-a-backup"));

        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(
                backupVolume);

        final List<BackupMetadata> backups = backend.listBackups();

        assertEquals(List.of(new BackupMetadata(100L, false), new BackupMetadata(300L, true)), backups);
        assertEquals(new BackupMetadata(300L, true), backend.getLastBackup(0).orElseThrow());
        assertEquals(new BackupMetadata(100L, false), backend.getLastBackup(1).orElseThrow());
    }

    @Test
    void readsManifestAndDownloadsIntoNewDestination(@TempDir final Path backupVolume, @TempDir final Path root)
            throws Exception {
        final Path complete = backupVolume.resolve("10");
        createBackupShape(complete, true, true, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(
                backupVolume);

        assertEquals(0L, backend.getCursorFromPreviousBackup(0).orElseThrow().logicalSequence());
        final Path destination = root.resolve("new");
        backend.downloadLatestBackup(destination);
        assertTrue(Files.isDirectory(destination.resolve("storage")));
        assertFalse(Files.exists(destination.resolve("manifest")));
        assertFalse(Files.exists(destination.resolve("ready")));
        assertThrows(NodelibraryException.class, () -> backend.downloadLatestBackup(destination));
    }

    @Test
    void listsByNumericTimestampAndDeleteIsIdempotent(@TempDir final Path backupVolume) throws Exception {
        createBackupShape(backupVolume.resolve("9"), true, true, true);
        createBackupShape(backupVolume.resolve("10"), true, true, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(
                backupVolume);

        assertEquals(List.of(new BackupMetadata(9L, false), new BackupMetadata(10L, false)), backend.listBackups());
        backend.deleteBackup(new BackupMetadata(10L, false));
        backend.deleteBackup(new BackupMetadata(10L, false));
        assertEquals(List.of(new BackupMetadata(9L, false)), backend.listBackups());
    }

    @Test
    void publishesManifestAndReadyOnlyAfterStorageBackupCompletes(@TempDir final Path backupVolume) throws Exception {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final BackupMetadata metadata = new BackupMetadata(11L, false);
        final ReplicationCursor cursor = new ReplicationCursor("test", null, 3L, new byte[]{7});
        Files.createDirectories(backupVolume.resolve("11").resolve(BackupFileNames.STORAGE));

        backend.createAndUploadBackup(noOpStorageConnection(), cursor, metadata);

        assertEquals(List.of(metadata), backend.listBackups());
        assertArrayEquals(ReplicationCursorStore.encode(cursor),
                Files.readAllBytes(backupVolume.resolve("11").resolve(BackupFileNames.MANIFEST)));
    }

    @Test
    void failedCopyDoesNotLeavePartialDestinationStorage(@TempDir final Path backupVolume, @TempDir final Path root)
            throws Exception {
        final Path source = backupVolume.resolve("12").resolve(BackupFileNames.STORAGE);
        Files.createDirectories(source);
        Files.writeString(source.resolve("first"), "copied before failure");
        Files.createSymbolicLink(source.resolve("unsafe"), root.resolve("outside"));
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("destination");

        assertThrows(NodelibraryException.class,
                () -> backend.downloadBackup(destination, new BackupMetadata(12L, false)));
        assertFalse(Files.exists(destination.resolve(BackupFileNames.STORAGE)));
    }

    @Test
    void rejectsAnExistingPathReachedThroughASymbolicAncestor(@TempDir final Path root) throws Exception {
        final Path real = root.resolve("real");
        Files.createDirectories(real.resolve("child"));
        final Path symbolic = root.resolve("symbolic");
        try {
            Files.createSymbolicLink(symbolic, real);
        } catch (final UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "symbolic links are unavailable");
        }

        assertThrows(java.io.IOException.class,
                () -> StorageFileOperations.ensureNoSymbolicLinks(symbolic.resolve("child")));
    }

    @Test
    void lastBackupDoesNotMutateAnImmutableBackendSnapshot() {
        final StorageBackupBackend backend = new StorageBackupBackend() {
            @Override
            public List<BackupMetadata> listBackups() {
                return List.of(new BackupMetadata(10L, false), new BackupMetadata(20L, false));
            }

            @Override
            public Optional<ReplicationCursor> getCursorFromPreviousBackup(final int skip) {
                return Optional.empty();
            }

            @Override
            public void downloadLatestBackup(final Path destination) {
            }

            @Override
            public void deleteBackup(final BackupMetadata backup) {
            }

            @Override
            public void createAndUploadBackup(
                    final org.eclipse.store.storage.types.StorageConnection connection,
                    final ReplicationCursor cursor,
                    final BackupMetadata backup
            ) {
            }

            @Override
            public void downloadBackup(final Path destination, final BackupMetadata backup) {
            }

            @Override
            public boolean hasUserUploadedStorage() {
                return false;
            }

            @Override
            public void downloadUserUploadedStorage(final Path destination) {
            }

            @Override
            public void deleteUserUploadedStorage() {
            }
        };

        assertEquals(Optional.of(new BackupMetadata(20L, false)), backend.getLastBackup(0));
        assertEquals(Optional.of(new BackupMetadata(10L, false)), backend.getLastBackup(1));
    }
}
