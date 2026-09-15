package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.ReplicationCursorStore;

import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies filesystem archive publication, restore, listing, and deletion.
class FilesystemVolumeBackupBackendTest {
    private static final ReplicationCursor CURSOR =
            new ReplicationCursor("test", null, 3L, "07");

    private static StorageConnection noOpStorageConnection() {
        return new TestStorageConnection();
    }

    private static void createArchive(
            final Path volume,
            final String archiveName,
            final String data,
            final ReplicationCursor cursor,
            final boolean metadata
    ) throws Exception {
        final Path source = Files.createTempDirectory(volume, ".archive-source-");
        try {
            Files.createDirectories(source.resolve(StorageBackupBackend.STORAGE_ENTRY));
            if (data != null) {
                Files.writeString(source.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data"), data);
            }
            if (metadata) {
                Files.write(source.resolve(StorageBackupBackend.MANIFEST_ENTRY), ReplicationCursorStore.encode(cursor));
                Files.writeString(source.resolve(StorageBackupBackend.READY_ENTRY), "");
            }
            BackupArchive.compressStorage(source, volume.resolve(archiveName));
        } finally {
            peruncs.datagrid.cluster.node.store.StorageFileOperations.deleteDirectory(source);
        }
    }

    private static void createUserArchive(final Path volume, final String data) throws Exception {
        try (OutputStream output = Files.newOutputStream(
                volume.resolve(StorageBackupBackend.USER_UPLOADED_STORAGE_ARCHIVE));
             ZipOutputStream zip = new ZipOutputStream(output)) {
            zip.putNextEntry(new ZipEntry(StorageBackupBackend.STORAGE_ENTRY + "/data"));
            zip.write(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            zip.closeEntry();
        }
    }

    @Test
    void listsArchiveFilesAndIgnoresDirectoriesAndMalformedNames(@TempDir final Path backupVolume) throws Exception {
        createArchive(backupVolume, "100.zip", "one", CURSOR, true);
        createArchive(backupVolume, "300.manual.zip", "manual", CURSOR, true);
        Files.createDirectories(backupVolume.resolve("400"));
        Files.writeString(backupVolume.resolve("123.evil.zip"), "not a backup");
        Files.writeString(backupVolume.resolve("500.zip.tmp"), "not a backup");

        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);

        assertEquals(List.of(new BackupMetadata(100L, false), new BackupMetadata(300L, true)),
                backend.listBackups());
        assertEquals(new BackupMetadata(300L, true), backend.getLastBackup(0));
        assertEquals(new BackupMetadata(100L, false), backend.getLastBackup(1));
        assertNull(backend.getLastBackup(2));
    }

    @Test
    void readsManifestAndRestoresStorage(@TempDir final Path backupVolume, @TempDir final Path root)
            throws Exception {
        createArchive(backupVolume, "10.zip", "payload", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("new");

        assertEquals(CURSOR, backend.getCursorFromPreviousBackup(0));
        backend.restoreLatestBackup(destination);

        assertEquals("payload", Files.readString(destination.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
        assertFalse(Files.exists(destination.resolve(StorageBackupBackend.MANIFEST_ENTRY)));
        assertFalse(Files.exists(destination.resolve(StorageBackupBackend.READY_ENTRY)));
        assertThrows(NodeLibraryException.class, () -> backend.restoreLatestBackup(destination));
    }

    @Test
    void publishesAnAtomicZipAndCursor(@TempDir final Path backupVolume) throws Exception {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final BackupMetadata metadata = new BackupMetadata(11L, false);

        backend.createBackup(noOpStorageConnection(), CURSOR, metadata);

        final Path archive = backupVolume.resolve("11.zip");
        assertTrue(Files.isRegularFile(archive));
        assertEquals(List.of(metadata), backend.listBackups());
        assertEquals(CURSOR, ReplicationCursorStore.decode(
                BackupArchive.readManifest(archive, BackupArchiveLimits.Default().maxExtractedBytes())));
    }

    @Test
    void roundTripsStorageAndCursorThroughTheVolumeArchive(
            @TempDir final Path backupVolume,
            @TempDir final Path root
    ) throws Exception {
        createArchive(backupVolume, "20.zip", "round-trip", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("destination");

        backend.restoreBackup(destination, new BackupMetadata(20L, false));

        assertEquals("round-trip", Files.readString(destination.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
        assertEquals(CURSOR, backend.getCursorFromPreviousBackup(0));
    }

    @Test
    void deletesArchivesIdempotently(@TempDir final Path backupVolume) throws Exception {
        createArchive(backupVolume, "9.zip", "nine", CURSOR, true);
        createArchive(backupVolume, "10.zip", "ten", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);

        backend.deleteBackup(new BackupMetadata(10L, false));
        backend.deleteBackup(new BackupMetadata(10L, false));

        assertEquals(List.of(new BackupMetadata(9L, false)), backend.listBackups());
    }

    @Test
    void restoresAndDeletesUserUploadedArchive(@TempDir final Path backupVolume, @TempDir final Path root)
            throws Exception {
        createUserArchive(backupVolume, "user");
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("destination");

        assertTrue(backend.hasUserUploadedStorage());
        backend.restoreUserUploadedStorage(destination);
        assertEquals("user", Files.readString(destination.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
        backend.deleteUserUploadedStorage();
        assertFalse(backend.hasUserUploadedStorage());
    }

    @Test
    void tempDirectoryAttributesFallBackWithoutAPosixView(@TempDir final Path root) throws Exception {
        assertEquals(1, FilesystemVolumeBackupBackend.Default
                .privateDirectoryAttributes(root).length);
        final Path zip = root.resolve("fs.zip");
        final URI uri = URI.create("jar:" + zip.toUri());
        try (final FileSystem zipfs = FileSystems.newFileSystem(uri, Map.of("create", "true"))) {
            assertEquals(0, FilesystemVolumeBackupBackend.Default
                    .privateDirectoryAttributes(zipfs.getPath("/")).length);
        }
    }

    @Test
    void rejectsExistingStorageDestination(@TempDir final Path backupVolume, @TempDir final Path root)
            throws Exception {
        createArchive(backupVolume, "12.zip", "payload", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("destination");
        Files.createDirectories(destination.resolve(StorageBackupBackend.STORAGE_ENTRY));

        assertThrows(NodeLibraryException.class,
                () -> backend.restoreBackup(destination, new BackupMetadata(12L, false)));
    }

    @Test
    void lastBackupDoesNotMutateAnImmutableBackendSnapshot() {
        final StorageBackupBackend backend = new StorageBackupBackend() {
            @Override
            public List<BackupMetadata> listBackups() {
                return List.of(new BackupMetadata(10L, false), new BackupMetadata(20L, false));
            }

            @Override
            public ReplicationCursor getCursorFromPreviousBackup(final int skip) {
                return null;
            }

            @Override
            public void restoreLatestBackup(final Path destination) {
            }

            @Override
            public void deleteBackup(final BackupMetadata backup) {
            }

            @Override
            public void createBackup(
                    final StorageConnection connection,
                    final ReplicationCursor cursor,
                    final BackupMetadata backup
            ) {
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
        };

        assertEquals(new BackupMetadata(20L, false), backend.getLastBackup(0));
        assertEquals(new BackupMetadata(10L, false), backend.getLastBackup(1));
    }
}
