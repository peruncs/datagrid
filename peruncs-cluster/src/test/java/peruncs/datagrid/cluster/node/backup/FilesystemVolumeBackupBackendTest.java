package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.ReplicationCursorStore;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;

import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies filesystem archive publication, restore, listing, and deletion,
/// including backup generations on one shared volume.
class FilesystemVolumeBackupBackendTest {
    private static final ReplicationCursor CURSOR =
            new ReplicationCursor("test", null, 3L, "07");
    private static final UUID CLUSTER_ONE = UUID.randomUUID();
    private static final UUID GENERATION_ONE = UUID.randomUUID();
    private static final UUID NODE_ONE = UUID.randomUUID();
    private static final UUID CLUSTER_TWO = UUID.randomUUID();
    private static final UUID GENERATION_TWO = UUID.randomUUID();

    private static StorageConnection noOpStorageConnection() {
        return new TestStorageConnection();
    }

    private static BackupMetadata backup(
            final long timestamp,
            final boolean manualSlot,
            final UUID clusterId,
            final UUID storeGeneration,
            final long epoch,
            final long recordingId,
            final UUID backupId
    ) {
        return new BackupMetadata(timestamp, manualSlot, clusterId, storeGeneration,
                epoch, recordingId, null, backupId, BackupMetadata.UNKNOWN);
    }

    private static ReplicationCursor aeronCursor(
            final UUID clusterId,
            final UUID nodeId,
            final UUID generation,
            final long epoch,
            final long recordingId,
            final long sequence
    ) {
        return ReplicationCursor.of("aeron", generation, sequence,
                new AeronReplicationCursor(
                        clusterId, nodeId, generation, epoch, 1L, recordingId, 0L, sequence).encode());
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
        final BackupMetadata first = backup(100L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        final BackupMetadata manual = backup(300L, true, CLUSTER_ONE, GENERATION_ONE, 5L, 42L, UUID.randomUUID());
        createArchive(backupVolume, BackupArchive.toArchiveFileName(first), "one", CURSOR, true);
        createArchive(backupVolume, BackupArchive.toArchiveFileName(manual), "manual", CURSOR, true);
        Files.createDirectories(backupVolume.resolve("400"));
        Files.writeString(backupVolume.resolve("123.evil.zip"), "not a backup");
        Files.writeString(backupVolume.resolve("500.zip.tmp"), "not a backup");
        /* Legacy millisecond names from before generations carry no identity
         * and are ignored instead of mistaken for current backups. */
        Files.writeString(backupVolume.resolve("600.zip"), "not a backup");

        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);

        assertEquals(List.of(first, manual), backend.listBackups());
        assertEquals(manual, backend.getLastBackup(0));
        assertEquals(first, backend.getLastBackup(1));
        assertNull(backend.getLastBackup(2));
    }

    @Test
    void readsManifestAndRestoresStorage(@TempDir final Path backupVolume, @TempDir final Path root)
            throws Exception {
        final BackupMetadata backup = backup(10L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        createArchive(backupVolume, BackupArchive.toArchiveFileName(backup), "payload", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("new");

        assertEquals(CURSOR, backend.getCursorForBackup(backend.getLastBackup(0)));
        assertEquals(CURSOR, backend.getCursorForBackup(backup));
        backend.restoreBackup(destination, backend.getLastBackup(0));

        assertEquals("payload", Files.readString(destination.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
        assertFalse(Files.exists(destination.resolve(StorageBackupBackend.MANIFEST_ENTRY)));
        assertFalse(Files.exists(destination.resolve(StorageBackupBackend.READY_ENTRY)));
        assertThrows(NodeLibraryException.class,
                () -> backend.restoreBackup(destination, backend.getLastBackup(0)));
    }

    @Test
    void publishesAGenerationArchiveWithIdentityAndDigest(@TempDir final Path backupVolume) throws Exception {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final ReplicationCursor cursor = aeronCursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 7L);
        final BackupMetadata metadata = BackupMetadata.New(11L, false, cursor);

        assertEquals(CLUSTER_ONE, metadata.clusterId());
        assertEquals(GENERATION_ONE, metadata.storeGeneration());
        assertEquals(5L, metadata.epoch());
        assertEquals(42L, metadata.recordingId());
        assertEquals(NODE_ONE, metadata.nodeId());

        backend.createBackup(noOpStorageConnection(), cursor, metadata);

        final List<BackupMetadata> listed = backend.listBackups();
        assertEquals(1, listed.size());
        final BackupMetadata stored = listed.getFirst();
        assertEquals(metadata.backupId(), stored.backupId());
        assertEquals(metadata.clusterId(), stored.clusterId());
        assertEquals(metadata.storeGeneration(), stored.storeGeneration());
        assertEquals(metadata.epoch(), stored.epoch());
        assertEquals(metadata.recordingId(), stored.recordingId());
        assertEquals(metadata.nodeId(), stored.nodeId());
        assertTrue(stored.digest() >= 0L, "published backup must carry a content digest");
        final Path archive = backupVolume.resolve(BackupArchive.toArchiveFileName(stored));
        assertTrue(Files.isRegularFile(archive));
        assertEquals(cursor, ReplicationCursorStore.decode(
                BackupArchive.readManifest(archive, BackupArchiveLimits.defaults().maxExtractedBytes())));
    }

    @Test
    void mixedGenerationsSelectOnlyTheCompatibleBackup(@TempDir final Path backupVolume) {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final ReplicationCursor oldCursor = aeronCursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 7L);
        final ReplicationCursor newCursor = aeronCursor(CLUSTER_TWO, UUID.randomUUID(), GENERATION_TWO, 9L, 77L, 11L);
        backend.createBackup(noOpStorageConnection(), oldCursor, BackupMetadata.New(100L, false, oldCursor));
        backend.createBackup(noOpStorageConnection(), newCursor, BackupMetadata.New(200L, false, newCursor));

        assertEquals(2, backend.listBackups().size());

        final BackupMetadata.Identity generationOne =
                new BackupMetadata.Identity(CLUSTER_ONE, GENERATION_ONE, 5L, 42L);
        final BackupMetadata.Identity generationTwo =
                new BackupMetadata.Identity(CLUSTER_TWO, GENERATION_TWO, 9L, 77L);

        final BackupMetadata selectedOne = backend.findLatestCompatibleBackup(generationOne);
        assertNotNull(selectedOne);
        assertEquals(GENERATION_ONE, selectedOne.storeGeneration());
        assertEquals(100L, selectedOne.timestamp());

        final BackupMetadata selectedTwo = backend.findLatestCompatibleBackup(generationTwo);
        assertNotNull(selectedTwo);
        assertEquals(GENERATION_TWO, selectedTwo.storeGeneration());

        /* The newest backup overall belongs to generation two, yet generation
         * one still resolves to its own older archive. */
        assertEquals(200L, backend.getLastBackup(0).timestamp());

        assertNull(backend.findLatestCompatibleBackup(
                new BackupMetadata.Identity(UUID.randomUUID(), UUID.randomUUID(), 1L, 1L)));
        assertEquals(selectedTwo, backend.findLatestCompatibleBackup(BackupMetadata.Identity.unknown()));
    }

    @Test
    void concurrentPublicationYieldsDistinctArchives(@TempDir final Path backupVolume) throws Exception {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final ReplicationCursor firstCursor = aeronCursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 7L);
        final ReplicationCursor secondCursor = aeronCursor(CLUSTER_ONE, UUID.randomUUID(), GENERATION_ONE, 5L, 42L, 7L);

        /* Both nodes publish in the same millisecond; the random backup id
         * still separates their archives. */
        backend.createBackup(noOpStorageConnection(), firstCursor, BackupMetadata.New(100L, false, firstCursor));
        backend.createBackup(noOpStorageConnection(), secondCursor, BackupMetadata.New(100L, false, secondCursor));

        final List<BackupMetadata> listed = backend.listBackups();
        assertEquals(2, listed.size());
        assertNotEquals(listed.get(0).backupId(), listed.get(1).backupId());
        try (var files = Files.list(backupVolume)) {
            /* Count backup archives only: the volume also holds the
             * publication lock sidecar serializing same-name publishers. */
            assertEquals(2, files
                    .map(path -> path.getFileName().toString())
                    .filter(BackupArchive::isBackupFileName)
                    .count(),
                    "concurrent publishers must not clobber one shared archive name");
        }
    }

    @Test
    void sameNamePublicationIsIdempotentOnlyForIdenticalContent(@TempDir final Path backupVolume) {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final ReplicationCursor cursor = aeronCursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 7L);
        final BackupMetadata backup = BackupMetadata.New(100L, false, cursor);

        backend.createBackup(noOpStorageConnection(), cursor, backup);
        /* Retrying the same publication after a crash acknowledgement is safe. */
        backend.createBackup(noOpStorageConnection(), cursor, backup);
        assertEquals(1, backend.listBackups().size());

        /* The same backup id with a different replication position is a
         * conflicting publication and must fail instead of overwriting. */
        final ReplicationCursor moved = aeronCursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 8L);
        assertThrows(NodeLibraryException.class,
                () -> backend.createBackup(noOpStorageConnection(), moved, backup));
        assertEquals(cursor, backend.getCursorForBackup(backup),
                "a conflicting publication must leave the durable archive untouched");
    }

    @Test
    void concurrentSameNamePublicationsElectOneWinnerWithoutSilentOverwrite(@TempDir final Path backupVolume)
            throws Exception {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        /* Every publisher shares one backup id — hence one archive name — but
         * carries different content: the replication position differs, so the
         * manifest and the content digest differ too. */
        final UUID sharedBackupId = UUID.randomUUID();
        final int publishers = 8;
        final List<ReplicationCursor> cursors = new ArrayList<>();
        for (int index = 0; index < publishers; index++) {
            cursors.add(aeronCursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 100L + index));
        }

        final CountDownLatch gate = new CountDownLatch(1);
        final AtomicInteger published = new AtomicInteger();
        final AtomicInteger conflicts = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<?>> futures = new ArrayList<>();
            for (final ReplicationCursor cursor : cursors) {
                futures.add(executor.submit(() ->
                {
                    final BackupMetadata shared = new BackupMetadata(
                            100L, false, CLUSTER_ONE, GENERATION_ONE, 5L, 42L, NODE_ONE,
                            sharedBackupId, BackupMetadata.UNKNOWN);
                    gate.await(1, TimeUnit.MINUTES);
                    try {
                        backend.createBackup(noOpStorageConnection(), cursor, shared);
                        published.incrementAndGet();
                    } catch (final NodeLibraryException conflict) {
                        conflicts.incrementAndGet();
                    }
                    return null;
                }));
            }
            gate.countDown();
            for (final Future<?> future : futures) future.get(1, TimeUnit.MINUTES);
        }

        /* Publication is serialized on the volume while the existing archive
         * is compared: exactly one publisher wins and every loser fails
         * instead of overwriting — or being overwritten by — the winner. */
        assertEquals(1, published.get(), "exactly one same-name publication must win");
        assertEquals(publishers - 1, conflicts.get(), "every loser must fail instead of overwriting");
        assertEquals(1, backend.listBackups().size());
        final ReplicationCursor stored = backend.getCursorForBackup(backend.getLastBackup(0));
        assertTrue(cursors.contains(stored),
                "the surviving archive must hold one publisher's content, not a mixture");
        try (var files = Files.list(backupVolume)) {
            assertEquals(1, files
                    .map(path -> path.getFileName().toString())
                    .filter(BackupArchive::isBackupFileName)
                    .count(), "concurrent publishers must leave exactly one archive");
        }
    }

    @Test
    void restoreRejectsADigestMismatch(@TempDir final Path backupVolume, @TempDir final Path root)
            throws Exception {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final UUID backupId = UUID.randomUUID();
        final BackupMetadata tampered = new BackupMetadata(50L, false, CLUSTER_ONE, GENERATION_ONE,
                5L, 42L, NODE_ONE, backupId, 12345L);
        final Path source = Files.createTempDirectory(backupVolume, ".tampered-source-");
        try {
            Files.createDirectories(source.resolve(StorageBackupBackend.STORAGE_ENTRY));
            Files.writeString(source.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data"), "payload");
            Files.write(source.resolve(StorageBackupBackend.MANIFEST_ENTRY), ReplicationCursorStore.encode(CURSOR));
            Files.writeString(source.resolve(StorageBackupBackend.READY_ENTRY), "");
            BackupArchive.writeIdentity(source.resolve(BackupArchive.BACKUP_IDENTITY_ENTRY), tampered);
            BackupArchive.compressStorage(source, backupVolume.resolve(BackupArchive.toArchiveFileName(tampered)));
        } finally {
            peruncs.datagrid.cluster.node.store.StorageFileOperations.deleteDirectory(source);
        }

        assertThrows(NodeLibraryException.class,
                () -> backend.restoreBackup(root.resolve("destination"), tampered));
        assertFalse(Files.exists(root.resolve("destination").resolve(StorageBackupBackend.STORAGE_ENTRY)));
    }

    @Test
    void roundTripsStorageAndCursorThroughTheVolumeArchive(
            @TempDir final Path backupVolume,
            @TempDir final Path root
    ) throws Exception {
        final BackupMetadata backup = backup(20L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        createArchive(backupVolume, BackupArchive.toArchiveFileName(backup), "round-trip", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("destination");

        backend.restoreBackup(destination, backup);

        assertEquals("round-trip", Files.readString(destination.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
        assertEquals(CURSOR, backend.getCursorForBackup(backend.getLastBackup(0)));
    }

    @Test
    void deletesArchivesIdempotently(@TempDir final Path backupVolume) throws Exception {
        final BackupMetadata first = backup(9L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        final BackupMetadata second = backup(10L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        createArchive(backupVolume, BackupArchive.toArchiveFileName(first), "nine", CURSOR, true);
        createArchive(backupVolume, BackupArchive.toArchiveFileName(second), "ten", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);

        backend.deleteBackup(second);
        backend.deleteBackup(second);

        assertEquals(List.of(first), backend.listBackups());
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
        final BackupMetadata backup = backup(12L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        createArchive(backupVolume, BackupArchive.toArchiveFileName(backup), "payload", CURSOR, true);
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(backupVolume);
        final Path destination = root.resolve("destination");
        Files.createDirectories(destination.resolve(StorageBackupBackend.STORAGE_ENTRY));

        assertThrows(NodeLibraryException.class,
                () -> backend.restoreBackup(destination, backup));
    }

    @Test
    void lastBackupDoesNotMutateAnImmutableBackendSnapshot() {
        final BackupMetadata first = backup(10L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        final BackupMetadata second = backup(20L, false, null, null,
                BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, UUID.randomUUID());
        final StorageBackupBackend backend = new StorageBackupBackend() {
            @Override
            public List<BackupMetadata> listBackups() {
                return List.of(first, second);
            }


            @Override
            public ReplicationCursor getCursorForBackup(final BackupMetadata backup) {
                return null;
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

        assertEquals(second, backend.getLastBackup(0));
        assertEquals(first, backend.getLastBackup(1));
    }
}
