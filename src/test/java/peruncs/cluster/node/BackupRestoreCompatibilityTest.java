package peruncs.cluster.node;

import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.backup.BackupMetadata;
import peruncs.cluster.node.backup.FilesystemVolumeBackupBackend;
import peruncs.cluster.node.replication.ReplicationCursorStore;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCursor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


/// Verifies generation-filtered restores: on a shared backup volume a node
/// selects the newest backup of its own cluster, generation, epoch, and
/// recording, and an incompatible newest backup never deletes or overwrites
/// valid local storage.
class BackupRestoreCompatibilityTest {
    private static final UUID CLUSTER_ONE = UUID.randomUUID();
    private static final UUID GENERATION_ONE = UUID.randomUUID();
    private static final UUID NODE_ONE = UUID.randomUUID();
    private static final UUID CLUSTER_TWO = UUID.randomUUID();
    private static final UUID GENERATION_TWO = UUID.randomUUID();

    private static ReplicationCursor cursor(
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

    private static StorageConnection noOpStorageConnection() {
        return (StorageConnection) Proxy.newProxyInstance(
                StorageConnection.class.getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("issueFullBackup")) {
                        return null;
                    }
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return null;
                });
    }

    private static void publishBackup(
            final Path volume,
            final ReplicationCursor backupCursor,
            final long timestamp
    ) {
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.create(volume);
        backend.createBackup(
                noOpStorageConnection(), backupCursor, BackupMetadata.create(timestamp, false, backupCursor));
    }

    private static void writeOffset(final Path storageParent, final ReplicationCursor offset) {
        try {
            Files.createDirectories(storageParent);
            ReplicationCursorStore.write(storageParent.resolve("offset"), offset);
        } catch (final IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private static ReplicationCursor readOffset(final Path storageParent) {
        try {
            return ReplicationCursorStore.read(storageParent.resolve("offset"));
        } catch (final IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private static ReaderSeedBootstrapTest.TestProperties properties(
            final Path storagePath, final Path backupPath, final String role) {
        return new ReaderSeedBootstrapTest.TestProperties(storagePath, backupPath, role);
    }

    /// Verifies valid local storage survives a restart when the volume's newest backup belongs to an unrelated generation, keeping the local root and replication offset untouched.
    @Test
    void incompatibleNewestKeepsValidLocalStorage(@TempDir final Path root) {
        final Path home = root.resolve("node-home");
        final Path volume = root.resolve("shared-volume");
        final ReplicationCursor local = cursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 9L);

        try (final NodeAssembly node = NodeAssembly.create()
                .setNodeSettingsSource(properties(
                        home, volume, NodeSettingsSource.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var manager = node.startStorageManager();
            final ArrayList<String> writerRoot = new ArrayList<>();
            writerRoot.add("local-value");
            manager.setRoot(writerRoot);
            manager.storeRoot();
        }
        assertTrue(Files.isDirectory(home.resolve("storage")), "node must have created its Store directory");

        /* The node ran generation one and is ahead of its older backup; an
         * unrelated newer generation shares the volume. */
        writeOffset(home, local);
        publishBackup(volume, cursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 5L), 100L);
        publishBackup(volume, cursor(CLUSTER_TWO, UUID.randomUUID(), GENERATION_TWO, 9L, 77L, 11L), 200L);

        try (final NodeAssembly restarted = NodeAssembly.create()
                .setNodeSettingsSource(properties(
                        home, volume, NodeSettingsSource.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            @SuppressWarnings("unchecked")
            final ArrayList<String> restartedRoot = restarted.startStorageManager()
                    .readRoot(stored -> new ArrayList<>((ArrayList<String>) stored));
            assertTrue(restartedRoot.contains("local-value"),
                    "valid local storage must survive an incompatible newest backup, was: %s".formatted(restartedRoot));
        }

        assertEquals(local, readOffset(home),
                "the local replication boundary must not move to the unrelated generation");
        assertEquals(2, FilesystemVolumeBackupBackend.create(volume).listBackups().size(),
                "the unrelated backup must be left alone on the volume");
    }

    /// Verifies a node with no local Store image restores the older compatible backup when the volume's newest backup belongs to another generation.
    @Test
    void mixedGenerationsRestoreTheCompatibleBackup(@TempDir final Path root) {
        final Path home = root.resolve("node-home");
        final Path volume = root.resolve("shared-volume");
        final ReplicationCursor generationOne = cursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 7L);

        /* No local Store image, but a durable boundary from generation one;
         * the volume's newest backup belongs to generation two. */
        writeOffset(home, cursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 3L));
        publishBackup(volume, generationOne, 100L);
        publishBackup(volume, cursor(CLUSTER_TWO, UUID.randomUUID(), GENERATION_TWO, 9L, 77L, 11L), 200L);

        try (final NodeAssembly node = NodeAssembly.create()
                .setNodeSettingsSource(properties(
                        home, volume, NodeSettingsSource.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            assertTrue(node.startStorageManager().readRoot((Object stored) -> stored != null),
                    "the node must start from the compatible backup");
        }

        assertEquals(generationOne, readOffset(home),
                "the restored boundary must come from the compatible backup, not the newest one");
        assertEquals(2, FilesystemVolumeBackupBackend.create(volume).listBackups().size());
    }

    /// Verifies a fresh node with only incompatible backups fails fast with a compatibility error and installs no Store image.
    @Test
    void freshNodeWithOnlyIncompatibleBackupsRefusesToInstall(@TempDir final Path root) {
        final Path home = root.resolve("node-home");
        final Path volume = root.resolve("shared-volume");

        writeOffset(home, cursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 3L));
        publishBackup(volume, cursor(CLUSTER_TWO, UUID.randomUUID(), GENERATION_TWO, 9L, 77L, 11L), 200L);

        try (final NodeAssembly node = NodeAssembly.create()
                .setNodeSettingsSource(properties(
                        home, volume, NodeSettingsSource.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final NodeException failure =
                    assertThrows(NodeException.class, node::startStorageManager);
            assertTrue(failure.getMessage().contains("compatible"),
                    "refusal must name the compatibility cause, was: %s".formatted(failure.getMessage()));
        }

        assertFalse(Files.exists(home.resolve("storage")),
                "no unrelated image may be installed when nothing compatible exists");
    }

    /// Verifies a seeded reader keeps its copied Store image when the volume's newest backup belongs to an unrelated generation.
    @Test
    void readerKeepsSeededStorageWhenNewestIsIncompatible(@TempDir final Path root) throws Exception {
        final Path writerHome = root.resolve("writer-home");
        final Path readerHome = root.resolve("reader-home");
        final Path volume = root.resolve("shared-volume");
        final ReplicationCursor local = cursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 9L);

        try (final NodeAssembly writer = NodeAssembly.create()
                .setNodeSettingsSource(properties(
                        writerHome, volume, NodeSettingsSource.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var manager = writer.startStorageManager();
            final ArrayList<String> writerRoot = new ArrayList<>();
            writerRoot.add("seeded-value");
            manager.setRoot(writerRoot);
            manager.storeRoot();
        }

        copyStorage(writerHome.resolve("storage"), readerHome.resolve("storage"));
        writeOffset(readerHome, local);
        publishBackup(volume, cursor(CLUSTER_ONE, NODE_ONE, GENERATION_ONE, 5L, 42L, 5L), 100L);
        publishBackup(volume, cursor(CLUSTER_TWO, UUID.randomUUID(), GENERATION_TWO, 9L, 77L, 11L), 200L);

        try (final NodeAssembly reader = NodeAssembly.create()
                .setNodeSettingsSource(properties(
                        readerHome, volume, NodeSettingsSource.READER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            @SuppressWarnings("unchecked")
            final ArrayList<String> readerRoot = reader.startStorageManager()
                    .readRoot(stored -> new ArrayList<>((ArrayList<String>) stored));
            assertTrue(readerRoot.contains("seeded-value"),
                    "seeded reader storage must survive an incompatible newest backup, was: %s".formatted(readerRoot));
        }

        assertEquals(local, readOffset(readerHome));
    }

    private static void copyStorage(final Path source, final Path target) throws IOException {
        try (var paths = Files.walk(source)) {
            for (final var iterator = paths.iterator(); iterator.hasNext(); ) {
                final Path from = iterator.next();
                final Path to = target.resolve(source.relativize(from).toString());
                if (Files.isDirectory(from)) {
                    Files.createDirectories(to);
                } else {
                    Files.copy(from, to);
                }
            }
        }
    }
}
