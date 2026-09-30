package peruncs.cluster.node;

import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.aeron.TestNodeConfig;
import peruncs.cluster.node.backup.BackupMetadata;
import peruncs.cluster.node.backup.StorageBackupBackend;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.storage.ReplicationPosition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies startup keeps uploads until a starter backup is durably published.
class NodeLifecycleStarterBackupTest {
    /// A failed or bounded-out backup never deletes its source upload.
    @Test
    void failedOrTimedOutBackupKeepsTheUpload() {
        final AtomicBoolean deleted = new AtomicBoolean();

        assertThrows(NodeException.class, () -> NodeLifecycle.awaitStarterBackup(
                CompletableFuture.failedFuture(new IllegalStateException("export failed")), 1_000L,
                () -> deleted.set(true)));
        assertFalse(deleted.get());

        final NodeException timeout = assertThrows(NodeException.class, () -> NodeLifecycle.awaitStarterBackup(
                new CompletableFuture<>(), 1L, () -> deleted.set(true)));
        assertInstanceOf(TimeoutException.class, timeout.getCause());
        assertTrue(timeout.getMessage().contains("timed out"));
        assertFalse(deleted.get());

        final CompletableFuture<String> cancelled = new CompletableFuture<>();
        cancelled.cancel(false);
        final NodeException cancellation = assertThrows(NodeException.class, () -> NodeLifecycle.awaitStarterBackup(
                cancelled, 1_000L, () -> deleted.set(true)));
        assertTrue(cancellation.getMessage().contains("cancelled"));
        assertFalse(deleted.get());
    }

    /// A successful starter backup releases the retained upload.
    @Test
    void successfulBackupDeletesTheUploadAfterPublication() throws NodeException {
        final AtomicBoolean deleted = new AtomicBoolean();

        NodeLifecycle.awaitStarterBackup(CompletableFuture.completedFuture("published"), 1_000L,
                () -> deleted.set(true));

        assertTrue(deleted.get());
    }

    /// A failure from the actual backup-node startup path leaves its restored upload available for retry.
    @Test
    @Timeout(30)
    void failedBackupNodeBootstrapKeepsUploadedStorage(@TempDir final Path root) throws Exception {
        final Path sourceStore = root.resolve("uploaded-store");
        writeStoreImage(sourceStore);

        final FailingUploadBackend backend = new FailingUploadBackend(sourceStore);
        final NodeConfig config = TestNodeConfig.aeron(root.resolve("node"), "backup-reader", true, Map.of());
        final NodeCollaborators collaborators = new NodeCollaborators(
                () -> Map.of(), null, config, backend, ClusterReplicationTransport.noOp());
        try (final NodeLifecycle lifecycle = new NodeLifecycle(collaborators)) {
            final NodeException failure = assertThrows(NodeException.class, lifecycle::startStorageManager);

            assertTrue(failure.getMessage().contains("Starter backup failed"));
            assertEquals(1, backend.backupAttempts.get(), "startup must fail during the starter backup");
            assertTrue(backend.uploadPresent.get(), "the upload must remain available for a startup retry");
            assertEquals(0, backend.deleteAttempts.get());
        }
    }

    private static void writeStoreImage(final Path path) {
        final EmbeddedStorageFoundation<?> foundation = EmbeddedStorageFoundation.New()
                .setConfiguration(StorageConfiguration.Builder()
                        .setStorageFileProvider(Storage.FileProvider(path))
                        .createConfiguration());
        final EmbeddedStorageManager storage = foundation.start();
        try {
            storage.setRoot(Lazy.Reference(Map.of("seed", "uploaded")));
            storage.storeRoot();
        } finally {
            storage.shutdown();
        }
    }

    private static final class FailingUploadBackend implements StorageBackupBackend {
        private final Path sourceStore;
        private final AtomicBoolean uploadPresent = new AtomicBoolean(true);
        private final AtomicInteger backupAttempts = new AtomicInteger();
        private final AtomicInteger deleteAttempts = new AtomicInteger();

        private FailingUploadBackend(final Path sourceStore) {
            this.sourceStore = sourceStore;
        }

        @Override
        public List<BackupMetadata> listBackups() {
            return List.of();
        }

        @Override
        public ReplicationPosition retentionBoundary(final BackupMetadata backup) {
            return ReplicationPosition.NONE;
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) {
        }

        @Override
        public void createBackup(final StorageConnection connection, final BackupMetadata backup) {
            this.backupAttempts.incrementAndGet();
            throw new NodeException("forced starter backup failure");
        }

        @Override
        public void restoreBackup(final Path destination, final BackupMetadata backup) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasUserUploadedStorage() {
            return this.uploadPresent.get();
        }

        @Override
        public void restoreUserUploadedStorage(final Path destinationParent) {
            final Path target = destinationParent.resolve(StorageBackupBackend.STORAGE_ENTRY);
            try (var paths = Files.walk(this.sourceStore)) {
                for (final Path source : paths.toList()) {
                    final Path destination = target.resolve(this.sourceStore.relativize(source));
                    if (Files.isDirectory(source)) Files.createDirectories(destination);
                    else Files.copy(source, destination);
                }
            } catch (final IOException failure) {
                throw new NodeException("Failed to restore test upload", failure);
            }
        }

        @Override
        public void deleteUserUploadedStorage() {
            this.deleteAttempts.incrementAndGet();
            this.uploadPresent.set(false);
        }
    }
}
