package peruncs.datagrid.cluster.node.backup;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.NodeException;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the backup task executor's single-flight contract: one backup at a
/// time, an explicit `BUSY` for concurrent requests, and no new work after close.
class StorageBackupTaskExecutorTest {
    private static void await(final BooleanSupplier condition) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition was not met in time");
            Thread.sleep(5L);
        }
    }

    /// A manager whose backup blocks until the test releases it.
    private static class BlockingManager implements StorageBackupManager {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger runs = new AtomicInteger();

        @Override
        public void createStorageBackup(final boolean useManualSlot) throws NodeException {
            this.runs.incrementAndGet();
            this.entered.countDown();
            try {
                if (!this.release.await(1, TimeUnit.MINUTES)) {
                    throw new NodeException("backup was never released");
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NodeException("backup interrupted", interrupted);
            }
        }

        @Override
        public List<BackupMetadata> listBackups() {
            return List.of();
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) {
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

    /// Verifies a second request while a backup runs is rejected as BUSY instead
    /// of queued or silently dropped.
    @Test
    void rejectsAConcurrentBackupWithBusy() throws Exception {
        final BlockingManager manager = new BlockingManager();
        try (final StorageBackupTaskExecutor executor =
                     StorageBackupTaskExecutor.create(new TestStorageConnection(), manager)) {
            assertEquals(StorageBackupTaskExecutor.BackupStartResult.STARTED, executor.runBackup(false));
            assertTrue(manager.entered.await(1, TimeUnit.MINUTES));

            assertEquals(StorageBackupTaskExecutor.BackupStartResult.BUSY, executor.runBackup(false));
            assertTrue(executor.isRunningBackup());

            manager.release.countDown();
            await(() -> !executor.isRunningBackup());
            assertEquals(1, manager.runs.get(), "a BUSY request must never start a second backup");
            assertNull(executor.backupFailure());
        }
    }

    /// Verifies a failed backup remains observable through the executor.
    @Test
    void exposesBackupFailure() throws Exception {
        final StorageBackupManager failing = new BlockingManager() {
            @Override
            public void createStorageBackup(final boolean useManualSlot) throws NodeException {
                throw new NodeException("backup failed");
            }
        };
        try (final StorageBackupTaskExecutor executor =
                     StorageBackupTaskExecutor.create(new TestStorageConnection(), failing)) {
            assertEquals(StorageBackupTaskExecutor.BackupStartResult.STARTED, executor.runBackup(false));
            await(() -> !executor.isRunningBackup());
            assertNotNull(executor.backupFailure());
        }
    }

    /// Verifies a closed executor accepts no new backup and tolerates a repeated close.
    @Test
    void rejectsBackupsAfterCloseAndClosesIdempotently() {
        final StorageBackupTaskExecutor executor =
                StorageBackupTaskExecutor.create(new TestStorageConnection(), new BlockingManager());
        executor.close();
        assertThrows(IllegalStateException.class, () -> executor.runBackup(false));
        executor.close();
    }
}
