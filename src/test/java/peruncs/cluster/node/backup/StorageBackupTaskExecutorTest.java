package peruncs.cluster.node.backup;

import org.junit.jupiter.api.Test;
import peruncs.cluster.api.BackupInfo;
import peruncs.cluster.api.BackupSlot;
import peruncs.cluster.errors.BackupBusyException;
import peruncs.cluster.errors.NodeException;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the backup task executor's single-flight contract: one backup at a
/// time, an explicit `BUSY` for concurrent requests, and no new work after close.
class StorageBackupTaskExecutorTest {
    private static final BackupInfo BACKUP = new BackupInfo(
            UUID.fromString("00000000-0000-0000-0000-000000000001"), Instant.EPOCH, 7L, false);

    /// A manager whose backup blocks until the test releases it.
    private static class BlockingManager implements StorageBackupManager {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger runs = new AtomicInteger();

        @Override
        public Throwable maintenanceFailure() {
            return null;
        }

        @Override
        public BackupInfo createStorageBackup(final BackupSlot slot) throws NodeException {
            this.runs.incrementAndGet();
            this.entered.countDown();
            try {
                if (!this.release.await(1, TimeUnit.MINUTES)) {
                    throw new NodeException("backup was never released");
                }
                return BACKUP;
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
            final var first = executor.runBackup(BackupSlot.SCHEDULED);
            assertTrue(manager.entered.await(1, TimeUnit.MINUTES));

            final var concurrent = executor.runBackup(BackupSlot.SCHEDULED);
            assertInstanceOf(BackupBusyException.class,
                    assertThrows(ExecutionException.class, concurrent::get).getCause());
            assertTrue(executor.isRunningBackup());
            assertTrue(executor.isBackupExecuting());

            manager.release.countDown();
            assertEquals(BACKUP, first.get(1, TimeUnit.MINUTES));
            assertFalse(executor.isRunningBackup());
            assertFalse(executor.isBackupExecuting());
            assertEquals(1, manager.runs.get(), "a BUSY request must never start a second backup");
            assertNull(executor.backupFailure());
            assertTrue(executor.lastSuccessEpochMillis() > 0L);
        }
    }

    /// Verifies a failed backup remains observable through the executor.
    @Test
    void exposesBackupFailure() throws Exception {
        final StorageBackupManager failing = new BlockingManager() {
            @Override
            public BackupInfo createStorageBackup(final BackupSlot slot) throws NodeException {
                throw new NodeException("backup failed");
            }
        };
        try (final StorageBackupTaskExecutor executor =
            StorageBackupTaskExecutor.create(new TestStorageConnection(), failing)) {
            final var result = executor.runBackup(BackupSlot.SCHEDULED);
            assertInstanceOf(NodeException.class,
                    assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS)).getCause());
            assertNotNull(executor.backupFailure());
        }
    }

    @Test
    void completesBackupCallbacksOutsideTheStateLock() throws Exception {
        final BlockingManager manager = new BlockingManager();
        try (final StorageBackupTaskExecutor executor =
                     StorageBackupTaskExecutor.create(new TestStorageConnection(), manager)) {
            final CompletableFuture<BackupInfo> result = executor.runBackup(BackupSlot.SCHEDULED);
            assertTrue(manager.entered.await(5, TimeUnit.SECONDS));
            final CountDownLatch observed = new CountDownLatch(1);
            final AtomicReference<Throwable> observerFailure = new AtomicReference<>();
            final CompletableFuture<Void> callback = result.thenRun(() -> {
                Thread.ofVirtual().start(() -> {
                    try {
                        assertFalse(executor.isRunningBackup());
                    } catch (final Throwable failure) {
                        observerFailure.set(failure);
                    } finally {
                        observed.countDown();
                    }
                });
                try {
                    if (!observed.await(1, TimeUnit.SECONDS)) {
                        throw new AssertionError("completion callback held the backup state lock");
                    }
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                if (observerFailure.get() != null) throw new AssertionError(observerFailure.get());
            });

            manager.release.countDown();
            assertEquals(BACKUP, result.get(5, TimeUnit.SECONDS));
            callback.get(5, TimeUnit.SECONDS);
        }
    }

    /// Verifies a closed executor accepts no new backup and tolerates a repeated close.
    @Test
    void rejectsBackupsAfterCloseAndClosesIdempotently() {
        final StorageBackupTaskExecutor executor =
                StorageBackupTaskExecutor.create(new TestStorageConnection(), new BlockingManager());
        executor.close();
        assertInstanceOf(IllegalStateException.class,
                assertThrows(ExecutionException.class,
                        () -> executor.runBackup(BackupSlot.SCHEDULED).get(5, TimeUnit.SECONDS)).getCause());
        executor.close();
    }

    @Test
    void closeWaitsWithinItsBudgetForRunningBackupAndCanRetry() throws Exception {
        final BlockingManager manager = new BlockingManager();
        final StorageBackupTaskExecutor executor =
                StorageBackupTaskExecutor.create(new TestStorageConnection(), manager, 100L);
        final CompletableFuture<BackupInfo> result = executor.runBackup(BackupSlot.SCHEDULED);
        assertTrue(manager.entered.await(5, TimeUnit.SECONDS));

        assertThrows(IllegalStateException.class, executor::close,
                "close must fail after its configured drain budget rather than wait forever");
        assertTrue(executor.isRunningBackup());
        manager.release.countDown();
        assertEquals(BACKUP, result.get(5, TimeUnit.SECONDS));
        assertFalse(executor.isRunningBackup());
        assertDoesNotThrow(executor::close, "close can finish after the export releases the Store");
        assertFalse(executor.isRunningBackup());
    }

    @Test
    void closeClearsQueuedBackupBeforeItsBodyCanEnter() {
        final BlockingManager manager = new BlockingManager();
        final PausedExecutor worker = new PausedExecutor();
        final StorageBackupTaskExecutor.Default executor = new StorageBackupTaskExecutor.Default(
                new TestStorageConnection(), manager, 25L, worker);
        final CompletableFuture<BackupInfo> queued = executor.runBackup(BackupSlot.SCHEDULED);
        assertTrue(executor.isRunningBackup());
        assertFalse(executor.isBackupExecuting());

        executor.close();

        assertFalse(executor.isRunningBackup());
        assertFalse(executor.isBackupExecuting());
        assertThrows(CancellationException.class, queued::join);
        assertEquals(0, manager.runs.get(), "close must prevent a queued body from entering Store export");
    }

    @Test
    void rejectedSubmissionRestoresIdlePhase() throws Exception {
        final ExecutorService rejected = Executors.newSingleThreadExecutor();
        rejected.shutdown();
        final StorageBackupTaskExecutor.Default executor = new StorageBackupTaskExecutor.Default(
                new TestStorageConnection(), new BlockingManager(), 100L, rejected);
        try (executor) {
            final CompletableFuture<BackupInfo> result = executor.runBackup(BackupSlot.SCHEDULED);
            assertInstanceOf(RejectedExecutionException.class,
                    assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS)).getCause());
            assertFalse(executor.isRunningBackup());
        }
    }

    @Test
    void propagatesFatalSubmissionErrorAfterRestoringIdlePhase() {
        final Error fatal = new AssertionError("fatal thread creation failure");
        final ExecutorService executorService = Executors.newSingleThreadExecutor(ignored -> { throw fatal; });
        try (final StorageBackupTaskExecutor.Default executor = new StorageBackupTaskExecutor.Default(
                new TestStorageConnection(), new BlockingManager(), 100L, executorService)) {
            assertSame(fatal, assertThrows(AssertionError.class,
                    () -> executor.runBackup(BackupSlot.SCHEDULED)));
            assertFalse(executor.isRunningBackup());
        }
    }

    /// Holds the submitted body until close cancels its future, then invokes the body to model late worker entry.
    private static final class PausedExecutor extends AbstractExecutorService {
        private Runnable task;
        private boolean shutdown;
        private boolean terminated;

        @Override
        public void execute(final Runnable command) {
            if (this.shutdown) throw new RejectedExecutionException();
            this.task = command;
        }

        @Override
        public Future<?> submit(final Runnable command) {
            if (this.shutdown) throw new RejectedExecutionException();
            this.task = command;
            return new FutureTask<>(command, null);
        }

        @Override
        public void shutdown() {
            this.shutdown = true;
            if (this.task != null) this.task.run();
            this.terminated = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            this.shutdown();
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return this.shutdown;
        }

        @Override
        public boolean isTerminated() {
            return this.terminated;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) {
            return this.terminated;
        }
    }
}
