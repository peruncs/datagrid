package peruncs.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.api.BackupInfo;
import peruncs.cluster.api.BackupSlot;
import peruncs.cluster.errors.BackupBusyException;
import peruncs.cluster.node.CloseSequencer;
import peruncs.cluster.node.store.StorageTaskExecutor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Objects;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static org.eclipse.serializer.util.X.notNull;

/// Runs backups and storage checks without blocking a request.
///
/// At most one backup thread is active. A concurrent request is rejected
/// explicitly instead of being silently discarded, so callers can retry or
/// report the busy state to an operator.
public interface StorageBackupTaskExecutor extends StorageTaskExecutor {
    /// Creates a backup task executor.
    ///
    /// @param connection    Store connection
    /// @param backupManager backup manager
    /// @return task executor
    static StorageBackupTaskExecutor create(final StorageConnection connection, final StorageBackupManager backupManager) {
        return new Default(notNull(connection), notNull(backupManager));
    }

    /// Creates a backup task executor with a bounded close wait.
    static StorageBackupTaskExecutor create(final StorageConnection connection,
                                            final StorageBackupManager backupManager,
                                            final long closeTimeoutMillis) {
        return new Default(notNull(connection), notNull(backupManager), closeTimeoutMillis);
    }

    /// Starts a backup and returns its eventual result.
    ///
    /// @param slot scheduled or manual retention slot
    /// @return a future completed with backup details or exceptionally if busy or failed
    CompletableFuture<BackupInfo> runBackup(BackupSlot slot);

    /// Creates the periodic full-backup task.
    ///
    /// The task uses the automatic backup slot of this shared single-flight
    /// executor. A run while another backup is active is skipped instead of
    /// queuing behind it.
    ///
    /// @return backup task for maintenance scheduling
    default Runnable createScheduledWork() {
        /* Looked up once when the task is created rather than on every run. The
         * interface cannot hold static state, and the test double inherits
         * this single implementation. */
        final System.Logger logger = System.getLogger(StorageBackupTaskExecutor.class.getName());
        return () ->
        {
            logger.log(INFO, "Issuing full backup");
            this.runBackup(BackupSlot.SCHEDULED).whenComplete((backup, failure) -> {
                if (failure instanceof BackupBusyException) {
                    logger.log(INFO, "Skipping scheduled backup because one is already running");
                } else if (failure != null) {
                    logger.log(ERROR, "Scheduled storage backup failed", failure);
                }
            });
        };
    }

    /// Reports whether a backup is queued or running.
    ///
    /// @return `true` while the single-flight slot is occupied
    boolean isRunningBackup();

    /// Reports whether the backup body entered storage export and has not exited.
    ///
    /// @return `true` while the Store export is executing
    boolean isBackupExecuting();

    /// Reports the failure of the most recently completed backup, if any.
    ///
    /// A failed asynchronous task must remain observable by health checks and
    /// operators; logging it and allowing the executor future to complete
    /// normally would make the failure indistinguishable from success. A
    /// successful publication followed by a maintenance failure is not
    /// reported here; see [#maintenanceFailure()].
    ///
    /// @return the most recent backup failure, or `null` after a successful backup
    Throwable backupFailure();

    /// Returns when the most recent backup succeeded, or -1 if none has.
    long lastSuccessEpochMillis();

    /// Reports the most recent post-publication maintenance failure, if any.
    ///
    /// Pruning and replication-retention failures happen after the archive is
    /// already durable, so they are exposed separately from
    /// [#backupFailure()] and never mark a completed backup as failed.
    ///
    /// @return the most recent maintenance failure, or `null`
    Throwable maintenanceFailure();

    /// Provides one virtual backup executor and the inherited storage-check executor.
    final class Default implements StorageBackupTaskExecutor {
        private static final System.Logger LOGGER = System.getLogger(StorageBackupTaskExecutor.class.getName());
        private final StorageTaskExecutor storageChecks;
        private final StorageBackupManager backupManager;
        private final ExecutorService backupExecutor;
        private final long closeTimeoutMillis;

        private Future<?> backupTask;
        private CompletableFuture<BackupInfo> backupResult;
        private final AtomicReference<Throwable> backupFailure = new AtomicReference<>();
        private final AtomicLong lastSuccessEpochMillis = new AtomicLong(-1L);
        private BackupPhase phase = BackupPhase.IDLE;
        private boolean backupClosing;
        private boolean backupClosed;

        private enum BackupPhase { IDLE, QUEUED, RUNNING }

        private Default(final StorageConnection connection, final StorageBackupManager backupManager) {
            this(connection, backupManager, 60_000L);
        }

        private Default(final StorageConnection connection, final StorageBackupManager backupManager, final long closeTimeoutMillis) {
            this(connection, backupManager, closeTimeoutMillis, Executors.newSingleThreadExecutor(Thread.ofVirtual()
                    .name("EclipseStore-StorageBackup", 0L)
                    .factory()));
        }

        Default(final StorageConnection connection,
                final StorageBackupManager backupManager,
                final long closeTimeoutMillis,
                final ExecutorService backupExecutor) {
            if (closeTimeoutMillis <= 0L) throw new IllegalArgumentException("closeTimeoutMillis must be positive");
            this.storageChecks = StorageTaskExecutor.create(connection);
            this.backupManager = backupManager;
            this.closeTimeoutMillis = closeTimeoutMillis;
            this.backupExecutor = Objects.requireNonNull(backupExecutor, "backupExecutor");
        }

        @Override
        public void runChecks() {
            this.storageChecks.runChecks();
        }

        @Override
        public boolean isRunningChecks() {
            return this.storageChecks.isRunningChecks();
        }

        @Override
        public Throwable failure() {
            return this.storageChecks.failure();
        }

        @Override
        public synchronized CompletableFuture<BackupInfo> runBackup(final BackupSlot slot) {
            Objects.requireNonNull(slot, "slot");
            if (this.backupClosing || this.backupClosed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Storage backup task executor is closed"));
            }
            if (this.phase != BackupPhase.IDLE) {
                return CompletableFuture.failedFuture(new BackupBusyException("Storage backup is already running"));
            }
            LOGGER.log(System.Logger.Level.DEBUG, "Issuing new storage backup");
            final CompletableFuture<BackupInfo> result = new CompletableFuture<>();
            this.backupResult = result;
            this.phase = BackupPhase.QUEUED;
            try {
                this.backupTask = this.backupExecutor.submit(() -> {
                    synchronized (this) {
                        if (this.phase != BackupPhase.QUEUED) return;
                        this.phase = BackupPhase.RUNNING;
                    }
                    BackupInfo backup = null;
                    Throwable failure = null;
                    try {
                        backup = this.backupManager.createStorageBackup(slot);
                        this.backupFailure.set(null);
                        this.lastSuccessEpochMillis.set(System.currentTimeMillis());
                    } catch (final Exception backupFailure) {
                        failure = backupFailure;
                        this.backupFailure.set(backupFailure);
                        LOGGER.log(ERROR, "Storage backup failed", backupFailure);
                    } catch (final Error fatalFailure) {
                        failure = fatalFailure;
                        this.backupFailure.set(fatalFailure);
                        LOGGER.log(ERROR, "Fatal storage-backup failure", fatalFailure);
                    } finally {
                        synchronized (this) {
                            this.phase = BackupPhase.IDLE;
                            this.backupTask = null;
                            this.backupResult = null;
                            if (failure == null) result.complete(backup);
                            else result.completeExceptionally(failure);
                        }
                    }
                    if (failure instanceof Error fatalFailure) throw fatalFailure;
                });
            } catch (final RuntimeException | Error failure) {
                this.phase = BackupPhase.IDLE;
                this.backupTask = null;
                this.backupResult = null;
                result.completeExceptionally(failure);
            }
            return result;
        }

        @Override
        public synchronized boolean isRunningBackup() {
            return this.phase != BackupPhase.IDLE;
        }

        @Override
        public synchronized boolean isBackupExecuting() {
            return this.phase == BackupPhase.RUNNING;
        }

        @Override
        public Throwable backupFailure() {
            return this.backupFailure.get();
        }

        @Override
        public long lastSuccessEpochMillis() {
            return this.lastSuccessEpochMillis.get();
        }

        @Override
        public Throwable maintenanceFailure() {
            return this.backupManager.maintenanceFailure();
        }

        /// Stops the backup executor, cancelling queued work and draining an active export.
        ///
        /// Shutdown is retry-until-clean, matching [StorageTaskExecutor]: after
        /// the first call no new backup is accepted, a bounded wait failure is
        /// rethrown, and a later call retries the wait instead of losing the
        /// resource. A running export is never interrupted: Eclipse Store may
        /// swallow interruption and leave a partial image that looks complete.
        /// The executor is allowed to drain the export, then storage checks are
        /// stopped; failures from both phases are aggregated.
        @Override
        public void close() {
            final boolean closeBackup;
            synchronized (this) {
                closeBackup = !this.backupClosed;
                if (closeBackup) {
                    this.backupClosing = true;
                    if (this.backupTask != null) this.backupTask.cancel(false);
                    if (this.phase == BackupPhase.QUEUED) {
                        this.phase = BackupPhase.IDLE;
                        if (this.backupResult != null) {
                            this.backupResult.completeExceptionally(
                                    new CancellationException("queued backup cancelled during close"));
                        }
                        this.backupResult = null;
                    }
                }
            }
            Throwable failure = null;
            if (closeBackup) {
                this.backupExecutor.shutdown();
                try {
                    if (!this.backupExecutor.awaitTermination(this.closeTimeoutMillis, TimeUnit.MILLISECONDS)) {
                        failure = new IllegalStateException(
                                "Storage backup did not stop within %s ms".formatted(this.closeTimeoutMillis));
                    }
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure = new IllegalStateException("Interrupted while stopping storage backup", interrupted);
                }
                if (failure == null) {
                    synchronized (this) {
                        this.backupClosed = true;
                        this.backupClosing = false;
                    }
                }
            }
            try {
                this.storageChecks.close();
            } catch (final Throwable closeFailure) {
                failure = CloseSequencer.append(failure, closeFailure);
            }
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException runtime) throw runtime;
        }
    }
}
