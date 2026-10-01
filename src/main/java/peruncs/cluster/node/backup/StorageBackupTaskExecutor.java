package peruncs.cluster.node.backup;

import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.api.BackupInfo;
import peruncs.cluster.api.BackupSlot;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.errors.BackupBusyException;
import peruncs.cluster.node.CloseSequencer;
import peruncs.cluster.node.store.StorageTaskExecutor;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static org.eclipse.serializer.util.X.notNull;

/// Runs backups and storage checks without blocking a request.
///
/// At most one backup thread is active. A concurrent request is rejected
/// explicitly instead of being silently discarded, so callers can retry or
/// report the busy state to an operator.
///
/// An interface so the maintenance scheduler and the backup node manager can be tested against a
/// controllable fake (`NodeMaintenanceSchedulerTest`, `BackupNodeManagerTest`).
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
        return new Default(notNull(connection), notNull(backupManager), closeTimeoutMillis,
                NodeConfig.Operations.DEFAULT.storageCheckCloseTimeout());
    }

    /// Creates both executors with their respective bounded close waits.
    ///
    /// @param connection Store connection
    /// @param backupManager backup manager
    /// @param closeTimeoutMillis backup executor close wait
    /// @param operations configured maintenance bounds
    /// @return task executor
    static StorageBackupTaskExecutor create(final StorageConnection connection,
                                            final StorageBackupManager backupManager,
                                            final long closeTimeoutMillis,
                                            final NodeConfig.Operations operations) {
        return new Default(notNull(connection), notNull(backupManager), closeTimeoutMillis,
                Objects.requireNonNull(operations, "operations").storageCheckCloseTimeout());
    }

    /// Starts a backup and returns its eventual result.
    ///
    /// @param slot scheduled or manual retention slot
    /// @return a future completed with backup details or exceptionally if busy, closed, rejected, or failed
    /// @throws Error if submission fails fatally; the single-flight state is reset first
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
        /* Serializes the phase, task, and close-state fields below. Never run
         * Store work, wait for shutdown, or complete user futures while held. */
        private final LockedExecutor state = LockedExecutor.New();

        private Future<?> backupTask;
        private CompletableFuture<BackupInfo> backupResult;
        private final AtomicReference<Throwable> backupFailure = new AtomicReference<>();
        private final AtomicLong lastSuccessEpochMillis = new AtomicLong(-1L);
        private BackupPhase phase = BackupPhase.IDLE;
        private boolean backupClosing;
        private boolean backupClosed;

        private enum BackupPhase { IDLE, QUEUED, RUNNING }

        private Default(final StorageConnection connection, final StorageBackupManager backupManager) {
            this(connection, backupManager, 60_000L, NodeConfig.Operations.DEFAULT.storageCheckCloseTimeout());
        }

        private Default(final StorageConnection connection, final StorageBackupManager backupManager, final long closeTimeoutMillis) {
            this(connection, backupManager, closeTimeoutMillis,
                    NodeConfig.Operations.DEFAULT.storageCheckCloseTimeout());
        }

        private Default(final StorageConnection connection, final StorageBackupManager backupManager,
                        final long closeTimeoutMillis, final Duration storageCheckCloseTimeout) {
            this(connection, backupManager, closeTimeoutMillis, storageCheckCloseTimeout,
                    Executors.newSingleThreadExecutor(runnable ->
                            Thread.ofPlatform().daemon().name("peruncs-storage-backup").unstarted(runnable)));
        }

        Default(final StorageConnection connection,
                final StorageBackupManager backupManager,
                final long closeTimeoutMillis,
                final ExecutorService backupExecutor) {
            this(connection, backupManager, closeTimeoutMillis,
                    NodeConfig.Operations.DEFAULT.storageCheckCloseTimeout(), backupExecutor);
        }

        private Default(final StorageConnection connection,
                        final StorageBackupManager backupManager,
                        final long closeTimeoutMillis,
                        final Duration storageCheckCloseTimeout,
                        final ExecutorService backupExecutor) {
            if (closeTimeoutMillis <= 0L) throw new IllegalArgumentException("closeTimeoutMillis must be positive");
            this.storageChecks = StorageTaskExecutor.create(connection, storageCheckCloseTimeout);
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
        public CompletableFuture<BackupInfo> runBackup(final BackupSlot slot) {
            Objects.requireNonNull(slot, "slot");
            final CompletableFuture<BackupInfo> result = new CompletableFuture<>();
            final RuntimeException rejection = this.state.write(() -> {
                if (this.backupClosing || this.backupClosed) {
                    return new IllegalStateException("Storage backup task executor is closed");
                }
                if (this.phase != BackupPhase.IDLE) {
                    return new BackupBusyException("Storage backup is already running");
                }
                LOGGER.log(System.Logger.Level.DEBUG, "Issuing new storage backup");
                this.backupResult = result;
                this.phase = BackupPhase.QUEUED;
                return null;
            });
            if (rejection != null) return CompletableFuture.failedFuture(rejection);
            try {
                final Future<?> task = this.backupExecutor.submit(() -> this.runBackup(slot, result));
                final boolean cancelTask = this.state.write(() -> {
                    if (this.phase == BackupPhase.QUEUED && this.backupResult == result) {
                        this.backupTask = task;
                        return false;
                    }
                    /* close() may have cancelled this queued run while
                     * submit() was outside the state lock. */
                    return true;
                });
                if (cancelTask) task.cancel(false);
            } catch (final RuntimeException | Error failure) {
                this.state.write(() -> {
                    if (this.backupResult == result) {
                        this.phase = BackupPhase.IDLE;
                        this.backupTask = null;
                        this.backupResult = null;
                    }
                });
                if (failure instanceof Error error) throw error;
                result.completeExceptionally(failure);
            }
            return result;
        }

        private void runBackup(final BackupSlot slot, final CompletableFuture<BackupInfo> result) {
            final boolean entered = this.state.write(() -> {
                if (this.phase != BackupPhase.QUEUED || this.backupResult != result) return false;
                this.phase = BackupPhase.RUNNING;
                return true;
            });
            if (!entered) return;
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
                this.state.write(() -> {
                    this.phase = BackupPhase.IDLE;
                    this.backupTask = null;
                    this.backupResult = null;
                });
                if (failure == null) result.complete(backup);
                else result.completeExceptionally(failure);
            }
            if (failure instanceof Error fatalFailure) throw fatalFailure;
        }

        @Override
        public boolean isRunningBackup() {
            return this.state.read(() -> this.phase != BackupPhase.IDLE);
        }

        @Override
        public boolean isBackupExecuting() {
            return this.state.read(() -> this.phase == BackupPhase.RUNNING);
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
            final CloseState closeState = this.state.write(() -> {
                final boolean closeBackup = !this.backupClosed;
                if (closeBackup) {
                    this.backupClosing = true;
                    if (this.backupTask != null) this.backupTask.cancel(false);
                    CompletableFuture<BackupInfo> cancelledResult = null;
                    if (this.phase == BackupPhase.QUEUED) {
                        this.phase = BackupPhase.IDLE;
                        cancelledResult = this.backupResult;
                        this.backupTask = null;
                        this.backupResult = null;
                    }
                    return new CloseState(true, cancelledResult);
                }
                return new CloseState(false, null);
            });
            if (closeState.cancelledResult() != null) {
                closeState.cancelledResult().completeExceptionally(
                        new CancellationException("queued backup cancelled during close"));
            }
            Throwable failure = null;
            if (closeState.closeBackup()) {
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
                    this.state.write(() -> {
                        this.backupClosed = true;
                        this.backupClosing = false;
                    });
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

        private record CloseState(boolean closeBackup, CompletableFuture<BackupInfo> cancelledResult) {
        }
    }
}
