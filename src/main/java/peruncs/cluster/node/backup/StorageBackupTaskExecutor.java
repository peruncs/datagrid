package peruncs.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.node.CloseSequencer;
import peruncs.cluster.node.store.StorageTaskExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    /// Starts a backup if no backup is currently active.
    ///
    /// @param useManualSlot whether to use the manual slot
    /// @return the submission result; `BUSY` is an explicit, retryable outcome
    BackupStartResult runBackup(boolean useManualSlot);

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
            if (this.runBackup(false) == BackupStartResult.BUSY) {
                logger.log(INFO, "Skipping scheduled backup because one is already running");
            }
        };
    }

    /// Describes whether a backup request was accepted by the single-flight executor.
    enum BackupStartResult {
        /// The backup task was submitted.
        STARTED,
        /// Another backup is still running; no task was queued.
        BUSY
    }

        /// Reports whether a backup task is running.
    ///
    /// @return `true` when running
    boolean isRunningBackup();

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

        /// Reports the most recent post-publication maintenance failure, if any.
    ///
    /// Pruning and replication-retention failures happen after the archive is
    /// already durable, so they are exposed separately from
    /// [#backupFailure()] and never mark a completed backup as failed.
    ///
    /// @return the most recent maintenance failure, or `null`
    default Throwable maintenanceFailure() {
        return null;
    }

    /// Provides one virtual backup executor and the inherited storage-check executor.
    final class Default implements StorageBackupTaskExecutor {
        private static final System.Logger LOGGER = System.getLogger(StorageBackupTaskExecutor.class.getName());
        private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;
        private final StorageTaskExecutor storageChecks;
        private final StorageBackupManager backupManager;
        private final ExecutorService backupExecutor;

        private Future<?> backupTask;
        private final AtomicReference<Throwable> backupFailure = new AtomicReference<>();
        private boolean backupClosing;
        private boolean backupClosed;

        private Default(final StorageConnection connection, final StorageBackupManager backupManager) {
            this.storageChecks = StorageTaskExecutor.create(connection);
            this.backupManager = backupManager;
            this.backupExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual()
                    .name("EclipseStore-StorageBackup", 0L)
                    .factory());
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
        public synchronized BackupStartResult runBackup(final boolean useManualSlot) {
            if (this.backupClosing || this.backupClosed) {
                throw new IllegalStateException("Storage backup task executor is closed");
            }
            if (this.backupTask != null && !this.backupTask.isDone()) {
                return BackupStartResult.BUSY;
            }
            LOGGER.log(System.Logger.Level.DEBUG, "Issuing new storage backup");
            this.backupTask = this.backupExecutor.submit(() ->
            {
                /* Mark the running status out of the body: cancel(false) on a
                 * queued task never runs it, so this flag is the only signal
                 * the close stage can trust — it sets on task start, so a
                 * cancelled-queued task keeps the Store-close stage free. */
                this.backupRunning = true;
                try {
                    this.backupManager.createStorageBackup(useManualSlot);
                    this.backupFailure.set(null);
                } catch (final Exception failure) {
                    this.backupFailure.set(failure);
                    LOGGER.log(ERROR, "Storage backup failed", failure);
                } catch (final Error failure) {
                    this.backupFailure.set(failure);
                    LOGGER.log(ERROR, "Fatal storage-backup failure", failure);
                    throw failure;
                } finally {
                    this.backupRunning = false;
                }
            });
            return BackupStartResult.STARTED;
        }

        /* cancel(false) marks the future done while the callable keeps
         * running, so isDone() is not evidence that the export has exited.
         * Track actual execution: the flag flips only when the task body
         * returns or throws. */
        private volatile boolean backupRunning;

        @Override
        public synchronized boolean isRunningBackup() {
            return this.backupRunning;
        }

        @Override
        public Throwable backupFailure() {
            return this.backupFailure.get();
        }

        @Override
        public Throwable maintenanceFailure() {
            return this.backupManager.maintenanceFailure();
        }

        /// Stops the backup executor, cancelling a running backup first.
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
            final Future<?> task;
            final boolean closeBackup;
            synchronized (this) {
                closeBackup = !this.backupClosed;
                if (closeBackup) this.backupClosing = true;
                task = this.backupTask;
            }
            Throwable failure = null;
            if (closeBackup) {
                if (task != null) task.cancel(false);
                this.backupExecutor.shutdown();
                try {
                    if (!this.backupExecutor.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                        failure = new IllegalStateException("Storage backup did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
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
