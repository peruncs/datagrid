package peruncs.datagrid.cluster.nodelibrary.backup;


import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peruncs.datagrid.cluster.nodelibrary.store.StorageTaskExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.eclipse.serializer.util.X.notNull;

/// This executor runs backups and storage checks without blocking a request.
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
    static StorageBackupTaskExecutor New(final StorageConnection connection, final StorageBackupManager backupManager) {
        return new Default(notNull(connection), notNull(backupManager));
    }

        /// Starts a backup task.
    ///
    /// @param useManualSlot whether to use the manual slot
    void runBackup(boolean useManualSlot);

        /// Reports whether a backup task is running.
    ///
    /// @return `true` when running
    boolean isRunningBackup();

        /// Provides one backup thread and the inherited storage-check thread.
    final class Default extends StorageTaskExecutor.Abstract implements StorageBackupTaskExecutor {
        private static final Logger LOG = LoggerFactory.getLogger(StorageBackupTaskExecutor.class);
        private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;
        private final StorageBackupManager backupManager;
        private final ExecutorService backupExecutor;

        private Future<?> backupTask;
        private boolean backupCloseRequested;

        private Default(final StorageConnection connection, final StorageBackupManager backupManager) {
            super(connection);
            this.backupManager = backupManager;
            this.backupExecutor = Executors.newSingleThreadExecutor(task ->
            {
                final Thread thread = new Thread(task, "EclipseStore-StorageBackup");
                thread.setDaemon(true);
                return thread;
            });
        }

        @Override
        public synchronized void runBackup(final boolean useManualSlot) {
            if (this.backupCloseRequested) throw new IllegalStateException("Storage backup task executor is closed");
            if (this.backupTask != null && !this.backupTask.isDone()) {
                throw new IllegalStateException("Storage backup is already running");
            }
            LOG.debug("Issuing new storage backup");
            this.backupTask = this.backupExecutor.submit(() ->
            {
                try {
                    this.backupManager.createStorageBackup(useManualSlot);
                } catch (final Throwable failure) {
                    LOG.error("Storage backup failed", failure);
                }
            });
        }

        @Override
        public synchronized boolean isRunningBackup() {
            return this.backupTask != null && !this.backupTask.isDone();
        }

        /// Stops the backup executor, cancelling a running backup first.
        ///
        /// A second call is a no-op once closing was requested. An
        /// overrunning backup is interrupted after a bounded wait, and any
        /// failure is aggregated with the inherited storage-check shutdown
        /// instead of masking it.
        @Override
        public void close() {
            final Future<?> task;
            synchronized (this) {
                if (this.backupCloseRequested) return;
                this.backupCloseRequested = true;
                task = this.backupTask;
            }
            Throwable failure = null;
            if (task != null) task.cancel(true);
            this.backupExecutor.shutdownNow();
            try {
                if (!this.backupExecutor.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    failure = new IllegalStateException(
                            "Storage backup did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failure = new IllegalStateException("Interrupted while stopping storage backup", interrupted);
            }
            try {
                super.close();
            } catch (final Throwable closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
            if (failure != null) {
                if (failure instanceof Error error)
                    throw error;
                throw (RuntimeException) failure;
            }
        }
    }
}
