package peruncs.datagrid.cluster.nodelibrary.backup;

import org.eclipse.serializer.concurrency.XThreads;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterStorageBinaryDataClient;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursor;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationLogRetention;
import peruncs.datagrid.storage.distributed.types.ReplicationRetry;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/**
 * This manager creates, lists, downloads, and deletes storage backups.
 *
 * <p>Backup creation is single-flight because it temporarily stops the
 * replication reader and captures the current message position. Read-only
 * listing and download operations do not wait for that lock.</p>
 */
public interface StorageBackupManager {
    /**
     * Creates a backup manager.
     *
     * @param storageConnection    Store connection
     * @param maxBackupCount       maximum backup count
     * @param storageBackupBackend backup backend
     * @param cursorSupplier       replication cursor supplier
     * @param dataClient           replication client
     * @param retention            log retention policy
     * @return backup manager
     */
    static StorageBackupManager New(
            final StorageConnection storageConnection,
            final int maxBackupCount,
            final StorageBackupBackend storageBackupBackend,
            final Supplier<ReplicationCursor> cursorSupplier,
            final ClusterStorageBinaryDataClient dataClient,
            final ReplicationLogRetention retention
    ) {
        return new Default(
                notNull(storageConnection),
                positive(maxBackupCount),
                notNull(storageBackupBackend),
                notNull(cursorSupplier),
                notNull(dataClient),
                notNull(retention)
        );
    }

    /**
     * Creates a storage backup.
     *
     * @param useManualSlot whether to use the manual slot
     * @throws NodelibraryException if backup creation fails
     */
    void createStorageBackup(boolean useManualSlot) throws NodelibraryException;

    /**
     * Downloads the latest backup.
     *
     * @param targetRootPath destination root
     * @throws NodelibraryException if download fails
     */
    void downloadLatestBackup(Path targetRootPath) throws NodelibraryException;

    /**
     * Lists available backups.
     *
     * @return backup metadata
     * @throws NodelibraryException if listing fails
     */
    List<BackupMetadata> listBackups() throws NodelibraryException;

    /**
     * Deletes one backup.
     *
     * @param backup backup to delete
     * @throws NodelibraryException if deletion fails
     */
    void deleteBackup(BackupMetadata backup) throws NodelibraryException;

    /**
     * Downloads one backup.
     *
     * @param storageDestinationParentPath destination parent
     * @param backup                       backup to download
     * @throws NodelibraryException if download fails
     */
    void downloadBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodelibraryException;

    /**
     * Reports whether user storage exists.
     *
     * @return {@code true} when user storage exists
     * @throws NodelibraryException if the check fails
     */
    boolean hasUserUploadedStorage() throws NodelibraryException;

    /**
     * Downloads user storage.
     *
     * @param storageDestinationParentPath destination parent
     * @throws NodelibraryException if download fails
     */
    void downloadUserUploadedStorage(Path storageDestinationParentPath) throws NodelibraryException;

    /**
     * Deletes user storage.
     *
     * @throws NodelibraryException if deletion fails
     */
    void deleteUserUploadedStorage() throws NodelibraryException;

    /** Implements the stop, backup, retention, and resume sequence. */
    class Default implements StorageBackupManager {
        private static final Logger LOG = LoggerFactory.getLogger(StorageBackupManager.class);
        private static final long STOP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(1);
        private static final int RETENTION_RETRY_ATTEMPTS = 3;
        private static final long RETENTION_RETRY_DELAY_MILLIS = 100L;

        private final StorageConnection storageConnection;
        private final int maxBackupCount;
        private final StorageBackupBackend backend;
        private final Supplier<ReplicationCursor> cursorSupplier;
        private final ClusterStorageBinaryDataClient dataClient;
        private final ReplicationLogRetention retention;
        private final ReentrantLock backupLock = new ReentrantLock();

        private Default(
                final StorageConnection storageConnection,
                final int maxBackupCount,
                final StorageBackupBackend backupBackend,
                final Supplier<ReplicationCursor> cursorSupplier,
                final ClusterStorageBinaryDataClient dataClient,
                final ReplicationLogRetention retention
        ) {
            this.storageConnection = storageConnection;
            this.maxBackupCount = maxBackupCount;
            this.backend = backupBackend;
            this.cursorSupplier = cursorSupplier;
            this.dataClient = dataClient;
            this.retention = retention;
        }

        @Override
        public void createStorageBackup(final boolean useManualSlot) throws NodelibraryException {
            this.backupLock.lock();
            try {
                LOG.trace("Creating new storage backup");

                final List<BackupMetadata> backups = this.listBackups();
                final long timestamp = this.nextBackupTimestamp(backups, useManualSlot);
                final var newBackup = new BackupMetadata(timestamp, useManualSlot);
                final RuntimeException readerFailure = this.dataClient.failure();
                if (readerFailure != null) {
                    throw new IllegalStateException("Cannot create backup after replication reader failure", readerFailure);
                }

                final boolean isRunning = this.dataClient.isRunning();

                if (isRunning) {
                    this.stopDataClient();
                } else {
                    /* A reader can report isRunning()==false while a timed-out stop still
                     * owns a live polling thread.  Never treat that intermediate state as a
                     * safe backup boundary.  STOPPED/NOT_STARTED remain valid for simple
                     * clients that never expose a stop-at-latest operation. */
                    final ClusterStorageBinaryDataClient.StopOutcome outcome = this.dataClient.stopResult().outcome();
                    if (outcome == ClusterStorageBinaryDataClient.StopOutcome.STOPPING ||
                        outcome == ClusterStorageBinaryDataClient.StopOutcome.TIMED_OUT ||
                        outcome == ClusterStorageBinaryDataClient.StopOutcome.FAILED) {
                        throw new IllegalStateException(
                                "Cannot create backup while replication reader stop is unresolved: " + outcome);
                    }
                }

                Throwable operationFailure = null;
                try {
                    this.backend.createAndUploadBackup(this.storageConnection, this.cursorSupplier.get(), newBackup);

                    /* Prune only after the new backup is durable.  If upload fails, every
                     * previously recoverable backup remains available for recovery. */
                    if (!backups.isEmpty()) {
                        if (useManualSlot) {
                            backups.stream().filter(BackupMetadata::manualSlot).forEach(this::deleteBackup);
                        } else {
                            // just in case there are multiple backups too many
                            final int toDeleteCount = (int) backups.stream().filter(b -> !b.manualSlot()).count()
                                                      - this.maxBackupCount + 1;
                            LOG.debug("Deleting {} oldest backup(s)", toDeleteCount);
                            final List<BackupMetadata> nonManual = backups.stream()
                                    .filter(b -> !b.manualSlot())
                                    .sorted(Comparator.comparingLong(BackupMetadata::timestamp))
                                    .toList();
                            for (int i = 0; i < Math.max(0, toDeleteCount) && i < nonManual.size(); i++) {
                                this.deleteBackup(nonManual.get(i));
                            }
                        }
                    }

                    if (!useManualSlot) {
                        // delete up to the previous backup to save on replication log storage
                        if (this.retention.isSupported()) {
                            this.backend.getCursorFromPreviousBackup(1)
                                    .ifPresent(info ->
                                    {
                                        final ReplicationLogRetention.MaintenanceResult result =
                                                this.deleteThroughWithReplayRetry(info);
                                        switch (result.status()) {
                                            case DELETED -> LOG.debug("Replication retention deleted Archive history through {}", result.position());
                                            case NOTHING_TO_DELETE -> LOG.debug("Replication retention found no complete Archive segment to delete");
                                            case DEFERRED_ACTIVE_REPLAY -> LOG.warn(
                                                    "Replication retention deferred because an Archive replay is active at {}", result.position());
                                        }
                                    });
                        } else {
                            LOG.warn("Replication retention is unsupported; preserving Archive history");
                        }
                    }
                } catch (final RuntimeException | Error failure) {
                    operationFailure = failure;
                    throw failure;
                } finally {
                    // only resume if the data client was running previously
                    /* A stop timeout records a terminal reader failure and deliberately leaves
                     * the client stopped.  Calling resume() from this finally block would mask
                     * the original backup error and race a still-draining poller. */
                    if (isRunning && this.dataClient.failure() == null &&
                        this.dataClient.stopResult().outcome() == ClusterStorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY) {
                        try {
                            this.dataClient.resume();
                        } catch (final RuntimeException | Error resumeFailure) {
                            /* Never hide a failed backup behind a shutdown/resume error;
                             * preserve both causes for operators and retry logic. */
                            if (operationFailure != null) operationFailure.addSuppressed(resumeFailure);
                            else throw resumeFailure;
                        }
                    }
                }
            } finally {
                this.backupLock.unlock();
            }
        }

        private long nextBackupTimestamp(final List<BackupMetadata> backups, final boolean manualSlot) {
            long timestamp = System.currentTimeMillis();
            for (final BackupMetadata backup : backups) {
                if (backup.manualSlot() == manualSlot && backup.timestamp() >= timestamp) {
                    timestamp = backup.timestamp() == Long.MAX_VALUE ? Long.MAX_VALUE : backup.timestamp() + 1L;
                }
            }
            if (timestamp == Long.MAX_VALUE) {
                throw new NodelibraryException("No unique timestamp is available for a new backup");
            }
            return timestamp;
        }

        @Override
        public void downloadLatestBackup(final Path targetRootPath) {
            final var backup = this.backend.latestBackup(false);
            if (backup == null) {
                throw new NodelibraryException("No backups are available to download");
            }
            this.downloadBackup(targetRootPath, backup);
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodelibraryException {
            this.backend.deleteBackup(backup);
        }

        @Override
        public void deleteUserUploadedStorage() throws NodelibraryException {
            this.backend.deleteUserUploadedStorage();
        }

        @Override
        public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
                throws NodelibraryException {
            this.backend.downloadBackup(storageDestinationParentPath, backup);
        }

        @Override
        public void downloadUserUploadedStorage(final Path storageDestinationParentPath)
                throws NodelibraryException {
            this.backend.downloadUserUploadedStorage(storageDestinationParentPath);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodelibraryException {
            return this.backend.hasUserUploadedStorage();
        }

        @Override
        public List<BackupMetadata> listBackups() throws NodelibraryException {
            return this.backend.listBackups();
        }

        private void stopDataClient() {
            LOG.trace("Waiting for data client to stop reading");
            this.dataClient.stopAtLatestMessage();
            final long deadline = ReplicationRetry.deadlineNanos(STOP_TIMEOUT_NANOS);
            while (true) {
                final ClusterStorageBinaryDataClient.StopResult result = this.dataClient.stopResult();
                final ClusterStorageBinaryDataClient.StopOutcome outcome = result.outcome();
                if (outcome == ClusterStorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY) {
                    return;
                }
                final RuntimeException failure = this.dataClient.failure();
                if (failure != null) {
                    throw new IllegalStateException("Cannot create backup after replication reader failure", failure);
                }
                if (outcome == ClusterStorageBinaryDataClient.StopOutcome.TIMED_OUT ||
                    outcome == ClusterStorageBinaryDataClient.StopOutcome.FAILED) {
                    throw new IllegalStateException("Cannot create backup after replication reader stop " + outcome);
                }
                if (ReplicationRetry.expired(deadline)) {
                    throw new IllegalStateException("Timed out waiting for replication reader boundary at " +
                                                    this.dataClient.cursor() + " (last resolved sequence=" + result.sequence() +
                                                    ", position=" + result.position() + ")");
                }
                XThreads.sleep(100);
            }
        }

        /**
         * Retries a purge that is temporarily blocked by an active Archive replay.
         * Replay ownership is intentionally not interrupted: the retention provider
         * returns a deferred result, the bounded retry gives a short-lived replay a
         * chance to finish, and a still-active replay is retained for the next backup
         * cycle with an explicit warning.
         */
        private ReplicationLogRetention.MaintenanceResult deleteThroughWithReplayRetry(
                final ReplicationCursor cursor) {
            ReplicationLogRetention.MaintenanceResult result = this.retention.deleteThrough(cursor);
            for (int attempt = 1; result.status() == ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY &&
                                  attempt < RETENTION_RETRY_ATTEMPTS; attempt++) {
                XThreads.sleep(RETENTION_RETRY_DELAY_MILLIS);
                result = this.retention.deleteThrough(cursor);
            }
            return result;
        }
    }
}
