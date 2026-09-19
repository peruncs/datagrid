package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationLogRetention;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/// This manager creates, lists, restores, and deletes storage backups.
///
/// Backup creation stops the replication reader, captures the current message
/// position, publishes one archive, resumes the reader, and then runs
/// maintenance. Creation is single-flight at [StorageBackupTaskExecutor]:
/// that executor rejects a concurrent request with `BUSY` and this manager
/// itself does not queue or lock, so callers must sequence backup requests
/// through the task executor.
public interface StorageBackupManager {
        /// Creates a backup manager.
    ///
    /// @param storageConnection    Store connection
    /// @param maxBackupCount       maximum backup count
    /// @param storageBackupBackend backup backend
    /// @param cursorSupplier       replication cursor supplier
    /// @param dataClient           replication client
    /// @param retention            log retention policy
    /// @return backup manager
    static StorageBackupManager New(
            final StorageConnection storageConnection,
            final int maxBackupCount,
            final StorageBackupBackend storageBackupBackend,
            final Supplier<ReplicationCursor> cursorSupplier,
            final StorageBinaryDataClient dataClient,
            final ReplicationLogRetention retention) {
        return new Default(
                notNull(storageConnection),
                positive(maxBackupCount),
                notNull(storageBackupBackend),
                notNull(cursorSupplier),
                notNull(dataClient),
                notNull(retention)
        );
    }

        /// Creates a storage backup.
    ///
    /// Backups are single-flight at [StorageBackupTaskExecutor]. A failed
    /// reader, or a reader stop stuck in an unresolved state, aborts the
    /// backup — a merely non-running reader is not trusted, because a
    /// timed-out stop can still own a live polling thread. Old backups are
    /// pruned only after the new one is durable, so a failed upload never
    /// destroys the last recoverable backup; log retention then advances
    /// through the previous backup.
    ///
    /// A successful publication is final: a later pruning or retention
    /// failure does not fail the backup, because the durable archive already
    /// exists. Such failures are logged and exposed through
    /// [#maintenanceFailure()], and a failure in one maintenance step does not
    /// skip the remaining steps. Retention always advances through the newest
    /// compatible backup older than the one just published, or is skipped for
    /// the manual slot.
    ///
    /// @param useManualSlot whether to use the manual slot
    /// @throws NodeLibraryException if backup creation or publication fails
    void createStorageBackup(boolean useManualSlot) throws NodeLibraryException;

        /// Lists available backups.
    ///
    /// @return backup metadata
    /// @throws NodeLibraryException if listing fails
    List<BackupMetadata> listBackups() throws NodeLibraryException;

        /// Returns the failure of the most recent post-publication maintenance
    /// step (resolving retention, pruning, or log retention), if any.
    ///
    /// The value is reset when the next backup reaches its maintenance phase
    /// and never marks a successfully published backup as failed. A `null`
    /// return means the latest maintenance phase completed cleanly or no
    /// backup has run yet.
    ///
    /// @return last maintenance failure, or `null`
    default Throwable maintenanceFailure() {
        return null;
    }

        /// Deletes one backup.
    ///
    /// @param backup backup to delete
    /// @throws NodeLibraryException if deletion fails
    void deleteBackup(BackupMetadata backup) throws NodeLibraryException;

        /// Restores one backup.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @param backup                       backup to restore
    /// @throws NodeLibraryException if restore fails
    void restoreBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodeLibraryException;

        /// Reports whether user storage exists.
    ///
    /// @return `true` when user storage exists
    /// @throws NodeLibraryException if the check fails
    boolean hasUserUploadedStorage() throws NodeLibraryException;

        /// Restores user storage.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @throws NodeLibraryException if restore fails
    void restoreUserUploadedStorage(Path storageDestinationParentPath) throws NodeLibraryException;

        /// Deletes user storage.
    ///
    /// @throws NodeLibraryException if deletion fails
    void deleteUserUploadedStorage() throws NodeLibraryException;

        /// Implements the stop, backup, retention, and resume sequence.
    class Default implements StorageBackupManager {
        private static final System.Logger LOGGER = System.getLogger(StorageBackupManager.class.getName());
        private static final long STOP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(1);
        private static final long POLL_INTERVAL_MILLIS = 100L;
        private static final int RETENTION_RETRY_ATTEMPTS = 3;
        private static final long RETENTION_RETRY_DELAY_MILLIS = 100L;

        private final StorageConnection storageConnection;
        private final int maxBackupCount;
        private final StorageBackupBackend backend;
        private final Supplier<ReplicationCursor> cursorSupplier;
        private final StorageBinaryDataClient dataClient;
        private final ReplicationLogRetention retention;
        private final AtomicReference<Throwable> maintenanceFailure = new AtomicReference<>();

        private Default(
                final StorageConnection storageConnection,
                final int maxBackupCount,
                final StorageBackupBackend backupBackend,
                final Supplier<ReplicationCursor> cursorSupplier,
                final StorageBinaryDataClient dataClient,
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
        public void createStorageBackup(final boolean useManualSlot) throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.TRACE, "Creating new storage backup");

            final List<BackupMetadata> backups = this.listBackups();
            final long timestamp = this.nextBackupTimestamp(backups, useManualSlot);
            final RuntimeException readerFailure = this.dataClient.failure();
            if (readerFailure != null) {
                throw new NodeLibraryException("Cannot create backup after replication reader failure", readerFailure);
            }

            final boolean isRunning = this.dataClient.isRunning();

            if (isRunning) {
                this.stopDataClient();
            } else {
                /* A reader can report isRunning()==false while a timed-out stop still
                 * owns a live polling thread.  Never treat that intermediate state as a
                 * safe backup boundary.  STOPPED/NOT_STARTED remain valid for simple
                 * clients that never expose a stop-at-latest operation. */
                final StorageBinaryDataClient.StopOutcome outcome = this.dataClient.stopResult().outcome();
                if (outcome == StorageBinaryDataClient.StopOutcome.STOPPING ||
                    outcome == StorageBinaryDataClient.StopOutcome.TIMED_OUT ||
                    outcome == StorageBinaryDataClient.StopOutcome.FAILED) {
                    throw new NodeLibraryException(
                            "Cannot create backup while replication reader stop is unresolved: %s".formatted(outcome));
                }
            }

            /* The manifest cursor must describe the stopped boundary, not the
             * position observed before the stop resolved. Anything captured
             * earlier can lag the boundary the backup actually quiesced at.
             * The backup generation comes from that same cursor, so a node
             * on a shared volume only ever restores its own cluster, epoch,
             * and recording. The backup id is random, so concurrent
             * publishers never share an archive name. */
            final ReplicationCursor cursor = this.cursorSupplier.get();
            final var newBackup = BackupMetadata.New(timestamp, useManualSlot, cursor);
            final var localIdentity = BackupMetadata.Identity.of(cursor);

            Throwable operationFailure = null;
            try {
                this.backend.createBackup(this.storageConnection, cursor, newBackup);
                this.runMaintenance(backups, useManualSlot, newBackup, localIdentity);
            } catch (final RuntimeException | Error failure) {
                operationFailure = failure;
                throw failure;
            } finally {
                // only resume if the data client was running previously
                /* A stop timeout records a terminal reader failure and deliberately leaves
                 * the client stopped.  Calling resume() from this finally block would mask
                 * the original backup error and race a still-draining poller. */
                if (isRunning && this.dataClient.failure() == null &&
                    this.dataClient.stopResult().outcome() == StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY) {
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
        }

        /// Runs the post-publication maintenance steps without failing the backup.
        ///
        /// Each step is independent: scanning for unreadable archives, resolving
        /// the retention cursor, pruning old archives, and advancing log
        /// retention all catch their own runtime failures, log them, and publish
        /// them through [#maintenanceFailure()]. The durable backup published
        /// before this method runs is never affected.
        private void runMaintenance(
                final List<BackupMetadata> backups,
                final boolean useManualSlot,
                final BackupMetadata created,
                final BackupMetadata.Identity localIdentity
        ) {
            this.maintenanceFailure.set(null);
            this.sweepUnreadableArchives();

            /* The retention cursor is resolved before pruning while the
             * previous archive still exists to read it from. */
            final ReplicationCursor retentionCursor;
            try {
                retentionCursor = useManualSlot ? null : this.retentionCursorExcluding(created, localIdentity);
            } catch (final RuntimeException failure) {
                this.recordMaintenanceFailure("resolve the retention cursor", failure);
                this.pruneBackups(backups, useManualSlot, localIdentity);
                return;
            }

            this.pruneBackups(backups, useManualSlot, localIdentity);
            try {
                this.advanceRetention(useManualSlot, retentionCursor);
            } catch (final RuntimeException failure) {
                this.recordMaintenanceFailure("advance replication retention", failure);
            }
        }

        /// Reports archives on the volume that can never be selected or pruned.
        ///
        /// Unreadable archives are not deleted here: a corrupt identity may
        /// still guard a recoverable image, so the sweep only makes them
        /// explicit for operators instead of losing them silently.
        private void sweepUnreadableArchives() {
            try {
                final List<String> unreadable = this.backend.listUnreadableArchives();
                if (!unreadable.isEmpty()) {
                    LOGGER.log(System.Logger.Level.WARNING,
                            "Backup volume holds %s archive(s) with untrustworthy identity that are never selected for restore: %s"
                                    .formatted(unreadable.size(), unreadable));
                }
            } catch (final RuntimeException failure) {
                this.recordMaintenanceFailure("scan for unreadable archives", failure);
            }
        }

        /// Prunes old compatible backups, reporting but not rethrowing failures.
        ///
        /// Only backups compatible with this node count: pruning must never
        /// delete another generation sharing the volume.
        private void pruneBackups(
                final List<BackupMetadata> backups,
                final boolean useManualSlot,
                final BackupMetadata.Identity localIdentity
        ) {
            try {
                final List<BackupMetadata> compatible = backups.stream()
                        .filter(backup -> backup.isCompatibleWith(localIdentity))
                        .toList();
                if (compatible.isEmpty()) {
                    return;
                }
                if (useManualSlot) {
                    compatible.stream().filter(BackupMetadata::manualSlot).forEach(this::deleteBackup);
                    return;
                }
                // just in case there are multiple backups too many
                final List<BackupMetadata> nonManual = compatible.stream()
                        .filter(backup -> !backup.manualSlot())
                        .sorted(BackupMetadata.OLDEST_FIRST)
                        .toList();
                final int toDeleteCount = nonManual.size() - this.maxBackupCount + 1;
                LOGGER.log(System.Logger.Level.DEBUG, "Deleting %s oldest backup(s)".formatted(toDeleteCount));
                for (int index = 0; index < Math.max(0, toDeleteCount) && index < nonManual.size(); index++) {
                    this.deleteBackup(nonManual.get(index));
                }
            } catch (final RuntimeException failure) {
                this.recordMaintenanceFailure("prune old backups", failure);
            }
        }

        /// Advances log retention, reporting but not rethrowing failures.
        private void advanceRetention(final boolean useManualSlot, final ReplicationCursor retentionCursor)
                throws NodeLibraryException {
            if (useManualSlot) {
                return;
            }
            if (!this.retention.isSupported()) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Replication retention is unsupported; preserving Archive history");
                return;
            }
            if (retentionCursor == null) {
                return;
            }
            final ReplicationLogRetention.MaintenanceResult result =
                    this.deleteThroughWithReplayRetry(retentionCursor);
            switch (result.status()) {
                case DELETED -> LOGGER.log(System.Logger.Level.DEBUG,
                        "Replication retention deleted Archive history through %s".formatted(result.position()));
                case NOTHING_TO_DELETE -> LOGGER.log(System.Logger.Level.DEBUG,
                        "Replication retention found no complete Archive segment to delete");
                case DEFERRED_ACTIVE_REPLAY -> LOGGER.log(System.Logger.Level.WARNING,
                        "Replication retention deferred because an Archive replay is active at %s"
                                .formatted(result.position()));
            }
        }

        private void recordMaintenanceFailure(final String operation, final RuntimeException failure) {
            this.maintenanceFailure.set(failure);
            LOGGER.log(System.Logger.Level.WARNING,
                    "Storage backup is durable, but the manager failed to %s; retention may lag"
                            .formatted(operation), failure);
        }

        @Override
        public Throwable maintenanceFailure() {
            return this.maintenanceFailure.get();
        }

        private long nextBackupTimestamp(final List<BackupMetadata> backups, final boolean manualSlot) {
            long timestamp = System.currentTimeMillis();
            for (final BackupMetadata backup : backups) {
                if (backup.manualSlot() == manualSlot && backup.timestamp() >= timestamp) {
                    timestamp = backup.timestamp() == Long.MAX_VALUE ? Long.MAX_VALUE : backup.timestamp() + 1L;
                }
            }
            if (timestamp == Long.MAX_VALUE) {
                throw new NodeLibraryException("No unique timestamp is available for a new backup");
            }
            return timestamp;
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodeLibraryException {
            this.backend.deleteBackup(backup);
        }

        @Override
        public void deleteUserUploadedStorage() throws NodeLibraryException {
            this.backend.deleteUserUploadedStorage();
        }

        @Override
        public void restoreBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
                throws NodeLibraryException {
            this.backend.restoreBackup(storageDestinationParentPath, backup);
        }

        @Override
        public void restoreUserUploadedStorage(final Path storageDestinationParentPath)
                throws NodeLibraryException {
            this.backend.restoreUserUploadedStorage(storageDestinationParentPath);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodeLibraryException {
            return this.backend.hasUserUploadedStorage();
        }

        @Override
        public List<BackupMetadata> listBackups() throws NodeLibraryException {
            return this.backend.listBackups();
        }

        private void stopDataClient() throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.TRACE, "Waiting for data client to stop reading");
            this.dataClient.stopAtLatestMessage();
            final long deadline = ReplicationRetry.deadlineNanos(STOP_TIMEOUT_NANOS);
            while (true) {
                final StorageBinaryDataClient.StopResult result = this.dataClient.stopResult();
                final StorageBinaryDataClient.StopOutcome outcome = result.outcome();
                if (outcome == StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY) {
                    return;
                }
                final RuntimeException failure = this.dataClient.failure();
                if (failure != null) {
                    throw new NodeLibraryException("Cannot create backup after replication reader failure", failure);
                }
                if (outcome == StorageBinaryDataClient.StopOutcome.TIMED_OUT ||
                    outcome == StorageBinaryDataClient.StopOutcome.FAILED) {
                    throw new NodeLibraryException(
                            "Cannot create backup after replication reader stop %s".formatted(outcome));
                }
                if (ReplicationRetry.expired(deadline)) {
                    throw new NodeLibraryException(
                            "Timed out waiting for replication reader boundary at %s (last resolved sequence=%s, position=%s)"
                                    .formatted(this.dataClient.cursor(), result.sequence(), result.position()));
                }
                awaitNextPoll();
            }
        }

        /// Waits one poll interval, converting interruption into a domain failure.
        ///
        /// An interrupted caller must not surface a raw `InterruptedException`
        /// wrapper: it is translated into a [NodeLibraryException], and the
        /// interrupt flag is restored so the caller's cancellation policy still
        /// sees it.
        private static void awaitNextPoll() throws NodeLibraryException {
            if (Thread.currentThread().isInterrupted()) {
                throw new NodeLibraryException("Interrupted while waiting for the replication reader boundary");
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NodeLibraryException(
                        "Interrupted while waiting for the replication reader boundary", interrupted);
            }
        }

                /// Resolves the retention cursor from the newest backup compatible
        /// with this node, excluding the backup that was just created.
        ///
        /// Counting from the newest backup overall would hand retention a
        /// cursor from an unrelated generation sharing the volume, deleting
        /// log history this node's own restores still need.
        ///
        /// @param created        backup that was just published
        /// @param localIdentity  this node's backup identity
        /// @return cursor of the previous compatible backup, or `null` when none exists
        private ReplicationCursor retentionCursorExcluding(
                final BackupMetadata created,
                final BackupMetadata.Identity localIdentity) {
            final var previous = this.backend.listBackups().stream()
                    .filter(backup -> !backup.backupId().equals(created.backupId()))
                    .filter(backup -> backup.isCompatibleWith(localIdentity))
                    .max(BackupMetadata.OLDEST_FIRST)
                    .orElse(null);
            if (previous == null) {
                return null;
            }
            return this.backend.getCursorForBackup(previous);
        }

                /// Retries a purge that is temporarily blocked by an active Archive replay.
        /// Replay ownership is intentionally not interrupted: the retention provider
        /// returns a deferred result, the bounded retry gives a short-lived replay a
        /// chance to finish, and a still-active replay is retained for the next backup
        /// cycle with an explicit warning.
        private ReplicationLogRetention.MaintenanceResult deleteThroughWithReplayRetry(
                final ReplicationCursor cursor) throws NodeLibraryException {
            ReplicationLogRetention.MaintenanceResult result = this.retention.deleteThrough(cursor);
            for (int attempt = 1; result.status() == ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY &&
                                  attempt < RETENTION_RETRY_ATTEMPTS; attempt++) {
                sleepRetentionRetryDelay();
                result = this.retention.deleteThrough(cursor);
            }
            return result;
        }

        private static void sleepRetentionRetryDelay() throws NodeLibraryException {
            if (Thread.currentThread().isInterrupted()) {
                throw new NodeLibraryException("Interrupted while waiting to retry replication retention");
            }
            try {
                Thread.sleep(RETENTION_RETRY_DELAY_MILLIS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NodeLibraryException("Interrupted while waiting to retry replication retention", interrupted);
            }
        }
    }
}
