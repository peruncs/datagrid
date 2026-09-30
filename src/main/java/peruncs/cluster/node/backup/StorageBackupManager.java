package peruncs.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.api.BackupInfo;
import peruncs.cluster.api.BackupSlot;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.replication.ReplicationLogRetention;
import peruncs.cluster.storage.ReplicationPosition;
import peruncs.cluster.storage.ReplicationRetry;
import peruncs.cluster.storage.binary.ReplicationApplier;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/// Creates, lists, restores, and deletes storage backups.
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
    /// @param positionSupplier       replication position supplier
    /// @param dataClient           replication client
    /// @param retention            log retention policy
    /// @return backup manager
    static StorageBackupManager create(
            final StorageConnection storageConnection,
            final int maxBackupCount,
            final StorageBackupBackend storageBackupBackend,
            final Supplier<ReplicationPosition> positionSupplier,
            final ReplicationApplier dataClient,
            final ReplicationLogRetention retention) {
        return create(storageConnection, maxBackupCount, storageBackupBackend, positionSupplier, dataClient,
                retention, NodeConfig.Operations.DEFAULT);
    }

    /// Creates a backup manager with configured retry and stop bounds.
    ///
    /// @param storageConnection Store connection
    /// @param maxBackupCount maximum backup count
    /// @param storageBackupBackend backup backend
    /// @param positionSupplier replication position supplier
    /// @param dataClient replication client
    /// @param retention log retention policy
    /// @param operations configured retry and stop bounds
    /// @return backup manager
    static StorageBackupManager create(
            final StorageConnection storageConnection,
            final int maxBackupCount,
            final StorageBackupBackend storageBackupBackend,
            final Supplier<ReplicationPosition> positionSupplier,
            final ReplicationApplier dataClient,
            final ReplicationLogRetention retention,
            final NodeConfig.Operations operations) {
        return new Default(
                notNull(storageConnection),
                positive(maxBackupCount),
                notNull(storageBackupBackend),
                notNull(positionSupplier),
                notNull(dataClient),
                notNull(retention),
                Objects.requireNonNull(operations, "operations")
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
    /// @param slot scheduled or manual retention slot
    /// @throws NodeException if backup creation or publication fails
    BackupInfo createStorageBackup(BackupSlot slot) throws NodeException;

        /// Lists available backups.
    ///
    /// @return backup metadata
    /// @throws NodeException if listing fails
    List<BackupMetadata> listBackups() throws NodeException;

    /// Returns the failure of the most recent post-publication maintenance
    /// step (resolving retention, pruning, or log retention), if any.
    ///
    /// The value is reset when the next backup reaches its maintenance phase
    /// and never marks a successfully published backup as failed. A `null`
    /// return means the latest maintenance phase completed cleanly or no
    /// backup has run yet.
    ///
    /// @return last maintenance failure, or `null`
    Throwable maintenanceFailure();

    /// Deletes one backup.
    ///
    /// @param backup backup to delete
    /// @throws NodeException if deletion fails
    void deleteBackup(BackupMetadata backup) throws NodeException;

    /// Restores one backup.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @param backup                       backup to restore
    /// @throws NodeException if restore fails
    void restoreBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodeException;

    /// Reports whether user storage exists.
    ///
    /// @return `true` when user storage exists
    /// @throws NodeException if the check fails
    boolean hasUserUploadedStorage() throws NodeException;

    /// Restores user storage.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @throws NodeException if restore fails
    void restoreUserUploadedStorage(Path storageDestinationParentPath) throws NodeException;

    /// Deletes user storage.
    ///
    /// @throws NodeException if deletion fails
    void deleteUserUploadedStorage() throws NodeException;

    /// Implements the stop, backup, retention, and resume sequence.
    class Default implements StorageBackupManager {
        private static final System.Logger LOGGER = System.getLogger(StorageBackupManager.class.getName());
        private final StorageConnection storageConnection;
        private final int maxBackupCount;
        private final StorageBackupBackend backend;
        private final Supplier<ReplicationPosition> positionSupplier;
        private final ReplicationApplier dataClient;
        private final ReplicationLogRetention retention;
        private final NodeConfig.Operations operations;
        private final AtomicReference<Throwable> maintenanceFailure = new AtomicReference<>();

        private Default(
                final StorageConnection storageConnection,
                final int maxBackupCount,
                final StorageBackupBackend backupBackend,
                final Supplier<ReplicationPosition> positionSupplier,
                final ReplicationApplier dataClient,
                final ReplicationLogRetention retention,
                final NodeConfig.Operations operations
        ) {
            this.storageConnection = storageConnection;
            this.maxBackupCount = maxBackupCount;
            this.backend = backupBackend;
            this.positionSupplier = positionSupplier;
            this.dataClient = dataClient;
            this.retention = retention;
            this.operations = operations;
        }

        @Override
        public BackupInfo createStorageBackup(final BackupSlot slot) throws NodeException {
            Objects.requireNonNull(slot, "slot");
            LOGGER.log(System.Logger.Level.TRACE, "Creating new storage backup");

            final List<BackupMetadata> backups = this.listBackups();
            final long timestamp = this.nextBackupTimestamp(backups, slot);
            final RuntimeException readerFailure = this.dataClient.failure();
            if (readerFailure != null) {
                throw new NodeException("Cannot create backup after replication reader failure", readerFailure);
            }

            final boolean isRunning = this.dataClient.isRunning();

            if (isRunning) {
                this.stopDataClient();
            } else {
                /* A reader can report isRunning()==false while a timed-out stop still
                 * owns a live polling thread.  Never treat that intermediate state as a
                 * safe backup boundary.  STOPPED/NOT_STARTED remain valid for simple
                 * clients that never expose a stop-at-latest operation. */
                final ReplicationApplier.StopOutcome outcome = this.dataClient.stopResult().outcome();
                if (outcome == ReplicationApplier.StopOutcome.STOPPING ||
                    outcome == ReplicationApplier.StopOutcome.TIMED_OUT ||
                    outcome == ReplicationApplier.StopOutcome.FAILED) {
                    throw new NodeException(
                            "Cannot create backup while replication reader stop is unresolved: %s".formatted(outcome));
                }
            }

            /* Backup identity and retention boundary must describe the
             * stopped Store mark, not a position observed before the stop.
             * The backup generation comes from that same position, so a node
             * on a shared volume only ever restores its own cluster, epoch,
             * and recording. The backup id is random, so concurrent
             * publishers never share an archive name. Acquire the position and
             * build the metadata INSIDE the try: a failed preparation after a
             * successful stop must resume the reader just the same. */
            Throwable operationFailure = null;
            try {
                final ReplicationPosition position = this.positionSupplier.get();
                final var newBackup = BackupMetadata.create(timestamp, slot == BackupSlot.MANUAL, position);
                final var localIdentity = BackupMetadata.Identity.of(position);
                this.backend.createBackup(this.storageConnection, newBackup);
                this.runMaintenance(backups, slot, newBackup, localIdentity);
                return new BackupInfo(newBackup.backupId(),
                        Instant.ofEpochMilli(newBackup.timestamp()),
                        newBackup.logicalSequence(), slot == BackupSlot.MANUAL);
            } catch (final RuntimeException | Error failure) {
                operationFailure = failure;
                throw failure;
            } finally {
                // only resume if the replication applier was running before the stop
                /* A stop timeout records a terminal reader failure and deliberately leaves
                 * the client stopped.  Calling resume() from this finally block would mask
                 * the original backup error and race a still-draining poller. */
                if (isRunning && this.dataClient.failure() == null &&
                    this.dataClient.stopResult().outcome() == ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY) {
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
        /// the retention position, pruning old archives, and advancing log
        /// retention all catch their own runtime failures, log them, and publish
        /// them through [#maintenanceFailure()]. The durable backup published
        /// before this method runs is never affected.
        private void runMaintenance(
                final List<BackupMetadata> backups,
                final BackupSlot slot,
                final BackupMetadata created,
                final BackupMetadata.Identity localIdentity
        ) {
            this.maintenanceFailure.set(null);
            this.sweepUnreadableArchives();

            /* The retention position is resolved before pruning while the
             * previous archive still exists to read it from. */
            final ReplicationPosition retentionPosition;
            try {
                retentionPosition = slot == BackupSlot.MANUAL
                        ? null : this.retentionPositionExcluding(created, localIdentity);
            } catch (final RuntimeException failure) {
                this.recordMaintenanceFailure("resolve the retention position", failure);
                this.pruneBackups(backups, slot, localIdentity);
                return;
            }

            this.pruneBackups(backups, slot, localIdentity);
            try {
                this.advanceRetention(slot, retentionPosition);
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
                final BackupSlot slot,
                final BackupMetadata.Identity localIdentity
        ) {
            try {
                final List<BackupMetadata> compatible = backups.stream()
                        .filter(backup -> backup.isCompatibleWith(localIdentity))
                        .toList();
                if (compatible.isEmpty()) {
                    return;
                }
                if (slot == BackupSlot.MANUAL) {
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
        private void advanceRetention(final BackupSlot slot, final ReplicationPosition retentionPosition)
                throws NodeException {
            if (slot == BackupSlot.MANUAL) {
                return;
            }
            if (!this.retention.isSupported()) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Replication retention is unsupported; preserving Archive history");
                return;
            }
            if (retentionPosition == null) {
                return;
            }
            final ReplicationLogRetention.MaintenanceResult result =
                    this.deleteThroughWithReplayRetry(retentionPosition);
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

        private long nextBackupTimestamp(final List<BackupMetadata> backups, final BackupSlot slot) {
            long timestamp = System.currentTimeMillis();
            for (final BackupMetadata backup : backups) {
                if (backup.manualSlot() == (slot == BackupSlot.MANUAL) && backup.timestamp() >= timestamp) {
                    timestamp = backup.timestamp() == Long.MAX_VALUE ? Long.MAX_VALUE : backup.timestamp() + 1L;
                }
            }
            if (timestamp == Long.MAX_VALUE) {
                throw new NodeException("No unique timestamp is available for a new backup");
            }
            return timestamp;
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodeException {
            this.backend.deleteBackup(backup);
        }

        @Override
        public void deleteUserUploadedStorage() throws NodeException {
            this.backend.deleteUserUploadedStorage();
        }

        @Override
        public void restoreBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
                throws NodeException {
            this.backend.restoreBackup(storageDestinationParentPath, backup);
        }

        @Override
        public void restoreUserUploadedStorage(final Path storageDestinationParentPath)
                throws NodeException {
            this.backend.restoreUserUploadedStorage(storageDestinationParentPath);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodeException {
            return this.backend.hasUserUploadedStorage();
        }

        @Override
        public List<BackupMetadata> listBackups() throws NodeException {
            return this.backend.listBackups();
        }

        private void stopDataClient() throws NodeException {
            LOGGER.log(System.Logger.Level.TRACE, "Waiting for data client to stop reading");
            this.dataClient.stopAtLatestMessage();
            final long deadline = ReplicationRetry.deadlineNanos(TimeUnit.MILLISECONDS.toNanos(
                    this.operations.backupStopTimeout().toMillis()));
            while (true) {
                final ReplicationApplier.StopResult result = this.dataClient.stopResult();
                final ReplicationApplier.StopOutcome outcome = result.outcome();
                if (outcome == ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY) {
                    final RuntimeException failure = this.dataClient.failure();
                    if (failure != null) {
                        throw new NodeException("Cannot create backup after replication reader failure", failure);
                    }
                    return;
                }
                final RuntimeException failure = this.dataClient.failure();
                if (failure != null) {
                    throw new NodeException("Cannot create backup after replication reader failure", failure);
                }
                if (outcome == ReplicationApplier.StopOutcome.TIMED_OUT ||
                    outcome == ReplicationApplier.StopOutcome.FAILED) {
                    throw new NodeException(
                            "Cannot create backup after replication reader stop %s".formatted(outcome));
                }
                if (ReplicationRetry.expired(deadline)) {
                    throw new NodeException(
                            "Timed out waiting for replication reader boundary at %s (last resolved sequence=%s, position=%s)"
                                    .formatted(this.dataClient.position(), result.sequence(), result.position()));
                }
                this.awaitNextPoll();
            }
        }

        /// Waits one poll interval, converting interruption into a domain failure.
        ///
        /// An interrupted caller must not surface a raw `InterruptedException`
        /// wrapper: it is translated into a [NodeException], and the
        /// interrupt flag is restored so the caller's cancellation policy still
        /// sees it.
        private void awaitNextPoll() throws NodeException {
            if (Thread.currentThread().isInterrupted()) {
                throw new NodeException("Interrupted while waiting for the replication reader boundary");
            }
            try {
                Thread.sleep(this.operations.backupStopPollInterval().toMillis());
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NodeException(
                        "Interrupted while waiting for the replication reader boundary", interrupted);
            }
        }

                /// Resolves the retention position from the newest backup compatible
        /// with this node, excluding the backup that was just created.
        ///
        /// Counting from the newest backup overall would hand retention a
        /// position from an unrelated generation sharing the volume, deleting
        /// log history this node's own restores still need.
        ///
        /// @param created        backup that was just published
        /// @param localIdentity  this node's backup identity
        /// @return position of the previous compatible backup, or `null` when none exists
        private ReplicationPosition retentionPositionExcluding(
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
            return this.backend.retentionBoundary(previous);
        }

                /// Retries a purge that is temporarily blocked by an active Archive replay.
        /// Replay ownership is intentionally not interrupted: the retention provider
        /// returns a deferred result, the bounded retry gives a short-lived replay a
        /// chance to finish, and a still-active replay is retained for the next backup
        /// cycle with an explicit warning.
        private ReplicationLogRetention.MaintenanceResult deleteThroughWithReplayRetry(
                final ReplicationPosition position) throws NodeException {
            ReplicationLogRetention.MaintenanceResult result = this.retention.deleteThrough(position);
            for (int attempt = 1; result.status() == ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY &&
                                  attempt < this.operations.backupRetentionRetryAttempts(); attempt++) {
                this.sleepRetentionRetryDelay();
                result = this.retention.deleteThrough(position);
            }
            return result;
        }

        private void sleepRetentionRetryDelay() throws NodeException {
            if (Thread.currentThread().isInterrupted()) {
                throw new NodeException("Interrupted while waiting to retry replication retention");
            }
            try {
                Thread.sleep(this.operations.backupRetentionRetryDelay().toMillis());
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NodeException("Interrupted while waiting to retry replication retention", interrupted);
            }
        }
    }
}
