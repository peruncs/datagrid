package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageController;
import peruncs.datagrid.cluster.node.ClusterNodeManager;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;

import static java.lang.System.Logger.Level.INFO;
import static org.eclipse.serializer.util.X.notNull;

/// This manager exposes node health while coordinating backups and replication.
///
/// A backup stops the reader at a safe message boundary, creates the backup,
/// and then resumes reading. Callers must not close the storage while either
/// operation is active.
public interface BackupNodeManager extends ClusterNodeManager, BackupNodeControl {
        /// Creates a backup manager for the supplied collaborators.
    ///
    /// @param storageBackupTaskExecutor backup task executor
    /// @param dataClient                replication data client
    /// @param storageController         storage controller
    /// @param storageDiskSpaceReader    storage space reader
    /// @param replicationTransport      selected transport id for monitoring
    /// @return backup manager
    static BackupNodeManager New(
            final StorageBackupTaskExecutor storageBackupTaskExecutor,
            final StorageBinaryDataClient dataClient,
            final StorageController storageController,
            final StorageDiskSpaceReader storageDiskSpaceReader,
            final String replicationTransport
    ) {
        return new Default(
                notNull(storageBackupTaskExecutor),
                notNull(dataClient),
                notNull(storageController),
                notNull(storageDiskSpaceReader),
                replicationTransport
        );
    }

        /// Stops the reader at the latest safe message boundary.
    void stopReadingAtLatestMessage();

        /// Resumes the reader after backup work.
    ///
    /// @throws NodeLibraryException if the reader cannot resume
    void resumeReading() throws NodeLibraryException;

        /// Reports whether the reader is active.
    ///
    /// @return `true` when the reader is active
    boolean isReading();

        /// Creates a storage backup.
    ///
    /// @param useManualSlot whether to use the manual backup slot
    /// @throws NodeLibraryException if backup creation fails
    void createStorageBackup(final boolean useManualSlot) throws NodeLibraryException;

        /// Reports whether a backup is running.
    ///
    /// @return `true` when backup work is active
    boolean isBackupRunning();

        /// Coordinates backup work with the replication reader and storage controller.
    final class Default implements BackupNodeManager {
        private static final System.Logger LOGGER = System.getLogger(BackupNodeManager.class.getName());

        private final StorageBackupTaskExecutor tasks;
        private final StorageBinaryDataClient dataClient;
        private final StorageController storageController;
        private final StorageDiskSpaceReader storageDiskSpaceReader;
        private final String replicationTransport;
        private volatile boolean closed;

        private Default(
                final StorageBackupTaskExecutor storageBackupTaskExecutor,
                final StorageBinaryDataClient dataClient,
                final StorageController storageController,
                final StorageDiskSpaceReader storageDiskSpaceReader,
                final String replicationTransport
        ) {
            this.tasks = storageBackupTaskExecutor;
            this.dataClient = dataClient;
            this.storageController = storageController;
            this.storageDiskSpaceReader = storageDiskSpaceReader;
            if (replicationTransport == null || replicationTransport.isBlank()) {
                throw new IllegalArgumentException("replicationTransport must not be blank");
            }
            this.replicationTransport = replicationTransport;
        }

        @Override
        public String getReplicationTransport() {
            return this.replicationTransport;
        }

        @Override
        public long getCurrentSequence() {
            /* A backup node is an active reader; report its applied cursor
             * instead of the -1 placeholder a non-replicated node would use. */
            return this.dataClient.cursor().logicalSequence();
        }

        @Override
        public void stopReadingAtLatestMessage() {
            this.dataClient.stopAtLatestMessage();
        }

        @Override
        public void resumeReading() throws NodeLibraryException {
            this.dataClient.resume();
        }

        @Override
        public boolean isReading() {
            return this.dataClient.isRunning();
        }

        @Override
        public void createStorageBackup(final boolean useManualSlot) throws NodeLibraryException {
            if (this.tasks.runBackup(useManualSlot) == StorageBackupTaskExecutor.BackupStartResult.BUSY) {
                throw new BackupBusyException("Storage backup is already running");
            }
        }

        @Override
        public boolean isBackupRunning() {
            return this.tasks.isRunningBackup();
        }

        @Override
        public boolean isHealthy() {
            /* A stopped reader is not healthy: without it the node silently
             * stops replicating. An intentional stop while a backup holds the
             * single-flight lock is the one exemption, so every backup cycle
             * does not flap the health endpoint. */
            return this.isStorageAvailable()
                    && this.dataClient.failure() == null
                    && this.tasks.backupFailure() == null
                    && (this.dataClient.isRunning() || this.tasks.isRunningBackup());
        }

        @Override
        public boolean isReady() throws NodeLibraryException {
            return this.isStorageAvailable()
                    && this.dataClient.isRunning()
                    && this.dataClient.failure() == null
                    && this.tasks.backupFailure() == null;
        }

        @Override
        public boolean isRunningStorageChecks() {
            return this.tasks.isRunningChecks();
        }

        @Override
        public long readStorageSizeBytes() throws NodeLibraryException {
            return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
        }

        @Override
        public void startStorageChecks() {
            this.tasks.runChecks();
        }

        @Override
        public synchronized void close() {
            /* A borrowed manager may be closed concurrently with the foundation. */
            if (this.closed) {
                return;
            }
            this.closed = true;
            LOGGER.log(INFO, "Closing BackupNodeManager.");
            Throwable failure = null;
            try {
                this.dataClient.dispose();
            } catch (final Throwable closeFailure) {
                failure = closeFailure;
            }
            try {
                this.tasks.close();
            } catch (final Throwable closeFailure) {
                if (failure == null) failure = closeFailure;
                else if (failure != closeFailure) failure.addSuppressed(closeFailure);
            }
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException runtime) throw runtime;
        }

        private boolean isStorageAvailable() {
            return this.storageController.isRunning() && !this.storageController.isStartingUp();
        }

    }
}
