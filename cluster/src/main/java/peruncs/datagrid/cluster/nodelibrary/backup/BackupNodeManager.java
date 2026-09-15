package peruncs.datagrid.cluster.nodelibrary.backup;


import org.eclipse.store.storage.types.StorageController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.nodelibrary.node.ClusterNodeManager;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterStorageBinaryDataClient;
import peruncs.datagrid.cluster.nodelibrary.store.StorageDiskSpaceReader;

import static org.eclipse.serializer.util.X.notNull;

/// This manager exposes node health while coordinating backups and replication.
///
/// A backup stops the reader at a safe message boundary, creates the backup,
/// and then resumes reading. Callers must not close the storage while either
/// operation is active.
public interface BackupNodeManager extends ClusterNodeManager {
        /// Creates a backup manager for the supplied collaborators.
    ///
    /// @param storageBackupTaskExecutor backup task executor
    /// @param dataClient                replication data client
    /// @param storageController         storage controller
    /// @param storageDiskSpaceReader    storage space reader
    /// @return backup manager
    static BackupNodeManager New(
            final StorageBackupTaskExecutor storageBackupTaskExecutor,
            final ClusterStorageBinaryDataClient dataClient,
            final StorageController storageController,
            final StorageDiskSpaceReader storageDiskSpaceReader
    ) {
        return new Default(
                notNull(storageBackupTaskExecutor),
                notNull(dataClient),
                notNull(storageController),
                notNull(storageDiskSpaceReader)
        );
    }

        /// Stops the reader at the latest safe message boundary.
    void stopReadingAtLatestMessage();

        /// Resumes the reader after backup work.
    ///
    /// @throws NodelibraryException if the reader cannot resume
    void resumeReading() throws NodelibraryException;

        /// Reports whether the reader is active.
    ///
    /// @return `true` when the reader is active
    boolean isReading();

        /// Creates a storage backup.
    ///
    /// @param useManualSlot whether to use the manual backup slot
    /// @throws NodelibraryException if backup creation fails
    void createStorageBackup(final boolean useManualSlot) throws NodelibraryException;

        /// Reports whether a backup is running.
    ///
    /// @return `true` when backup work is active
    boolean isBackupRunning();

        /// Coordinates backup work with the replication reader and storage controller.
    final class Default implements BackupNodeManager {
        private static final Logger LOG = LoggerFactory.getLogger(BackupNodeManager.class);

        private final StorageBackupTaskExecutor tasks;
        private final ClusterStorageBinaryDataClient dataClient;
        private final StorageController storageController;
        private final StorageDiskSpaceReader storageDiskSpaceReader;

        private Default(
                final StorageBackupTaskExecutor storageBackupTaskExecutor,
                final ClusterStorageBinaryDataClient dataClient,
                final StorageController storageController,
                final StorageDiskSpaceReader storageDiskSpaceReader
        ) {
            this.tasks = storageBackupTaskExecutor;
            this.dataClient = dataClient;
            this.storageController = storageController;
            this.storageDiskSpaceReader = storageDiskSpaceReader;
        }

        @Override
        public void stopReadingAtLatestMessage() {
            this.dataClient.stopAtLatestMessage();
        }

        @Override
        public void resumeReading() throws NodelibraryException {
            this.dataClient.resume();
        }

        @Override
        public boolean isReading() {
            return this.dataClient.isRunning();
        }

        @Override
        public void createStorageBackup(final boolean useManualSlot) throws NodelibraryException {
            this.tasks.runBackup(useManualSlot);
        }

        @Override
        public boolean isBackupRunning() {
            return this.tasks.isRunningBackup();
        }

        @Override
        public boolean isHealthy() {
            return this.isStorageAvailable();
        }

        @Override
        public boolean isReady() throws NodelibraryException {
            return this.isStorageAvailable();
        }

        @Override
        public boolean isRunningStorageChecks() {
            return this.tasks.isRunningChecks();
        }

        @Override
        public long readStorageSizeBytes() throws NodelibraryException {
            return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
        }

        @Override
        public void startStorageChecks() {
            this.tasks.runChecks();
        }

        @Override
        public void close() {
            LOG.info("Closing BackupNodeManager.");
            try {
                this.dataClient.dispose();
            } finally {
                this.tasks.close();
            }
        }

        private boolean isStorageAvailable() {
            return this.storageController.isRunning() && !this.storageController.isStartingUp();
        }

    }
}
