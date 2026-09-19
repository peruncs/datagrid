package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageController;
import peruncs.datagrid.cluster.node.CloseSequencer;
import peruncs.datagrid.cluster.node.StorageNodeControl;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;

import static java.lang.System.Logger.Level.INFO;
import static org.eclipse.serializer.util.X.notNull;

/// This manager exposes node health while coordinating backups and replication.
///
/// A backup stops the reader at a safe message boundary, creates the backup,
/// and then resumes reading. Callers must not close the storage while either
/// operation is active. Readiness requires an actively reading, 
/// failure-free reader; health additionally tolerates the intentional reader
/// stop a running backup performs, so a normal backup cycle does not flap the
/// health endpoint.
///
/// @since 1.0
public interface BackupNodeManager extends StorageNodeControl, BackupNodeControl, AutoCloseable {
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
        if (replicationTransport == null || replicationTransport.isBlank()) {
            throw new IllegalArgumentException("replicationTransport must not be blank");
        }
        return new Default(
                notNull(storageBackupTaskExecutor),
                notNull(dataClient),
                notNull(storageController),
                notNull(storageDiskSpaceReader),
                replicationTransport
        );
    }

        /// Closes the backup executor and the reader client.
    @Override
    void close();

        /// Coordinates backup work with the replication reader and storage controller.
    final class Default implements BackupNodeManager {
        private static final System.Logger LOGGER = System.getLogger(BackupNodeManager.class.getName());

        private final StorageBackupTaskExecutor tasks;
        private final StorageBinaryDataClient dataClient;
        private final StorageController storageController;
        private final StorageDiskSpaceReader storageDiskSpaceReader;
        private final String replicationTransport;
        private volatile boolean closed;
        private boolean clientDisposed;

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
            this.replicationTransport = replicationTransport;
        }

        @Override
        public String replicationTransport() {
            return this.replicationTransport;
        }

        @Override
        public long currentSequence() {
            /* A backup node is an active reader; report its applied cursor
             * instead of the -1 placeholder a non-replicated node would use. */
            return this.dataClient.currentSequence();
        }

        @Override
        public void stopReadingAtLatestMessage() {
            this.dataClient.stopAtLatestMessage();
        }

        @Override
        public void resumeReading() throws NodeLibraryException {
            try {
                this.dataClient.resume();
            } catch (final NodeLibraryException alreadyDomain) {
                throw alreadyDomain;
            } catch (final RuntimeException failure) {
                throw new NodeLibraryException("Failed to resume the replication reader", failure);
            }
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
             * does not flap the health endpoint. Maintenance failures after a
             * durable backup are exposed through the task executor and
             * intentionally do not make the node unhealthy. */
            return this.isOperational() && (this.dataClient.isRunning() || this.tasks.isRunningBackup());
        }

        @Override
        public boolean isReady() throws NodeLibraryException {
            /* Readiness is stricter than health: the reader must actually be
             * running, not merely stopped for an intentional backup. */
            return this.isOperational() && this.dataClient.isRunning();
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

        /// Closes the backup executor and the reader client, aggregating
        /// failures. The first call disposes everything it can; later calls
        /// retry the reader disposal while the executor still reports an
        /// active backup, so a failed close never permanently strands the
        /// client while a backup holds it.
        @Override
        public synchronized void close() {
            if (this.closed && this.clientDisposed) {
                return;
            }
            this.closed = true;
            LOGGER.log(INFO, "Closing BackupNodeManager.");
            Throwable failure = null;
            try {
                this.tasks.close();
            } catch (final Throwable closeFailure) {
                failure = closeFailure;
            }
            if (!this.clientDisposed && !this.tasks.isRunningBackup()) {
                try {
                    this.dataClient.dispose();
                    this.clientDisposed = true;
                } catch (final Throwable closeFailure) {
                    failure = CloseSequencer.append(failure, closeFailure);
                }
            } else if (!this.clientDisposed) {
                failure = CloseSequencer.append(failure, new IllegalStateException(
                        "backup reader remains active; data client disposal is deferred to a later close"));
            }
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException runtime) throw runtime;
        }

        private boolean isStorageAvailable() {
            return this.storageController.isRunning() && !this.storageController.isStartingUp();
        }

        /// Reports the conditions shared by health and readiness.
        ///
        /// Health adds tolerance for the intentional reader stop a running
        /// backup performs; readiness additionally requires the reader to be
        /// running. This keeps the two truth tables from drifting apart.
        private boolean isOperational() {
            return this.isStorageAvailable()
                    && this.dataClient.failure() == null
                    && this.tasks.backupFailure() == null;
        }
    }
}
