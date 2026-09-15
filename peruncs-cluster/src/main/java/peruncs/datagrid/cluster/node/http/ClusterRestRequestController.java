package peruncs.datagrid.cluster.node.http;

import peruncs.datagrid.cluster.node.ClusterNodeManager;
import peruncs.datagrid.cluster.node.PromotableStorageNodeManager;
import peruncs.datagrid.cluster.node.ReplicationMetrics;
import peruncs.datagrid.cluster.node.StorageNodeManager;
import peruncs.datagrid.cluster.node.backup.BackupBusyException;
import peruncs.datagrid.cluster.node.backup.BackupNodeManager;
import peruncs.datagrid.cluster.node.exceptions.HttpResponseException;

import java.util.function.Supplier;

import static org.eclipse.serializer.util.X.notNull;
import static org.eclipse.serializer.util.X.unbox;

/// HTTP operation facade of the node. Implementations translate storage,
/// replication, and transport failures into the node's HTTP exception
/// types.
public interface ClusterRestRequestController extends AutoCloseable {
        /// Creates a controller for a storage node.
    ///
    /// @param storageNodeManager storage node manager
    /// @return request controller
    static ClusterRestRequestController StorageNode(
            final StorageNodeManager storageNodeManager
    ) {
        return new StorageNode(notNull(storageNodeManager));
    }

        /// Creates a controller that serves no endpoints.
    ///
    /// @return request controller
    static ClusterRestRequestController NoEndpoints() {
        return new NoEndpoints();
    }

        /// Creates a controller for a backup node.
    ///
    /// @param backupNodeManager backup node manager
    /// @return request controller
    static ClusterRestRequestController BackupNode(
            final BackupNodeManager backupNodeManager
    ) {
        return new BackupNode(notNull(backupNodeManager));
    }

        /// Reports whether the distributor is active.
    ///
    /// @return distributor state
    /// @throws HttpResponseException if the request fails
    boolean getDistributor() throws HttpResponseException;

        /// Starts distributor activation.
    ///
    /// @throws HttpResponseException if the request fails
    void postActivateDistributorStart() throws HttpResponseException;

        /// Finishes distributor activation.
    ///
    /// @return whether activation finished
    /// @throws HttpResponseException if the request fails
    boolean postActivateDistributorFinish() throws HttpResponseException;

        /// Checks node health.
    ///
    /// @throws HttpResponseException if the request fails
    void getHealth() throws HttpResponseException;

        /// Checks node readiness.
    ///
    /// @throws HttpResponseException if the request fails
    void getHealthReady() throws HttpResponseException;

        /// Returns current storage size.
    ///
    /// @return storage size in bytes
    /// @throws HttpResponseException if the request fails
    long getStorageBytes() throws HttpResponseException;

        /// Returns the raw replication observability values for this node.
    ///
    /// @return raw replication metrics
    /// @throws HttpResponseException if the request fails
    ReplicationMetrics getReplicationMetrics() throws HttpResponseException;

        /// Starts a backup.
    ///
    /// @param body backup request body
    /// @throws HttpResponseException if the request fails
    void postBackup(PostBackupRequest body) throws HttpResponseException;

        /// Reports whether a backup is running.
    ///
    /// @return backup state
    /// @throws HttpResponseException if the request fails
    boolean getBackup() throws HttpResponseException;

        /// Stops or pauses updates.
    ///
    /// @throws HttpResponseException if the request fails
    void postUpdates() throws HttpResponseException;

        /// Reports whether updates are paused.
    ///
    /// @return `true` when the replication reader is stopped for backup work
    /// @throws HttpResponseException if the request fails
    boolean getUpdates() throws HttpResponseException;

        /// Resumes updates.
    ///
    /// @throws HttpResponseException if the request fails
    void postResumeUpdates() throws HttpResponseException;

        /// Starts storage checks.
    ///
    /// @throws HttpResponseException if the request fails
    void postGc() throws HttpResponseException;

        /// Reports whether storage checks are running.
    ///
    /// @return check state
    /// @throws HttpResponseException if the request fails
    boolean getGc() throws HttpResponseException;

    @Override
    void close();

        /// Shares validation, error mapping, and common monitoring requests.
    abstract class Abstract implements ClusterRestRequestController {
        private static final System.Logger LOGGER = System.getLogger(ClusterRestRequestController.class.getName());

        private final ClusterNodeManager nodeManager;

                /// Creates the shared request controller.
        ///
        /// @param nodeManager node manager
        protected Abstract(final ClusterNodeManager nodeManager) {
            this.nodeManager = nodeManager;
        }

        @Override
        public void postGc() throws HttpResponseException {
            LOGGER.log(System.Logger.Level.TRACE, "Handling postDataGridGc request");
            this.handleRequest(this.nodeManager::startStorageChecks);
        }

        @Override
        public boolean getGc() throws HttpResponseException {
            LOGGER.log(System.Logger.Level.TRACE, "Handling getDataGridGc request");
            return this.handleRequest(this.nodeManager::isRunningStorageChecks);
        }

        @Override
        public void getHealth() throws HttpResponseException {
            this.handleRequest(() ->
            {
                if (!this.nodeManager.isHealthy()) {
                    throw HttpResponseException.serviceUnavailable("node is not healthy");
                }
            });
        }

        @Override
        public void getHealthReady() throws HttpResponseException {
            this.handleRequest(() ->
            {
                if (!this.nodeManager.isReady()) {
                    throw HttpResponseException.serviceUnavailable("node is not ready");
                }
            });
        }

        @Override
        public long getStorageBytes() throws HttpResponseException {
            return this.handleRequest(this.nodeManager::readStorageSizeBytes);
        }

        /// Reads the replication observability values for this node.
        ///
        /// The values are produced by the node manager; unknown positions
        /// report -1, so a scrape never fails just because the writer
        /// boundary is currently unknowable. The embedding application
        /// renders the wire format.
        ///
        /// @return raw replication metrics
        @Override
        public ReplicationMetrics getReplicationMetrics() throws HttpResponseException {
            return this.handleRequest(this.nodeManager::replicationMetrics);
        }

        @Override
        public boolean postActivateDistributorFinish() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public boolean getDistributor() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public boolean getUpdates() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postResumeUpdates() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postActivateDistributorStart() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postBackup(PostBackupRequest body) throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public boolean getBackup() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postUpdates() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

                /// Runs a request and maps failures to HTTP exceptions.
        ///
        /// @param request request action
        /// @throws HttpResponseException if the request fails
        protected void handleRequest(final Runnable request) throws HttpResponseException {
            this.handleRequest(() ->
            {
                request.run();
                return null;
            });
        }

                /// Runs a value request and maps failures to HTTP exceptions.
        ///
        /// @param <T>     result type
        /// @param request request action
        /// @return request result
        /// @throws HttpResponseException if the request fails
        protected <T> T handleRequest(final Supplier<T> request) throws HttpResponseException {
            try {
                return request.get();
            } catch (final Exception e) {
                // the exception has already been handled
                if (e instanceof HttpResponseException handled) {
                    throw handled;
                }

                LOGGER.log(System.Logger.Level.ERROR, "Failed to handle request", e);
                throw HttpResponseException.internalServerError(e);
            }
        }
    }

        /// Serves requests for a node that can become the distributor.
    final class StorageNode extends Abstract {
        private static final System.Logger LOGGER = System.getLogger(StorageNode.class.getName());
        private final StorageNodeManager storageNodeManager;

        private StorageNode(final StorageNodeManager storageNodeManager) {
            super(storageNodeManager);
            this.storageNodeManager = storageNodeManager;
        }

        @Override
        public boolean postActivateDistributorFinish() throws HttpResponseException {
            return this.handleRequest(() -> this.promotable().finishDistributionSwitch());
        }

        @Override
        public boolean getDistributor() throws HttpResponseException {
            return this.handleRequest(this.storageNodeManager::isDistributor);
        }

        @Override
        public void postActivateDistributorStart() throws HttpResponseException {
            LOGGER.log(System.Logger.Level.TRACE, "Handling postDataGridActivateDistributorStart request");
            this.handleRequest(() ->
            {
                final PromotableStorageNodeManager promotable = this.promotable();
                if (!promotable.isDistributor()) {
                    promotable.switchToDistribution();
                }
            });
        }

                /// Returns the promotable manager, or fails when this node is a fixed-role reader.
        ///
        /// @return promotable storage node manager
        /// @throws HttpResponseException when the transport has no promotion operation
        private PromotableStorageNodeManager promotable() throws HttpResponseException {
            if (this.storageNodeManager instanceof PromotableStorageNodeManager promotable) {
                return promotable;
            }
            throw HttpResponseException.notADistributor(
                    "reader promotion is unsupported for transport %s".formatted(this.storageNodeManager.getReplicationTransport()));
        }

        @Override
        public void close() {
            this.storageNodeManager.close();
        }
    }

        /// Serves requests for a node that reads from a backup.
    final class BackupNode extends Abstract {
        private static final System.Logger LOGGER = System.getLogger(BackupNode.class.getName());
        private final BackupNodeManager backupNodeManager;

        private BackupNode(final BackupNodeManager backupNodeManager) {
            super(backupNodeManager);
            this.backupNodeManager = backupNodeManager;
        }

        @Override
        public void postBackup(PostBackupRequest body) throws HttpResponseException {
            LOGGER.log(System.Logger.Level.TRACE, "Handling postDataGridBackup request");
            if (body == null || body.useManualSlot() == null) {
                throw HttpResponseException.badRequest("backup request must specify useManualSlot");
            }
            this.handleRequest(() ->
            {
                try {
                    this.backupNodeManager.createStorageBackup(unbox(body.useManualSlot()));
                } catch (final BackupBusyException busy) {
                    throw HttpResponseException.conflict("Storage backup is already running", busy);
                }
            });
        }

        @Override
        public boolean getBackup() throws HttpResponseException {
            return this.handleRequest(this.backupNodeManager::isBackupRunning);

        }

        @Override
        public void postUpdates() throws HttpResponseException {
            LOGGER.log(System.Logger.Level.TRACE, "Handling postDataGridUpdates request");
            this.handleRequest(this.backupNodeManager::stopReadingAtLatestMessage);
        }

        @Override
        public boolean getUpdates() throws HttpResponseException {
            return this.handleRequest(() ->
            {
                final boolean isReading = this.backupNodeManager.isReading();
                return !isReading;
            });
        }

        @Override
        public void postResumeUpdates() throws HttpResponseException {
            LOGGER.log(System.Logger.Level.TRACE, "Handling postDataGridResumeUpdates request");
            this.handleRequest(this.backupNodeManager::resumeReading);
        }

        @Override
        public void close() {
            this.backupNodeManager.close();

        }
    }

        /// A controller with no endpoints. Installed for roles that serve no
    /// REST surface, it rejects every route so an unserved path can never
    /// behave as an unprotected operation.
    final class NoEndpoints implements ClusterRestRequestController {
        private NoEndpoints() {
        }

        @Override
        public boolean getDistributor() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postActivateDistributorStart() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public boolean postActivateDistributorFinish() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void getHealth() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void getHealthReady() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public long getStorageBytes() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public ReplicationMetrics getReplicationMetrics() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postBackup(PostBackupRequest body) throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public boolean getBackup() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postUpdates() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public boolean getUpdates() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postResumeUpdates() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void postGc() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public boolean getGc() throws HttpResponseException {
            throw HttpResponseException.badRequest();
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
