package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import static org.eclipse.serializer.util.X.notNull;

/// This manager controls a storage node's distributor role.
///
/// The role is fixed when the manager is created: a reader never exposes a
/// promotion operation, so an unsupported transition is unrepresentable.
/// Nodes whose transport supports promotion use
/// [PromotableStorageNodeManager] instead.
public interface StorageNodeManager extends ClusterNodeManager {
        /// Creates a reader manager with an explicit transport id for monitoring labels.
    ///
    /// The returned manager is never a distributor and offers no promotion
    /// operation. Use [PromotableStorageNodeManager] for transports that
    /// support the reader-to-distributor transition.
    ///
    /// @param dataDistributor        binary distributor
    /// @param storageTaskExecutor    storage task executor
    /// @param dataClient             replication client
    /// @param healthCheck            health check
    /// @param storageDiskSpaceReader disk-space reader
    /// @param positionProvider       position provider
    /// @param replicationTransport   transport id
    /// @return reader storage node manager
    static StorageNodeManager New(
            final StorageBinaryDataDistributor dataDistributor,
            final StorageTaskExecutor storageTaskExecutor,
            final StorageBinaryDataClient dataClient,
            final StorageNodeHealthCheck healthCheck,
            final StorageDiskSpaceReader storageDiskSpaceReader,
            final ReplicationPositionProvider positionProvider,
            final String replicationTransport
    ) {
        return new Reader(
                notNull(dataDistributor), notNull(storageTaskExecutor), notNull(dataClient), notNull(healthCheck),
                notNull(storageDiskSpaceReader), notNull(positionProvider),
                replicationTransport
        );
    }

        /// Reports whether this node is a distributor.
    ///
    /// @return `true` when distributing
    boolean isDistributor();

        /// Returns the last applied or published logical replication sequence, or `-1`.
    long getCurrentSequence();

        /// Returns the latest known writer logical sequence, or `-1` when unavailable.
    long getLatestSequence();

        /// Returns the selected transport id for monitoring (for example `aeron`).
    String getReplicationTransport();

        /// Returns the provider lifecycle state shown by monitoring endpoints.
    ReplicationHealth.State getReplicationState();

        /// Shared reader monitoring, health, and lifecycle for both roles.
    abstract class Base implements StorageNodeManager {
        private static final System.Logger LOGGER = System.getLogger(StorageNodeManager.class.getName());

        /// Binary distributor used after promotion.
        protected final StorageBinaryDataDistributor dataDistributor;
        /// Executor for storage checks.
        protected final StorageTaskExecutor storageTaskExecutor;
        /// Replication client drained before promotion.
        protected final StorageBinaryDataClient dataClient;
        /// Health check closed on promotion.
        protected final StorageNodeHealthCheck healthCheck;
        /// Disk-space reader for size reporting.
        protected final StorageDiskSpaceReader storageDiskSpaceReader;
        /// Provider for the latest writer sequence.
        protected final ReplicationPositionProvider positionProvider;
        /// Transport id for monitoring labels.
        protected final String replicationTransport;

        private volatile boolean closed;

                /// Creates a manager with the selected transport label.
        ///
        /// @param dataDistributor        binary distributor
        /// @param storageTaskExecutor    storage task executor
        /// @param dataClient             replication client
        /// @param healthCheck            health check
        /// @param storageDiskSpaceReader disk-space reader
        /// @param positionProvider       position provider
        /// @param replicationTransport   transport id
        protected Base(
                final StorageBinaryDataDistributor dataDistributor,
                final StorageTaskExecutor storageTaskExecutor,
                final StorageBinaryDataClient dataClient,
                final StorageNodeHealthCheck healthCheck,
                final StorageDiskSpaceReader storageDiskSpaceReader,
                final ReplicationPositionProvider positionProvider,
                final String replicationTransport
        ) {
            this.dataDistributor = dataDistributor;
            this.dataClient = dataClient;
            this.healthCheck = healthCheck;
            this.storageDiskSpaceReader = storageDiskSpaceReader;
            this.storageTaskExecutor = storageTaskExecutor;
            this.positionProvider = positionProvider;
            if (replicationTransport == null || replicationTransport.isBlank()) {
                throw new IllegalArgumentException("replicationTransport must not be blank");
            }
            this.replicationTransport = replicationTransport;
        }

        @Override
        public void startStorageChecks() {
            this.storageTaskExecutor.runChecks();
        }

        @Override
        public boolean isRunningStorageChecks() {
            return this.storageTaskExecutor.isRunningChecks();
        }

        @Override
        public boolean isReady() throws NodeLibraryException {
            return this.storageTaskExecutor.failure() == null && this.healthCheck.isReady();
        }

        @Override
        public boolean isHealthy() {
            return this.storageTaskExecutor.failure() == null && this.healthCheck.isHealthy();
        }

        @Override
        public long readStorageSizeBytes() throws NodeLibraryException {
            return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
        }

        @Override
        public abstract boolean isDistributor();

        @Override
        public long getCurrentSequence() {
            if (this.isDistributor()) {
                return this.dataDistributor.messageIndex();
            } else {
                return this.dataClient.cursor().logicalSequence();
            }
        }

        @Override
        public long getLatestSequence() {
            try {
                return this.positionProvider.latestSequence();
            } catch (final ReplicationPositionUnavailableException unavailable) {
                /* Reader roles cannot infer the writer boundary from an applied cursor.
                 * Expose unknown as -1 to monitoring rather than turning a metrics scrape
                 * into a node failure. */
                LOGGER.log(System.Logger.Level.DEBUG, "Latest replication position is unavailable for this node role", unavailable);
                return -1L;
            } catch (final NodeLibraryException failure) {
                throw new IllegalStateException("Failed to read latest replication position", failure);
            }
        }

        @Override
        public String getReplicationTransport() {
            return this.replicationTransport;
        }

        @Override
        public ReplicationHealth.State getReplicationState() {
            return this.healthCheck.replicationState();
        }

        @Override
        public long getArchiveUsableSpaceBytes() {
            return this.healthCheck.archiveUsableSpaceBytes();
        }

        @Override
        public long getWriterDurablePosition() {
            return this.healthCheck.writerDurablePosition();
        }

        @Override
        public long getWriterDurableSequence() {
            return this.healthCheck.writerDurableSequence();
        }

        @Override
        public long getAppliedSequence() {
            return this.healthCheck.appliedSequence();
        }

        /// Closes distributor, reader, health check, and position provider,
        /// aggregating every failure. Idempotent: every resource is attempted
        /// exactly once even when a previous attempt failed, so a retry never
        /// re-disposes an already released resource.
        @Override
        public synchronized void close() {
            LOGGER.log(System.Logger.Level.INFO, "Closing StorageNodeManager");
            if (this.closed) {
                return;
            }
            this.closed = true;
            Throwable failure = null;
            try {
                this.dataDistributor.dispose();
            } catch (final RuntimeException | Error closeFailure) {
                failure = closeFailure;
            }
            /* Promotion may have released one reader resource and failed on the
             * other, so each is guarded independently; an aggregate guard would
             * re-dispose the resource that already closed. */
            if (!this.readerClientReleased()) {
                try {
                    this.dataClient.dispose();
                } catch (final RuntimeException | Error closeFailure) {
                    if (failure == null) failure = closeFailure;
                    else failure.addSuppressed(closeFailure);
                }
            }
            if (!this.readerHealthReleased()) {
                try {
                    this.healthCheck.close();
                } catch (final RuntimeException | Error closeFailure) {
                    if (failure == null) failure = closeFailure;
                    else failure.addSuppressed(closeFailure);
                }
            }
            try {
                this.positionProvider.close();
            } catch (final RuntimeException | Error closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
            if (failure != null) {
                if (failure instanceof Error error) throw error;
                throw new NodeLibraryException("failed to close storage node resources", failure);
            }
        }

        /// Reports whether promotion already released the reader data client.
        ///
        /// A promoted node disposes its reader client during the role transition;
        /// closing it again would double-dispose. The base implementation never
        /// promotes and always releases it here.
        ///
        /// @return `true` when [PromotableStorageNodeManager] promotion released the client
        protected boolean readerClientReleased() {
            return false;
        }

        /// Reports whether promotion already released the reader health check.
        ///
        /// @return `true` when [PromotableStorageNodeManager] promotion released the health check
        protected boolean readerHealthReleased() {
            return false;
        }
    }

        /// A storage node that only reads. It exposes no promotion operation:
    /// activating distribution on it is a wiring error, not a runtime refusal.
    final class Reader extends Base {
        private Reader(
                final StorageBinaryDataDistributor dataDistributor,
                final StorageTaskExecutor storageTaskExecutor,
                final StorageBinaryDataClient dataClient,
                final StorageNodeHealthCheck healthCheck,
                final StorageDiskSpaceReader storageDiskSpaceReader,
                final ReplicationPositionProvider positionProvider,
                final String replicationTransport
        ) {
            super(dataDistributor, storageTaskExecutor, dataClient, healthCheck,
                    storageDiskSpaceReader, positionProvider, replicationTransport);
        }

        @Override
        public boolean isDistributor() {
            return false;
        }
    }
}
