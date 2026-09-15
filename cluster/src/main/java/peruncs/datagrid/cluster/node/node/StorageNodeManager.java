package peruncs.datagrid.cluster.node.node;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peruncs.datagrid.cluster.node.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.node.exceptions.NotADistributorException;
import peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.node.replication.ClusterStorageBinaryDataClient;
import peruncs.datagrid.cluster.node.replication.ClusterStorageBinaryDataDistributor;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;

import static org.eclipse.serializer.util.X.notNull;

/// This manager controls a storage node's distributor role.
///
/// A node starts as a reader and can switch to distribution only through the
/// two-phase activation methods. The switch is complete only after the finish
/// step confirms that the new role is ready.
public interface StorageNodeManager extends ClusterNodeManager {
        /// Creates a manager with an explicit transport id for monitoring labels.
    ///
    /// @param dataDistributor        binary distributor
    /// @param storageTaskExecutor    storage task executor
    /// @param dataClient             replication client
    /// @param healthCheck            health check
    /// @param storageDiskSpaceReader disk-space reader
    /// @param positionProvider       position provider
    /// @param replicationTransport   transport id
    /// @return storage node manager
    static StorageNodeManager New(
            final ClusterStorageBinaryDataDistributor dataDistributor,
            final StorageTaskExecutor storageTaskExecutor,
            final ClusterStorageBinaryDataClient dataClient,
            final StorageNodeHealthCheck healthCheck,
            final StorageDiskSpaceReader storageDiskSpaceReader,
            final ReplicationPositionProvider positionProvider,
            final String replicationTransport
    ) {
        return new Default(
                notNull(dataDistributor), notNull(storageTaskExecutor), notNull(dataClient), notNull(healthCheck),
                notNull(storageDiskSpaceReader), notNull(positionProvider),
                replicationTransport
        );
    }

        /// Reports whether this node is a distributor.
    ///
    /// @return `true` when distributing
    boolean isDistributor();

        /// Starts the reader-to-distributor transition.
    void switchToDistribution();

        /// Finishes the reader-to-distributor transition.
    ///
    /// @return `true` when the transition completed
    /// @throws NotADistributorException if the node is not ready
    boolean finishDistributionSwitch() throws NotADistributorException;

        /// Returns the last applied or published logical replication sequence, or `-1`.
    long getCurrentMessageIndex();

        /// Returns the latest known writer logical sequence, or `-1` when unavailable.
    long getLatestMessageIndex();

        /// Returns the selected transport id for monitoring (for example `aeron`).
    String getReplicationTransport();

        /// Returns the provider lifecycle state shown by monitoring endpoints.
    ReplicationHealth.State getReplicationState();

        /// Implements the reader-to-distributor role transition.
    final class Default implements StorageNodeManager {
        private static final Logger LOG = LoggerFactory.getLogger(StorageNodeManager.class);

        private final ClusterStorageBinaryDataDistributor dataDistributor;
        private final StorageTaskExecutor storageTaskExecutor;
        private final ClusterStorageBinaryDataClient dataClient;
        private final StorageNodeHealthCheck healthCheck;
        private final StorageDiskSpaceReader storageDiskSpaceReader;
        private final ReplicationPositionProvider positionProvider;
        private final String replicationTransport;

        private volatile boolean isDistributor;
        private volatile boolean isSwitchingToDistributor;
        private volatile boolean closed;
        private volatile boolean positionProviderClosed;

                /// Creates a manager with the selected transport label.
        ///
        /// @param dataDistributor        binary distributor
        /// @param storageTaskExecutor    storage task executor
        /// @param dataClient             replication client
        /// @param healthCheck            health check
        /// @param storageDiskSpaceReader disk-space reader
        /// @param positionProvider       position provider
        /// @param replicationTransport   transport id
        public Default(
                final ClusterStorageBinaryDataDistributor dataDistributor,
                final StorageTaskExecutor storageTaskExecutor,
                final ClusterStorageBinaryDataClient dataClient,
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
        public boolean isReady() throws NodelibraryException {
            return this.healthCheck.isReady();
        }

        @Override
        public boolean isHealthy() {
            return this.healthCheck.isHealthy();
        }

        @Override
        public long readStorageSizeBytes() throws NodelibraryException {
            return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
        }

        @Override
        public boolean isDistributor() {
            return this.isDistributor;
        }

        /// Begins the one-way transition from replication reader to distributor.
        ///
        /// Aeron roles are fixed at transport creation, so an Aeron reader
        /// can never promote — start a node configured as writer instead.
        /// Otherwise the reader is asked to stop at its latest message; a
        /// failed reader or a failed stop aborts the transition and resets
        /// it for a retry.
        @Override
        public synchronized void switchToDistribution() {
            if (this.isDistributor() || this.isSwitchingToDistributor) {
                return;
            }
            if ("aeron".equalsIgnoreCase(this.replicationTransport)) {
                /* Aeron roles are fixed at transport creation.  A reader owns a
                 * persistent subscription and its provider deliberately has no writer
                 * publication factory, so promoting it would report a distributor that
                 * cannot replicate.  Reject the transition before stopping the reader. */
                throw new UnsupportedOperationException(
                        "%s reader promotion is unsupported; start a node configured as writer".formatted(this.replicationTransport));
            }

            if (this.dataClient.failure() != null) {
                throw new IllegalStateException("Cannot promote a failed replication reader",
                        this.dataClient.failure());
            }
            LOG.info("Turning on distribution.");
            this.isSwitchingToDistributor = true;
            try {
                this.dataClient.stopAtLatestMessage();
            } catch (final RuntimeException | Error failure) {
                this.isSwitchingToDistributor = false;
                throw failure;
            }
        }

        /// Completes the transition started by [switchToDistribution()][#switchToDistribution()].
        ///
        /// Returns `false` while the reader is still draining; once it has
        /// stopped at a resolved boundary, the reader resources are closed,
        /// the distributor continues from the reader's cursor, and the node
        /// becomes a distributor permanently — it can never become a reader
        /// again. A failed or incompletely stopped reader fails the switch
        /// instead of promoting over an unresolved boundary.
        ///
        /// @return `true` once this node distributes
        /// @throws NotADistributorException if no switch was started
        @Override
        public synchronized boolean finishDistributionSwitch() throws NotADistributorException {
            if (!this.isSwitchingToDistributor) {
                throw new NotADistributorException("switchToDistribution() has to be called first");
            }

            if (this.isDistributor) {
                return true;
            }
            if ("aeron".equalsIgnoreCase(this.replicationTransport)) {
                /* Keep the invariant defensive if a stale flag or an older caller reaches
                 * this method without passing through switchToDistribution(). */
                throw new UnsupportedOperationException(
                        "%s reader promotion is unsupported; start a node configured as writer".formatted(this.replicationTransport));
            }

            final RuntimeException readerFailure = this.dataClient.failure();
            if (readerFailure != null) {
                throw new IllegalStateException("Cannot promote a failed replication reader", readerFailure);
            }
            if (this.dataClient.isRunning()) {
                return false;
            }
            final ClusterStorageBinaryDataClient.StopOutcome stopOutcome = this.dataClient.stopResult().outcome();
            if (stopOutcome == ClusterStorageBinaryDataClient.StopOutcome.STOPPING ||
                stopOutcome == ClusterStorageBinaryDataClient.StopOutcome.TIMED_OUT ||
                stopOutcome == ClusterStorageBinaryDataClient.StopOutcome.FAILED) {
                throw new IllegalStateException("Cannot promote before replication reader stopped at a resolved boundary: %s".formatted(stopOutcome));
            }

            final var cursor = this.dataClient.cursor();

            // once a node has been switched to distribution it will never become a reader node anymore
            RuntimeException failure = null;
            try {
                this.healthCheck.close();
            } catch (final RuntimeException closeFailure) {
                failure = closeFailure;
            }
            try {
                this.dataClient.dispose();
            } catch (final RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
            if (failure != null) {
                throw new IllegalStateException("failed to close reader resources during promotion", failure);
            }
            this.dataDistributor.messageIndex(cursor.logicalSequence());

            this.isDistributor = true;
            this.isSwitchingToDistributor = false;
            return true;
        }

        @Override
        public long getCurrentMessageIndex() {
            if (this.isDistributor()) {
                return this.dataDistributor.messageIndex();
            } else {
                return this.dataClient.cursor().logicalSequence();
            }
        }

        @Override
        public long getLatestMessageIndex() {
            try {
                return this.positionProvider.latestSequence();
            } catch (final ReplicationPositionUnavailableException unavailable) {
                /* Reader roles cannot infer the writer boundary from an applied cursor.
                 * Expose unknown as -1 to monitoring rather than turning a metrics scrape
                 * into a node failure. */
                LOG.debug("Latest replication position is unavailable for this node role", unavailable);
                return -1L;
            } catch (final NodelibraryException failure) {
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
        /// aggregating every failure. Idempotent.
        @Override
        public synchronized void close() {
            LOG.info("Closing StorageNodeManager");
            if (this.closed) {
                return;
            }
            RuntimeException failure = null;
            try {
                this.dataDistributor.dispose();
            } catch (final RuntimeException closeFailure) {
                failure = closeFailure;
            }
            try {
                this.dataClient.dispose();
            } catch (final RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
            try {
                this.healthCheck.close();
            } catch (final RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
            try {
                this.closePositionProvider();
            } catch (final RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
            if (failure != null) {
                throw new IllegalStateException("failed to close storage node resources", failure);
            }
            this.closed = true;
        }

        private void closePositionProvider() {
            if (this.positionProviderClosed) {
                return;
            }
            this.positionProvider.close();
            /* Mark ownership released only after close succeeds.  A provider can
             * legitimately fail during a bounded shutdown (for example while its
             * Archive control session is stopping); the enclosing close() is retryable
             * and must not turn that first failure into a silent resource leak. */
            this.positionProviderClosed = true;
        }
    }
}
