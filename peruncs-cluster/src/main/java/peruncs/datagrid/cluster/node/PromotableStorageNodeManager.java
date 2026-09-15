package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.exceptions.HttpResponseException;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import static org.eclipse.serializer.util.X.notNull;

/// A storage node that starts as a reader and can switch to distribution.
///
/// Only transports with a promotable reader use this type. Aeron roles are
/// fixed at transport creation, so Aeron nodes use the plain
/// [StorageNodeManager] reader, which exposes no promotion operation at all.
public interface PromotableStorageNodeManager extends StorageNodeManager {
        /// Creates a promotable manager with an explicit transport id for monitoring labels.
    ///
    /// @param dataDistributor        binary distributor
    /// @param storageTaskExecutor    storage task executor
    /// @param dataClient             replication client
    /// @param healthCheck            health check
    /// @param storageDiskSpaceReader disk-space reader
    /// @param positionProvider       position provider
    /// @param replicationTransport   transport id, never `aeron`
    /// @return promotable storage node manager
    static PromotableStorageNodeManager New(
            final StorageBinaryDataDistributor dataDistributor,
            final StorageTaskExecutor storageTaskExecutor,
            final StorageBinaryDataClient dataClient,
            final StorageNodeHealthCheck healthCheck,
            final StorageDiskSpaceReader storageDiskSpaceReader,
            final ReplicationPositionProvider positionProvider,
            final String replicationTransport
    ) {
        if ("aeron".equalsIgnoreCase(replicationTransport)) {
            /* Aeron roles are fixed at transport creation. A reader owns a
             * persistent subscription and its provider deliberately has no writer
             * publication factory, so promoting it would report a distributor that
             * cannot replicate. Refuse the wiring instead of the call. */
            throw new IllegalArgumentException(
                    "aeron reader promotion is unsupported; start a node configured as writer");
        }
        return new Default(
                notNull(dataDistributor), notNull(storageTaskExecutor), notNull(dataClient), notNull(healthCheck),
                notNull(storageDiskSpaceReader), notNull(positionProvider),
                replicationTransport
        );
    }

        /// Starts the reader-to-distributor transition.
    void switchToDistribution();

        /// Finishes the reader-to-distributor transition.
    ///
    /// @return `true` when the transition completed
    /// @throws HttpResponseException if the node is not ready
    boolean finishDistributionSwitch() throws HttpResponseException;

        /// Implements the reader-to-distributor role transition.
    final class Default extends StorageNodeManager.Base implements PromotableStorageNodeManager {
        private static final System.Logger LOGGER = System.getLogger(PromotableStorageNodeManager.class.getName());

        private volatile boolean isDistributor;
        private volatile boolean isSwitchingToDistributor;
        private volatile boolean healthReleased;
        private volatile boolean clientReleased;

        private Default(
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
            return this.isDistributor;
        }

        /// Begins the one-way transition from replication reader to distributor.
        ///
        /// The reader is asked to stop at its latest message; a failed reader
        /// or a failed stop aborts the transition and resets it for a retry.
        @Override
        public synchronized void switchToDistribution() {
            if (this.isDistributor() || this.isSwitchingToDistributor) {
                return;
            }
            if (this.dataClient.failure() != null) {
                throw new IllegalStateException("Cannot promote a failed replication reader",
                        this.dataClient.failure());
            }
            LOGGER.log(System.Logger.Level.INFO, "Turning on distribution.");
            this.isSwitchingToDistributor = true;
            try {
                this.dataClient.stopAtLatestMessage();
            } catch (final RuntimeException | Error failure) {
                this.isSwitchingToDistributor = false;
                throw failure;
            }
        }

        /// Completes the transition started by [#switchToDistribution()].
        ///
        /// Returns `false` while the reader is still draining; once it has
        /// stopped at a resolved boundary, the reader resources are closed,
        /// the distributor continues from the reader's cursor, and the node
        /// becomes a distributor permanently — it can never become a reader
        /// again. A failed or incompletely stopped reader fails the switch
        /// instead of promoting over an unresolved boundary.
        ///
        /// @return `true` once this node distributes
        /// @throws HttpResponseException if no switch was started
        @Override
        public synchronized boolean finishDistributionSwitch() throws HttpResponseException {
            if (!this.isSwitchingToDistributor) {
                throw HttpResponseException.notADistributor("switchToDistribution() has to be called first");
            }

            if (this.isDistributor) {
                return true;
            }

            final RuntimeException readerFailure = this.dataClient.failure();
            if (readerFailure != null) {
                throw new IllegalStateException("Cannot promote a failed replication reader", readerFailure);
            }
            if (this.dataClient.isRunning()) {
                return false;
            }
            final StorageBinaryDataClient.StopOutcome stopOutcome = this.dataClient.stopResult().outcome();
            if (stopOutcome != StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY) {
                throw new IllegalStateException("Cannot promote before replication reader stopped at a resolved boundary: %s".formatted(stopOutcome));
            }

            final var cursor = this.dataClient.cursor();

            // once a node has been switched to distribution it will never become a reader node anymore
            RuntimeException failure = null;
            if (!this.healthReleased) {
                try {
                    this.healthCheck.close();
                    this.healthReleased = true;
                } catch (final RuntimeException closeFailure) {
                    failure = closeFailure;
                }
            }
            if (!this.clientReleased) {
                try {
                    this.dataClient.dispose();
                    this.clientReleased = true;
                } catch (final RuntimeException closeFailure) {
                    if (failure == null) failure = closeFailure;
                    else failure.addSuppressed(closeFailure);
                }
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
        protected boolean readerClientReleased() {
            return this.clientReleased;
        }

        @Override
        protected boolean readerHealthReleased() {
            return this.healthReleased;
        }
    }
}
