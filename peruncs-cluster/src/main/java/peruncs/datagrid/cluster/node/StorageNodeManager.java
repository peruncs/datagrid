package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import static org.eclipse.serializer.util.X.notNull;

/// This manager controls a storage node with a fixed replication role.
///
/// The role is fixed when the manager is created: a reader only applies
/// replicated writes and never distributes, while a distributor owns the
/// write path and always distributes. There is no reader-to-distributor
/// transition; a node that must distribute is started in the distributor
/// role, so an unsupported transition is unrepresentable.
public interface StorageNodeManager extends ClusterNodeManager {
        /// The fixed replication role a storage node manager is created for.
    enum Role {
            /// Applies replicated writes and never distributes.
        READER,
            /// Owns the write path and always distributes.
        DISTRIBUTOR
    }

        /// Creates a storage node manager for a fixed replication role.
    ///
    /// A [Role#READER] manager never distributes and offers no way to start
    /// distributing; a [Role#DISTRIBUTOR] manager always distributes from startup.
    /// The manager borrows every collaborator: the caller retains ownership of
    /// all of them, including `storageTaskExecutor`, and [Base#close()] never
    /// closes the executor, the disk-space reader, or any other collaborator
    /// beyond the distributor, client, health check, and position provider.
    ///
    /// @param dataDistributor        binary distributor
    /// @param storageTaskExecutor    storage task executor, owned by the caller
    /// @param dataClient             replication client
    /// @param healthCheck            health check
    /// @param storageDiskSpaceReader disk-space reader
    /// @param positionProvider       position provider
    /// @param replicationTransport   transport id
    /// @param role                   fixed replication role
    /// @return storage node manager
    static StorageNodeManager New(
            final StorageBinaryDataDistributor dataDistributor,
            final StorageTaskExecutor storageTaskExecutor,
            final StorageBinaryDataClient dataClient,
            final StorageNodeHealthCheck healthCheck,
            final StorageDiskSpaceReader storageDiskSpaceReader,
            final ReplicationPositionProvider positionProvider,
            final String replicationTransport,
            final Role role
    ) {
        return switch (notNull(role)) {
            case DISTRIBUTOR -> new Distributor(
                    notNull(dataDistributor), notNull(storageTaskExecutor), notNull(dataClient),
                    notNull(healthCheck), notNull(storageDiskSpaceReader), notNull(positionProvider),
                    replicationTransport);
            case READER -> new Reader(
                    notNull(dataDistributor), notNull(storageTaskExecutor), notNull(dataClient),
                    notNull(healthCheck), notNull(storageDiskSpaceReader), notNull(positionProvider),
                    replicationTransport);
        };
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

        /// Binary distributor used by the distributor role.
        protected final StorageBinaryDataDistributor dataDistributor;
        /// Executor for storage checks.
        protected final StorageTaskExecutor storageTaskExecutor;
        /// Replication client drained by the reader role.
        protected final StorageBinaryDataClient dataClient;
        /// Health check for the reader role.
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
            return this.storageTaskExecutor.failure() == null && this.replicationReady();
        }

        @Override
        public boolean isHealthy() {
            return this.storageTaskExecutor.failure() == null && this.replicationHealthy();
        }

        /// Reports replication readiness for the current role.
        ///
        /// A distributor reports its own failure state instead of consulting
        /// the reader health check, which never observes the publication path.
        private boolean replicationReady() throws NodeLibraryException {
            return this.isDistributor() ? this.dataDistributor.failure() == null : this.healthCheck.isReady();
        }

        /// Reports replication health for the current role.
        private boolean replicationHealthy() {
            return this.isDistributor() ? this.dataDistributor.failure() == null : this.healthCheck.isHealthy();
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
            } catch (final NodeLibraryException unavailable) {
                /* Any provider failure — an unavailable boundary for this role or a
                 * transport fault — exposes unknown as -1 to monitoring rather than
                 * turning a metrics scrape into a node failure. */
                LOGGER.log(System.Logger.Level.DEBUG, "Latest replication position is unavailable", unavailable);
                return -1L;
            }
        }

        @Override
        public String getReplicationTransport() {
            return this.replicationTransport;
        }

        @Override
        public ReplicationHealth.State getReplicationState() {
            if (this.isDistributor()) {
                return this.dataDistributor.failure() == null
                        ? ReplicationHealth.State.LIVE
                        : ReplicationHealth.State.FAILED;
            }
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
            final CloseFailures failures = new CloseFailures();
            try {
                this.dataDistributor.dispose();
            } catch (final RuntimeException | Error closeFailure) {
                failures.add(closeFailure);
            }
            try {
                this.dataClient.dispose();
            } catch (final RuntimeException | Error closeFailure) {
                failures.add(closeFailure);
            }
            try {
                this.healthCheck.close();
            } catch (final RuntimeException | Error closeFailure) {
                failures.add(closeFailure);
            }
            try {
                this.positionProvider.close();
            } catch (final RuntimeException | Error closeFailure) {
                failures.add(closeFailure);
            }
            failures.throwIfFailed();
        }

        /// Aggregates close failures while keeping an Error ahead of any
        /// RuntimeException, so an Error is never buried under the first
        /// RuntimeException as a suppressed cause.
        private static final class CloseFailures {
            private Error fatal;
            private Throwable failure;

            private void add(final Throwable closeFailure) {
                if (closeFailure instanceof Error error) {
                    if (this.fatal == null) this.fatal = error;
                    else this.fatal.addSuppressed(error);
                } else if (this.failure == null) this.failure = closeFailure;
                else this.failure.addSuppressed(closeFailure);
            }

            private void throwIfFailed() {
                if (this.fatal != null) {
                    if (this.failure != null) this.fatal.addSuppressed(this.failure);
                    throw this.fatal;
                }
                if (this.failure != null) {
                    throw new NodeLibraryException("failed to close storage node resources", this.failure);
                }
            }
        }
    }

        /// A storage node that only reads. Its role is fixed at creation:
    /// it applies replicated writes and never distributes.
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

        /// A fixed writer that always distributes.
    ///
    /// The fixed writer owns the publication path from startup, so it reports
    /// itself as the distributor and publishes the distributor's message index
    /// as its current sequence instead of the placeholder cursor a reader
    /// client would expose.
    final class Distributor extends Base {
        private Distributor(
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
            return true;
        }
    }
}
