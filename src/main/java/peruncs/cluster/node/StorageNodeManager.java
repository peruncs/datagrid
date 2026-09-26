package peruncs.cluster.node;

import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.replication.ReplicationMetrics;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.StorageNodeHealthCheck;
import peruncs.cluster.node.store.StorageTaskExecutor;
import peruncs.cluster.node.store.StorageUsageGauge;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.ReplicationPublisher;

import java.util.Objects;

/// Controls a storage node with a fixed replication role.
///
/// The role is fixed when the manager is created: a reader only applies
/// replicated writes and never distributes, while the writer owns the
/// write path and always distributes. There is no reader-to-writer
/// transition; a node that must write is started in the writer
/// role, so an unsupported transition is unrepresentable.
///
/// @since 1.0
interface StorageNodeManager extends StorageNodeControl, AutoCloseable {
        /// The fixed replication role a storage node manager is created for.
    enum Role {
            /// Applies replicated writes and never distributes.
        READER,
            /// Owns the write path and always distributes.
        WRITER
    }

        /// Immutable collaborators and role used to create a storage node manager.
    ///
    /// @param dataDistributor        replication publisher
    /// @param storageTaskExecutor    storage task executor, owned by the caller
    /// @param dataClient             replication client
    /// @param healthCheck            health check
    /// @param storageUsageGauge disk-space reader
    /// @param positionProvider       position provider
    /// @param replicationTransport   transport id
    /// @param role                   fixed replication role
    /// @param graphCoordinator       the Store graph coordinator whose latched invalidity
    ///                               makes the node unhealthy and not ready
    record Configuration(
            ReplicationPublisher dataDistributor,
            StorageTaskExecutor storageTaskExecutor,
            ReplicationApplier dataClient,
            StorageNodeHealthCheck healthCheck,
            StorageUsageGauge storageUsageGauge,
            ReplicationPositionProvider positionProvider,
            String replicationTransport,
            Role role,
            StorageGraphCoordinator graphCoordinator
    ) {
        /// Validates the manager wiring once at the configuration boundary.
        public Configuration {
            Objects.requireNonNull(dataDistributor, "dataDistributor");
            Objects.requireNonNull(storageTaskExecutor, "storageTaskExecutor");
            Objects.requireNonNull(dataClient, "dataClient");
            Objects.requireNonNull(healthCheck, "healthCheck");
            Objects.requireNonNull(storageUsageGauge, "storageUsageGauge");
            Objects.requireNonNull(positionProvider, "positionProvider");
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(graphCoordinator, "graphCoordinator");
            if (replicationTransport == null || replicationTransport.isBlank()) {
                throw new IllegalArgumentException("replicationTransport must not be blank");
            }
        }
    }

        /// Creates a storage node manager for a fixed replication role.
    ///
    /// A [Role#READER] manager never distributes and offers no way to start
    /// distributing; a [Role#WRITER] manager always distributes from startup.
    /// The manager borrows every collaborator: the caller retains ownership of
    /// all of them, including `storageTaskExecutor`, and [Default#close()] never
    /// closes the executor, the disk-space reader, or any other collaborator
    /// beyond the publisher, applier, health check, and position provider.
    ///
    /// @param configuration immutable manager configuration
    /// @return storage node manager
    static StorageNodeManager create(final Configuration configuration) {
        return new Default(Objects.requireNonNull(configuration, "configuration"));
    }

        /// Closes every collaborator this manager owns.
    @Override
    void close();

        /// Shared reader monitoring, health, and lifecycle for both roles.
    final class Default implements StorageNodeManager {
        private static final System.Logger LOGGER = System.getLogger(StorageNodeManager.class.getName());

        private final ReplicationPublisher dataDistributor;
        private final StorageTaskExecutor storageTaskExecutor;
        private final ReplicationApplier dataClient;
        private final StorageNodeHealthCheck healthCheck;
        private final StorageUsageGauge storageUsageGauge;
        private final ReplicationPositionProvider positionProvider;
        private final String replicationTransport;
        private final Role role;
        private final StorageGraphCoordinator graphCoordinator;

        private volatile boolean closed;
        /* Per-collaborator completion: a failed close must stay retryable so
         * the remaining disposals finish on a later attempt instead of being
         * silently swallowed by a fail-final flag. */
        private boolean distributorDisposed;
        private boolean clientDisposed;
        private boolean healthCheckClosed;
        private boolean positionProviderClosed;

        /// Creates a manager for the configured fixed role.
        ///
        /// @param configuration collaborators selected for this manager
        private Default(final Configuration configuration) {
            this.dataDistributor = configuration.dataDistributor();
            this.dataClient = configuration.dataClient();
            this.healthCheck = configuration.healthCheck();
            this.storageUsageGauge = configuration.storageUsageGauge();
            this.storageTaskExecutor = configuration.storageTaskExecutor();
            this.positionProvider = configuration.positionProvider();
            this.replicationTransport = configuration.replicationTransport();
            this.role = configuration.role();
            this.graphCoordinator = configuration.graphCoordinator();
        }

        @Override
        public boolean isWriter() {
            return this.role == Role.WRITER;
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
        public boolean isReady() throws NodeException {
            return this.validGraph() && this.storageTaskExecutor.failure() == null && this.replicationReady();
        }

        @Override
        public boolean isHealthy() {
            return this.validGraph() && this.storageTaskExecutor.failure() == null && this.replicationHealthy();
        }

        /// A latched graph invalidity — a store update section that failed
        /// mid-application — makes the node neither healthy nor ready: queries
        /// fail closed until the node reloads or reseeds.
        private boolean validGraph() {
            return this.graphCoordinator.graphFailure() == null;
        }

        /// Reports replication readiness for the current role.
        ///
        /// A writer reports its own failure state instead of consulting
        /// the reader health check, which never observes the publication path.
        private boolean replicationReady() throws NodeException {
            return this.isWriter() ? this.dataDistributor.failure() == null : this.healthCheck.isReady();
        }

        /// Reports replication health for the current role.
        private boolean replicationHealthy() {
            return this.isWriter() ? this.dataDistributor.failure() == null : this.healthCheck.isHealthy();
        }

        @Override
        public long readStorageSizeBytes() throws NodeException {
            return this.storageUsageGauge.readUsedDiskSpaceBytes();
        }

        @Override
        public long currentSequence() {
            return this.isWriter()
                    ? this.dataDistributor.messageIndex()
                    : this.dataClient.currentSequence();
        }

        @Override
        public long latestSequence() {
            try {
                return this.positionProvider.latest().logicalSequence();
            } catch (final NodeException unavailable) {
                /* Any provider failure — an unavailable boundary for this role or a
                 * transport fault — exposes unknown as -1 to monitoring rather than
                 * turning a metrics scrape into a node failure. */
                LOGGER.log(System.Logger.Level.DEBUG, "Latest replication position is unavailable", unavailable);
                return -1L;
            }
        }

        /// Reports no metrics at all for a node without replication; the
        /// exported status then carries no placeholder values.
        @Override
        public ReplicationMetrics replicationMetrics() {
            if ("none".equalsIgnoreCase(this.replicationTransport)) {
                return null;
            }
            return new ReplicationMetrics(
                    this.currentSequence(), this.latestSequence(), this.replicationState(),
                    this.isReady(), this.isHealthy(), this.archiveUsableSpaceBytes(),
                    this.writerDurablePosition(), this.writerDurableSequence(), this.appliedSequence());
        }

        @Override
        public ReplicationState replicationState() {
            if (this.isWriter()) {
                return this.dataDistributor.failure() == null
                        ? ReplicationState.LIVE
                        : ReplicationState.FAILED;
            }
            return this.healthCheck.replicationState();
        }

        /// Returns the reader-side Archive capacity; the writer role reports `-1`
        /// because the reader health check does not observe the publication path.
        @Override
        public long archiveUsableSpaceBytes() {
            return this.isWriter() ? -1L : this.healthCheck.archiveUsableSpaceBytes();
        }

        /// Returns the reader-observed writer boundary; the writer role reports
        /// its live publication index through [#currentSequence()] instead.
        @Override
        public long writerDurablePosition() {
            return this.isWriter() ? -1L : this.healthCheck.writerDurablePosition();
        }

        /// Returns the reader-observed writer boundary; the writer reports its
        /// own published sequence through the publisher instead, since no
        /// reader exists to observe it.
        @Override
        public long writerDurableSequence() {
            return this.isWriter() ? this.dataDistributor.messageIndex()
                    : this.healthCheck.writerDurableSequence();
        }

        /// Returns the reader's last applied sequence; the writer reports through
        /// [#currentSequence()] instead.
        @Override
        public long appliedSequence() {
            return this.isWriter() ? -1L : this.healthCheck.appliedSequence();
        }

        /// Closes publisher, reader, health check, and position provider,
        /// aggregating every failure. Each disposal is tracked separately, so
        /// a failed close is retryable: a later call finishes exactly the
        /// disposals that did not complete instead of returning silently.
        @Override
        public synchronized void close() {
            if (this.closed && this.distributorDisposed && this.clientDisposed &&
                this.healthCheckClosed && this.positionProviderClosed) {
                return;
            }
            this.closed = true;
            LOGGER.log(System.Logger.Level.INFO, "Closing StorageNodeManager");
            final CloseFailures failures = new CloseFailures();
            if (!this.distributorDisposed) {
                try {
                    this.dataDistributor.dispose();
                    this.distributorDisposed = true;
                } catch (final RuntimeException | Error closeFailure) {
                    failures.add(closeFailure);
                }
            }
            if (!this.clientDisposed) {
                try {
                    this.dataClient.dispose();
                    this.clientDisposed = true;
                } catch (final RuntimeException | Error closeFailure) {
                    failures.add(closeFailure);
                }
            }
            if (!this.healthCheckClosed) {
                try {
                    this.healthCheck.close();
                    this.healthCheckClosed = true;
                } catch (final RuntimeException | Error closeFailure) {
                    failures.add(closeFailure);
                }
            }
            if (!this.positionProviderClosed) {
                try {
                    this.positionProvider.close();
                    this.positionProviderClosed = true;
                } catch (final RuntimeException | Error closeFailure) {
                    failures.add(closeFailure);
                }
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
                    throw new NodeException("failed to close storage node resources", this.failure);
                }
            }
        }
    }
}
