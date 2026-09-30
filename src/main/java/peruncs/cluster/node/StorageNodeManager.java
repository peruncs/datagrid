package peruncs.cluster.node;

import peruncs.cluster.api.NodeRole;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.api.ReplicationStatus;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReplicationPendingException;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.StorageNodeHealthCheck;
import peruncs.cluster.node.store.StorageTaskExecutor;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.ReplicationPublisher;

import java.util.Objects;
import java.util.function.LongSupplier;

/// Controls a storage node with a fixed replication role.
///
/// The role is fixed when the manager is created: readers only apply
/// replicated writes, while writers and standalone nodes may write locally.
/// Only writers distribute, and there is no role transition at runtime.
///
/// @since 1.0
final class StorageNodeManager implements StorageNodeControl, AutoCloseable {
    /// Immutable collaborators and role used to create a storage node manager.
    ///
    /// @param dataDistributor        replication publisher
    /// @param storageTaskExecutor    storage task executor, owned by the caller
    /// @param dataClient             replication client
    /// @param healthCheck            health check
    /// @param storageSizeBytes supplier for the latest disk-space snapshot
    /// @param positionProvider       position provider
    /// @param replicationEnabled     whether the node has replication
    /// @param role                   fixed node role
    /// @param graphCoordinator       the Store graph coordinator whose latched invalidity
    ///                               makes the node unhealthy and not ready
    record Configuration(
            ReplicationPublisher dataDistributor,
            StorageTaskExecutor storageTaskExecutor,
            ReplicationApplier dataClient,
            StorageNodeHealthCheck healthCheck,
            LongSupplier storageSizeBytes,
            ReplicationPositionProvider positionProvider,
            boolean replicationEnabled,
            NodeRole role,
            StorageGraphCoordinator graphCoordinator
    ) {
        /// Validates the manager wiring once at the configuration boundary.
        public Configuration {
            Objects.requireNonNull(dataDistributor, "dataDistributor");
            Objects.requireNonNull(storageTaskExecutor, "storageTaskExecutor");
            Objects.requireNonNull(dataClient, "dataClient");
            Objects.requireNonNull(healthCheck, "healthCheck");
            Objects.requireNonNull(storageSizeBytes, "storageSizeBytes");
            Objects.requireNonNull(positionProvider, "positionProvider");
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(graphCoordinator, "graphCoordinator");
            if (role == NodeRole.BACKUP_READER) {
                throw new IllegalArgumentException("backup readers use BackupNodeManager");
            }
        }
    }

    /// Creates a storage node manager for a fixed replication role.
    ///
    /// A [NodeRole#READER] manager never distributes; a
    /// [NodeRole#WRITER] manager distributes from startup. Standalone nodes
    /// use the writer-facing manager without replication.
    /// The manager borrows every collaborator: the caller retains ownership of
    /// all of them, including `storageTaskExecutor`, and [#close()] never closes the
    /// executor,
    /// disk-space reader, or any collaborator beyond the publisher, applier,
    /// health check, and position provider.
    ///
    /// @param configuration immutable manager configuration
    /// @return storage node manager
    static StorageNodeManager create(final Configuration configuration) {
        return new StorageNodeManager(Objects.requireNonNull(configuration, "configuration"));
    }

    private static final System.Logger LOGGER = System.getLogger(StorageNodeManager.class.getName());

    private final ReplicationPublisher dataDistributor;
    private final StorageTaskExecutor storageTaskExecutor;
    private final ReplicationApplier dataClient;
    private final StorageNodeHealthCheck healthCheck;
    private final LongSupplier storageSizeBytes;
    private final ReplicationPositionProvider positionProvider;
    private final boolean replicationEnabled;
    private final NodeRole role;
    private final StorageGraphCoordinator graphCoordinator;

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
    private StorageNodeManager(final Configuration configuration) {
        this.dataDistributor = configuration.dataDistributor();
        this.dataClient = configuration.dataClient();
        this.healthCheck = configuration.healthCheck();
        this.storageSizeBytes = configuration.storageSizeBytes();
        this.storageTaskExecutor = configuration.storageTaskExecutor();
        this.positionProvider = configuration.positionProvider();
        this.replicationEnabled = configuration.replicationEnabled();
        this.role = configuration.role();
        this.graphCoordinator = configuration.graphCoordinator();
    }

    @Override
    public boolean isWriter() {
        return this.role.canWrite();
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
        return this.storageSizeBytes.getAsLong();
    }

    private long latestSequence() {
        try {
            return this.positionProvider.latest().sequence();
        } catch (final NodeException unavailable) {
            /* Any provider failure — an unavailable boundary for this role or a
             * transport fault — exposes an unknown boundary to monitoring rather than
             * turning a metrics scrape into a node failure. */
            LOGGER.log(System.Logger.Level.DEBUG, "Latest replication position is unavailable", unavailable);
            return -1L;
        }
    }

    @Override
    public ReplicationStatus replicationStatus() {
        if (!this.replicationEnabled) return ReplicationStatus.notConfigured();
        final boolean writer = this.isWriter();
        final long currentSequence = writer
                ? this.dataDistributor.messageIndex() : this.dataClient.currentSequence();
        return new ReplicationStatus(
                this.replicationState(),
                unknownIfNegative(currentSequence),
                this.latestSequence(),
                unknownIfNegative(writer ? -1L : this.healthCheck.archiveUsableSpaceBytes()),
                unknownIfNegative(writer ? -1L : this.healthCheck.writerDurablePosition()),
                unknownIfNegative(writer ? currentSequence
                        : this.healthCheck.writerDurableSequence()),
                unknownIfNegative(writer ? -1L : this.healthCheck.appliedSequence()));
    }

    private static long unknownIfNegative(final long value) {
        return Math.max(-1L, value);
    }

    private ReplicationState replicationState() {
        if (this.isWriter()) {
            final RuntimeException failure = this.dataDistributor.failure();
            if (failure instanceof ReplicationPendingException) return ReplicationState.REPLICATION_SUSPENDED;
            return failure == null ? ReplicationState.LIVE : ReplicationState.FAILED;
        }
        return this.healthCheck.replicationState();
    }

    /// Closes publisher, reader, health check, and position provider,
    /// aggregating every failure. Each disposal is tracked separately, so
    /// a failed close is retryable: a later call finishes exactly the
    /// disposals that did not complete instead of returning silently.
    @Override
    public void close() {
        if (this.distributorDisposed && this.clientDisposed &&
            this.healthCheckClosed && this.positionProviderClosed) {
            return;
        }
        /* NodeLifecycle's CloseSequencer is the sole caller and serializes
         * close attempts; the per-resource flags preserve retry progress. */
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
