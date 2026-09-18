package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.util.Objects;

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

        /// Immutable collaborators and role used to create a storage node manager.
    ///
    /// The builder keeps role wiring readable at the call site and makes it
    /// impossible to swap two same-typed collaborators accidentally.
    ///
    /// @param dataDistributor        binary distributor
    /// @param storageTaskExecutor    storage task executor, owned by the caller
    /// @param dataClient             replication client
    /// @param healthCheck            health check
    /// @param storageDiskSpaceReader disk-space reader
    /// @param positionProvider       position provider
    /// @param replicationTransport   transport id
    /// @param role                   fixed replication role
    record Configuration(
            StorageBinaryDataDistributor dataDistributor,
            StorageTaskExecutor storageTaskExecutor,
            StorageBinaryDataClient dataClient,
            StorageNodeHealthCheck healthCheck,
            StorageDiskSpaceReader storageDiskSpaceReader,
            ReplicationPositionProvider positionProvider,
            String replicationTransport,
            Role role
    ) {
        /// Validates the manager wiring once at the configuration boundary.
        public Configuration {
            Objects.requireNonNull(dataDistributor, "dataDistributor");
            Objects.requireNonNull(storageTaskExecutor, "storageTaskExecutor");
            Objects.requireNonNull(dataClient, "dataClient");
            Objects.requireNonNull(healthCheck, "healthCheck");
            Objects.requireNonNull(storageDiskSpaceReader, "storageDiskSpaceReader");
            Objects.requireNonNull(positionProvider, "positionProvider");
            Objects.requireNonNull(role, "role");
            if (replicationTransport == null || replicationTransport.isBlank()) {
                throw new IllegalArgumentException("replicationTransport must not be blank");
            }
        }

        /// Starts a builder for a fixed-role storage node.
        ///
        /// @return empty configuration builder
        public static Builder builder() {
            return new Builder();
        }

        /// Builds a storage node manager configuration without positional
        /// arguments whose identical types can be confused at a call site.
        public static final class Builder {
            private StorageBinaryDataDistributor dataDistributor;
            private StorageTaskExecutor storageTaskExecutor;
            private StorageBinaryDataClient dataClient;
            private StorageNodeHealthCheck healthCheck;
            private StorageDiskSpaceReader storageDiskSpaceReader;
            private ReplicationPositionProvider positionProvider;
            private String replicationTransport;
            private Role role;

            /// Creates an empty storage node manager configuration builder.
            public Builder() {
            }

            /// Sets the binary distributor.
            ///
            /// @param value binary distributor
            /// @return this builder
            public Builder dataDistributor(final StorageBinaryDataDistributor value) {
                this.dataDistributor = value;
                return this;
            }

            /// Sets the storage task executor.
            ///
            /// @param value storage task executor
            /// @return this builder
            public Builder storageTaskExecutor(final StorageTaskExecutor value) {
                this.storageTaskExecutor = value;
                return this;
            }

            /// Sets the replication client.
            ///
            /// @param value replication client
            /// @return this builder
            public Builder dataClient(final StorageBinaryDataClient value) {
                this.dataClient = value;
                return this;
            }

            /// Sets the health check.
            ///
            /// @param value health check
            /// @return this builder
            public Builder healthCheck(final StorageNodeHealthCheck value) {
                this.healthCheck = value;
                return this;
            }

            /// Sets the disk-space reader.
            ///
            /// @param value disk-space reader
            /// @return this builder
            public Builder storageDiskSpaceReader(final StorageDiskSpaceReader value) {
                this.storageDiskSpaceReader = value;
                return this;
            }

            /// Sets the latest-position provider.
            ///
            /// @param value latest-position provider
            /// @return this builder
            public Builder positionProvider(final ReplicationPositionProvider value) {
                this.positionProvider = value;
                return this;
            }

            /// Sets the transport id.
            ///
            /// @param value transport id
            /// @return this builder
            public Builder replicationTransport(final String value) {
                this.replicationTransport = value;
                return this;
            }

            /// Sets the fixed replication role.
            ///
            /// @param value fixed replication role
            /// @return this builder
            public Builder role(final Role value) {
                this.role = value;
                return this;
            }

            /// Builds the validated immutable manager configuration.
            ///
            /// @return validated immutable manager configuration
            public Configuration build() {
                return new Configuration(dataDistributor, storageTaskExecutor, dataClient, healthCheck,
                        storageDiskSpaceReader, positionProvider, replicationTransport, role);
            }
        }
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
    /// @param configuration immutable manager configuration
    /// @return storage node manager
    static StorageNodeManager New(final Configuration configuration) {
        final Configuration settings = notNull(configuration);
        return switch (settings.role()) {
            case DISTRIBUTOR -> new Distributor(settings);
            case READER -> new Reader(settings);
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
        /// @param configuration collaborators selected for this manager
        protected Base(final Configuration configuration) {
            this.dataDistributor = configuration.dataDistributor();
            this.dataClient = configuration.dataClient();
            this.healthCheck = configuration.healthCheck();
            this.storageDiskSpaceReader = configuration.storageDiskSpaceReader();
            this.storageTaskExecutor = configuration.storageTaskExecutor();
            this.positionProvider = configuration.positionProvider();
            this.replicationTransport = configuration.replicationTransport();
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
        private Reader(final Configuration configuration) {
            super(configuration);
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
        private Distributor(final Configuration configuration) {
            super(configuration);
        }

        @Override
        public boolean isDistributor() {
            return true;
        }
    }
}
