package peruncs.datagrid.cluster.node;

import org.eclipse.serializer.exceptions.MissingFoundationPartException;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.Unpersistable;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.serializer.util.InstanceDispatcher;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.exceptions.StorageException;
import org.eclipse.store.storage.types.*;
import peruncs.datagrid.cluster.node.aeron.AeronClusterReplicationTransportProvider;
import peruncs.datagrid.cluster.node.backup.*;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.node.http.ClusterRestRequestController;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.node.store.*;
import peruncs.datagrid.cluster.storage.types.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

/// This foundation assembles the services that make one cluster node run.
///
/// A builder creates one immutable node configuration, and the resulting node
/// owns the lazily-created runtime resources for its complete lifecycle.
///
/// The node is single-use: start it at most once and close it when its work is
/// complete. Configuration cannot be changed after {@link Builder#build()}.
public interface ClusterFoundation extends InstanceDispatcher, AutoCloseable {
        /// Creates a mutable builder for one immutable node configuration.
    ///
    /// @return a new builder
    static Builder New() {
        return new Builder();
    }

        /// Builds the collaborators used by one node before lifecycle starts.
    final class Builder {
        private StorageBackupBackend backupBackend;
        private StorageTaskExecutor storageTaskExecutor;
        private StorageBackupTaskExecutor storageBackupTaskExecutor;
        private ClusterReplicationTransport replicationTransport;
        private StorageBinaryDataPacketAcceptor dataPacketAcceptor;
        private StorageBinaryDataMerger dataMerger;
        private AfterDataMessageConsumedListener afterDataMessageConsumedListener;
        private StoredReplicationCursorManager storedReplicationCursorManager;
        private StorageBackupManager storageBackupManager;
        private Supplier<Object> rootSupplier;
        private ObjectGraphUpdateHandler graphUpdateHandler;
        private EmbeddedStorageFoundation<?> embeddedStorageFoundation;
        private BackupNodeManager backupNodeManager;
        private StorageBinaryDataClient dataClient;
        private StorageBinaryDataDistributor dataDistributor;
        private StorageNodeHealthCheck healthCheck;
        private NodeLibraryPropertiesProvider propertiesProvider;
        private StorageDiskSpaceReader storageDiskSpaceReader;
        private StorageNodeManager storageNodeManager;
        private boolean enableAsyncDistribution;
        private ReplicationPositionProvider positionProvider;
        private ReplicationLogRetention replicationRetention;

        public Builder setStorageBackupBackend(final StorageBackupBackend value) { this.backupBackend = value; return this; }
        public Builder setStorageTaskExecutor(final StorageTaskExecutor value) { this.storageTaskExecutor = value; return this; }
        public Builder setStorageBackupTaskExecutor(final StorageBackupTaskExecutor value) { this.storageBackupTaskExecutor = value; return this; }
        public Builder setClusterReplicationTransport(final ClusterReplicationTransport value) { this.replicationTransport = value; return this; }
        public Builder setStorageBinaryDataPacketAcceptor(final StorageBinaryDataPacketAcceptor value) { this.dataPacketAcceptor = value; return this; }
        public Builder setStorageBinaryDataMerger(final StorageBinaryDataMerger value) { this.dataMerger = value; return this; }
        public Builder setAfterDataMessageConsumedListener(final AfterDataMessageConsumedListener value) { this.afterDataMessageConsumedListener = value; return this; }
        public Builder setStoredReplicationCursorManager(final StoredReplicationCursorManager value) { this.storedReplicationCursorManager = value; return this; }
        public Builder setStorageBackupManager(final StorageBackupManager value) { this.storageBackupManager = value; return this; }
        public Builder setRootSupplier(final Supplier<Object> value) { this.rootSupplier = value; return this; }
        public Builder setObjectGraphUpdateHandler(final ObjectGraphUpdateHandler value) { this.graphUpdateHandler = value; return this; }
        public Builder setEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> value) { this.embeddedStorageFoundation = value; return this; }
        public Builder setBackupNodeManager(final BackupNodeManager value) { this.backupNodeManager = value; return this; }
        public Builder setStorageBinaryDataClient(final StorageBinaryDataClient value) { this.dataClient = value; return this; }
        public Builder setStorageBinaryDataDistributor(final StorageBinaryDataDistributor value) { this.dataDistributor = value; return this; }
        public Builder setStorageNodeHealthCheck(final StorageNodeHealthCheck value) { this.healthCheck = value; return this; }
        public Builder setNodeLibraryPropertiesProvider(final NodeLibraryPropertiesProvider value) { this.propertiesProvider = value; return this; }
        public Builder setStorageDiskSpaceReader(final StorageDiskSpaceReader value) { this.storageDiskSpaceReader = value; return this; }
        public Builder setStorageNodeManager(final StorageNodeManager value) { this.storageNodeManager = value; return this; }
        public Builder setEnableAsyncDistribution(final boolean value) { this.enableAsyncDistribution = value; return this; }
        public Builder setReplicationPositionProvider(final ReplicationPositionProvider value) { this.positionProvider = value; return this; }
        public Builder setReplicationLogRetention(final ReplicationLogRetention value) { this.replicationRetention = value; return this; }

        public ClusterFoundation build() {
            return new Node(new NodeConfiguration(
                    this.backupBackend, this.storageTaskExecutor, this.storageBackupTaskExecutor,
                    this.replicationTransport, this.dataPacketAcceptor, this.dataMerger,
                    this.afterDataMessageConsumedListener, this.storedReplicationCursorManager,
                    this.storageBackupManager, this.rootSupplier, this.graphUpdateHandler,
                    this.embeddedStorageFoundation, this.backupNodeManager, this.dataClient,
                    this.dataDistributor, this.healthCheck, this.propertiesProvider,
                    this.storageDiskSpaceReader, this.storageNodeManager,
                    this.enableAsyncDistribution, this.positionProvider, this.replicationRetention));
        }
    }

        /// Immutable pre-start node configuration.
    record NodeConfiguration(
            StorageBackupBackend backupBackend,
            StorageTaskExecutor storageTaskExecutor,
            StorageBackupTaskExecutor storageBackupTaskExecutor,
            ClusterReplicationTransport replicationTransport,
            StorageBinaryDataPacketAcceptor dataPacketAcceptor,
            StorageBinaryDataMerger dataMerger,
            AfterDataMessageConsumedListener afterDataMessageConsumedListener,
            StoredReplicationCursorManager storedReplicationCursorManager,
            StorageBackupManager storageBackupManager,
            Supplier<Object> rootSupplier,
            ObjectGraphUpdateHandler graphUpdateHandler,
            EmbeddedStorageFoundation<?> embeddedStorageFoundation,
            BackupNodeManager backupNodeManager,
            StorageBinaryDataClient dataClient,
            StorageBinaryDataDistributor dataDistributor,
            StorageNodeHealthCheck healthCheck,
            NodeLibraryPropertiesProvider propertiesProvider,
            StorageDiskSpaceReader storageDiskSpaceReader,
            StorageNodeManager storageNodeManager,
            boolean enableAsyncDistribution,
            ReplicationPositionProvider positionProvider,
            ReplicationLogRetention replicationRetention) {
    }


        /// Starts the request controller.
    ///
    /// @return request controller
    /// @throws NodeLibraryException if startup fails
    ClusterRestRequestController startController() throws NodeLibraryException;

        /// Starts the storage manager.
    ///
    /// @return storage manager
    /// @throws NodeLibraryException if startup fails
    ClusterStorageManager<?> startStorageManager() throws NodeLibraryException;

        /// Closes every resource created by this foundation in reverse dependency order.
    @Override
    void close();

        /// Runs the lifecycle for one immutable node configuration.
    final class Node extends InstanceDispatcher.Default
            implements ClusterFoundation, Unpersistable {
        private static final System.Logger LOGGER = System.getLogger(ClusterFoundation.class.getName());

        private final LazyConstant<StorageBackupBackend> backupBackend;
        private final LazyConstant<EmbeddedStorageFoundation<?>> embeddedStorageFoundation;
        private final LazyConstant<NodeHousekeeper> housekeeper;
        private final LazyConstant<StorageLimitGate> storageLimitGate;
        private final LazyConstant<BackupNodeManager> backupNodeManager;
        private final LazyConstant<StorageBinaryDataClient> dataClient;
        private final LazyConstant<StorageBinaryDataDistributor> dataDistributor;
        private final LazyConstant<StorageNodeHealthCheck> healthCheck;
        private final LazyConstant<NodeLibraryPropertiesProvider> propertiesProvider;
        private final LazyConstant<StorageTaskExecutor> storageTaskExecutor;
        private final LazyConstant<StorageBackupTaskExecutor> storageBackupTaskExecutor;
        private final LazyConstant<StorageDiskSpaceReader> storageDiskSpaceReader;
        private final LazyConstant<StorageNodeManager> storageNodeManager;
        private final boolean enableAsyncDistribution;
        private final LazyConstant<Supplier<Object>> rootSupplier;
        private final LazyConstant<ObjectGraphUpdateHandler> graphUpdateHandler;
        private final LazyConstant<StorageBackupManager> storageBackupManager;
        private final LazyConstant<AfterDataMessageConsumedListener> afterDataMessageConsumedListener;
        private final LazyConstant<StorageBinaryDataMerger> dataMerger;
        private final LazyConstant<StorageBinaryDataPacketAcceptor> dataPacketAcceptor;
        /* Intentionally not a LazyConstant: a backup restore closes and replaces
         * this manager, which a one-shot memoized holder cannot express. The
         * volatile field with double-checked locking gives the same safe
         * publication without a per-access lock. */
        private volatile StoredReplicationCursorManager storedReplicationCursorManager;
        private final LazyConstant<ClusterReplicationTransport> replicationTransport;
        private final LazyConstant<ReplicationPositionProvider> positionProvider;
        private final LazyConstant<ReplicationLogRetention> replicationRetention;

        // cached created types
        private ClusterStorageManager<?> clusterStorageManager;
        private ClusterRestRequestController clusterRequestController;
        private boolean started;
        private boolean closed;
        private boolean closing;

        private Node(final NodeConfiguration configuration) {
            this.backupBackend = lazy(configuration.backupBackend(),
                    () -> this.dispatch(this.ensureBackupBackend()));
            this.storageTaskExecutor = lazy(configuration.storageTaskExecutor(),
                    () -> this.dispatch(this.ensureStorageTaskExecutor()));
            this.storageBackupTaskExecutor = lazy(
                    configuration.storageBackupTaskExecutor(),
                    () -> this.dispatch(this.ensureStorageBackupTaskExecutor()));
            this.housekeeper = LazyConstant.of(() -> this.dispatch(this.ensureNodeHousekeeper()));
            this.storageLimitGate = LazyConstant.of(() -> this.dispatch(this.ensureStorageLimitGate()));
            this.replicationTransport = lazy(
                    configuration.replicationTransport(),
                    () -> this.dispatch(this.ensureClusterReplicationTransport()));
            this.dataPacketAcceptor = lazy(configuration.dataPacketAcceptor(),
                    () -> this.dispatch(this.ensureDataPacketAcceptor()));
            this.dataMerger = lazy(configuration.dataMerger(),
                    () -> this.dispatch(this.ensureStorageBinaryDataMerger()));
            this.afterDataMessageConsumedListener = lazy(
                    configuration.afterDataMessageConsumedListener(),
                    () -> this.dispatch(this.ensureAfterDataMessageConsumedListener()));
            this.storedReplicationCursorManager = configuration.storedReplicationCursorManager();
            this.storageBackupManager = lazy(configuration.storageBackupManager(),
                    () -> this.dispatch(this.ensureStorageBackupManager()));
            this.rootSupplier = lazy(configuration.rootSupplier(),
                    () -> this.dispatch(this.ensureRootSupplier()));
            this.graphUpdateHandler = lazy(configuration.graphUpdateHandler(),
                    () -> this.dispatch(this.ensureGraphUpdateHandler()));
            this.embeddedStorageFoundation = lazy(
                    configuration.embeddedStorageFoundation(),
                    () -> this.dispatch(this.ensureEmbeddedStorageFoundation()));
            this.backupNodeManager = lazy(configuration.backupNodeManager(),
                    () -> this.dispatch(this.ensureBackupNodeManager()));
            this.dataClient = lazy(configuration.dataClient(),
                    () -> this.dispatch(this.ensureStorageBinaryDataClient()));
            this.dataDistributor = lazy(configuration.dataDistributor(),
                    () -> this.dispatch(this.ensureDataDistributor()));
            this.healthCheck = lazy(configuration.healthCheck(),
                    () -> this.dispatch(this.ensureStorageNodeHealthCheck()));
            this.propertiesProvider = lazy(
                    configuration.propertiesProvider(),
                    () -> this.dispatch(this.ensureNodeLibraryPropertiesProvider()));
            this.storageDiskSpaceReader = lazy(
                    configuration.storageDiskSpaceReader(),
                    () -> this.dispatch(this.ensureStorageDiskSpaceReader()));
            this.storageNodeManager = lazy(configuration.storageNodeManager(),
                    () -> this.dispatch(this.ensureStorageNodeManager()));
            this.enableAsyncDistribution = configuration.enableAsyncDistribution();
            this.positionProvider = lazy(
                    configuration.positionProvider(),
                    () -> this.dispatch(this.ensureReplicationPositionProvider()));
            this.replicationRetention = lazy(
                    configuration.replicationRetention(),
                    () -> this.dispatch(this.ensureReplicationLogRetention()));
        }

        private static <T> LazyConstant<T> lazy(final T configured, final Supplier<? extends T> factory) {
            return LazyConstant.of(() -> configured == null ? factory.get() : configured);
        }

        private static Path backupVolumePath(final NodeLibraryPropertiesProvider properties) {
            final String configured = properties.replicationProperty(
                    NodeLibraryPropertiesProvider.Env.EnvKeys.BACKUP_PATH);
            return Paths.get(configured == null || configured.isBlank() ? "backups" : configured)
                    .toAbsolutePath().normalize();
        }

                /// Resolves a maintenance interval with its node default.
        ///
        /// @param configured      configured minutes, or `null`
        /// @param property        property name used in failure messages
        /// @param fallbackMinutes node default in minutes
        /// @return interval
        private static Duration maintenanceInterval(
                final Integer configured,
                final String property,
                final int fallbackMinutes
        ) {
            if (configured == null) {
                return Duration.ofMinutes(fallbackMinutes);
            }
            if (configured <= 0) {
                throw new NodeLibraryException("%s must be configured as a positive integer".formatted(property));
            }
            return Duration.ofMinutes(configured);
        }

        private static int requiredPositive(final Integer value, final String property) {
            if (value == null || value <= 0) {
                throw new NodeLibraryException("%s must be configured as a positive integer".formatted(property));
            }
            return value;
        }

        private static Throwable closeResource(final Throwable current, final Runnable close) {
            try {
                close.run();
                return current;
            } catch (final Throwable closeFailure) {
                if (current == null) {
                    return closeFailure;
                }
                current.addSuppressed(closeFailure);
                return current;
            }
        }

        private static Throwable closeInitialized(
                final Throwable current,
                final LazyConstant<?> resource,
                final Runnable close
        ) {
            return resource.isInitialized() ? closeResource(current, close) : current;
        }

        private static void throwFailure(final Throwable failure, final String message) {
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new NodeLibraryException(message, failure);
        }


                /// Creates the configured backup backend.
        ///
        /// @return backup backend
        private StorageBackupBackend ensureBackupBackend() {
            return FilesystemVolumeBackupBackend.New(
                    backupVolumePath(this.getNodeLibraryPropertiesProvider())
            );
        }

                /// Creates the storage task executor.
        ///
        /// @return storage task executor
        private StorageTaskExecutor ensureStorageTaskExecutor() {
            if (this.getNodeLibraryPropertiesProvider().isBackupNode()) {
                return this.getStorageBackupTaskExecutor();
            }
            return StorageTaskExecutor.New(this.clusterStorageManager);
        }

                /// Creates the backup task executor.
        ///
        /// @return backup task executor
        private StorageBackupTaskExecutor ensureStorageBackupTaskExecutor() {
            return StorageBackupTaskExecutor.New(this.clusterStorageManager, this.getStorageBackupManager());
        }

                /// Creates the node maintenance housekeeper.
        ///
        /// @return housekeeper
        private NodeHousekeeper ensureNodeHousekeeper() {
            return NodeHousekeeper.New();
        }

                /// Creates the storage limit gate.
        ///
        /// @return limit gate
        private StorageLimitGate ensureStorageLimitGate() {
            return StorageLimitGate.New(
                    requiredPositive(
                            this.getNodeLibraryPropertiesProvider().storageLimitGB(),
                            "STORAGE_LIMIT_GB"
                    )
            );
        }

                /// Creates the Aeron replication transport, or a no-op transport when disabled.
        ///
        /// @return replication transport
        private ClusterReplicationTransport ensureClusterReplicationTransport() {
            final String configured = this.getNodeLibraryPropertiesProvider().replicationTransport();
            final String requested = configured == null ? "none" : configured.trim();
            if ("none".equalsIgnoreCase(requested)) {
                return ClusterReplicationTransport.noOp();
            }
            if ("aeron".equalsIgnoreCase(requested)) {
                return new AeronClusterReplicationTransportProvider()
                        .create(this.getNodeLibraryPropertiesProvider());
            }
            throw new NodeLibraryException("Replication transport must be 'aeron' or 'none'");
        }

                /// Creates the replication position provider.
        ///
        /// @return position provider
        private ReplicationPositionProvider ensureReplicationPositionProvider() {
            return this.getClusterReplicationTransport().positionProvider(
                    this.getNodeLibraryPropertiesProvider().replicationStreamName()
            );
        }

                /// Creates the replication retention policy.
        ///
        /// @return retention policy
        private ReplicationLogRetention ensureReplicationLogRetention() {
            return this.getClusterReplicationTransport().retention();
        }

                /// Creates the stored replication-cursor manager.
        ///
        /// @return stored replication-cursor manager
        private StoredReplicationCursorManager ensureStoredReplicationCursorManager() {
            final var cursorPath = this.storageParentPath().resolve("offset");
            LOGGER.log(System.Logger.Level.TRACE, "Creating StoredReplicationCursorManager for offset file at %s".formatted(cursorPath));
            return StoredReplicationCursorManager.NewAtomic(cursorPath);
        }

                /// Returns the configured storage root, defaulting to a node-local directory.
        private Path storageParentPath() {
            final String configured = this.getNodeLibraryPropertiesProvider()
                    .replicationProperty(NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_PATH);
            return Paths.get(configured == null || configured.isBlank() ? "storage" : configured)
                    .toAbsolutePath().normalize();
        }

                /// Creates the listener that persists consumed replication cursors.
        ///
        /// @return consumed-message listener
        private AfterDataMessageConsumedListener ensureAfterDataMessageConsumedListener() {
            final var props = this.getNodeLibraryPropertiesProvider();

            final var storedCursorUpdater = new AfterDataMessageConsumedListener() {
                final StoredReplicationCursorManager delegate = ClusterFoundation.Node.this
                        .getStoredReplicationCursorManager();

                @Override
                public void onApplied(final ReplicationCursor cursor) throws NodeLibraryException {
                    if (props.isBackupNode() || !"writer".equalsIgnoreCase(props.replicationRole())) {
                        // Every reader must persist its resolved boundary; writers do not consume replication.
                        this.delegate.set(cursor);
                    }
                }

                @Override
                public void close() {
                    this.delegate.close();
                }
            };
            LOGGER.log(System.Logger.Level.TRACE, "Created AfterDataMessageConsumedListener->StoredReplicationCursorManager delegate. WillRun=%s".formatted(props.isBackupNode() || !"writer".equalsIgnoreCase(props.replicationRole())));
            return storedCursorUpdater;
        }

                /// Creates the storage backup manager.
        ///
        /// @return storage backup manager
        private StorageBackupManager ensureStorageBackupManager() {
            final var props = this.getNodeLibraryPropertiesProvider();
            final Integer configuredBackupCount = props.keptBackupsCount();
            final int maxBackupCount = requiredPositive(
                    configuredBackupCount == null ? 3 : configuredBackupCount,
                    "KEPT_BACKUPS_COUNT");

            final Supplier<ReplicationCursor> cursorProvider = this.getStorageBinaryDataClient()::cursor;

            return StorageBackupManager.New(
                    this.clusterStorageManager,
                    maxBackupCount,
                    this.getStorageBackupBackend(),
                    cursorProvider,
                    this.getStorageBinaryDataClient(),
                    this.getReplicationLogRetention()
            );
        }

                /// Returns the configured root supplier.
        ///
        /// @return root supplier
        private Supplier<Object> ensureRootSupplier() {
            throw new MissingFoundationPartException(Supplier.class, "Missing root supplier");
        }

                /// Creates the default graph update handler.
        ///
        /// @return graph update handler
        private ObjectGraphUpdateHandler ensureGraphUpdateHandler() {
            return ObjectGraphUpdateHandler.Synchronized();
        }

                /// Creates the embedded storage foundation.
        ///
        /// @return embedded storage foundation
        private EmbeddedStorageFoundation<?> ensureEmbeddedStorageFoundation() {
            return EmbeddedStorageFoundation.New();
        }

                /// Creates the backup node manager.
        ///
        /// @return backup node manager
        private BackupNodeManager ensureBackupNodeManager() {
            return BackupNodeManager.New(
                    this.getStorageBackupTaskExecutor(),
                    this.getStorageBinaryDataClient(),
                    this.clusterStorageManager,
                    this.getStorageDiskSpaceReader(),
                    this.getClusterReplicationTransport().id()
            );
        }

                /// Creates the replication data client.
        ///
        /// @return replication data client
        private StorageBinaryDataClient ensureStorageBinaryDataClient() {
            final var props = this.getNodeLibraryPropertiesProvider();
            final boolean commitPosition = props.isBackupNode();
            return this.getClusterReplicationTransport().client(
                    this.getStorageBinaryDataPacketAcceptor(),
                    props.replicationStreamName(),
                    this.getAfterDataMessageConsumedListener(),
                    this.getStoredReplicationCursorManager().get(),
                    commitPosition
            );
        }

                /// Creates the storage node health check.
        ///
        /// @return storage health check
        private StorageNodeHealthCheck ensureStorageNodeHealthCheck() {
            return StorageNodeHealthCheck.New(
                    this.clusterStorageManager,
                    this.getClusterReplicationTransport().health(
                            () -> this.clusterStorageManager.isRunning() && !this.clusterStorageManager.isStartingUp(),
                            this.getStorageBinaryDataClient()
                    )
            );
        }

                /// Creates the environment-backed properties provider.
        ///
        /// @return properties provider
        private NodeLibraryPropertiesProvider ensureNodeLibraryPropertiesProvider() {
            return NodeLibraryPropertiesProvider.Env();
        }

                /// Creates the storage disk-space reader.
        ///
        /// @return disk-space reader
        private StorageDiskSpaceReader ensureStorageDiskSpaceReader() {
            return StorageDiskSpaceReader.New(
                    this.getEmbeddedStorageFoundation().getConfiguration().fileProvider().baseDirectory()
            );
        }

                /// Creates the storage node manager.
        ///
        /// Aeron roles are fixed at transport creation, so Aeron nodes get a
        /// fixed-role manager — the distributor for a writer, the reader for a
        /// reader or backup-reader. Other transports get a promotable manager
        /// for the reader-to-distributor transition.
        ///
        /// @return storage node manager
        private StorageNodeManager ensureStorageNodeManager() {
            final String transport = this.getClusterReplicationTransport().id();
            if ("aeron".equalsIgnoreCase(transport)) {
                final boolean writer = "writer".equalsIgnoreCase(
                        this.getNodeLibraryPropertiesProvider().replicationRole());
                return StorageNodeManager.New(
                        this.getStorageBinaryDataDistributor(),
                        this.getStorageTaskExecutor(),
                        this.getStorageBinaryDataClient(),
                        this.getStorageNodeHealthCheck(),
                        this.getStorageDiskSpaceReader(),
                        this.getReplicationPositionProvider(),
                        transport,
                        writer ? StorageNodeManager.Role.DISTRIBUTOR : StorageNodeManager.Role.READER
                );
            }
            return PromotableStorageNodeManager.New(
                    this.getStorageBinaryDataDistributor(),
                    this.getStorageTaskExecutor(),
                    this.getStorageBinaryDataClient(),
                    this.getStorageNodeHealthCheck(),
                    this.getStorageDiskSpaceReader(),
                    this.getReplicationPositionProvider(),
                    transport
            );
        }

                /// Creates the configured binary distributor.
        ///
        /// @return binary distributor
        private StorageBinaryDataDistributor ensureDataDistributor() {
            return StorageBinaryDataDistributor.Caching(
                    this.getClusterReplicationTransport().distributor(
                            this.getNodeLibraryPropertiesProvider().replicationStreamName(),
                            this.getEnableAsyncDistribution()
                    )
            );
        }

                /// Creates the binary merger with configured limits.
        ///
        /// @return binary merger
        private StorageBinaryDataMerger ensureStorageBinaryDataMerger() {
            final Long cachingTimeoutMsNullable = this.getNodeLibraryPropertiesProvider().dataMergerTimeoutMs();
            final long cachingTimeoutMs = cachingTimeoutMsNullable == null ? StorageBinaryDataMerger.Defaults
                    .CACHING_TIMEOUT_MS : cachingTimeoutMsNullable;

            final Long cachedDataLimitNullable = this.getNodeLibraryPropertiesProvider().dataMergerCachedDataLimit();
            final long cachedDataLimit = cachedDataLimitNullable == null ? StorageBinaryDataMerger.Defaults
                    .CACHING_LIMIT : cachedDataLimitNullable;

            return StorageBinaryDataMerger.New(
                    this.getEmbeddedStorageFoundation().getConnectionFoundation(),
                    this.clusterStorageManager,
                    this.getObjectGraphUpdateHandler(),
                    cachingTimeoutMs,
                    cachedDataLimit
            );
        }

                /// Creates the packet acceptor.
        ///
        /// @return packet acceptor
        private StorageBinaryDataPacketAcceptor ensureDataPacketAcceptor() {
            return StorageBinaryDataPacketAcceptor.New(this.getStorageBinaryDataMerger());
        }

        private StorageBackupBackend getStorageBackupBackend() {
            return this.backupBackend.get();
        }


        private StorageTaskExecutor getStorageTaskExecutor() {
            return this.storageTaskExecutor.get();
        }


        private StorageBackupTaskExecutor getStorageBackupTaskExecutor() {
            return this.storageBackupTaskExecutor.get();
        }


        private NodeHousekeeper getNodeHousekeeper() {
            return this.housekeeper.get();
        }


        private StorageLimitGate getStorageLimitGate() {
            return this.storageLimitGate.get();
        }

        private Duration backupNodeGcInterval() {
            return maintenanceInterval(
                    this.getNodeLibraryPropertiesProvider().gcIntervalMinutes(),
                    "GC_INTERVAL_MINUTES",
                    30
            );
        }

        private Duration backupNodeBackupInterval() {
            return maintenanceInterval(
                    this.getNodeLibraryPropertiesProvider().backupIntervalMinutes(),
                    "BACKUP_INTERVAL_MINUTES",
                    120
            );
        }

        private Duration storageNodeGcInterval() {
            return maintenanceInterval(
                    this.getNodeLibraryPropertiesProvider().gcIntervalMinutes(),
                    "GC_INTERVAL_MINUTES",
                    60
            );
        }

        private ClusterReplicationTransport getClusterReplicationTransport() {
            return this.replicationTransport.get();
        }


        private StorageBackupManager getStorageBackupManager() {
            return this.storageBackupManager.get();
        }


        private ObjectGraphUpdateHandler getObjectGraphUpdateHandler() {
            return this.graphUpdateHandler.get();
        }


        private Supplier<Object> getRootSupplier() {
            return this.rootSupplier.get();
        }


        private boolean getEnableAsyncDistribution() {
            return this.enableAsyncDistribution;
        }


        private EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation() {
            return this.embeddedStorageFoundation.get();
        }


        private BackupNodeManager getBackupNodeManager() {
            return this.backupNodeManager.get();
        }


        private StorageBinaryDataClient getStorageBinaryDataClient() {
            return this.dataClient.get();
        }


        private StorageBinaryDataDistributor getStorageBinaryDataDistributor() {
            return this.dataDistributor.get();
        }


        private StorageNodeHealthCheck getStorageNodeHealthCheck() {
            return this.healthCheck.get();
        }


        private NodeLibraryPropertiesProvider getNodeLibraryPropertiesProvider() {
            return this.propertiesProvider.get();
        }


        private StorageDiskSpaceReader getStorageDiskSpaceReader() {
            return this.storageDiskSpaceReader.get();
        }


        private StorageNodeManager getStorageNodeManager() {
            return this.storageNodeManager.get();
        }


        private AfterDataMessageConsumedListener getAfterDataMessageConsumedListener() {
            return this.afterDataMessageConsumedListener.get();
        }


        private StorageBinaryDataMerger getStorageBinaryDataMerger() {
            return this.dataMerger.get();
        }


        private StorageBinaryDataPacketAcceptor getStorageBinaryDataPacketAcceptor() {
            return this.dataPacketAcceptor.get();
        }


        private StoredReplicationCursorManager getStoredReplicationCursorManager() {
            StoredReplicationCursorManager manager = this.storedReplicationCursorManager;
            if (manager == null) {
                synchronized (this) {
                    manager = this.storedReplicationCursorManager;
                    if (manager == null) {
                        manager = this.dispatch(this.ensureStoredReplicationCursorManager());
                        this.storedReplicationCursorManager = manager;
                    }
                }
            }
            return manager;
        }


        private ReplicationPositionProvider getReplicationPositionProvider() {
            return this.positionProvider.get();
        }


        private ReplicationLogRetention getReplicationLogRetention() {
            return this.replicationRetention.get();
        }


        @Override
        public synchronized ClusterRestRequestController startController() throws NodeLibraryException {
            this.ensureOpen();
            if (this.clusterRequestController == null) {
                this.start();
            }

            return this.clusterRequestController;
        }

        @Override
        public synchronized ClusterStorageManager<?> startStorageManager() throws NodeLibraryException {
            this.ensureOpen();
            if (this.clusterStorageManager == null) {
                this.start();
            }

            return this.clusterStorageManager;
        }

                /// Starts the node in its configured role.
        ///
        /// @throws NodeLibraryException if startup fails
        private synchronized void start() throws NodeLibraryException {
            this.ensureOpen();
            if (this.started) {
                throw new IllegalStateException("Cluster foundation has already started");
            }
            this.started = true;
            try {
                final var properties = this.getNodeLibraryPropertiesProvider();

                if (!properties.isProdMode()) {
                    this.startDevNode();
                } else if (properties.isBackupNode()) {
                    this.startBackupNode();
                } else {
                    this.startStorageNode();
                }
            } catch (final Throwable failure) {
                try {
                    this.close();
                } catch (final Throwable cleanupFailure) {
                    if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
                }
                if (failure instanceof Error error) throw error;
                if (failure instanceof RuntimeException runtime) throw runtime;
                throw new NodeLibraryException("Failed to start cluster foundation", failure);
            }
        }

                /// Starts a node that restores and serves backups.
        ///
        /// @throws NodeLibraryException if startup fails
        private void startBackupNode() throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.INFO, "Starting backup cluster node");

            this.getReplicationPositionProvider().init();

            final var storageParentPath = this.storageParentPath();
            final var storageRootPath = storageParentPath.resolve("storage");

            // if we use a downloaded storage, always scroll to the latest message so we don't read old messages
            boolean useLatestCursor = false;
            boolean requiresStorageUpload = false;

            // don't send messages generated by starting the storage and storing the empty root
            this.getStorageBinaryDataDistributor().ignoreDistribution(true);

            final var backend = this.getStorageBackupBackend();

            /*
             * If there are backups already available, use those instead as a fresh cluster
             * has none, but an upgraded cluster has the previous storage backed up
             */

            // user uploaded a new storage
            if (backend.hasUserUploadedStorage()) {
                LOGGER.log(System.Logger.Level.INFO, "Restoring user uploaded storage");

                useLatestCursor = true;
                // since the storage is now different from before,
                // the storage nodes also need the exact same storage
                requiresStorageUpload = true;
                this.deleteDirectory(storageRootPath);
                backend.restoreUserUploadedStorage(storageParentPath);
                backend.deleteUserUploadedStorage();
            } else if (this.restoreLatestBackupIfRequired(storageRootPath, backend)) {
                LOGGER.log(System.Logger.Level.INFO, "Restored the newest compatible storage backup");
            } else {
                LOGGER.log(System.Logger.Level.INFO, "Starting with local storage");
            }

            if (useLatestCursor) {
                final ReplicationCursor cursor;
                try {
                    cursor = this.getReplicationPositionProvider().latest();
                } catch (final ReplicationPositionUnavailableException failure) {
                    throw new NodeLibraryException(
                            "Cannot bootstrap uploaded storage: replication transport does not expose a writer latest position",
                            failure);
                }
                LOGGER.log(System.Logger.Level.DEBUG, "Set starting replication cursor to: %s".formatted(cursor));
                this.getStoredReplicationCursorManager().set(cursor);
            }

            LOGGER.log(System.Logger.Level.INFO, "Creating node cluster controller");

            final var embeddedStorageManager = this.prepareEmbeddedStorage(storageRootPath).start();
            this.initializeRoot(embeddedStorageManager);

            this.getStorageBinaryDataDistributor().ignoreDistribution(false);
            this.queueWriterDictionary(embeddedStorageManager);

            final var housekeeper = this.getNodeHousekeeper();

            this.clusterStorageManager = ClusterStorageManager.Wrapper(embeddedStorageManager,
                    () -> this.closeHousekeeperAndReplication(housekeeper));

            this.getStorageBinaryDataClient().start();

            this.clusterRequestController = ClusterRestRequestController.BackupNode(
                    this.getBackupNodeManager());

            final StorageConnection gcConnection = this.clusterStorageManager;
            housekeeper.schedule("GcWorkaround", () ->
            {
                LOGGER.log(System.Logger.Level.INFO, "Issuing GC and CC");
                gcConnection.issueFullCacheCheck();
                gcConnection.issueFullGarbageCollection();
            }, this.backupNodeGcInterval());
            housekeeper.schedule(
                    "StorageBackup",
                    this.getStorageBackupTaskExecutor().createScheduledWork(),
                    this.backupNodeBackupInterval()
            );

            // storage nodes need an initial backup to start from
            if (requiresStorageUpload) {
                LOGGER.log(System.Logger.Level.INFO, "Uploading starter backup for storage nodes");
                /* This is a bootstrap barrier.  The storage nodes must not observe the
                 * uploaded-storage state until the archive is durable. */
                this.getStorageBackupManager().createStorageBackup(false);
            }

            housekeeper.start();
        }

                /// Starts a node that publishes storage data.
        ///
        /// @throws NodeLibraryException if startup fails
        private void startStorageNode() throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.INFO, "Starting storage cluster node");

            final var storageParentPath = this.storageParentPath();
            final var storageRootPath = storageParentPath.resolve("storage");

            // don't send messages generated by starting the storage and storing the empty root
            this.getStorageBinaryDataDistributor().ignoreDistribution(true);

            final var backend = this.getStorageBackupBackend();
            /*
             * If there are backups already available, use those instead
             */

            if (this.restoreLatestBackupIfRequired(storageRootPath, backend)) {
                LOGGER.log(System.Logger.Level.INFO, "Restored the newest compatible storage backup");
            } else {
                LOGGER.log(System.Logger.Level.INFO, Files.exists(storageRootPath)
                        ? "Resuming existing local storage and cursor"
                        : "Starting with local storage");
            }

            this.getReplicationPositionProvider().init();

            final var dataDistributor = this.getStorageBinaryDataDistributor();
            final var embeddedStorageFoundation = this.prepareEmbeddedStorage(storageRootPath);
            DistributedStorage.configureWriting(
                    embeddedStorageFoundation,
                    dataDistributor,
                    this.getClusterReplicationTransport().persistenceTargetFactory(
                            this.getNodeLibraryPropertiesProvider().replicationStreamName(), dataDistributor
                    )
            );

            final var embeddedStorageManager = embeddedStorageFoundation.start();
            this.initializeRoot(embeddedStorageManager);

            this.getStorageBinaryDataDistributor().ignoreDistribution(false);
            this.queueWriterDictionary(embeddedStorageManager);

            final var housekeeper = this.getNodeHousekeeper();
            final var limitGate = this.getStorageLimitGate();

            this.clusterStorageManager = ClusterStorageManager.New(
                    embeddedStorageManager,
                    limitGate::limitReached,
                    () -> this.closeHousekeeperAndReplication(housekeeper)
            );

            this.getStorageBinaryDataClient().start();

            this.getStorageNodeHealthCheck().init();

            this.clusterRequestController = ClusterRestRequestController.StorageNode(this.getStorageNodeManager());

            final StorageConnection gcConnection = this.clusterStorageManager;
            housekeeper.schedule("GcWorkaround", () ->
            {
                LOGGER.log(System.Logger.Level.INFO, "Issuing GC and CC");
                gcConnection.issueFullCacheCheck();
                gcConnection.issueFullGarbageCollection();
            }, this.storageNodeGcInterval());
            housekeeper.schedule(
                    "StorageLimitChecker",
                    limitGate.createScheduledWork(this.getStorageDiskSpaceReader()),
                    Duration.ofMinutes(requiredPositive(
                            this.getNodeLibraryPropertiesProvider().storageLimitCheckerIntervalMinutes(),
                            "STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES"
                    ))
            );

            housekeeper.start();
        }

                /// Queues the complete persisted dictionary for the first post-restart
        /// transaction. This covers types introduced by a rejected transaction whose
        /// incremental export was consumed before the writer crashed.
        private void queueWriterDictionary(final EmbeddedStorageManager storage) {
            final NodeLibraryPropertiesProvider props = this.getNodeLibraryPropertiesProvider();
            if (!"writer".equalsIgnoreCase(props.replicationRole())) {
                return;
            }
            final String dictionary = PersistenceTypeDictionaryAssembler.New().assemble(storage.typeDictionary());
            this.getStorageBinaryDataDistributor().queueTypeDictionaryForNextTransaction(dictionary);
        }

        private EmbeddedStorageFoundation<?> prepareEmbeddedStorage(final Path storageRootPath) {
            final var foundation = this.getEmbeddedStorageFoundation();
            final StorageConfiguration current = foundation.getConfiguration();
            foundation.setConfiguration(StorageConfiguration.Builder()
                    .setBackupSetup(current.backupSetup())
                    .setChannelCountProvider(current.channelCountProvider())
                    .setChunkChecksumProvider(current.chunkChecksumProvider())
                    .setDataFileEvaluator(current.dataFileEvaluator())
                    .setEntityCacheEvaluator(current.entityCacheEvaluator())
                    .setHousekeepingController(current.housekeepingController())
                    .setReferenceValidationPolicy(current.referenceValidationPolicy())
                    .setStorageFileProvider(
                            StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storageRootPath))
                    )
                    .createConfiguration());
            foundation.setExceptionHandler((throwable, channel) ->
            {
                try {
                    StorageExceptionHandler.defaultHandleException(throwable, channel);
                } catch (final StorageException exception) {
                    /* A node is embedded in an application and must not call
                     * System.exit. Log the fatal error and rethrow so the
                     * application supervisor decides on termination. */
                    LOGGER.log(System.Logger.Level.ERROR, "Shutting down application due to fatal error", exception);
                    throw exception;
                }
            });
            return foundation;
        }

        private void initializeRoot(final StorageManager storage) {
            if (storage.root() == null) {
                LOGGER.log(System.Logger.Level.DEBUG, "Setting and storing new root from root supplier");
                final Object root = this.getRootSupplier().get();
                storage.setRoot(root instanceof Lazy ? root : Lazy.Reference(root));
                storage.storeRoot();
            }
        }

                /// Starts the local development node.
        ///
        /// @throws NodeLibraryException if startup fails
        private void startDevNode() throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.INFO, "Starting dev cluster node");
            final var storage = this.getEmbeddedStorageFoundation().start();
            if (storage.root() == null) {
                final var root = this.getRootSupplier().get();
                if (root instanceof Lazy) {
                    storage.setRoot(root);
                } else {
                    storage.setRoot(Lazy.Reference(root));
                }
                storage.storeRoot();
            }

            this.clusterStorageManager = ClusterStorageManager.Wrapper(
                    storage,
                    this::closeReplicationTransportAndPositionProvider
            );
            this.clusterRequestController = ClusterRestRequestController.NoEndpoints();
        }

        private void deleteDirectory(final Path path) {
            if (!Files.exists(path)) {
                return;
            }

            LOGGER.log(System.Logger.Level.INFO, "Deleting files at %s".formatted(path));
            StorageFileOperations.deleteDirectory(path);
        }

                /// Selects a backup only when the local Store cannot be trusted to represent
        /// the newest compatible image.  A local cursor from another transport or
        /// Store generation is never mixed with local files; it is discarded together
        /// with the Store before extraction.  An equal-sequence cursor must also point
        /// at the same provider position.  A local cursor ahead of the newest backup
        /// is retained because restoring an older image would lose data.
        private boolean restoreLatestBackupIfRequired(
                final Path storageRootPath,
                final StorageBackupBackend backend
        ) throws NodeLibraryException {
            final boolean storageExists = Files.isDirectory(storageRootPath);
            if (!storageExists && !backend.containsBackups()) {
                return false;
            }
            if (!storageExists) {
                final ReplicationCursor backup = backend.getCursorFromPreviousBackup(0);
                if (backup == null) {
                    throw new NodeLibraryException("The newest storage backup has no replication cursor");
                }
                this.deleteOffsetFile();
                this.restoreBackupAndCursor(backend, backup, storageRootPath);
                return true;
            }

            final ReplicationCursor backup = backend.getCursorFromPreviousBackup(0);
            if (backup == null) {
                return false;
            }
            final ReplicationCursor local = this.getStoredReplicationCursorManager().get();
            final boolean localBoundaryUnknown = local.logicalSequence() < 0;
            final boolean identityMismatch = !Objects.equals(local.transport(), backup.transport()) ||
                                             !Objects.equals(local.storeGeneration(), backup.storeGeneration());
            final boolean localBehind = local.logicalSequence() < backup.logicalSequence();
            final boolean equalSequencePositionMismatch = local.logicalSequence() == backup.logicalSequence() &&
                                                           !Objects.equals(local.providerPosition(), backup.providerPosition());
            if (!localBoundaryUnknown && !identityMismatch && !localBehind && !equalSequencePositionMismatch) {
                return false;
            }

            LOGGER.log(System.Logger.Level.WARNING, "Replacing local storage with the newest compatible backup (local cursor=%s, backup cursor=%s, identityMismatch=%s, localBehind=%s, equalSequencePositionMismatch=%s)".formatted(local.logicalSequence(), backup.logicalSequence(), identityMismatch, localBehind, equalSequencePositionMismatch));
            this.closeStoredReplicationCursorManager();
            this.deleteDirectory(storageRootPath);
            this.deleteOffsetFile();
            this.restoreBackupAndCursor(backend, backup, storageRootPath);
            return true;
        }

                /// Installs a backup and its cursor as one trusted startup boundary.
        private void restoreBackupAndCursor(
                final StorageBackupBackend backend,
                final ReplicationCursor backup,
                final Path storageRootPath
        ) {
            try {
                backend.restoreLatestBackup(this.storageParentPath());
                this.getStoredReplicationCursorManager().set(backup);
            } catch (final RuntimeException | Error failure) {
                /* A downloaded Store without its matching cursor is not a valid
                 * restart image. Remove it so a later startup cannot mistake the
                 * partial boundary for trusted local state. */
                try {
                    this.deleteDirectory(storageRootPath);
                } catch (final RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        private void deleteOffsetFile() throws NodeLibraryException {
            try {
                AtomicFileStore.delete(this.storageParentPath().resolve("offset"), true);
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to remove stale replication cursor before backup restore", failure);
            }
        }

        private synchronized void closeStoredReplicationCursorManager() {
            if (this.storedReplicationCursorManager == null) {
                return;
            }
            try {
                this.storedReplicationCursorManager.close();
            } finally {
                this.storedReplicationCursorManager = null;
            }
        }

        private void closeReplicationTransportAndPositionProvider() {
            Throwable failure = null;
            if (this.replicationTransport.isInitialized()) {
                failure = closeResource(failure, () -> this.replicationTransport.get().close());
            }
            if (this.positionProvider.isInitialized()) {
                failure = closeResource(failure, () -> this.positionProvider.get().close());
            }
            if (this.replicationRetention.isInitialized()) {
                failure = closeResource(failure, () -> this.replicationRetention.get().close());
            }
            if (failure != null) {
                throwFailure(failure, "failed to close replication resources");
            }
        }

        private void closeHousekeeperAndReplication(final NodeHousekeeper housekeeper) {
            Throwable failure = null;
            failure = closeResource(failure, housekeeper::close);
            failure = closeResource(failure, this::closeReplicationTransportAndPositionProvider);
            if (failure != null) {
                throwFailure(failure, "failed to close housekeeper and replication resources");
            }
        }

                /// Closes the complete foundation graph once, retaining all failures.
        @Override
        public synchronized void close() {
            if (this.closed) {
                return;
            }
            if (this.closing) {
                throw new IllegalStateException("Cluster foundation is already closing");
            }
            this.closing = true;
            Throwable failure = null;
            final boolean storageManagerOwnsNodeResources = this.clusterStorageManager != null;
            final boolean requestControllerOwnsBackupResources =
                    this.clusterRequestController instanceof ClusterRestRequestController.BackupNode;
            final boolean nodeManagerClosedByController =
                    this.clusterRequestController instanceof ClusterRestRequestController.StorageNode;
            if (this.clusterRequestController != null) {
                failure = closeResource(failure, this.clusterRequestController::close);
            }
            if (this.clusterStorageManager != null) {
                failure = closeResource(failure, this.clusterStorageManager::close);
            }
            /* The StorageNode controller closed the node manager above, and the
             * manager owns the client, distributor, and health check: disposing
             * them again here would double-dispose after promotion. Close the
             * manager itself only when no controller took ownership, so its
             * resources are still disposed exactly once. */
            if (!nodeManagerClosedByController && this.storageNodeManager.isInitialized()) {
                failure = closeInitialized(
                        failure, this.storageNodeManager, () -> this.storageNodeManager.get().close());
            } else {
                /* A foundation can be closed after dependency creation but before the
                 * node-specific controllers are installed. Release those partially-built
                 * resources as well. The concrete implementations are idempotent. */
                if (!requestControllerOwnsBackupResources) {
                    failure = closeInitialized(failure, this.dataClient, () -> this.dataClient.get().dispose());
                }
                failure = closeInitialized(
                        failure, this.dataDistributor, () -> this.dataDistributor.get().dispose());
                failure = closeInitialized(failure, this.healthCheck, () -> this.healthCheck.get().close());
            }
            if (!requestControllerOwnsBackupResources) {
                failure = closeInitialized(
                        failure, this.storageBackupTaskExecutor, () -> this.storageBackupTaskExecutor.get().close());
            }
            final StorageTaskExecutor backupTaskExecutor = this.storageBackupTaskExecutor.isInitialized()
                    ? this.storageBackupTaskExecutor.get() : null;
            if (this.storageTaskExecutor.isInitialized()
                    && this.storageTaskExecutor.get() != backupTaskExecutor) {
                failure = closeInitialized(
                        failure, this.storageTaskExecutor, () -> this.storageTaskExecutor.get().close());
            }
            failure = closeInitialized(failure, this.dataMerger, () -> this.dataMerger.get().dispose());
            failure = closeInitialized(
                    failure, this.afterDataMessageConsumedListener,
                    () -> this.afterDataMessageConsumedListener.get().close());
            if (!this.afterDataMessageConsumedListener.isInitialized()
                    && this.storedReplicationCursorManager != null) {
                failure = closeResource(failure, this.storedReplicationCursorManager::close);
            }
            if (!storageManagerOwnsNodeResources &&
                (this.replicationTransport.isInitialized() || this.positionProvider.isInitialized() ||
                 this.replicationRetention.isInitialized())
            ) {
                failure = closeResource(failure, this::closeReplicationTransportAndPositionProvider);
            }
            if (!storageManagerOwnsNodeResources) {
                failure = closeInitialized(failure, this.housekeeper, () -> this.housekeeper.get().close());
            }
            if (failure != null) {
                this.closing = false;
                throwFailure(failure, "Failed to close cluster foundation");
            }
            this.closed = true;
            this.closing = false;
        }

        private void ensureOpen() {
            if (this.closed || this.closing) {
                throw new IllegalStateException("Cluster foundation is closed");
            }
        }

    }
}
