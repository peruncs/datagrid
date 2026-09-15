package peruncs.datagrid.cluster.node.node;

import org.eclipse.serializer.exceptions.MissingFoundationPartException;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.Unpersistable;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.serializer.util.InstanceDispatcher;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.exceptions.StorageException;
import org.eclipse.store.storage.types.*;
import peruncs.datagrid.cluster.node.aeron.AeronClusterReplicationTransportProvider;
import peruncs.datagrid.cluster.node.backup.*;
import peruncs.datagrid.cluster.node.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.node.http.ClusterRestRequestController;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.node.store.*;
import peruncs.datagrid.cluster.storage.types.AtomicFileStore;
import peruncs.datagrid.cluster.storage.types.DistributedStorage;
import peruncs.datagrid.cluster.storage.types.ObjectGraphUpdateHandler;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/// This foundation assembles the services that make one cluster node run.
///
/// Callers set the storage, transport, graph-update, and maintenance parts
/// before starting the storage manager or request controller. Start creates the
/// dependency graph; close releases it in the reverse direction. A foundation
/// belongs to one node and must not be reused after that node is closed.
///
/// @param <F> fluent foundation type
public interface ClusterFoundation<F extends ClusterFoundation<?>> extends InstanceDispatcher, AutoCloseable {
        /// Creates a foundation with default collaborators.
    ///
    /// @return new foundation
    static ClusterFoundation<?> New() {
        return new Default<>();
    }

        /// Returns the storage backup backend.
    ///
    /// @return backup backend
    StorageBackupBackend getStorageBackupBackend();

        /// Sets the storage backup backend.
    ///
    /// @param backend backup backend
    /// @return this foundation
    F setStorageBackupBackend(StorageBackupBackend backend);

        /// Returns the storage task executor.
    ///
    /// @return storage task executor
    StorageTaskExecutor getStorageTaskExecutor();

        /// Sets the storage task executor.
    ///
    /// @param executor storage task executor
    /// @return this foundation
    F setStorageTaskExecutor(StorageTaskExecutor executor);

        /// Returns the backup task executor.
    ///
    /// @return backup task executor
    StorageBackupTaskExecutor getStorageBackupTaskExecutor();

        /// Sets the backup task executor.
    ///
    /// @param executor backup task executor
    /// @return this foundation
    F setStorageBackupTaskExecutor(StorageBackupTaskExecutor executor);

        /// Returns the backup service client.
    ///
    /// @return backup service client
    BackupProxyHttpClient getBackupProxyHttpClient();

        /// Sets the backup service client.
    ///
    /// @param client backup service client
    /// @return this foundation
    F setBackupProxyHttpClient(BackupProxyHttpClient client);

        /// Returns the replication transport.
    ///
    /// @return replication transport
    ClusterReplicationTransport getClusterReplicationTransport();

        /// Sets the replication transport.
    ///
    /// @param transport replication transport
    /// @return this foundation
    F setClusterReplicationTransport(ClusterReplicationTransport transport);

        /// Returns the packet acceptor.
    ///
    /// @return packet acceptor
    ClusterStorageBinaryDataPacketAcceptor getClusterStorageBinaryDataPacketAcceptor();

        /// Sets the packet acceptor.
    ///
    /// @param acceptor packet acceptor
    /// @return this foundation
    F setClusterStorageBinaryDataPacketAcceptor(ClusterStorageBinaryDataPacketAcceptor acceptor);

        /// Returns the binary merger.
    ///
    /// @return binary merger
    ClusterStorageBinaryDataMerger getClusterStorageBinaryDataMerger();

        /// Sets the binary merger.
    ///
    /// @param merger binary merger
    /// @return this foundation
    F setClusterStorageBinaryDataMerger(ClusterStorageBinaryDataMerger merger);

        /// Returns the post-consumption listener.
    ///
    /// @return post-consumption listener
    AfterDataMessageConsumedListener getAfterDataMessageConsumedListener();

        /// Sets the post-consumption listener.
    ///
    /// @param listener post-consumption listener
    /// @return this foundation
    F setAfterDataMessageConsumedListener(AfterDataMessageConsumedListener listener);

        /// Returns the stored replication-cursor manager.
    ///
    /// @return stored replication-cursor manager
    StoredReplicationCursorManager getStoredReplicationCursorManager();

        /// Sets the stored replication-cursor manager.
    ///
    /// @param manager stored replication-cursor manager
    /// @return this foundation
    F setStoredReplicationCursorManager(StoredReplicationCursorManager manager);

        /// Returns the storage backup manager.
    ///
    /// @return storage backup manager
    StorageBackupManager getStorageBackupManager();

        /// Sets the storage backup manager.
    ///
    /// @param manager storage backup manager
    /// @return this foundation
    F setStorageBackupManager(StorageBackupManager manager);

        /// Returns the root supplier.
    ///
    /// @return root supplier
    Supplier<Object> getRootSupplier();

        /// Sets the root supplier.
    ///
    /// @param supplier root supplier
    /// @return this foundation
    F setRootSupplier(Supplier<Object> supplier);

        /// Returns the object-graph update handler.
    ///
    /// @return graph update handler
    ObjectGraphUpdateHandler getObjectGraphUpdateHandler();

        /// Sets the object-graph update handler.
    ///
    /// @param handler graph update handler
    /// @return this foundation
    F setObjectGraphUpdateHandler(ObjectGraphUpdateHandler handler);

        /// Returns the embedded storage foundation.
    ///
    /// @return embedded storage foundation
    EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation();

        /// Sets the embedded storage foundation.
    ///
    /// @param foundation embedded storage foundation
    /// @return this foundation
    F setEmbeddedStorageFoundation(EmbeddedStorageFoundation<?> foundation);

        /// Returns the backup node manager.
    ///
    /// @return backup node manager
    BackupNodeManager getBackupNodeManager();

        /// Sets the backup node manager.
    ///
    /// @param manager backup node manager
    /// @return this foundation
    F setBackupNodeManager(BackupNodeManager manager);

        /// Returns the binary data client.
    ///
    /// @return binary data client
    ClusterStorageBinaryDataClient getClusterStorageBinaryDataClient();

        /// Sets the binary data client.
    ///
    /// @param client binary data client
    /// @return this foundation
    F setClusterStorageBinaryDataClient(ClusterStorageBinaryDataClient client);

        /// Returns the binary data distributor.
    ///
    /// @return binary data distributor
    ClusterStorageBinaryDataDistributor getClusterStorageBinaryDataDistributor();

        /// Sets the binary data distributor.
    ///
    /// @param distributor binary data distributor
    /// @return this foundation
    F setClusterStorageBinaryDataDistributor(ClusterStorageBinaryDataDistributor distributor);

        /// Returns the storage health check.
    ///
    /// @return storage health check
    StorageNodeHealthCheck getStorageNodeHealthCheck();

        /// Sets the storage health check.
    ///
    /// @param check storage health check
    /// @return this foundation
    F setStorageNodeHealthCheck(StorageNodeHealthCheck check);

        /// Returns the properties provider.
    ///
    /// @return properties provider
    NodelibraryPropertiesProvider getNodelibraryPropertiesProvider();

        /// Sets the properties provider.
    ///
    /// @param provider properties provider
    /// @return this foundation
    F setNodelibraryPropertiesProvider(NodelibraryPropertiesProvider provider);

        /// Returns the storage disk-space reader.
    ///
    /// @return disk-space reader
    StorageDiskSpaceReader getStorageDiskSpaceReader();

        /// Sets the storage disk-space reader.
    ///
    /// @param reader disk-space reader
    /// @return this foundation
    F setStorageDiskSpaceReader(StorageDiskSpaceReader reader);

        /// Returns the storage node manager.
    ///
    /// @return storage node manager
    StorageNodeManager getStorageNodeManager();

        /// Sets the storage node manager.
    ///
    /// @param manager storage node manager
    /// @return this foundation
    F setStorageNodeManager(StorageNodeManager manager);

        /// Returns whether asynchronous distribution is enabled.
    ///
    /// @return `true` when enabled
    boolean getEnableAsyncDistribution();

        /// Sets asynchronous distribution.
    ///
    /// @param enable whether to enable it
    /// @return this foundation
    F setEnableAsyncDistribution(boolean enable);

        /// Returns the replication position provider.
    ///
    /// @return position provider
    ReplicationPositionProvider getReplicationPositionProvider();

        /// Sets the replication position provider.
    ///
    /// @param provider position provider
    /// @return this foundation
    F setReplicationPositionProvider(ReplicationPositionProvider provider);

        /// Returns the replication log retention policy.
    ///
    /// @return retention policy
    ReplicationLogRetention getReplicationLogRetention();

        /// Sets the replication log retention policy.
    ///
    /// @param retention retention policy
    /// @return this foundation
    F setReplicationLogRetention(ReplicationLogRetention retention);

        /// Starts the request controller.
    ///
    /// @return request controller
    /// @throws NodelibraryException if startup fails
    ClusterRestRequestController startController() throws NodelibraryException;

        /// Starts the storage manager.
    ///
    /// @return storage manager
    /// @throws NodelibraryException if startup fails
    ClusterStorageManager<?> startStorageManager() throws NodelibraryException;

        /// Closes every resource created by this foundation in reverse dependency order.
    @Override
    void close();

        /// Stores the parts and builds the default cluster service graph.
    ///
    /// @param <F> fluent implementation type
    class Default<F extends Default<?>> extends InstanceDispatcher.Default
            implements ClusterFoundation<F>, Unpersistable {
        private static final System.Logger LOGGER = System.getLogger(ClusterFoundation.class.getName());

        private StorageBackupBackend backupBackend;
        private BackupProxyHttpClient backupProxyHttpClient;
        private EmbeddedStorageFoundation<?> embeddedStorageFoundation;
        private NodeHousekeeper housekeeper;
        private StorageLimitGate storageLimitGate;
        private BackupNodeManager backupNodeManager;
        private ClusterStorageBinaryDataClient dataClient;
        private ClusterStorageBinaryDataDistributor dataDistributor;
        private StorageNodeHealthCheck healthCheck;
        private NodelibraryPropertiesProvider propertiesProvider;
        private StorageTaskExecutor storageTaskExecutor;
        private StorageBackupTaskExecutor storageBackupTaskExecutor;
        private StorageDiskSpaceReader storageDiskSpaceReader;
        private StorageNodeManager storageNodeManager;
        private boolean enableAsyncDistribution;
        private Supplier<Object> rootSupplier;
        private ObjectGraphUpdateHandler graphUpdateHandler;
        private StorageBackupManager storageBackupManager;
        private AfterDataMessageConsumedListener afterDataMessageConsumedListener;
        private ClusterStorageBinaryDataMerger dataMerger;
        private ClusterStorageBinaryDataPacketAcceptor dataPacketAcceptor;
        private StoredReplicationCursorManager storedReplicationCursorManager;
        private ClusterReplicationTransport replicationTransport;
        private ReplicationPositionProvider positionProvider;
        private ReplicationLogRetention replicationRetention;

        // cached created types
        private ClusterStorageManager<?> clusterStorageManager;
        private ClusterRestRequestController clusterRequestController;
        private boolean closed;

        private Default() {
        }

        private static Path backupVolumePath(final NodelibraryPropertiesProvider properties) {
            final String configured = properties.replicationProperty(
                    NodelibraryPropertiesProvider.Env.EnvKeys.BACKUP_PATH);
            return Paths.get(configured == null || configured.isBlank() ? "backups" : configured)
                    .toAbsolutePath().normalize();
        }

                /// Resolves a maintenance interval with its node default.
        ///
        /// @param configured      configured minutes, or `null`
        /// @param property        property name used in failure messages
        /// @param fallbackMinutes node default in minutes
        /// @return interval
        protected static Duration maintenanceInterval(
                final Integer configured,
                final String property,
                final int fallbackMinutes
        ) {
            if (configured == null) {
                return Duration.ofMinutes(fallbackMinutes);
            }
            if (configured <= 0) {
                throw new NodelibraryException("%s must be configured as a positive integer".formatted(property));
            }
            return Duration.ofMinutes(configured);
        }

        private static int requiredPositive(final Integer value, final String property) {
            if (value == null || value <= 0) {
                throw new NodelibraryException("%s must be configured as a positive integer".formatted(property));
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

        private static void throwFailure(final Throwable failure, final String message) {
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException(message, failure);
        }

                /// Returns this implementation with its fluent type.
        ///
        /// @return this foundation
        @SuppressWarnings("unchecked")
        protected final F $() {
            return (F) this;
        }

                /// Creates the configured backup backend.
        ///
        /// @return backup backend
        protected StorageBackupBackend ensureBackupBackend() {
            final var props = this.getNodelibraryPropertiesProvider();

            if (props.backupTarget() == BackupTarget.SAAS) {
                final var scratchSpace = this.storageParentPath().resolve("backup");
                if (!Files.exists(scratchSpace)) {
                    try {
                        Files.createDirectories(scratchSpace);
                    } catch (final IOException e) {
                        throw new NodelibraryException("Failed to create scratch space", e);
                    }
                }
                return NetworkArchiveBackupBackend.New(
                        scratchSpace,
                        this.getBackupProxyHttpClient()
                );
            } else {
                return FilesystemVolumeBackupBackend.New(
                        backupVolumePath(props)
                );
            }
        }

                /// Creates the storage task executor.
        ///
        /// @return storage task executor
        protected StorageTaskExecutor ensureStorageTaskExecutor() {
            if (this.getNodelibraryPropertiesProvider().isBackupNode()) {
                return this.getStorageBackupTaskExecutor();
            }
            return StorageTaskExecutor.New(this.clusterStorageManager);
        }

                /// Creates the backup task executor.
        ///
        /// @return backup task executor
        protected StorageBackupTaskExecutor ensureStorageBackupTaskExecutor() {
            return StorageBackupTaskExecutor.New(this.clusterStorageManager, this.getStorageBackupManager());
        }

                /// Creates the node maintenance housekeeper.
        ///
        /// @return housekeeper
        protected NodeHousekeeper ensureNodeHousekeeper() {
            return NodeHousekeeper.New();
        }

                /// Creates the storage limit gate.
        ///
        /// @return limit gate
        protected StorageLimitGate ensureStorageLimitGate() {
            return StorageLimitGate.New(
                    requiredPositive(
                            this.getNodelibraryPropertiesProvider().storageLimitGB(),
                            "STORAGE_LIMIT_GB"
                    )
            );
        }

                /// Creates the backup service client.
        ///
        /// @return backup service client
        protected BackupProxyHttpClient ensureBackupProxyHttpClient() {
            final String configured = this.getNodelibraryPropertiesProvider().backupProxyServiceUrl();
            if (configured == null || configured.isBlank()) {
                throw new NodelibraryException("Backup proxy service URL must be configured when SAAS backups are enabled");
            }
            final URI uri;
            try {
                uri = URI.create(configured.trim());
            } catch (final IllegalArgumentException failure) {
                throw new NodelibraryException("Invalid backup proxy service URL: %s".formatted(configured), failure);
            }
            return BackupProxyHttpClient.New(
                    uri
            );
        }

                /// Creates the Aeron replication transport, or a no-op transport when disabled.
        ///
        /// @return replication transport
        protected ClusterReplicationTransport ensureClusterReplicationTransport() {
            final String configured = this.getNodelibraryPropertiesProvider().replicationTransport();
            final String requested = configured == null ? "none" : configured.trim();
            if ("none".equalsIgnoreCase(requested)) {
                return ClusterReplicationTransport.noOp();
            }
            if ("aeron".equalsIgnoreCase(requested)) {
                return new AeronClusterReplicationTransportProvider()
                        .create(this.getNodelibraryPropertiesProvider());
            }
            throw new NodelibraryException("Replication transport must be 'aeron' or 'none'");
        }

                /// Creates the replication position provider.
        ///
        /// @return position provider
        protected ReplicationPositionProvider ensureReplicationPositionProvider() {
            return this.getClusterReplicationTransport().positionProvider(
                    this.getNodelibraryPropertiesProvider().replicationStreamName()
            );
        }

                /// Creates the replication retention policy.
        ///
        /// @return retention policy
        protected ReplicationLogRetention ensureReplicationLogRetention() {
            return this.getClusterReplicationTransport().retention();
        }

                /// Creates the stored replication-cursor manager.
        ///
        /// @return stored replication-cursor manager
        protected StoredReplicationCursorManager ensureStoredReplicationCursorManager() {
            final var cursorPath = this.storageParentPath().resolve("offset");
            LOGGER.log(System.Logger.Level.TRACE, "Creating StoredReplicationCursorManager for offset file at %s".formatted(cursorPath));
            return StoredReplicationCursorManager.NewAtomic(cursorPath);
        }

                /// Returns the configured storage root, defaulting to a node-local directory.
        private Path storageParentPath() {
            final String configured = this.getNodelibraryPropertiesProvider()
                    .replicationProperty(NodelibraryPropertiesProvider.Env.EnvKeys.STORAGE_PATH);
            return Paths.get(configured == null || configured.isBlank() ? "storage" : configured)
                    .toAbsolutePath().normalize();
        }

                /// Creates the listener that persists consumed replication cursors.
        ///
        /// @return consumed-message listener
        protected AfterDataMessageConsumedListener ensureAfterDataMessageConsumedListener() {
            final var props = this.getNodelibraryPropertiesProvider();

            final var storedCursorUpdater = new AfterDataMessageConsumedListener() {
                final StoredReplicationCursorManager delegate = ClusterFoundation.Default.this
                        .getStoredReplicationCursorManager();

                @Override
                public void onApplied(final ReplicationCursor cursor) throws NodelibraryException {
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
        protected StorageBackupManager ensureStorageBackupManager() {
            final var props = this.getNodelibraryPropertiesProvider();
            final int maxBackupCount = props.keptBackupsCount();

            final Supplier<ReplicationCursor> cursorProvider = this.getClusterStorageBinaryDataClient()::cursor;

            return StorageBackupManager.New(
                    this.clusterStorageManager,
                    maxBackupCount,
                    this.getStorageBackupBackend(),
                    cursorProvider,
                    this.getClusterStorageBinaryDataClient(),
                    this.getReplicationLogRetention()
            );
        }

                /// Returns the configured root supplier.
        ///
        /// @return root supplier
        protected Supplier<Object> ensureRootSupplier() {
            throw new MissingFoundationPartException(Supplier.class, "Missing root supplier");
        }

                /// Creates the default graph update handler.
        ///
        /// @return graph update handler
        protected ObjectGraphUpdateHandler ensureGraphUpdateHandler() {
            return ObjectGraphUpdateHandler.Synchronized();
        }

                /// Creates the embedded storage foundation.
        ///
        /// @return embedded storage foundation
        protected EmbeddedStorageFoundation<?> ensureEmbeddedStorageFoundation() {
            return EmbeddedStorageFoundation.New();
        }

                /// Creates the backup node manager.
        ///
        /// @return backup node manager
        protected BackupNodeManager ensureBackupNodeManager() {
            return BackupNodeManager.New(
                    this.getStorageBackupTaskExecutor(),
                    this.getClusterStorageBinaryDataClient(),
                    this.clusterStorageManager,
                    this.getStorageDiskSpaceReader()
            );
        }

                /// Reads the last replication cursor from stored information.
        ///
        /// @return stored replication cursor
        protected ReplicationCursor getReplicationCursorFromStoredInfo() {
            return this.getStoredReplicationCursorManager().get();
        }

                /// Creates the replication data client.
        ///
        /// @return replication data client
        protected ClusterStorageBinaryDataClient ensureClusterStorageBinaryDataClient() {
            final var props = this.getNodelibraryPropertiesProvider();
            final boolean commitPosition = props.isBackupNode();
            return this.getClusterReplicationTransport().client(
                    this.getClusterStorageBinaryDataPacketAcceptor(),
                    props.replicationStreamName(),
                    this.getAfterDataMessageConsumedListener(),
                    this.getReplicationCursorFromStoredInfo(),
                    commitPosition
            );
        }

                /// Creates the storage node health check.
        ///
        /// @return storage health check
        protected StorageNodeHealthCheck ensureStorageNodeHealthCheck() {
            return StorageNodeHealthCheck.New(
                    this.clusterStorageManager,
                    this.getClusterReplicationTransport().health(
                            () -> this.clusterStorageManager.isRunning() && !this.clusterStorageManager.isStartingUp(),
                            this.getClusterStorageBinaryDataClient()
                    )
            );
        }

                /// Creates the environment-backed properties provider.
        ///
        /// @return properties provider
        protected NodelibraryPropertiesProvider ensureNodelibraryPropertiesProvider() {
            return NodelibraryPropertiesProvider.Env();
        }

                /// Creates the storage disk-space reader.
        ///
        /// @return disk-space reader
        protected StorageDiskSpaceReader ensureStorageDiskSpaceReader() {
            return StorageDiskSpaceReader.New(
                    this.getEmbeddedStorageFoundation().getConfiguration().fileProvider().baseDirectory()
            );
        }

                /// Creates the storage node manager.
        ///
        /// @return storage node manager
        protected StorageNodeManager ensureStorageNodeManager() {
            return StorageNodeManager.New(
                    this.getClusterStorageBinaryDataDistributor(),
                    this.getStorageTaskExecutor(),
                    this.getClusterStorageBinaryDataClient(),
                    this.getStorageNodeHealthCheck(),
                    this.getStorageDiskSpaceReader(),
                    this.getReplicationPositionProvider(),
                    this.getClusterReplicationTransport().id()
            );
        }

                /// Creates the configured binary distributor.
        ///
        /// @return binary distributor
        protected ClusterStorageBinaryDataDistributor ensureDataDistributor() {
            return ClusterStorageBinaryDataDistributor.Caching(
                    this.getClusterReplicationTransport().distributor(
                            this.getNodelibraryPropertiesProvider().replicationStreamName(),
                            this.getEnableAsyncDistribution()
                    )
            );
        }

                /// Creates the binary merger with configured limits.
        ///
        /// @return binary merger
        protected ClusterStorageBinaryDataMerger ensureClusterStorageBinaryDataMerger() {
            final Long cachingTimeoutMsNullable = this.getNodelibraryPropertiesProvider().dataMergerTimeoutMs();
            final long cachingTimeoutMs = cachingTimeoutMsNullable == null ? ClusterStorageBinaryDataMerger.Defaults
                    .CACHING_TIMEOUT_MS : cachingTimeoutMsNullable;

            final Long cachedDataLimitNullable = this.getNodelibraryPropertiesProvider().dataMergerCachedDataLimit();
            final long cachedDataLimit = cachedDataLimitNullable == null ? ClusterStorageBinaryDataMerger.Defaults
                    .CACHING_LIMIT : cachedDataLimitNullable;

            return ClusterStorageBinaryDataMerger.New(
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
        protected ClusterStorageBinaryDataPacketAcceptor ensureDataPacketAcceptor() {
            return ClusterStorageBinaryDataPacketAcceptor.New(this.getClusterStorageBinaryDataMerger());
        }

        @Override
        public synchronized StorageBackupBackend getStorageBackupBackend() {
            if (this.backupBackend == null) {
                this.backupBackend = this.dispatch(this.ensureBackupBackend());
            }
            return this.backupBackend;
        }

        @Override
        public synchronized F setStorageBackupBackend(final StorageBackupBackend backend) {
            this.backupBackend = backend;
            return this.$();
        }

        @Override
        public synchronized StorageTaskExecutor getStorageTaskExecutor() {
            if (this.storageTaskExecutor == null) {
                this.storageTaskExecutor = this.dispatch(this.ensureStorageTaskExecutor());
            }
            return this.storageTaskExecutor;
        }

        @Override
        public synchronized F setStorageTaskExecutor(final StorageTaskExecutor executor) {
            this.storageTaskExecutor = executor;
            return this.$();
        }

        @Override
        public synchronized StorageBackupTaskExecutor getStorageBackupTaskExecutor() {
            if (this.storageBackupTaskExecutor == null) {
                this.storageBackupTaskExecutor = this.dispatch(this.ensureStorageBackupTaskExecutor());
            }
            return this.storageBackupTaskExecutor;
        }

        @Override
        public synchronized F setStorageBackupTaskExecutor(final StorageBackupTaskExecutor executor) {
            this.storageBackupTaskExecutor = executor;
            return this.$();
        }

        private synchronized NodeHousekeeper getNodeHousekeeper() {
            if (this.housekeeper == null) {
                this.housekeeper = this.dispatch(this.ensureNodeHousekeeper());
            }
            return this.housekeeper;
        }

        private synchronized F setNodeHousekeeper(final NodeHousekeeper housekeeper) {
            this.housekeeper = housekeeper;
            return this.$();
        }

        private synchronized StorageLimitGate getStorageLimitGate() {
            if (this.storageLimitGate == null) {
                this.storageLimitGate = this.dispatch(this.ensureStorageLimitGate());
            }
            return this.storageLimitGate;
        }

        @Override
        public synchronized BackupProxyHttpClient getBackupProxyHttpClient() {
            if (this.backupProxyHttpClient == null) {
                this.backupProxyHttpClient = this.dispatch(this.ensureBackupProxyHttpClient());
            }
            return this.backupProxyHttpClient;
        }

        @Override
        public synchronized F setBackupProxyHttpClient(final BackupProxyHttpClient client) {
            this.backupProxyHttpClient = client;
            return this.$();
        }

        private Duration backupNodeGcInterval() {
            return maintenanceInterval(
                    this.getNodelibraryPropertiesProvider().gcIntervalMinutes(),
                    "GC_INTERVAL_MINUTES",
                    30
            );
        }

        private Duration backupNodeBackupInterval() {
            return maintenanceInterval(
                    this.getNodelibraryPropertiesProvider().backupIntervalMinutes(),
                    "BACKUP_INTERVAL_MINUTES",
                    120
            );
        }

        private Duration storageNodeGcInterval() {
            return maintenanceInterval(
                    this.getNodelibraryPropertiesProvider().gcIntervalMinutes(),
                    "GC_INTERVAL_MINUTES",
                    60
            );
        }

        @Override
        public synchronized ClusterReplicationTransport getClusterReplicationTransport() {
            if (this.replicationTransport == null) {
                this.replicationTransport = this.dispatch(this.ensureClusterReplicationTransport());
            }
            return this.replicationTransport;
        }

        @Override
        public synchronized F setClusterReplicationTransport(final ClusterReplicationTransport transport) {
            this.replicationTransport = transport;
            return this.$();
        }

        @Override
        public synchronized StorageBackupManager getStorageBackupManager() {
            if (this.storageBackupManager == null) {
                this.storageBackupManager = this.dispatch(this.ensureStorageBackupManager());
            }
            return this.storageBackupManager;
        }

        @Override
        public synchronized F setStorageBackupManager(final StorageBackupManager manager) {
            this.storageBackupManager = manager;
            return this.$();
        }

        @Override
        public synchronized ObjectGraphUpdateHandler getObjectGraphUpdateHandler() {
            if (this.graphUpdateHandler == null) {
                this.graphUpdateHandler = this.dispatch(this.ensureGraphUpdateHandler());
            }
            return this.graphUpdateHandler;
        }

        @Override
        public synchronized F setObjectGraphUpdateHandler(final ObjectGraphUpdateHandler handler) {
            this.graphUpdateHandler = handler;
            return this.$();
        }

        @Override
        public synchronized Supplier<Object> getRootSupplier() {
            if (this.rootSupplier == null) {
                this.rootSupplier = this.dispatch(this.ensureRootSupplier());
            }
            return this.rootSupplier;
        }

        @Override
        public synchronized F setRootSupplier(final Supplier<Object> supplier) {
            this.rootSupplier = supplier;
            return this.$();
        }

        @Override
        public synchronized boolean getEnableAsyncDistribution() {
            return this.enableAsyncDistribution;
        }

        @Override
        public synchronized F setEnableAsyncDistribution(final boolean enable) {
            this.enableAsyncDistribution = enable;
            return this.$();
        }

        @Override
        public synchronized EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation() {
            if (this.embeddedStorageFoundation == null) {
                this.embeddedStorageFoundation = this.dispatch(this.ensureEmbeddedStorageFoundation());
            }
            return this.embeddedStorageFoundation;
        }

        @Override
        public synchronized F setEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> foundation) {
            this.embeddedStorageFoundation = foundation;
            return this.$();
        }

        @Override
        public synchronized BackupNodeManager getBackupNodeManager() {
            if (this.backupNodeManager == null) {
                this.backupNodeManager = this.dispatch(this.ensureBackupNodeManager());
            }
            return this.backupNodeManager;
        }

        @Override
        public synchronized F setBackupNodeManager(final BackupNodeManager manager) {
            this.backupNodeManager = manager;
            return this.$();
        }

        @Override
        public synchronized ClusterStorageBinaryDataClient getClusterStorageBinaryDataClient() {
            if (this.dataClient == null) {
                this.dataClient = this.dispatch(this.ensureClusterStorageBinaryDataClient());
            }
            return this.dataClient;
        }

        @Override
        public synchronized F setClusterStorageBinaryDataClient(final ClusterStorageBinaryDataClient client) {
            this.dataClient = client;
            return this.$();
        }

        @Override
        public synchronized ClusterStorageBinaryDataDistributor getClusterStorageBinaryDataDistributor() {
            if (this.dataDistributor == null) {
                this.dataDistributor = this.dispatch(this.ensureDataDistributor());
            }
            return this.dataDistributor;
        }

        @Override
        public synchronized F setClusterStorageBinaryDataDistributor(final ClusterStorageBinaryDataDistributor distributor) {
            this.dataDistributor = distributor;
            return this.$();
        }

        @Override
        public synchronized StorageNodeHealthCheck getStorageNodeHealthCheck() {
            if (this.healthCheck == null) {
                this.healthCheck = this.dispatch(this.ensureStorageNodeHealthCheck());
            }
            return this.healthCheck;
        }

        @Override
        public synchronized F setStorageNodeHealthCheck(final StorageNodeHealthCheck check) {
            this.healthCheck = check;
            return this.$();
        }

        @Override
        public synchronized NodelibraryPropertiesProvider getNodelibraryPropertiesProvider() {
            if (this.propertiesProvider == null) {
                this.propertiesProvider = this.dispatch(this.ensureNodelibraryPropertiesProvider());
            }
            return this.propertiesProvider;
        }

        @Override
        public synchronized F setNodelibraryPropertiesProvider(final NodelibraryPropertiesProvider provider) {
            this.propertiesProvider = provider;
            return this.$();
        }

        @Override
        public synchronized StorageDiskSpaceReader getStorageDiskSpaceReader() {
            if (this.storageDiskSpaceReader == null) {
                this.storageDiskSpaceReader = this.dispatch(this.ensureStorageDiskSpaceReader());
            }
            return this.storageDiskSpaceReader;
        }

        @Override
        public synchronized F setStorageDiskSpaceReader(final StorageDiskSpaceReader reader) {
            this.storageDiskSpaceReader = reader;
            return this.$();
        }

        @Override
        public synchronized StorageNodeManager getStorageNodeManager() {
            if (this.storageNodeManager == null) {
                this.storageNodeManager = this.dispatch(this.ensureStorageNodeManager());
            }
            return this.storageNodeManager;
        }

        @Override
        public synchronized F setStorageNodeManager(final StorageNodeManager manager) {
            this.storageNodeManager = manager;
            return this.$();
        }

        @Override
        public synchronized AfterDataMessageConsumedListener getAfterDataMessageConsumedListener() {
            if (this.afterDataMessageConsumedListener == null) {
                this.afterDataMessageConsumedListener = this.dispatch(this.ensureAfterDataMessageConsumedListener());
            }
            return this.afterDataMessageConsumedListener;
        }

        @Override
        public synchronized F setAfterDataMessageConsumedListener(final AfterDataMessageConsumedListener listener) {
            this.afterDataMessageConsumedListener = listener;
            return this.$();
        }

        @Override
        public synchronized ClusterStorageBinaryDataMerger getClusterStorageBinaryDataMerger() {
            if (this.dataMerger == null) {
                this.dataMerger = this.dispatch(this.ensureClusterStorageBinaryDataMerger());
            }
            return this.dataMerger;
        }

        @Override
        public synchronized F setClusterStorageBinaryDataMerger(final ClusterStorageBinaryDataMerger merger) {
            this.dataMerger = merger;
            return this.$();
        }

        @Override
        public synchronized ClusterStorageBinaryDataPacketAcceptor getClusterStorageBinaryDataPacketAcceptor() {
            if (this.dataPacketAcceptor == null) {
                this.dataPacketAcceptor = this.dispatch(this.ensureDataPacketAcceptor());
            }
            return this.dataPacketAcceptor;
        }

        @Override
        public synchronized F setClusterStorageBinaryDataPacketAcceptor(final ClusterStorageBinaryDataPacketAcceptor acceptor) {
            this.dataPacketAcceptor = acceptor;
            return this.$();
        }

        @Override
        public synchronized StoredReplicationCursorManager getStoredReplicationCursorManager() {
            if (this.storedReplicationCursorManager == null) {
                this.storedReplicationCursorManager = this.dispatch(this.ensureStoredReplicationCursorManager());
            }
            return this.storedReplicationCursorManager;
        }

        @Override
        public synchronized F setStoredReplicationCursorManager(final StoredReplicationCursorManager manager) {
            this.storedReplicationCursorManager = manager;
            return this.$();
        }

        @Override
        public synchronized ReplicationPositionProvider getReplicationPositionProvider() {
            if (this.positionProvider == null) {
                this.positionProvider = this.dispatch(this.ensureReplicationPositionProvider());
            }
            return this.positionProvider;
        }

        @Override
        public synchronized F setReplicationPositionProvider(final ReplicationPositionProvider provider) {
            this.positionProvider = provider;
            return this.$();
        }

        @Override
        public synchronized ReplicationLogRetention getReplicationLogRetention() {
            if (this.replicationRetention == null) {
                this.replicationRetention = this.dispatch(this.ensureReplicationLogRetention());
            }
            return this.replicationRetention;
        }

        @Override
        public synchronized F setReplicationLogRetention(final ReplicationLogRetention retention) {
            this.replicationRetention = retention;
            return this.$();
        }

        @Override
        public synchronized ClusterRestRequestController startController() throws NodelibraryException {
            this.ensureOpen();
            if (this.clusterRequestController == null) {
                this.start();
            }

            return this.clusterRequestController;
        }

        @Override
        public synchronized ClusterStorageManager<?> startStorageManager() throws NodelibraryException {
            this.ensureOpen();
            if (this.clusterStorageManager == null) {
                this.start();
            }

            return this.clusterStorageManager;
        }

                /// Starts the node in its configured role.
        ///
        /// @throws NodelibraryException if startup fails
        protected synchronized void start() throws NodelibraryException {
            this.ensureOpen();
            try {
                final var properties = this.getNodelibraryPropertiesProvider();

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
                throw (RuntimeException) failure;
            }
        }

                /// Starts a node that restores and serves backups.
        ///
        /// @throws NodelibraryException if startup fails
        protected void startBackupNode() throws NodelibraryException {
            LOGGER.log(System.Logger.Level.INFO, "Starting backup cluster node");

            this.getReplicationPositionProvider().init();

            final var storageParentPath = this.storageParentPath();
            final var storageRootPath = storageParentPath.resolve("storage");

            // if we use a downloaded storage, always scroll to the latest message so we don't read old messages
            boolean useLatestCursor = false;
            boolean requiresStorageUpload = false;

            // don't send messages generated by starting the storage and storing the empty root
            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

            final var backend = this.getStorageBackupBackend();

            /*
             * If there are backups already available, use those instead as a fresh cluster
             * has none, but an upgraded cluster has the previous storage backed up
             */

            // user uploaded a new storage
            if (backend.hasUserUploadedStorage()) {
                LOGGER.log(System.Logger.Level.INFO, "Downloading user uploaded storage");

                useLatestCursor = true;
                // since the storage is now different from before,
                // the storage nodes also need the exact same storage
                requiresStorageUpload = true;
                this.deleteDirectory(storageRootPath);
                backend.downloadUserUploadedStorage(storageParentPath);
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
                } catch (final peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException failure) {
                    throw new NodelibraryException(
                            "Cannot bootstrap uploaded storage: replication transport does not expose a writer latest position",
                            failure);
                }
                LOGGER.log(System.Logger.Level.DEBUG, "Set starting replication cursor to: %s".formatted(cursor));
                this.getStoredReplicationCursorManager().set(cursor);
            }

            LOGGER.log(System.Logger.Level.INFO, "Creating node cluster controller");

            final var embeddedStorageManager = this.prepareEmbeddedStorage(storageRootPath).start();
            this.initializeRoot(embeddedStorageManager);

            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(false);
            this.queueWriterDictionary(embeddedStorageManager);

            final var housekeeper = this.getNodeHousekeeper();

            this.clusterStorageManager = ClusterStorageManager.Wrapper(embeddedStorageManager,
                    () -> this.closeHousekeeperAndReplication(housekeeper));

            this.getClusterStorageBinaryDataClient().start();

            this.clusterRequestController = ClusterRestRequestController.BackupNode(
                    this.getBackupNodeManager(),
                    this.getNodelibraryPropertiesProvider()
            );

            final StorageConnection gcConnection = this.clusterStorageManager;
            housekeeper.schedule("GcWorkaround", () ->
            {
                LOGGER.log(System.Logger.Level.INFO, "Issuing GC and CC");
                gcConnection.issueFullCacheCheck();
                gcConnection.issueFullGarbageCollection();
            }, this.backupNodeGcInterval());
            housekeeper.schedule(
                    "StorageBackup",
                    NodeHousekeeper.backupWork(this.getStorageBackupTaskExecutor()),
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
        /// @throws NodelibraryException if startup fails
        protected void startStorageNode() throws NodelibraryException {
            LOGGER.log(System.Logger.Level.INFO, "Starting storage cluster node");

            final var storageParentPath = this.storageParentPath();
            final var storageRootPath = storageParentPath.resolve("storage");

            // don't send messages generated by starting the storage and storing the empty root
            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

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

            // don't send messages generated by starting the storage and storing the empty root
            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

            this.getReplicationPositionProvider().init();

            final var dataDistributor = this.getClusterStorageBinaryDataDistributor();
            final var embeddedStorageFoundation = this.prepareEmbeddedStorage(storageRootPath);
            DistributedStorage.configureWriting(
                    embeddedStorageFoundation,
                    dataDistributor,
                    this.getClusterReplicationTransport().persistenceTargetFactory(
                            this.getNodelibraryPropertiesProvider().replicationStreamName(), dataDistributor
                    )
            );

            final var embeddedStorageManager = embeddedStorageFoundation.start();
            this.initializeRoot(embeddedStorageManager);

            this.getClusterStorageBinaryDataDistributor().ignoreDistribution(false);
            this.queueWriterDictionary(embeddedStorageManager);

            final var housekeeper = this.getNodeHousekeeper();
            final var limitGate = this.getStorageLimitGate();

            this.clusterStorageManager = ClusterStorageManager.New(
                    embeddedStorageManager,
                    limitGate::limitReached,
                    () -> this.closeHousekeeperAndReplication(housekeeper)
            );

            this.getClusterStorageBinaryDataClient().start();

            this.getStorageNodeHealthCheck().init();

            this.clusterRequestController = ClusterRestRequestController.StorageNode(
                    this.getStorageNodeManager(),
                    this.getNodelibraryPropertiesProvider()
            );

            final StorageConnection gcConnection = this.clusterStorageManager;
            housekeeper.schedule("GcWorkaround", () ->
            {
                LOGGER.log(System.Logger.Level.INFO, "Issuing GC and CC");
                gcConnection.issueFullCacheCheck();
                gcConnection.issueFullGarbageCollection();
            }, this.storageNodeGcInterval());
            housekeeper.schedule(
                    "StorageLimitChecker",
                    NodeHousekeeper.limitCheckWork(this.getStorageDiskSpaceReader(), limitGate),
                    Duration.ofMinutes(requiredPositive(
                            this.getNodelibraryPropertiesProvider().storageLimitCheckerIntervalMinutes(),
                            "STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES"
                    ))
            );

            housekeeper.start();
        }

                /// Queues the complete persisted dictionary for the first post-restart
        /// transaction. This covers types introduced by a rejected transaction whose
        /// incremental export was consumed before the writer crashed.
        private void queueWriterDictionary(final org.eclipse.store.storage.embedded.types.EmbeddedStorageManager storage) {
            final NodelibraryPropertiesProvider props = this.getNodelibraryPropertiesProvider();
            if (!"writer".equalsIgnoreCase(props.replicationRole())) {
                return;
            }
            final String dictionary = PersistenceTypeDictionaryAssembler.New().assemble(storage.typeDictionary());
            this.getClusterStorageBinaryDataDistributor().queueTypeDictionaryForNextTransaction(dictionary);
        }

        private EmbeddedStorageFoundation<?> prepareEmbeddedStorage(final Path storageRootPath) {
            final var foundation = this.getEmbeddedStorageFoundation();
            final StorageConfiguration current = foundation.getConfiguration();
            foundation.setConfiguration(StorageConfiguration.Builder()
                    .setBackupSetup(current.backupSetup())
                    .setChannelCountProvider(current.channelCountProvider())
                    .setDataFileEvaluator(current.dataFileEvaluator())
                    .setEntityCacheEvaluator(current.entityCacheEvaluator())
                    .setHousekeepingController(current.housekeepingController())
                    .setStorageFileProvider(
                            StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storageRootPath))
                    )
                    .createConfiguration());
            foundation.setExceptionHandler((throwable, channel) ->
            {
                try {
                    StorageExceptionHandler.defaultHandleException(throwable, channel);
                } catch (final StorageException exception) {
                    GlobalErrorHandling.handleFatalError(exception);
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
        /// @throws NodelibraryException if startup fails
        protected void startDevNode() throws NodelibraryException {
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
            this.clusterRequestController = ClusterRestRequestController.DevNode();
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
        ) throws NodelibraryException {
            final boolean storageExists = Files.isDirectory(storageRootPath);
            if (!storageExists && !backend.containsBackups()) {
                return false;
            }
            if (!storageExists) {
                final ReplicationCursor backup = backend.getCursorFromPreviousBackup(0).orElseThrow(() ->
                        new NodelibraryException("The newest storage backup has no replication cursor"));
                this.deleteOffsetFile();
                this.restoreBackupAndCursor(backend, backup, storageRootPath);
                return true;
            }

            final var latest = backend.getCursorFromPreviousBackup(0);
            if (latest.isEmpty()) {
                return false;
            }
            final ReplicationCursor local = this.getStoredReplicationCursorManager().get();
            final ReplicationCursor backup = latest.get();
            final boolean localBoundaryUnknown = local.logicalSequence() < 0;
            final boolean identityMismatch = !Objects.equals(local.transport(), backup.transport()) ||
                                             !Objects.equals(local.storeGeneration(), backup.storeGeneration());
            final boolean localBehind = local.logicalSequence() < backup.logicalSequence();
            final boolean equalSequencePositionMismatch = local.logicalSequence() == backup.logicalSequence() &&
                                                          !Arrays.equals(local.providerPosition(), backup.providerPosition());
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
                backend.downloadLatestBackup(this.storageParentPath());
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

        private void deleteOffsetFile() throws NodelibraryException {
            try {
                AtomicFileStore.delete(this.storageParentPath().resolve("offset"), true);
            } catch (final IOException failure) {
                throw new NodelibraryException("Failed to remove stale replication cursor before backup restore", failure);
            }
        }

        private void closeStoredReplicationCursorManager() {
            if (this.storedReplicationCursorManager == null) {
                return;
            }
            this.storedReplicationCursorManager.close();
            this.storedReplicationCursorManager = null;
        }

        private void closeReplicationTransportAndPositionProvider() {
            Throwable failure = null;
            if (this.replicationTransport != null) {
                failure = closeResource(failure, this.replicationTransport::close);
            }
            if (this.positionProvider != null) {
                failure = closeResource(failure, this.positionProvider::close);
            }
            if (this.replicationRetention != null) {
                failure = closeResource(failure, this.replicationRetention::close);
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
            this.closed = true;
            Throwable failure = null;
            final boolean storageManagerOwnsNodeResources = this.clusterStorageManager != null;
            final boolean requestControllerOwnsBackupResources =
                    this.clusterRequestController instanceof ClusterRestRequestController.BackupNode;
            if (this.clusterRequestController != null) {
                failure = closeResource(failure, this.clusterRequestController::close);
            }
            if (this.clusterStorageManager != null) {
                failure = closeResource(failure, this.clusterStorageManager::close);
            }
            /* A foundation can be closed after dependency creation but before the
             * node-specific controllers are installed. Release those partially-built
             * resources as well. The concrete implementations are idempotent. */
            if (!requestControllerOwnsBackupResources && this.dataClient != null) {
                failure = closeResource(failure, this.dataClient::dispose);
            }
            if (this.dataDistributor != null) {
                failure = closeResource(failure, this.dataDistributor::dispose);
            }
            if (this.healthCheck != null) {
                failure = closeResource(failure, this.healthCheck::close);
            }
            if (!requestControllerOwnsBackupResources && this.storageBackupTaskExecutor != null) {
                failure = closeResource(failure, this.storageBackupTaskExecutor::close);
            }
            if (this.storageTaskExecutor != null && this.storageTaskExecutor != this.storageBackupTaskExecutor) {
                failure = closeResource(failure, this.storageTaskExecutor::close);
            }
            if (this.dataMerger != null) {
                failure = closeResource(failure, this.dataMerger::dispose);
            }
            if (this.afterDataMessageConsumedListener != null) {
                failure = closeResource(failure, this.afterDataMessageConsumedListener::close);
            }
            if (this.afterDataMessageConsumedListener == null && this.storedReplicationCursorManager != null) {
                failure = closeResource(failure, this.storedReplicationCursorManager::close);
            }
            if (!storageManagerOwnsNodeResources &&
                (this.replicationTransport != null || this.positionProvider != null ||
                 this.replicationRetention != null)
            ) {
                failure = closeResource(failure, this::closeReplicationTransportAndPositionProvider);
            }
            if (!storageManagerOwnsNodeResources && this.housekeeper != null) {
                failure = closeResource(failure, this.housekeeper::close);
            }
            if (failure != null) {
                throwFailure(failure, "Failed to close cluster foundation");
            }
        }

        private void ensureOpen() {
            if (this.closed) {
                throw new IllegalStateException("Cluster foundation is closed");
            }
        }
    }
}
