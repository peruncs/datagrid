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
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider.Env.EnvKeys;
import peruncs.datagrid.cluster.node.aeron.AeronClusterReplicationTransportProvider;
import peruncs.datagrid.cluster.node.aeron.ReseedRequiredException;
import peruncs.datagrid.cluster.node.backup.*;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.node.store.*;
import peruncs.datagrid.cluster.storage.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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

        /// Creates an empty builder whose collaborators are supplied by setters.
        public Builder() {
        }

        /// Sets the root object supplier.
        /// @param value root supplier
        /// @return this builder
        public Builder setRootSupplier(final Supplier<Object> value) { this.rootSupplier = value; return this; }
        /// Sets the embedded Store foundation.
        /// @param value embedded Store foundation
        /// @return this builder
        public Builder setEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> value) { this.embeddedStorageFoundation = value; return this; }
        /// Sets the properties provider.
        /// @param value properties provider
        /// @return this builder
        public Builder setNodeLibraryPropertiesProvider(final NodeLibraryPropertiesProvider value) { this.propertiesProvider = value; return this; }
        /// Sets whether asynchronous distribution is enabled.
        /// @param value whether asynchronous distribution is enabled
        /// @return this builder
        public Builder setEnableAsyncDistribution(final boolean value) { this.enableAsyncDistribution = value; return this; }
        /// Builds the immutable node foundation.
        /// @return configured cluster foundation
        public ClusterFoundation build() {
            return new Node(new NodeConfiguration(
                    this.backupBackend, this.storageTaskExecutor, this.storageBackupTaskExecutor,
                    this.replicationTransport, this.dataMerger,
                    this.afterDataMessageConsumedListener, this.storedReplicationCursorManager,
                    this.storageBackupManager, this.rootSupplier, this.graphUpdateHandler,
                    this.embeddedStorageFoundation, this.backupNodeManager, this.dataClient,
                    this.dataDistributor, this.healthCheck, this.propertiesProvider,
                    this.storageDiskSpaceReader, this.storageNodeManager,
                    this.enableAsyncDistribution, this.positionProvider, this.replicationRetention));
        }
    }

        /// Immutable pre-start node configuration.
    ///
    /// @param backupBackend backup archive backend
    /// @param storageTaskExecutor storage executor
    /// @param storageBackupTaskExecutor backup executor
    /// @param replicationTransport cluster replication transport
    /// @param dataMerger binary data merger
    /// @param afterDataMessageConsumedListener post-consumption listener
    /// @param storedReplicationCursorManager persisted cursor manager
    /// @param storageBackupManager backup manager
    /// @param rootSupplier Store root supplier
    /// @param graphUpdateHandler object-graph update handler
    /// @param embeddedStorageFoundation embedded Store foundation
    /// @param backupNodeManager backup node manager
    /// @param dataClient binary data client
    /// @param dataDistributor binary data distributor
    /// @param healthCheck node health check
    /// @param propertiesProvider node properties provider
    /// @param storageDiskSpaceReader disk-space reader
    /// @param storageNodeManager storage node manager
    /// @param enableAsyncDistribution whether asynchronous distribution is enabled
    /// @param positionProvider replication position provider
    /// @param replicationRetention replication-log retention policy
    record NodeConfiguration(
            StorageBackupBackend backupBackend,
            StorageTaskExecutor storageTaskExecutor,
            StorageBackupTaskExecutor storageBackupTaskExecutor,
            ClusterReplicationTransport replicationTransport,
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


        /// Starts the storage manager.
    ///
    /// The returned manager is a borrow: the foundation owns it and shuts it
    /// down on [ClusterFoundation#close]. Callers must not shut it down or
    /// close it; shutdown is idempotent, so a stray call stays harmless but
    /// still risks using a closed Store.
    ///
    /// @return storage manager
    /// @throws NodeLibraryException if startup fails
    /// @throws ReseedRequiredException if local recovery evidence cannot be reconciled and the node
    ///                                 must be reseeded from a compatible backup or Store image
    ClusterStorageManager<?> startStorageManager() throws NodeLibraryException;

        /// Returns the storage node control view, starting the node when necessary.
    ///
    /// This is the programmatic control surface the embedding application
    /// uses in place of a network boundary: role, health, readiness,
    /// storage size, and replication metrics. The view carries no `close()`:
    /// the foundation owns the manager and closes it on [ClusterFoundation#close].
    /// The role is validated before anything starts, so probing the wrong
    /// role never starts Store, Aeron, recovery, or background threads.
    ///
    /// @return storage node control view
    /// @throws NodeLibraryException if startup fails
    /// @throws IllegalStateException if this node is not a storage node
    StorageNodeControl storageNodeManager() throws NodeLibraryException;

        /// Returns the backup node control view, starting the node when necessary.
    ///
    /// This is the programmatic control surface the embedding application
    /// uses in place of a network boundary: backup triggers and reader
    /// pause/resume. The view carries no `close()`: the foundation owns the
    /// manager and closes it on [ClusterFoundation#close]. The role is
    /// validated before anything starts, so probing the wrong role never
    /// starts Store, Aeron, recovery, or background threads.
    ///
    /// @return backup node control view
    /// @throws NodeLibraryException if startup fails
    /// @throws IllegalStateException if this node is not a backup node
    BackupNodeControl backupNodeManager() throws NodeLibraryException;

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
        private final StorageGraphCoordinator graphCoordinator = new StorageGraphCoordinator();
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
        /* Keep the raw Store manager only for the internal replication merger.
         * Application code receives the guarded cluster manager, so a reader
         * cannot invoke importData/importFiles as an untracked write. */
        private volatile StorageManager embeddedStorageManager;
        private boolean started;
        private boolean closed;
        private boolean closing;

        private Node(final NodeConfiguration configuration) {
            this.backupBackend = lazy(configuration.backupBackend(), () -> this.dispatch(this.ensureBackupBackend()));
            this.storageTaskExecutor = lazy(configuration.storageTaskExecutor(), () -> this.dispatch(this.ensureStorageTaskExecutor()));
            this.storageBackupTaskExecutor = lazy(configuration.storageBackupTaskExecutor(), () -> this.dispatch(this.ensureStorageBackupTaskExecutor()));
            this.housekeeper = LazyConstant.of(() -> this.dispatch(this.ensureNodeHousekeeper()));
            this.storageLimitGate = LazyConstant.of(() -> this.dispatch(this.ensureStorageLimitGate()));
            this.replicationTransport = lazy(configuration.replicationTransport(), () -> this.dispatch(this.ensureClusterReplicationTransport()));
            this.dataMerger = lazy(configuration.dataMerger(), () -> this.dispatch(this.ensureStorageBinaryDataMerger()));
            this.afterDataMessageConsumedListener = lazy(configuration.afterDataMessageConsumedListener(), () -> this.dispatch(this.ensureAfterDataMessageConsumedListener()));
            this.storedReplicationCursorManager = configuration.storedReplicationCursorManager();
            this.storageBackupManager = lazy(configuration.storageBackupManager(), () -> this.dispatch(this.ensureStorageBackupManager()));
            this.rootSupplier = lazy(configuration.rootSupplier(), () -> this.dispatch(this.ensureRootSupplier()));
            this.graphUpdateHandler = lazy(configuration.graphUpdateHandler(), () -> this.dispatch(this.ensureGraphUpdateHandler()));
            this.embeddedStorageFoundation = lazy(configuration.embeddedStorageFoundation(), () -> this.dispatch(this.ensureEmbeddedStorageFoundation()));
            this.backupNodeManager = lazy(configuration.backupNodeManager(), () -> this.dispatch(this.ensureBackupNodeManager()));
            this.dataClient = lazy(configuration.dataClient(), () -> this.dispatch(this.ensureStorageBinaryDataClient()));
            this.dataDistributor = lazy(configuration.dataDistributor(), () -> this.dispatch(this.ensureDataDistributor()));
            this.healthCheck = lazy(configuration.healthCheck(), () -> this.dispatch(this.ensureStorageNodeHealthCheck()));
            this.propertiesProvider = lazy(configuration.propertiesProvider(), () -> this.dispatch(this.ensureNodeLibraryPropertiesProvider()));
            this.storageDiskSpaceReader = lazy(configuration.storageDiskSpaceReader(), () -> this.dispatch(this.ensureStorageDiskSpaceReader()));
            this.storageNodeManager = lazy(configuration.storageNodeManager(), () -> this.dispatch(this.ensureStorageNodeManager()));
            this.enableAsyncDistribution = configuration.enableAsyncDistribution();
            this.positionProvider = lazy(configuration.positionProvider(), () -> this.dispatch(this.ensureReplicationPositionProvider()));
            this.replicationRetention = lazy(configuration.replicationRetention(), () -> this.dispatch(this.ensureReplicationLogRetention()));
        }

        private static <T> LazyConstant<T> lazy(final T configured, final Supplier<? extends T> factory) {
            return LazyConstant.of(() -> configured == null ? factory.get() : configured);
        }

        private static Path backupVolumePath(final NodeLibraryPropertiesProvider properties) {
            final String configured = properties.replicationProperty(NodeLibraryPropertiesProvider.Env.EnvKeys.BACKUP_PATH);
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

        private static Throwable closeInitialized(final Throwable current, final LazyConstant<?> resource, final Runnable close) {
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
            if (this.getNodeLibraryPropertiesProvider().nodeRole() == NodeRole.BACKUP_READER) {
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
                            EnvKeys.STORAGE_LIMIT_GB
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
                    if (props.nodeRole() != NodeRole.WRITER) {
                        // Every reader must persist its resolved boundary; writers do not consume replication.
                        this.delegate.set(cursor);
                    }
                }

                @Override
                public void close() {
                    this.delegate.close();
                }
            };
            LOGGER.log(System.Logger.Level.TRACE, "Created AfterDataMessageConsumedListener->StoredReplicationCursorManager delegate. WillRun=%s".formatted(props.nodeRole() != NodeRole.WRITER));
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
                    EnvKeys.KEPT_BACKUPS_COUNT);

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
        /// The handler runs every materialization on this node's graph
        /// coordinator write side. Application code that touches the object
        /// graph directly must use [#storageGraphCoordinator()] read side;
        /// the global synchronized default cannot protect such reads.
        ///
        /// @return graph update handler
        private ObjectGraphUpdateHandler ensureGraphUpdateHandler() {
            return ObjectGraphUpdateHandler.PerStore(this.graphCoordinator);
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
        /// The merger receives replicated binaries directly; it stays owned
        /// by this foundation, which disposes it on close.
        ///
        /// @return replication data client
        private StorageBinaryDataClient ensureStorageBinaryDataClient() {
            final var props = this.getNodeLibraryPropertiesProvider();
            final boolean commitPosition = props.nodeRole() == NodeRole.BACKUP_READER;
            return this.getClusterReplicationTransport().client(
                    this.getStorageBinaryDataMerger(),
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
        /// Node roles are fixed at startup: a writer gets the distributor, a
        /// reader or backup-reader gets the reader. There is no
        /// reader-to-distributor transition.
        ///
        /// @return storage node manager
        private StorageNodeManager ensureStorageNodeManager() {
            final String transport = this.getClusterReplicationTransport().id();
            final boolean writer =
                    this.getNodeLibraryPropertiesProvider().nodeRole() == NodeRole.WRITER;
            return StorageNodeManager.New(StorageNodeManager.Configuration.builder()
                    .dataDistributor(this.getStorageBinaryDataDistributor())
                    .storageTaskExecutor(this.getStorageTaskExecutor())
                    .dataClient(this.getStorageBinaryDataClient())
                    .healthCheck(this.getStorageNodeHealthCheck())
                    .storageDiskSpaceReader(this.getStorageDiskSpaceReader())
                    .positionProvider(this.getReplicationPositionProvider())
                    .replicationTransport(transport)
                    .role(writer ? StorageNodeManager.Role.DISTRIBUTOR : StorageNodeManager.Role.READER)
                    .build());
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

            final Long applyTimeoutMsNullable = this.getNodeLibraryPropertiesProvider().dataMergerApplyTimeoutMs();
            final long applyTimeoutMs = applyTimeoutMsNullable == null ? StorageBinaryDataMerger.Defaults
                    .APPLY_TIMEOUT_MS : applyTimeoutMsNullable;

            final StorageConnection replicationStorage = this.embeddedStorageManager != null
                    ? this.embeddedStorageManager
                    : this.clusterStorageManager;
            if (replicationStorage == null) {
                throw new IllegalStateException(
                        "cannot create the replication merger before embedded storage has started");
            }
            return StorageBinaryDataMerger.New(StorageBinaryDataMerger.Configuration.builder()
                    .foundation(this.getEmbeddedStorageFoundation().getConnectionFoundation())
                    .storage(replicationStorage)
                    .objectGraphUpdateHandler(this.getObjectGraphUpdateHandler())
                    .cachingTimeoutMs(cachingTimeoutMs)
                    .cachedBinaryLimit(cachedDataLimit)
                    .applyTimeoutMs(applyTimeoutMs)
                    .graphCoordinator(this.graphCoordinator)
                    .build());
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
                    EnvKeys.GC_INTERVAL_MINUTES,
                    30
            );
        }

        private Duration backupNodeBackupInterval() {
            return maintenanceInterval(
                    this.getNodeLibraryPropertiesProvider().backupIntervalMinutes(),
                    EnvKeys.BACKUP_INTERVAL_MINUTES,
                    120
            );
        }

        private Duration storageNodeGcInterval() {
            return maintenanceInterval(
                    this.getNodeLibraryPropertiesProvider().gcIntervalMinutes(),
                    EnvKeys.GC_INTERVAL_MINUTES,
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
        public synchronized ClusterStorageManager<?> startStorageManager() throws NodeLibraryException {
            this.ensureOpen();
            if (this.clusterStorageManager == null) {
                this.start();
            }

            return this.clusterStorageManager;
        }

        @Override
        public synchronized StorageNodeControl storageNodeManager() throws NodeLibraryException {
            this.ensureOpen();
            final var properties = this.getNodeLibraryPropertiesProvider();
            if (!properties.isProdMode() || properties.nodeRole() == NodeRole.BACKUP_READER) {
                throw new IllegalStateException("this node is not a storage node");
            }
            if (this.clusterStorageManager == null) {
                this.start();
            }

            return this.getStorageNodeManager();
        }

        @Override
        public synchronized BackupNodeControl backupNodeManager() throws NodeLibraryException {
            this.ensureOpen();
            final var properties = this.getNodeLibraryPropertiesProvider();
            if (!properties.isProdMode() || properties.nodeRole() != NodeRole.BACKUP_READER) {
                throw new IllegalStateException("this node is not a backup node");
            }
            if (this.clusterStorageManager == null) {
                this.start();
            }

            return this.getBackupNodeManager();
        }

        private StorageGraphCoordinator storageGraphCoordinator() {
            return this.graphCoordinator;
        }

                /// Starts the node in its configured role.
        ///
        /// @throws NodeLibraryException if startup fails
        /// @throws ReseedRequiredException if local recovery evidence cannot be reconciled and the node
        ///                                 must be reseeded from a compatible backup or Store image
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
                } else if (properties.nodeRole() == NodeRole.BACKUP_READER) {
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
                RuntimeException runtime = (RuntimeException) failure;
                throw runtime;
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
                /* A user upload restores with backup metadata unchecked, so it
                 * is validated before the local image is destroyed: a partial
                 * or ambiguous upload must never replace working storage. */
                this.requireValidUserUploadedStorage();
                this.deleteDirectory(storageRootPath);
                backend.restoreUserUploadedStorage(storageParentPath);
                backend.deleteUserUploadedStorage();
            } else if (this.restoreLatestBackupIfRequired(storageRootPath, backend)) {
                LOGGER.log(System.Logger.Level.INFO, "Restored the newest compatible storage backup");
            } else {
                LOGGER.log(System.Logger.Level.INFO, "Starting with local storage");
            }
            /* A backup node owns no authoritative image of its own. It may
             * manufacture a root only from a user-uploaded Store it then
             * publishes as the starter backup for other nodes; without that
             * upload, without a restored compatible backup, and without local
             * Store files, creating an independent root would diverge the
             * cluster permanently — every later delta references object ids
             * the invented image never contained. Fail closed before opening
             * storage, like a reader without a seed. */
            if (!requiresStorageUpload && isMissingOrEmpty(storageRootPath)) {
                throw new ReseedRequiredException(
                        "backup node has no local Store image at %s, no compatible backup, and no user upload; seed the Store directory with the writer's Store and its replication cursor before starting"
                                .formatted(storageRootPath));
            }
            /* Same lost-cursor gate as replicated readers: existing Store
             * files without their durable offset cursor cannot be resumed
             * safely, because the backup node can no longer address the
             * history those files represent. */
            if (!requiresStorageUpload && this.usesAeronReplication() && !isMissingOrEmpty(storageRootPath)) {
                this.requireStoredCursorForExistingStore(storageRootPath);
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

            final var embeddedStorageManager = this.prepareEmbeddedStorage(storageRootPath).start();
            this.embeddedStorageManager = embeddedStorageManager;
            this.initializeRoot(embeddedStorageManager);
            /* Same one-time policy scan as storage nodes: a seeded or uploaded
             * image with an external index registration is rejected before the
             * backup node serves or publishes anything. */
            ClusterStoreIndexes.validateStorageRoots(embeddedStorageManager);

            this.getStorageBinaryDataDistributor().ignoreDistribution(false);
            this.queueWriterDictionary(embeddedStorageManager);

            final var housekeeper = this.getNodeHousekeeper();

            this.clusterStorageManager = ClusterStorageManager.ReadOnly(embeddedStorageManager,
                    () -> this.closeHousekeeperAndReplication(housekeeper), this.graphCoordinator);

            this.getStorageBinaryDataClient().start();
            /* Eagerly create the manager so misconfiguration fails at startup.
             * The foundation owns its lifecycle; embedders borrow it through
             * backupNodeManager(). */
            Objects.requireNonNull(this.getBackupNodeManager());

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

            final boolean restored = this.restoreLatestBackupIfRequired(storageRootPath, backend);
            if (restored) {
                LOGGER.log(System.Logger.Level.INFO, "Restored the newest compatible storage backup");
            } else {
                LOGGER.log(System.Logger.Level.INFO, Files.exists(storageRootPath)
                        ? "Resuming existing local storage and cursor"
                        : "Starting with local storage");
            }
            /* A reader owns no authoritative image: without a restored backup
             * or existing local Store files it could only manufacture an
             * independent root that later deltas cannot resolve against.
             * Fail fast before opening storage instead of diverging. */
            if (!restored && !this.mayCreateRoot() && isMissingOrEmpty(storageRootPath)) {
                throw new ReseedRequiredException(
                        "node role '%s' has no local Store image and no backup seed at %s; restore a compatible backup or seed the Store directory with its replication cursor before starting"
                                .formatted(this.getNodeLibraryPropertiesProvider().nodeRole().configName(), storageRootPath));
            }
            /* Lost-cursor gate, replicated readers only: Store files without
             * their durable offset cursor cannot be resumed safely, because
             * the reader can no longer address the history those files
             * represent. Nodes without replication keep the Store-only seed
             * flow above. */
            if (!restored && !this.mayCreateRoot() && this.usesAeronReplication() &&
                !isMissingOrEmpty(storageRootPath)) {
                this.requireStoredCursorForExistingStore(storageRootPath);
            }

            this.getReplicationPositionProvider().init();

            final var dataDistributor = this.getStorageBinaryDataDistributor();
            /* Pre-start root gate: the authoritative check in initializeRoot
             * runs after the Store is opened, when rejecting a reader already
             * leaves Store files behind. Re-check immediately before creating
             * the Store so a rejected reader leaves no fresh image. */
            if (!this.mayCreateRoot() && isMissingOrEmpty(storageRootPath)) {
                throw new ReseedRequiredException(
                        "node role '%s' has no local Store image at %s; restore a compatible backup or seed the Store directory with its replication cursor before starting"
                                .formatted(this.getNodeLibraryPropertiesProvider().nodeRole().configName(), storageRootPath));
            }
            final var embeddedStorageFoundation = this.prepareEmbeddedStorage(storageRootPath);
            DistributedStorage.configureWriting(
                    embeddedStorageFoundation,
                    dataDistributor,
                    this.getClusterReplicationTransport().persistenceTargetFactory(
                            this.getNodeLibraryPropertiesProvider().replicationStreamName(),
                            dataDistributor,
                            /* The writer's storage connection does not exist
                             * during wiring (root creation runs first); the
                             * supplier is resolved at write time and skips
                             * validation while it is absent. */
                            () -> this.clusterStorageManager
                    )
            );

            final var embeddedStorageManager = embeddedStorageFoundation.start();
            this.embeddedStorageManager = embeddedStorageManager;
            this.initializeRoot(embeddedStorageManager);
            /* One-time policy scan at Store start: a freshly deserialized or
             * seeded image containing an external index is rejected before
             * the node serves or publishes anything. */
            ClusterStoreIndexes.validateStorageRoots(embeddedStorageManager);

            this.getStorageBinaryDataDistributor().ignoreDistribution(false);
            this.queueWriterDictionary(embeddedStorageManager);

            final var housekeeper = this.getNodeHousekeeper();
            final var limitGate = this.getStorageLimitGate();

            /* Reader roles reproduce the writer's history through the internal
             * raw Store import path and must never persist a locally originated
             * write. The application-facing read-only view rejects every write;
             * only the node-owned merger receives the raw manager. */
            final boolean writer = NodeLibraryPropertiesProvider.WRITER_ROLE.equalsIgnoreCase(
                    this.getNodeLibraryPropertiesProvider().nodeRole().configName());
            this.clusterStorageManager = writer
                    ? ClusterStorageManager.New(
                            embeddedStorageManager,
                            limitGate::limitReached,
                            () -> this.closeHousekeeperAndReplication(housekeeper),
                            this.graphCoordinator)
                    : ClusterStorageManager.ReadOnly(
                            embeddedStorageManager,
                            () -> this.closeHousekeeperAndReplication(housekeeper),
                            this.graphCoordinator);

            this.getStorageBinaryDataClient().start();

            /* Eagerly create the manager so misconfiguration fails at startup.
             * The foundation owns its lifecycle; embedders borrow it through
             * storageNodeManager(). */
            Objects.requireNonNull(this.getStorageNodeManager());

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
                            EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES
                    ))
            );

            housekeeper.start();
        }

                /// Queues the complete persisted dictionary for the first post-restart
        /// transaction. This covers types introduced by a rejected transaction whose
        /// incremental export was consumed before the writer crashed.
        private void queueWriterDictionary(final EmbeddedStorageManager storage) {
            final NodeLibraryPropertiesProvider props = this.getNodeLibraryPropertiesProvider();
            if (props.nodeRole() != NodeRole.WRITER) {
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
                /* Only the writer and the backup seeder own an authoritative
                 * image. A reader that reaches this point with no root has no
                 * seed to reproduce the writer's history from; manufacturing
                 * an independent root here would permanently diverge it. */
                if (!this.mayCreateRoot()) {
                    throw new ReseedRequiredException(
                            "node role '%s' opened a Store without a root; seed the Store directory with its replication cursor before starting"
                                    .formatted(this.getNodeLibraryPropertiesProvider().nodeRole().configName()));
                }
                LOGGER.log(System.Logger.Level.DEBUG, "Setting and storing new root from root supplier");
                final Object root = this.getRootSupplier().get();
                storage.setRoot(root instanceof Lazy ? root : Lazy.Reference(root));
                storage.storeRoot();
            }
        }

                /// Reports whether this node may manufacture a fresh Store root.
        ///
        /// Only the writer and the backup seeder own an authoritative image;
        /// readers and backup-readers must reproduce the writer's history from
        /// a matching Store+cursor seed.
        ///
        /// @return `true` for the writer and backup-reader roles
        private boolean mayCreateRoot() {
            final var properties = this.getNodeLibraryPropertiesProvider();
            return properties.nodeRole() != NodeRole.READER;
        }

                /// Reports whether this node replicates through the Aeron transport.
        ///
        /// Nodes without replication keep the Store-only seed flow: their Store
        /// image alone is the state. Replicated readers additionally need their
        /// durable offset cursor to address history.
        ///
        /// @return `true` when the configured replication transport is Aeron
        private boolean usesAeronReplication() {
            return "aeron".equalsIgnoreCase(this.getNodeLibraryPropertiesProvider().replicationTransport());
        }

                /// Fails closed when existing Store files lost their durable offset cursor.
        ///
        /// @param storageRootPath Store directory known to hold files
        /// @throws ReseedRequiredException when no usable stored cursor exists
        private void requireStoredCursorForExistingStore(final Path storageRootPath) {
            RuntimeException cursorFailure = null;
            ReplicationCursor stored = null;
            try {
                stored = this.getStoredReplicationCursorManager().get();
            } catch (final RuntimeException failure) {
                cursorFailure = failure;
            }
            if (stored == null || stored.logicalSequence() < 0) {
                throw new ReseedRequiredException(
                        "node role '%s' has Store files at %s but no durable replication cursor; restore a compatible backup or seed the Store directory with its replication cursor before starting"
                                .formatted(this.getNodeLibraryPropertiesProvider().nodeRole().configName(), storageRootPath),
                        cursorFailure);
            }
        }

        /* Upper bound for the user-uploaded manifest probe below. Generated
         * backups cap their manifest at the same size; anything larger is not
         * a manifest but a damaged or hostile upload. */
        private static final int USER_UPLOAD_MANIFEST_LIMIT_BYTES = 1 << 20;

                /// Validates a user-uploaded storage archive before it may replace local state.
        ///
        /// Generated backups always carry `storage/`, `manifest`, and `ready`;
        /// user uploads skip the metadata checks of the restore path, so this
        /// pre-check requires the same essentials: exactly one readable,
        /// non-empty `manifest` entry (ambiguity fails) and at least one Store
        /// payload file under `storage/`. It runs before the local image is
        /// deleted, so a partial upload never destroys working storage, and a
        /// rejected upload is left in place for inspection instead of being
        /// silently consumed.
        ///
        /// @throws NodeLibraryException when the upload is missing, ambiguous, or partial
        private void requireValidUserUploadedStorage() {
            final Path archive = backupVolumePath(this.getNodeLibraryPropertiesProvider())
                    .resolve(StorageBackupBackend.USER_UPLOADED_STORAGE_ARCHIVE);
            if (!Files.isRegularFile(archive, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(archive)) {
                throw new NodeLibraryException(
                        "User-uploaded storage archive is missing or ambiguous at %s; refusing to install".formatted(archive));
            }
            int manifests = 0;
            boolean manifestDecodable = false;
            boolean storagePayload = false;
            try (ZipFile zip = new ZipFile(archive.toFile())) {
                final Enumeration<? extends ZipEntry> entries = zip.entries();
                while (entries.hasMoreElements()) {
                    final ZipEntry entry = entries.nextElement();
                    final String name = entry.getName();
                    if (StorageBackupBackend.MANIFEST_ENTRY.equals(name)) {
                        manifests++;
                        manifestDecodable = manifestDecodable || isDecodableManifest(zip, entry);
                    } else if (!entry.isDirectory() &&
                               name.startsWith(StorageBackupBackend.STORAGE_ENTRY + "/")) {
                        storagePayload = true;
                    }
                }
            } catch (final IOException failure) {
                throw new NodeLibraryException(
                        "User-uploaded storage archive cannot be read at %s; refusing to install".formatted(archive),
                        failure);
            }
            if (manifests != 1 || !manifestDecodable) {
                throw new NodeLibraryException(
                        "User-uploaded storage archive must contain exactly one decodable manifest at %s; refusing to install".formatted(archive));
            }
            if (!storagePayload) {
                throw new NodeLibraryException(
                        "User-uploaded storage archive contains no storage payload at %s; refusing to install a partial upload".formatted(archive));
            }
        }

                /// Reports whether a manifest entry can be fully decoded within its budget.
        ///
        /// A directory entry never counts; otherwise the entry must be a
        /// non-empty readable byte sequence that fits the manifest budget.
        ///
        /// @param zip   open upload archive
        /// @param entry manifest entry
        /// @return `true` when the entry is a non-empty readable byte sequence
        private static boolean isDecodableManifest(final ZipFile zip, final ZipEntry entry) {
            if (entry.isDirectory()) {
                return false;
            }
            try (InputStream data = zip.getInputStream(entry)) {
                final byte[] bytes = data.readNBytes(USER_UPLOAD_MANIFEST_LIMIT_BYTES);
                return bytes.length != 0 && data.read() == -1;
            } catch (final IOException unreadable) {
                return false;
            }
        }

                /// Reports whether a directory is missing or holds no entries.
        ///
        /// @param directory directory to inspect
        /// @return `true` when the directory does not exist or is empty
        private static boolean isMissingOrEmpty(final Path directory) {
            if (!Files.isDirectory(directory)) {
                return true;
            }
            try (var entries = Files.list(directory)) {
                return entries.findAny().isEmpty();
            } catch (final IOException failure) {
                throw new NodeLibraryException("Cannot inspect storage directory %s".formatted(directory), failure);
            }
        }

                /// Starts the local development node.
        ///
        /// @throws NodeLibraryException if startup fails
        private void startDevNode() throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.INFO, "Starting dev cluster node");
            final var storage = this.getEmbeddedStorageFoundation().start();
            this.embeddedStorageManager = storage;
            if (storage.root() == null) {
                final var root = this.getRootSupplier().get();
                if (root instanceof Lazy) {
                    storage.setRoot(root);
                } else {
                    storage.setRoot(Lazy.Reference(root));
                }
                storage.storeRoot();
            }

            this.clusterStorageManager = ClusterStorageManager.New(
                    storage,
                    () -> false,
                    this::closeReplicationTransportAndPositionProvider,
                    this.graphCoordinator
            );
        }

        private void deleteDirectory(final Path path) {
            if (!Files.exists(path)) {
                return;
            }

            LOGGER.log(System.Logger.Level.INFO, "Deleting files at %s".formatted(path));
            StorageFileOperations.deleteDirectory(path);
        }

                /// Selects a backup only when the local Store cannot be trusted to represent
        /// the newest compatible image. Compatibility by cluster, store
        /// generation, epoch, and recording is validated before anything is
        /// deleted or installed, so a backup from an unrelated generation on
        /// a shared volume can never overwrite valid local storage. A local
        /// cursor from another transport or Store generation is never mixed
        /// with local files; it is discarded together with the Store before
        /// extraction. An equal-sequence cursor must also point at the same
        /// provider position. A local cursor ahead of the newest compatible
        /// backup is retained because restoring an older image would lose data.
        private boolean restoreLatestBackupIfRequired(
                final Path storageRootPath,
                final StorageBackupBackend backend
        ) throws NodeLibraryException {
            final boolean storageExists = Files.isDirectory(storageRootPath);
            final BackupMetadata.Identity configured = this.configuredBackupIdentity();
            final BackupMetadata selected = backend.findLatestCompatibleBackup(configured);
            if (selected == null) {
                if (!storageExists && backend.containsBackups()) {
                    throw new NodeLibraryException(
                            "No backup on the shared volume is compatible with this node %s; refusing to install an unrelated image"
                                    .formatted(configured));
                }
                if (!storageExists) {
                    return false;
                }
                LOGGER.log(System.Logger.Level.WARNING,
                        "No backup on the shared volume is compatible with this node %s; keeping local storage"
                                .formatted(configured));
                return false;
            }
            final ReplicationCursor backup = backend.getCursorForBackup(selected);
            final BackupMetadata.Identity backupIdentity = BackupMetadata.Identity.of(backup);
            if (!configured.matches(backupIdentity)) {
                throw new NodeLibraryException(
                        "Backup metadata and its stored replication cursor disagree with this node identity; refusing to modify local storage");
            }
            /* Verify metadata against its own cursor before any deletion: a
             * corrupt or mixed archive can pass the configured check (for
             * example when recording is unconfigured) yet disagree internally.
             * A later startup check would reject the installed cursor, but only
             * after local storage was already destroyed. */
            try {
                BackupMetadata.requireConsistentWithCursor(selected, backup);
            } catch (final IllegalArgumentException inconsistent) {
                throw new NodeLibraryException(
                        "Backup metadata disagrees with its stored replication cursor; refusing to modify local storage",
                        inconsistent);
            }
            if (!storageExists) {
                this.deleteOffsetFile();
                this.restoreBackupAndCursor(backend, selected, backup, storageRootPath);
                return true;
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
            this.restoreBackupAndCursor(backend, selected, backup, storageRootPath);
            return true;
        }

                /// Resolves the backup identity this node restores as.
        ///
        /// The transport configuration is authoritative for stable cluster and
        /// Store-generation identity. A live position and durable local cursor
        /// may fill an epoch or recording that was unknown during wiring, but
        /// they never erase configured values. Aeron nodes fail closed if the
        /// required cluster/generation identity is still unavailable.
        ///
        /// @return best available node backup identity
        private BackupMetadata.Identity configuredBackupIdentity() {
            final ClusterReplicationTransport transport = this.getClusterReplicationTransport();
            BackupMetadata.Identity provider = transport.configuredBackupIdentity();
            try {
                /* The transport configuration is authoritative for stable
                 * cluster/generation identity. A live position may contribute
                 * a recording or epoch that was unknown during wiring, but an
                 * unavailable/empty position must never erase configured values. */
                provider = provider.fillUnknowns(
                        BackupMetadata.Identity.of(this.getReplicationPositionProvider().latest()));
            } catch (final RuntimeException unavailable) {
                LOGGER.log(System.Logger.Level.DEBUG,
                        "Replication provider reports no backup identity; falling back to the local cursor",
                        unavailable);
            }
            try {
                final BackupMetadata.Identity merged = provider.fillUnknowns(
                        BackupMetadata.Identity.of(this.getStoredReplicationCursorManager().get()));
                if ("aeron".equalsIgnoreCase(transport.id()) &&
                        (merged.clusterId() == null || merged.storeGeneration() == null)) {
                    throw new ReseedRequiredException(
                            "Aeron backup selection requires configured cluster and Store-generation identity");
                }
                return merged;
            } catch (final RuntimeException unreadable) {
                LOGGER.log(System.Logger.Level.DEBUG,
                        "Local replication cursor is unreadable; restoring without its identity", unreadable);
                if ("aeron".equalsIgnoreCase(transport.id()) &&
                        (provider.clusterId() == null || provider.storeGeneration() == null)) {
                    throw new ReseedRequiredException(
                            "Aeron backup selection requires configured cluster and Store-generation identity",
                            unreadable);
                }
                return provider;
            }
        }

                /// Installs a backup and its cursor as one trusted startup boundary.
        private void restoreBackupAndCursor(
                final StorageBackupBackend backend,
                final BackupMetadata selected,
                final ReplicationCursor backup,
                final Path storageRootPath
        ) {
            try {
                backend.restoreBackup(this.storageParentPath(), selected);
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
            if (this.clusterStorageManager != null) {
                failure = closeResource(failure, this.clusterStorageManager::close);
            }
            /* A started node manager owns its collaborators: closing it
             * cascades to the client, distributor, tasks, and health check.
             * Those collaborators are disposed individually only when their
             * manager never started — a close after partial construction —
             * or when the manager does not own them. Every implementation is
             * idempotent. Roles are fixed, so at most one manager started. */
            final boolean storageManagerClosed = this.storageNodeManager.isInitialized();
            if (storageManagerClosed) {
                failure = closeInitialized(
                        failure, this.storageNodeManager, () -> this.storageNodeManager.get().close());
            }
            final boolean backupManagerClosed = this.backupNodeManager.isInitialized();
            if (backupManagerClosed) {
                failure = closeInitialized(
                        failure, this.backupNodeManager, () -> this.backupNodeManager.get().close());
            }
            if (!storageManagerClosed) {
                /* A backup manager owns only its client and tasks, never the
                 * distributor or the health check. */
                failure = closeInitialized(
                        failure, this.dataDistributor, () -> this.dataDistributor.get().dispose());
                failure = closeInitialized(failure, this.healthCheck, () -> this.healthCheck.get().close());
            }
            if (!storageManagerClosed && !backupManagerClosed) {
                failure = closeInitialized(failure, this.dataClient, () -> this.dataClient.get().dispose());
            }
            if (!backupManagerClosed) {
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
