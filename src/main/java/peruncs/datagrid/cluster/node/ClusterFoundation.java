package peruncs.datagrid.cluster.node;

import org.eclipse.serializer.exceptions.MissingFoundationPartException;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.Unpersistable;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.exceptions.StorageException;
import org.eclipse.store.storage.types.*;
import peruncs.datagrid.cluster.errors.ReseedRequiredException;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider.Env.EnvKeys;
import peruncs.datagrid.cluster.node.aeron.AeronClusterReplicationTransportProvider;
import peruncs.datagrid.cluster.node.backup.*;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.node.store.*;
import peruncs.datagrid.cluster.storage.types.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.function.Supplier;

/// This foundation assembles the services that make one cluster node run.
///
/// A builder creates one immutable node configuration, and the resulting node
/// owns the lazily-created runtime resources for its complete lifecycle.
///
/// The node is single-use: start it at most once and close it when its work is
/// complete. Configuration cannot be changed after {@link Builder#build()}.
///
/// @since 1.0
public interface ClusterFoundation extends AutoCloseable {
        /// Creates a mutable builder for one immutable node configuration.
    ///
    /// @return a new builder
    static Builder create() {
        return new Builder();
    }

        /// Builds the collaborators used by one node before lifecycle starts.
    final class Builder {
        private Supplier<Object> rootSupplier;
        private EmbeddedStorageFoundation<?> embeddedStorageFoundation;
        private NodeLibraryPropertiesProvider propertiesProvider;

        /// Creates an empty builder whose collaborators are supplied by setters.
        public Builder() {
        }

        /// Sets the root object supplier.
        ///
        /// @param value root supplier
        /// @return this builder
        public Builder setRootSupplier(final Supplier<Object> value) {
            this.rootSupplier = value;
            return this;
        }

                /// Sets the embedded Store foundation.
        ///
        /// The node always derives the live file provider from the configured
        /// storage path before starting the Store, so a custom provider set on
        /// the supplied foundation is intentionally replaced. Every other
        /// inherited setting is preserved. Supply a foundation to change
        /// Store tuning, not file layout.
        ///
        /// @param value embedded Store foundation
        /// @return this builder
        public Builder setEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> value) {
            this.embeddedStorageFoundation = value;
            return this;
        }

        /// Sets the properties provider.
        ///
        /// @param value properties provider
        /// @return this builder
        public Builder setNodeLibraryPropertiesProvider(final NodeLibraryPropertiesProvider value) {
            this.propertiesProvider = value;
            return this;
        }

        /// Builds the immutable node foundation.
        ///
        /// @return configured cluster foundation
        public ClusterFoundation build() {
            return new Node(this.rootSupplier, this.embeddedStorageFoundation, this.propertiesProvider);
        }
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

    /// Returns the role fixed when this node was built.
    ///
    /// @return configured node role
    NodeRole nodeRole();

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
    final class Node implements ClusterFoundation, Unpersistable {
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
        private final LazyConstant<Supplier<Object>> rootSupplier;
        private final LazyConstant<ObjectGraphUpdateHandler> graphUpdateHandler;
        private final LazyConstant<StorageBackupManager> storageBackupManager;
        private final LazyConstant<DataMessageAppliedListener> dataMessageAppliedListener;
        private final LazyConstant<StorageBinaryDataMerger> dataMerger;
        private final LazyConstant<BackupRestorePolicy> backupRestorePolicy;
        private final StorageGraphCoordinator graphCoordinator = new StorageGraphCoordinator();
        /* Intentionally not a LazyConstant: a backup restore closes and replaces
         * this manager, which a one-shot memoized holder cannot express. The
         * volatile field with double-checked locking gives the same safe
         * publication without a per-access lock. */
        private volatile StoredReplicationCursorManager storedReplicationCursorManager;
        private final LazyConstant<ClusterReplicationTransport> replicationTransport;
        private final LazyConstant<ReplicationPositionProvider> positionProvider;
        private final LazyConstant<ReplicationLogRetention> replicationRetention;
        private final NodeRole nodeRole;

        // cached created types
        /* Both managers are published to monitoring threads; the volatile
         * fields make the cross-thread observation safe even before a
         * synchronized access. */
        private volatile ClusterStorageManager<?> clusterStorageManager;
        /* Keep the raw Store manager only for the internal replication merger.
         * Application code receives the guarded cluster manager, so a reader
         * cannot invoke importData/importFiles as an untracked write. */
        private volatile StorageManager embeddedStorageManager;
        private volatile boolean started;
        private volatile boolean closed;
        private volatile boolean closing;
        private volatile Throwable closeFailure;

        private Node(final Supplier<Object> configuredRoot,
                     final EmbeddedStorageFoundation<?> configuredFoundation,
                     final NodeLibraryPropertiesProvider configuredProperties) {
            this.backupBackend = LazyConstant.of(this::ensureBackupBackend);
            this.storageTaskExecutor = LazyConstant.of(this::ensureStorageTaskExecutor);
            this.storageBackupTaskExecutor = LazyConstant.of(this::ensureStorageBackupTaskExecutor);
            this.housekeeper = LazyConstant.of(this::ensureNodeHousekeeper);
            this.storageLimitGate = LazyConstant.of(this::ensureStorageLimitGate);
            this.replicationTransport = LazyConstant.of(this::ensureClusterReplicationTransport);
            this.dataMerger = LazyConstant.of(this::ensureStorageBinaryDataMerger);
            this.dataMessageAppliedListener = LazyConstant.of(this::ensureDataMessageAppliedListener);
            this.storageBackupManager = LazyConstant.of(this::ensureStorageBackupManager);
            this.rootSupplier = lazy(configuredRoot, this::ensureRootSupplier);
            this.graphUpdateHandler = LazyConstant.of(this::ensureGraphUpdateHandler);
            this.embeddedStorageFoundation = lazy(configuredFoundation, this::ensureEmbeddedStorageFoundation);
            this.backupNodeManager = LazyConstant.of(this::ensureBackupNodeManager);
            this.dataClient = LazyConstant.of(this::ensureStorageBinaryDataClient);
            this.dataDistributor = LazyConstant.of(this::ensureDataDistributor);
            this.healthCheck = LazyConstant.of(this::ensureStorageNodeHealthCheck);
            this.propertiesProvider = lazy(configuredProperties, this::ensureNodeLibraryPropertiesProvider);
            this.storageDiskSpaceReader = LazyConstant.of(this::ensureStorageDiskSpaceReader);
            this.storageNodeManager = LazyConstant.of(this::ensureStorageNodeManager);
            this.positionProvider = LazyConstant.of(this::ensureReplicationPositionProvider);
            this.replicationRetention = LazyConstant.of(this::ensureReplicationLogRetention);
            this.backupRestorePolicy = LazyConstant.of(this::ensureBackupRestorePolicy);
            this.nodeRole = this.propertiesProvider.get().nodeRole();
        }

        private static <T> LazyConstant<T> lazy(final T configured, final Supplier<? extends T> factory) {
            return LazyConstant.of(() -> configured == null ? factory.get() : configured);
        }

        private static Path backupVolumePath(final NodeLibraryPropertiesProvider properties) {
            final String configured = properties.replicationProperty(EnvKeys.BACKUP_PATH);
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

                /// Creates the configured backup backend.
        ///
        /// @return backup backend
        private StorageBackupBackend ensureBackupBackend() {
            return FilesystemVolumeBackupBackend.create(
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
            return StorageTaskExecutor.create(this.clusterStorageManager);
        }

                /// Creates the backup task executor.
        ///
        /// @return backup task executor
        private StorageBackupTaskExecutor ensureStorageBackupTaskExecutor() {
            return StorageBackupTaskExecutor.create(this.clusterStorageManager, this.getStorageBackupManager());
        }

                /// Creates the node maintenance housekeeper.
        ///
        /// @return housekeeper
        private NodeHousekeeper ensureNodeHousekeeper() {
            return NodeHousekeeper.create();
        }

                /// Creates the storage limit gate.
        ///
        /// @return limit gate
        private StorageLimitGate ensureStorageLimitGate() {
            return StorageLimitGate.create(
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
                    .replicationProperty(EnvKeys.STORAGE_PATH);
            return Paths.get(configured == null || configured.isBlank() ? "storage" : configured)
                    .toAbsolutePath().normalize();
        }

                /// Creates the listener that persists consumed replication cursors.
        ///
        /// @return consumed-message listener
        private DataMessageAppliedListener ensureDataMessageAppliedListener() {
            final boolean persistCursor = this.nodeRole != NodeRole.WRITER;

            final var storedCursorUpdater = new DataMessageAppliedListener() {
                final StoredReplicationCursorManager delegate = Node.this
                        .getStoredReplicationCursorManager();

                @Override
                public void onApplied(final ReplicationCursor cursor) throws NodeLibraryException {
                    if (persistCursor) {
                        // Every reader must persist its resolved boundary; writers do not consume replication.
                        this.delegate.set(cursor);
                    }
                }

                @Override
                public void close() {
                    this.delegate.close();
                }
            };
            LOGGER.log(System.Logger.Level.TRACE, "Created DataMessageAppliedListener->StoredReplicationCursorManager delegate. WillRun=%s".formatted(persistCursor));
            return storedCursorUpdater;
        }

        @Override
        public NodeRole nodeRole() {
            return this.nodeRole;
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

            return StorageBackupManager.create(
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
        /// graph directly must join the coordinator write side around the
        /// mutation; the global synchronized default cannot protect such
        /// touches.
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
            return BackupNodeManager.create(
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
                    this.getDataMessageAppliedListener(),
                    this.getStoredReplicationCursorManager().get(),
                    commitPosition
            );
        }

                /// Creates the storage node health check.
        ///
        /// @return storage health check
        private StorageNodeHealthCheck ensureStorageNodeHealthCheck() {
            return StorageNodeHealthCheck.create(
                    this.clusterStorageManager,
                    this.getClusterReplicationTransport().health(
                            () -> this.clusterStorageManager.isRunning() && !this.clusterStorageManager.isStartingUp(),
                            this.getStorageBinaryDataClient()
                    ),
                    () -> this.getNodeHousekeeper().failure() == null
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
            return StorageDiskSpaceReader.create(
                    this.getEmbeddedStorageFoundation().getConfiguration().fileProvider().baseDirectory()
            );
        }

                /// Creates the storage node manager.
        ///
        /// Node roles are fixed at startup: a writer gets the writer manager, a
        /// reader or backup-reader gets the reader manager. There is no
        /// reader-to-writer transition.
        ///
        /// @return storage node manager
        private StorageNodeManager ensureStorageNodeManager() {
            final String transport = this.getClusterReplicationTransport().id();
            final boolean writer =
                    this.getNodeLibraryPropertiesProvider().nodeRole() == NodeRole.WRITER;
            return StorageNodeManager.create(new StorageNodeManager.Configuration(
                    this.getStorageBinaryDataDistributor(),
                    this.getStorageTaskExecutor(),
                    this.getStorageBinaryDataClient(),
                    this.getStorageNodeHealthCheck(),
                    this.getStorageDiskSpaceReader(),
                    this.getReplicationPositionProvider(),
                    transport,
                    writer ? StorageNodeManager.Role.WRITER : StorageNodeManager.Role.READER));
        }

                /// Creates the configured binary distributor.
        ///
        /// @return binary distributor
        private StorageBinaryDataDistributor ensureDataDistributor() {
            return StorageBinaryDataDistributor.Caching(
                    this.getClusterReplicationTransport().distributor(
                            this.getNodeLibraryPropertiesProvider().replicationStreamName(),
                            false
                    )
            );
        }

                /// Creates the binary merger with configured limits.
        ///
        /// @return binary merger
        private StorageBinaryDataMerger ensureStorageBinaryDataMerger() {
            final StorageConnection replicationStorage = this.embeddedStorageManager != null
                    ? this.embeddedStorageManager
                    : this.clusterStorageManager;
            if (replicationStorage == null) {
                throw new NodeLibraryException(
                        "cannot create the replication merger before embedded storage has started");
            }
            final var configuration = StorageBinaryDataMerger.Configuration.create(
                    this.getEmbeddedStorageFoundation().getConnectionFoundation(),
                    replicationStorage,
                    this.getObjectGraphUpdateHandler(),
                    this.graphCoordinator);

            /* Env overrides fall back to the merger defaults for every knob the
             * provider does not expose, so this stays the single place where a
             * deployment tunes the merger. */
            final Long cachingTimeoutMs = this.getNodeLibraryPropertiesProvider().dataMergerTimeoutMs();
            final Long cachedBytesLimit = this.getNodeLibraryPropertiesProvider().dataMergerCachedDataLimit();
            final Long applyTimeoutMs = this.getNodeLibraryPropertiesProvider().dataMergerApplyTimeoutMs();
            if (cachingTimeoutMs == null && cachedBytesLimit == null && applyTimeoutMs == null) {
                return StorageBinaryDataMerger.create(configuration);
            }
            return StorageBinaryDataMerger.create(new StorageBinaryDataMerger.Configuration(
                    configuration.foundation(),
                    configuration.storage(),
                    configuration.objectGraphUpdateHandler(),
                    cachingTimeoutMs == null ? configuration.cachingTimeoutMs() : cachingTimeoutMs,
                    cachedBytesLimit == null ? configuration.cachedBytesLimit() : cachedBytesLimit,
                    configuration.maxCachedBytes(),
                    applyTimeoutMs == null ? configuration.applyTimeoutMs() : applyTimeoutMs,
                    configuration.disposeOrderlyTimeoutMs(),
                    configuration.disposeInterruptTimeoutMs(),
                    configuration.maxValidatedIndexObjects(),
                    configuration.graphCoordinator()));
        }

                /// Creates the restore policy for this node.
        ///
        /// @return backup restore policy
        private BackupRestorePolicy ensureBackupRestorePolicy() {
            return new BackupRestorePolicy(
                    this.getClusterReplicationTransport(),
                    this.getReplicationPositionProvider(),
                    this::getStoredReplicationCursorManager,
                    this::storageParentPath,
                    this::deleteDirectory,
                    this::closeStoredReplicationCursorManager,
                    this::deleteOffsetFile);
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

        private DataMessageAppliedListener getDataMessageAppliedListener() {
            return this.dataMessageAppliedListener.get();
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
                        manager = this.ensureStoredReplicationCursorManager();
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

        private BackupRestorePolicy getBackupRestorePolicy() {
            return this.backupRestorePolicy.get();
        }

        @Override
        public synchronized ClusterStorageManager<?> startStorageManager() throws NodeLibraryException {
            this.ensureOpen();
            /* This is the generic lifecycle entry used by development nodes and
             * tests as well as production roles. Role-specific public handles
             * enforce their production-mode and role guards below; applying
             * those guards here would make the supported non-production Store
             * lifecycle unreachable. */
            if (this.clusterStorageManager == null) {
                this.start();
            }
            final ClusterStorageManager<?> manager = this.clusterStorageManager;
            if (manager == null) {
                throw new NodeLibraryException("cluster foundation did not produce a storage manager");
            }
            return manager;
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
                if (failure instanceof RuntimeException runtime) throw runtime;
                throw new NodeLibraryException("Cluster node startup failed", failure);
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

            /* A user upload installs a different image, so the node starts from
             * the latest writer position and must publish a starter backup;
             * without an upload the node resumes or restores like a reader. */
            boolean installedUserUpload = false;
            boolean mustPublishStarterBackup = false;

            // don't send messages generated by starting the storage and storing the empty root
            this.getStorageBinaryDataDistributor().ignoreDistribution(true);

            final var backend = this.getStorageBackupBackend();

            // user uploaded a new storage
            if (backend.hasUserUploadedStorage()) {
                LOGGER.log(System.Logger.Level.INFO, "Restoring user uploaded storage");

                installedUserUpload = true;
                // since the storage is now different from before,
                // the storage nodes also need the exact same storage
                mustPublishStarterBackup = true;
                /* A user upload restores with backup metadata unchecked, so it
                 * is validated before the local image is destroyed: a partial,
                 * ambiguous, or over-budget upload must never replace working
                 * storage. The backend re-validates on the archive it extracts,
                 * closing the shared-volume window between this check and the
                 * restore. */
                backend.validateUserUploadedStorage();
                this.deleteDirectory(storageRootPath);
                backend.restoreUserUploadedStorage(storageParentPath);
                backend.deleteUserUploadedStorage();
            } else if (this.getBackupRestorePolicy().restoreLatestBackupIfRequired(storageRootPath, backend)) {
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
            if (!installedUserUpload && isMissingOrEmpty(storageRootPath)) {
                throw new ReseedRequiredException(
                        "backup node has no local Store image at %s, no compatible backup, and no user upload; seed the Store directory with the writer's Store and its replication cursor before starting"
                                .formatted(storageRootPath));
            }
            /* Same lost-cursor gate as replicated readers: existing Store
             * files without their durable offset cursor cannot be resumed
             * safely, because the backup node can no longer address the
             * history those files represent. */
            if (!installedUserUpload && this.usesAeronReplication() && !isMissingOrEmpty(storageRootPath)) {
                this.requireStoredCursorForExistingStore(storageRootPath);
            }

            if (installedUserUpload) {
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
            this.initializeRoot(embeddedStorageManager, installedUserUpload);
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
            this.getBackupNodeManager();

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
            if (mustPublishStarterBackup) {
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

            /* The position provider is initialized before restore selection:
             * both startup paths resolve the backup identity the same way, and
             * an uninitialized provider would silently degrade the identity on
             * every writer start. */
            this.getReplicationPositionProvider().init();

            // don't send messages generated by starting the storage and storing the empty root
            this.getStorageBinaryDataDistributor().ignoreDistribution(true);

            final var backend = this.getStorageBackupBackend();

            final boolean restored = this.getBackupRestorePolicy()
                    .restoreLatestBackupIfRequired(storageRootPath, backend);
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
            this.initializeRoot(embeddedStorageManager, this.mayCreateRoot());
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
            final boolean writer = this.getNodeLibraryPropertiesProvider().nodeRole() == NodeRole.WRITER;
            this.clusterStorageManager = writer
                    ? ClusterStorageManager.create(
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
            this.getStorageNodeManager();

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

                /// Applies the node's Store configuration to the embedded foundation.
        ///
        /// The live file provider is always derived from the configured
        /// storage path so backup and restore can address the Store directory;
        /// every other setting inherited from the supplied foundation is
        /// preserved.
        ///
        /// @param storageRootPath Store root directory
        /// @return prepared embedded foundation
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

                /// Creates the root when the Store has none and this node may own one.
        ///
        /// @param storage     started Store
        /// @param allowCreate whether this node owns an authoritative image
        /// @throws ReseedRequiredException when the node may not create a root
        private void initializeRoot(final StorageManager storage, final boolean allowCreate) {
            if (storage.root() != null) {
                return;
            }
            if (!allowCreate) {
                throw new ReseedRequiredException(
                        "node role '%s' opened a Store without a root; seed the Store directory with its replication cursor before starting"
                                .formatted(this.getNodeLibraryPropertiesProvider().nodeRole().configName()));
            }
            LOGGER.log(System.Logger.Level.DEBUG, "Setting and storing new root from root supplier");
            final Object root = this.getRootSupplier().get();
            storage.setRoot(root instanceof Lazy ? root : Lazy.Reference(root));
            storage.storeRoot();
        }

                /// Reports whether this node may manufacture a fresh Store root.
        ///
        /// Only the writer owns an authoritative image; a backup reader may
        /// create a root only from a user-uploaded Store, which is handled by
        /// the backup startup path. Readers must reproduce the writer's
        /// history from a matching Store+cursor seed.
        ///
        /// @return `true` only for the writer role
        private boolean mayCreateRoot() {
            return this.getNodeLibraryPropertiesProvider().nodeRole() == NodeRole.WRITER;
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
            this.initializeRoot(storage, true);

            this.clusterStorageManager = ClusterStorageManager.create(
                    storage,
                    ClusterStorageManager.StorageSizeValidation.notReached(),
                    this::closeReplicationTransportAndPositionProvider,
                    this.graphCoordinator
            );
        }

                /// Deletes a directory tree after checking that the path is safe to delete.
        ///
        /// @param path root to delete
        private void deleteDirectory(final Path path) {
            if (!Files.exists(path)) {
                return;
            }
            StorageFileOperations.deleteDirectory(path);
        }

        private void deleteOffsetFile() throws NodeLibraryException {
            try {
                AtomicFileWriter.delete(this.storageParentPath().resolve("offset"), true);
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
            final CloseSequencer sequencer = new CloseSequencer();
            if (this.replicationTransport.isInitialized()) {
                sequencer.add("replication transport", true, () -> this.replicationTransport.get().close());
            }
            if (this.positionProvider.isInitialized()) {
                sequencer.add("position provider", true, () -> this.positionProvider.get().close());
            }
            if (this.replicationRetention.isInitialized()) {
                sequencer.add("replication retention", true, () -> this.replicationRetention.get().close());
            }
            sequencer.run("failed to close replication resources");
        }

        private void closeHousekeeperAndReplication(final NodeHousekeeper housekeeper) {
            final CloseSequencer sequencer = new CloseSequencer();
            sequencer.add("housekeeper", true, housekeeper::close);
            sequencer.add("replication resources", true, this::closeReplicationTransportAndPositionProvider);
            sequencer.run("failed to close housekeeper and replication resources");
        }

        /// Stops the node and all resources it created.
        ///
        /// The node monitor protects the lifecycle flags and is taken only
        /// briefly here, so close waits for an in-flight start to finish
        /// (start's whole body is synchronized on the same monitor) while the
        /// long resource waits run outside it — waiting on the monitor
        /// itself would freeze every node API and can deadlock against the
        /// housekeeper termination join. Individual stages may briefly take
        /// the node monitor themselves; each is a flag flip, never a wait.
        @Override
        public void close() {
            synchronized (this) {
                if (this.closed) {
                    return;
                }
                if (this.closing) {
                    throw new IllegalStateException("Cluster foundation is already closing");
                }
                this.closing = true;
            }
            Throwable failure = null;
            try {
                final boolean storageManagerOwnsNodeResources = this.clusterStorageManager != null;
                final boolean storageManagerClosed = this.storageNodeManager.isInitialized();
                final boolean backupManagerClosed = this.backupNodeManager.isInitialized();
                final StorageTaskExecutor backupTaskExecutor = this.storageBackupTaskExecutor.isInitialized()
                        ? this.storageBackupTaskExecutor.get() : null;

                final CloseSequencer sequencer = new CloseSequencer();
                /* A started node manager owns its collaborators: closing it
                 * cascades to the client, distributor, tasks, and health check.
                 * Those collaborators are disposed individually only when their
                 * manager never started — a close after partial construction —
                 * or when the manager does not own them. Every implementation is
                 * idempotent. Roles are fixed, so at most one manager started. */
                sequencer
                        /* Stop new maintenance work before waiting for task
                         * executors or closing anything those tasks use. */
                        .add("housekeeper", this.housekeeper.isInitialized(),
                                () -> this.housekeeper.get().close())
                        .add("backup task executor", this.storageBackupTaskExecutor.isInitialized(),
                                () -> this.storageBackupTaskExecutor.get().close())
                        .add("storage task executor",
                                this.storageTaskExecutor.isInitialized() && this.storageTaskExecutor.get() != backupTaskExecutor,
                                () -> this.storageTaskExecutor.get().close())
                        .add("cluster storage manager", this.clusterStorageManager != null,
                                () -> this.clusterStorageManager.close())
                        /* A startup failure can leave the raw Store started but
                         * unwrapped: root creation, index validation, or the
                         * writer dictionary failed after the Store opened but
                         * before the cluster manager existed. The cluster
                         * manager shuts the Store down when it exists, so this
                         * stage owns the raw manager only in the failure path. */
                        .add("embedded storage", this.clusterStorageManager == null && this.embeddedStorageManager != null,
                                () -> this.embeddedStorageManager.shutdown())
                        .add("storage node manager", storageManagerClosed,
                                () -> this.storageNodeManager.get().close())
                        .add("backup node manager", backupManagerClosed,
                                () -> this.backupNodeManager.get().close())
                        .add("data distributor", !storageManagerClosed && this.dataDistributor.isInitialized(),
                                () -> this.dataDistributor.get().dispose())
                        .add("health check", !storageManagerClosed && this.healthCheck.isInitialized(),
                                () -> this.healthCheck.get().close())
                        .add("data client", !storageManagerClosed && !backupManagerClosed && this.dataClient.isInitialized(),
                                () -> this.dataClient.get().dispose())
                        .add("data merger", this.dataMerger.isInitialized(),
                                () -> this.dataMerger.get().dispose())
                        .add("applied listener", this.dataMessageAppliedListener.isInitialized(),
                                () -> this.dataMessageAppliedListener.get().close())
                        .add("stored cursor manager",
                                !this.dataMessageAppliedListener.isInitialized() && this.storedReplicationCursorManager != null,
                                this::closeStoredReplicationCursorManager)
                        .add("replication resources", !storageManagerOwnsNodeResources,
                                this::closeReplicationTransportAndPositionProvider);
                sequencer.run("Failed to close cluster foundation");
            } catch (final RuntimeException | Error closeFailure) {
                failure = closeFailure;
                throw closeFailure;
            } finally {
                synchronized (this) {
                    if (failure == null) {
                        this.clusterStorageManager = null;
                        this.embeddedStorageManager = null;
                        this.closeFailure = null;
                        this.closed = true;
                    } else {
                        /* Keep resource references so a later close can retry a
                         * stage such as deferred backup-client disposal. Other
                         * APIs remain unavailable while teardown is incomplete. */
                        this.closeFailure = failure;
                    }
                    this.closing = false;
                }
            }
        }

        private void ensureOpen() {
            if (this.closed || this.closing || this.closeFailure != null) {
                throw new IllegalStateException("Cluster foundation is closed");
            }
        }
    }
}
