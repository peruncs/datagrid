package peruncs.cluster.node;

import org.eclipse.serializer.exceptions.MissingFoundationPartException;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.StorageConnection;
import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.api.NodeSettingsSource;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.api.NodeSettingsSource.Env.EnvKeys;
import peruncs.cluster.node.aeron.AeronTransport;
import peruncs.cluster.node.backup.*;
import peruncs.cluster.node.replication.*;
import peruncs.cluster.node.store.*;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.ObjectGraphUpdateHandler;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.ReplicationPublisher;
import peruncs.cluster.storage.binary.StorageBinaryDataMerger;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.function.Supplier;

/// Assembles the services that make one cluster node run.
///
/// A builder creates one immutable node configuration; the resulting node
/// wires its collaborators through {@link NodeCollaborators} and runs their
/// lifecycle through {@link NodeLifecycle}. Each collaborator is created on
/// first use and cached, because the wiring graph is circular: the storage
/// managers, replication client, merger, and health check all reference one
/// another and the Store connection that exists only after startup begins.
///
/// The node is single-use: start it at most once and close it when its work is
/// complete. Configuration cannot be changed after {@link Builder#build()}.
///
/// @since 1.0
public interface NodeAssembly extends AutoCloseable {
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
        private NodeSettingsSource propertiesProvider;

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
        public Builder setNodeSettingsSource(final NodeSettingsSource value) {
            this.propertiesProvider = value;
            return this;
        }

        /// Builds the immutable node assembly.
        ///
        /// @return configured node assembly
        public NodeAssembly build() {
            return new NodeLifecycle(
                    new NodeCollaborators(this.rootSupplier, this.embeddedStorageFoundation, this.propertiesProvider));
        }
    }

    /// Starts the storage manager.
    ///
    /// The returned manager is a borrow: the assembly owns it and shuts it
    /// down on [NodeAssembly#close]. Callers must not shut it down or
    /// close it; shutdown is idempotent, so a stray call stays harmless but
    /// still risks using a closed Store.
    ///
    /// @return storage manager
    /// @throws NodeException if startup fails
    /// @throws ReseedRequiredException if local recovery evidence cannot be reconciled and the node
    ///                                 must be reseeded from a compatible backup or Store image
    ClusterStorageManager<?> startStorageManager() throws NodeException;

    /// Returns the role fixed when this node was built.
    ///
    /// @return configured node role
    NodeRole nodeRole();

    /// Returns the storage node control view, starting the node when necessary.
    ///
    /// This is the programmatic control surface the embedding application
    /// uses in place of a network boundary: role, health, readiness,
    /// storage size, and replication metrics. The view carries no `close()`:
    /// the assembly owns the manager and closes it on [NodeAssembly#close].
    /// The role is validated before anything starts, so probing the wrong
    /// role never starts Store, Aeron, recovery, or background threads.
    ///
    /// @return storage node control view
    /// @throws NodeException if startup fails
    /// @throws IllegalStateException if this node is not a storage node
    StorageNodeControl storageNodeManager() throws NodeException;

    /// Returns the backup node control view, starting the node when necessary.
    ///
    /// This is the programmatic control surface the embedding application
    /// uses in place of a network boundary: backup triggers and reader
    /// pause/resume. The view carries no `close()`: the assembly owns the
    /// manager and closes it on [NodeAssembly#close]. The role is
    /// validated before anything starts, so probing the wrong role never
    /// starts Store, Aeron, recovery, or background threads.
    ///
    /// @return backup node control view
    /// @throws NodeException if startup fails
    /// @throws IllegalStateException if this node is not a backup node
    BackupNodeControl backupNodeManager() throws NodeException;

    /// Closes every resource created by this assembly in reverse dependency order.
    @Override
    void close();
}

/// Wires and caches the collaborators that make one cluster node run.
///
/// This is the pure assembly half of the node: it knows how to construct
/// every collaborator and remembers each one, but it owns no started/closed
/// state — {@link NodeLifecycle} drives startup and teardown. The two owners
/// share the `clusterStorageManager` and `embeddedStorageManager` slots: the
/// lifecycle publishes the running managers there, because several lazy
/// wirings (the replication merger, the task executors, the health check,
/// the writer persistence target) read them at construction or at write time.
///
/// Laziness is deliberate everywhere: the settings source, backup backend,
/// transport, cursor file, and managers form a circular, order-sensitive
/// graph, so eager construction in the constructor would deadlock the wiring
/// or build resources a probing node never needs. The close path relies on
/// the holders' initialization tracking to dispose only what was created.
final class NodeCollaborators {
    /* Logger name deliberately stays on the public assembly type so log
     * configuration keyed to NodeAssembly keeps working after the split. */
    private static final System.Logger LOGGER = System.getLogger(NodeAssembly.class.getName());

    final LazyHolder<StorageBackupBackend> backupBackend;
    final LazyHolder<EmbeddedStorageFoundation<?>> embeddedStorageFoundation;
    final LazyHolder<NodeMaintenanceScheduler> maintenanceScheduler;
    final LazyHolder<StorageLimitGate> storageLimitGate;
    final LazyHolder<BackupNodeManager> backupNodeManager;
    final LazyHolder<ReplicationApplier> dataClient;
    final LazyHolder<ReplicationPublisher> dataDistributor;
    final LazyHolder<StorageNodeHealthCheck> healthCheck;
    final LazyHolder<NodeSettingsSource> propertiesProvider;
    final LazyHolder<StorageTaskExecutor> storageTaskExecutor;
    final LazyHolder<StorageBackupTaskExecutor> storageBackupTaskExecutor;
    final LazyHolder<StorageUsageGauge> storageUsageGauge;
    final LazyHolder<StorageNodeManager> storageNodeManager;
    final LazyHolder<Supplier<Object>> rootSupplier;
    final LazyHolder<ObjectGraphUpdateHandler> graphUpdateHandler;
    final LazyHolder<StorageBackupManager> storageBackupManager;
    final LazyHolder<CommitAppliedListener> commitAppliedListener;
    final LazyHolder<StorageBinaryDataMerger> dataMerger;
    final StorageGraphCoordinator graphCoordinator = new StorageGraphCoordinator();
    /* Intentionally not a LazyHolder: a backup restore closes and replaces
     * this manager, which a one-shot memoized holder cannot express. The
     * volatile field with double-checked locking gives the same safe
     * publication without a per-access lock. */
    volatile DurableCursorFile durableCursorFile;
    final LazyHolder<ClusterReplicationTransport> replicationTransport;
    final LazyHolder<ReplicationPositionProvider> positionProvider;
    final LazyHolder<ReplicationLogRetention> replicationRetention;
    final NodeRole nodeRole;

    /* Both managers are published to monitoring threads; the volatile
     * fields make the cross-thread observation safe even before a
     * synchronized access. The lifecycle owner writes these slots; the
     * assembly reads them while wiring the manager-dependent
     * collaborators. */
    volatile ClusterStorageManager<?> clusterStorageManager;
    /* Keep the raw Store manager only for the internal replication merger.
     * Application code receives the guarded cluster manager, so a reader
     * cannot invoke importData/importFiles as an untracked write. */
    volatile StorageManager embeddedStorageManager;

    NodeCollaborators(final Supplier<Object> configuredRoot,
                      final EmbeddedStorageFoundation<?> configuredFoundation,
                      final NodeSettingsSource configuredProperties) {
        this.backupBackend = LazyHolder.of(this::ensureBackupBackend);
        this.storageTaskExecutor = LazyHolder.of(this::ensureStorageTaskExecutor);
        this.storageBackupTaskExecutor = LazyHolder.of(this::ensureStorageBackupTaskExecutor);
        this.maintenanceScheduler = LazyHolder.of(this::ensureNodeMaintenanceScheduler);
        this.storageLimitGate = LazyHolder.of(this::ensureStorageLimitGate);
        this.replicationTransport = LazyHolder.of(this::ensureClusterReplicationTransport);
        this.dataMerger = LazyHolder.of(this::ensureStorageBinaryDataMerger);
        this.commitAppliedListener = LazyHolder.of(this::ensureCommitAppliedListener);
        this.storageBackupManager = LazyHolder.of(this::ensureStorageBackupManager);
        this.rootSupplier = lazy(configuredRoot, this::ensureRootSupplier);
        this.graphUpdateHandler = LazyHolder.of(this::ensureGraphUpdateHandler);
        this.embeddedStorageFoundation = lazy(configuredFoundation, this::ensureEmbeddedStorageFoundation);
        this.backupNodeManager = LazyHolder.of(this::ensureBackupNodeManager);
        this.dataClient = LazyHolder.of(this::ensureReplicationApplier);
        this.dataDistributor = LazyHolder.of(this::ensureDataDistributor);
        this.healthCheck = LazyHolder.of(this::ensureStorageNodeHealthCheck);
        this.propertiesProvider = lazy(configuredProperties, this::ensureNodeSettingsSource);
        this.storageUsageGauge = LazyHolder.of(this::ensureStorageUsageGauge);
        this.storageNodeManager = LazyHolder.of(this::ensureStorageNodeManager);
        this.positionProvider = LazyHolder.of(this::ensureReplicationPositionProvider);
        this.replicationRetention = LazyHolder.of(this::ensureReplicationLogRetention);
        this.nodeRole = NodeRole.of(this.propertiesProvider.get());
    }

    private static <T> LazyHolder<T> lazy(final T configured, final Supplier<? extends T> factory) {
        return LazyHolder.of(() -> configured == null ? factory.get() : configured);
    }

    /// Memoized holder that remembers whether it was computed.
    ///
    /// `java.lang.LazyConstant` (JDK 27 third preview, JEP 531) deliberately
    /// offers no initialization query — `isInitialized` was removed. The close
    /// path must dispose only resources the node created, without creating
    /// them, so this holder records successful computation around the constant.
    static final class LazyHolder<T> implements Supplier<T> {
        private final LazyConstant<T> constant;
        private volatile boolean initialized;

        private LazyHolder(final Supplier<? extends T> computingFunction) {
            this.constant = LazyConstant.of(computingFunction);
        }

        private static <T> LazyHolder<T> of(final Supplier<? extends T> computingFunction) {
            return new LazyHolder<>(computingFunction);
        }

        @Override
        public T get() {
            final T value = this.constant.get();
            this.initialized = true;
            return value;
        }

        boolean isInitialized() {
            return this.initialized;
        }
    }

    private static Path backupVolumePath(final NodeSettingsSource properties) {
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
    static Duration maintenanceInterval(
            final Integer configured,
            final String property,
            final int fallbackMinutes
    ) {
        if (configured == null) {
            return Duration.ofMinutes(fallbackMinutes);
        }
        if (configured <= 0) {
            throw new NodeException("%s must be configured as a positive integer".formatted(property));
        }
        return Duration.ofMinutes(configured);
    }

    static int requiredPositive(final Integer value, final String property) {
        if (value == null || value <= 0) {
            throw new NodeException("%s must be configured as a positive integer".formatted(property));
        }
        return value;
    }

    /// Reports whether a directory is missing or holds no entries.
    ///
    /// @param directory directory to inspect
    /// @return `true` when the directory does not exist or is empty
    static boolean isMissingOrEmpty(final Path directory) {
        if (!Files.isDirectory(directory)) {
            return true;
        }
        try (var entries = Files.list(directory)) {
            return entries.findAny().isEmpty();
        } catch (final IOException failure) {
            throw new NodeException("Cannot inspect storage directory %s".formatted(directory), failure);
        }
    }

    /// Creates the configured backup backend.
    ///
    /// @return backup backend
    private StorageBackupBackend ensureBackupBackend() {
        return FilesystemVolumeBackupBackend.create(
                backupVolumePath(this.getNodeSettingsSource())
        );
    }

    /// Creates the storage task executor.
    ///
    /// @return storage task executor
    private StorageTaskExecutor ensureStorageTaskExecutor() {
        if (this.nodeRole == NodeRole.BACKUP_READER) {
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

    /// Creates the node maintenance scheduler.
    ///
    /// @return maintenance scheduler
    private NodeMaintenanceScheduler ensureNodeMaintenanceScheduler() {
        return NodeMaintenanceScheduler.create();
    }

    /// Creates the storage limit gate.
    ///
    /// @return limit gate
    private StorageLimitGate ensureStorageLimitGate() {
        return StorageLimitGate.create(
                requiredPositive(
                        this.getNodeSettingsSource().storageLimitGB(),
                        EnvKeys.STORAGE_LIMIT_GB
                )
        );
    }

    /// Creates the Aeron replication transport, or a no-op transport when disabled.
    ///
    /// @return replication transport
    private ClusterReplicationTransport ensureClusterReplicationTransport() {
        final String configured = this.getNodeSettingsSource().replicationTransport();
        final String requested = configured == null ? "none" : configured.trim();
        if ("none".equalsIgnoreCase(requested)) {
            return ClusterReplicationTransport.noOp();
        }
        if ("aeron".equalsIgnoreCase(requested)) {
            return new AeronTransport(this.getNodeSettingsSource());
        }
        throw new NodeException("Replication transport must be 'aeron' or 'none'");
    }

    /// Creates the replication position provider.
    ///
    /// @return position provider
    private ReplicationPositionProvider ensureReplicationPositionProvider() {
        return this.getClusterReplicationTransport().positionProvider(
                this.getNodeSettingsSource().replicationStreamName()
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
    private DurableCursorFile ensureDurableCursorFile() {
        final var cursorPath = this.storageParentPath().resolve("offset");
        LOGGER.log(System.Logger.Level.TRACE, "Creating DurableCursorFile for offset file at %s".formatted(cursorPath));
        return DurableCursorFile.of(cursorPath);
    }

    /// Returns the configured storage root, defaulting to a node-local directory.
    Path storageParentPath() {
        final String configured = this.getNodeSettingsSource()
                .replicationProperty(EnvKeys.STORAGE_PATH);
        return Paths.get(configured == null || configured.isBlank() ? "storage" : configured)
                .toAbsolutePath().normalize();
    }

    /// Creates the listener that persists consumed replication cursors.
    ///
    /// @return consumed-message listener
    private CommitAppliedListener ensureCommitAppliedListener() {
        final boolean persistCursor = this.nodeRole != NodeRole.WRITER;

        final var storedCursorUpdater = new CommitAppliedListener() {
            final DurableCursorFile delegate = NodeCollaborators.this
                    .getDurableCursorFile();

            @Override
            public void onApplied(final ReplicationCursor cursor) throws NodeException {
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
        LOGGER.log(System.Logger.Level.TRACE, "Created CommitAppliedListener->DurableCursorFile delegate. WillRun=%s".formatted(persistCursor));
        return storedCursorUpdater;
    }

    /// Creates the storage backup manager.
    ///
    /// @return storage backup manager
    private StorageBackupManager ensureStorageBackupManager() {
        final var props = this.getNodeSettingsSource();
        final Integer configuredBackupCount = props.keptBackupsCount();
        final int maxBackupCount = requiredPositive(
                configuredBackupCount == null ? 3 : configuredBackupCount,
                EnvKeys.KEPT_BACKUPS_COUNT);

        final Supplier<ReplicationCursor> cursorProvider = this.getReplicationApplier()::cursor;

        return StorageBackupManager.create(
                this.clusterStorageManager,
                maxBackupCount,
                this.getStorageBackupBackend(),
                cursorProvider,
                this.getReplicationApplier(),
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
                this.getReplicationApplier(),
                this.clusterStorageManager,
                this.getStorageUsageGauge(),
                this.getClusterReplicationTransport().id()
        );
    }

    /// Creates the replication data client.
    ///
    /// The merger receives replicated binaries directly; it stays owned
    /// by this assembly, which disposes it on close.
    ///
    /// @return replication data client
    private ReplicationApplier ensureReplicationApplier() {
        final var props = this.getNodeSettingsSource();
        return this.getClusterReplicationTransport().client(
                this.getStorageBinaryDataMerger(),
                props.replicationStreamName(),
                this.getCommitAppliedListener(),
                this.getDurableCursorFile().get()
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
                        this.getReplicationApplier()
                ),
                () -> this.getNodeMaintenanceScheduler().failure() == null
        );
    }

    /// Creates the environment-backed properties provider.
    ///
    /// @return properties provider
    private NodeSettingsSource ensureNodeSettingsSource() {
        return NodeSettingsSource.env();
    }

    /// Creates the storage disk-space reader.
    ///
    /// @return disk-space reader
    private StorageUsageGauge ensureStorageUsageGauge() {
        return StorageUsageGauge.create(
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
        final boolean writer = this.nodeRole == NodeRole.WRITER;
        return StorageNodeManager.create(new StorageNodeManager.Configuration(
                this.getReplicationPublisher(),
                this.getStorageTaskExecutor(),
                this.getReplicationApplier(),
                this.getStorageNodeHealthCheck(),
                this.getStorageUsageGauge(),
                this.getReplicationPositionProvider(),
                transport,
                writer ? StorageNodeManager.Role.WRITER : StorageNodeManager.Role.READER,
                this.graphCoordinator));
    }

    /// Creates the configured replication publisher.
    ///
    /// @return replication publisher
    private ReplicationPublisher ensureDataDistributor() {
        return ReplicationPublisher.Caching(
                this.getClusterReplicationTransport().distributor(
                        this.getNodeSettingsSource().replicationStreamName()
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
            throw new NodeException(
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
        final Long cachingTimeoutMs = this.getNodeSettingsSource().dataMergerTimeoutMs();
        final Long cachedBytesLimit = this.getNodeSettingsSource().dataMergerCachedDataLimit();
        final Long applyTimeoutMs = this.getNodeSettingsSource().dataMergerApplyTimeoutMs();
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
    /// The policy is not cached: a node starts at most once, so the single
    /// startup path that runs restore is also the only user. Constructing it
    /// at that use site keeps the wiring graph smaller without changing
    /// when its dependencies initialize.
    ///
    /// @return a new backup restore policy
    BackupRestorePolicy createBackupRestorePolicy() {
        return new BackupRestorePolicy(
                this.getClusterReplicationTransport(),
                this.getReplicationPositionProvider(),
                this::getDurableCursorFile,
                this::storageParentPath,
                this::deleteDirectory,
                this::closeDurableCursorFile,
                this::deleteOffsetFile);
    }

    /// Reports whether this node may manufacture a fresh Store root.
    ///
    /// Only the writer owns an authoritative image; a backup reader may
    /// create a root only from a user-uploaded Store, which is handled by
    /// the backup startup path. Readers must reproduce the writer's
    /// history from a matching Store+cursor seed.
    ///
    /// @return `true` only for the writer role
    boolean mayCreateRoot() {
        return this.nodeRole == NodeRole.WRITER;
    }

    /// Reports whether this node replicates through the Aeron transport.
    ///
    /// Nodes without replication keep the Store-only seed flow: their Store
    /// image alone is the state. Replicated readers additionally need their
    /// durable offset cursor to address history.
    ///
    /// @return `true` when the configured replication transport is Aeron
    boolean usesAeronReplication() {
        return "aeron".equalsIgnoreCase(this.getNodeSettingsSource().replicationTransport());
    }

    /// Deletes a directory tree after checking that the path is safe to delete.
    ///
    /// @param path root to delete
    void deleteDirectory(final Path path) {
        if (!Files.exists(path)) {
            return;
        }
        AtomicFileWriter.deleteDirectory(path);
    }

    void deleteOffsetFile() throws NodeException {
        try {
            AtomicFileWriter.delete(this.storageParentPath().resolve("offset"), true);
        } catch (final IOException failure) {
            throw new NodeException("Failed to remove stale replication cursor before backup restore", failure);
        }
    }

    synchronized void closeDurableCursorFile() {
        if (this.durableCursorFile == null) {
            return;
        }
        try {
            this.durableCursorFile.close();
        } finally {
            this.durableCursorFile = null;
        }
    }

    StorageBackupBackend getStorageBackupBackend() {
        return this.backupBackend.get();
    }

    private StorageTaskExecutor getStorageTaskExecutor() {
        return this.storageTaskExecutor.get();
    }

    StorageBackupTaskExecutor getStorageBackupTaskExecutor() {
        return this.storageBackupTaskExecutor.get();
    }

    NodeMaintenanceScheduler getNodeMaintenanceScheduler() {
        return this.maintenanceScheduler.get();
    }

    StorageLimitGate getStorageLimitGate() {
        return this.storageLimitGate.get();
    }

    ClusterReplicationTransport getClusterReplicationTransport() {
        return this.replicationTransport.get();
    }

    StorageBackupManager getStorageBackupManager() {
        return this.storageBackupManager.get();
    }

    private ObjectGraphUpdateHandler getObjectGraphUpdateHandler() {
        return this.graphUpdateHandler.get();
    }

    Supplier<Object> getRootSupplier() {
        return this.rootSupplier.get();
    }

    EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation() {
        return this.embeddedStorageFoundation.get();
    }

    BackupNodeManager getBackupNodeManager() {
        return this.backupNodeManager.get();
    }

    ReplicationApplier getReplicationApplier() {
        return this.dataClient.get();
    }

    ReplicationPublisher getReplicationPublisher() {
        return this.dataDistributor.get();
    }

    private StorageNodeHealthCheck getStorageNodeHealthCheck() {
        return this.healthCheck.get();
    }

    NodeSettingsSource getNodeSettingsSource() {
        return this.propertiesProvider.get();
    }

    StorageUsageGauge getStorageUsageGauge() {
        return this.storageUsageGauge.get();
    }

    StorageNodeManager getStorageNodeManager() {
        return this.storageNodeManager.get();
    }

    private CommitAppliedListener getCommitAppliedListener() {
        return this.commitAppliedListener.get();
    }

    private StorageBinaryDataMerger getStorageBinaryDataMerger() {
        return this.dataMerger.get();
    }

    DurableCursorFile getDurableCursorFile() {
        DurableCursorFile manager = this.durableCursorFile;
        if (manager == null) {
            synchronized (this) {
                manager = this.durableCursorFile;
                if (manager == null) {
                    manager = this.ensureDurableCursorFile();
                    this.durableCursorFile = manager;
                }
            }
        }
        return manager;
    }

    ReplicationPositionProvider getReplicationPositionProvider() {
        return this.positionProvider.get();
    }

    private ReplicationLogRetention getReplicationLogRetention() {
        return this.replicationRetention.get();
    }
}
