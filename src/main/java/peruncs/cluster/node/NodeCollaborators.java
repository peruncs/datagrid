package peruncs.cluster.node;

import org.eclipse.serializer.exceptions.MissingFoundationPartException;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.api.NodeRole;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.backup.*;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.store.StorageLimitGate;
import peruncs.cluster.node.store.StorageNodeHealthCheck;
import peruncs.cluster.node.store.StorageTaskExecutor;
import peruncs.cluster.node.store.StorageUsageGauge;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Supplier;

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
/// Laziness is deliberate everywhere: the configuration, backup backend,
/// transport, Store mark, and managers form a circular, order-sensitive
/// graph, so eager construction in the constructor would deadlock the wiring
/// or build resources a probing node never needs. The close path relies on
/// the holders' initialization tracking to dispose only what was created.
final class NodeCollaborators {
    final LazyHolder<EmbeddedStorageFoundation<?>> embeddedStorageFoundation;
    final LazyHolder<NodeMaintenanceScheduler> maintenanceScheduler;
    final LazyHolder<StorageLimitGate> storageLimitGate;
    final LazyHolder<StorageNodeHealthCheck> healthCheck;
    final NodeConfig nodeConfig;
    final LazyHolder<StorageTaskExecutor> storageTaskExecutor;
    final LazyHolder<StorageUsageGauge> storageUsageGauge;
    final LazyHolder<StorageNodeManager> storageNodeManager;
    final LazyHolder<Supplier<Object>> rootSupplier;
    final StorageGraphCoordinator graphCoordinator;
    final NodeRole nodeRole;
    /// Backup backend, manager, task executor and backup-node manager.
    final BackupCollaborators backup;
    /// Replication transport, position provider, retention, reader client, merger and writer-side state.
    final ReplicationCollaborators replication;

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
                      final NodeConfig configuredConfig) {
        this(configuredRoot, configuredFoundation, configuredConfig, null, null);
    }

    /// Creates the collaborator graph with optional internal provider overrides.
    ///
    /// @param configuredRoot root supplier
    /// @param configuredFoundation Store foundation
    /// @param configuredConfig node configuration
    /// @param configuredBackupBackend backup backend override, or `null` for the filesystem backend
    /// @param configuredReplicationTransport transport override, or `null` for the configured transport
    NodeCollaborators(final Supplier<Object> configuredRoot,
                      final EmbeddedStorageFoundation<?> configuredFoundation,
                      final NodeConfig configuredConfig,
                      final StorageBackupBackend configuredBackupBackend,
                      final ClusterReplicationTransport configuredReplicationTransport) {
        this.storageTaskExecutor = LazyHolder.of(this::ensureStorageTaskExecutor);
        this.maintenanceScheduler = LazyHolder.of(this::ensureNodeMaintenanceScheduler);
        this.storageLimitGate = LazyHolder.of(this::ensureStorageLimitGate);
        this.rootSupplier = lazy(configuredRoot, this::ensureRootSupplier);
        this.embeddedStorageFoundation = lazy(configuredFoundation, this::ensureEmbeddedStorageFoundation);
        this.healthCheck = LazyHolder.of(this::ensureStorageNodeHealthCheck);
        this.nodeConfig = configuredConfig == null ? NodeConfig.fromEnvironment() : configuredConfig;
        this.graphCoordinator = new StorageGraphCoordinator(this.nodeConfig.timeouts().graphDrain().toMillis());
        this.storageUsageGauge = LazyHolder.of(this::ensureStorageUsageGauge);
        this.storageNodeManager = LazyHolder.of(this::ensureStorageNodeManager);
        this.nodeRole = this.nodeConfig.role();
        this.backup = new BackupCollaborators(this, configuredBackupBackend);
        this.replication = new ReplicationCollaborators(this, configuredReplicationTransport);
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
        private boolean initialized;

        private LazyHolder(final Supplier<? extends T> computingFunction) {
            this.constant = LazyConstant.of(computingFunction);
        }

        static <T> LazyHolder<T> of(final Supplier<? extends T> computingFunction) {
            return new LazyHolder<>(computingFunction);
        }

        @Override
        public synchronized T get() {
            final T value = this.constant.get();
            this.initialized = true;
            return value;
        }

        synchronized boolean isInitialized() {
            return this.initialized;
        }
    }

    /// Resolves a maintenance interval with its node default.
    ///
    /// @param configured      configured interval, or `null`
    /// @param fallbackMinutes node default in minutes
    /// @return interval
    static Duration maintenanceInterval(
            final Duration configured,
            final int fallbackMinutes
    ) {
        return configured == null ? Duration.ofMinutes(fallbackMinutes) : configured;
    }

    /// Reports whether a directory is missing or holds no entries.
    ///
    /// @param directory directory to inspect
    /// @return `true` when the directory does not exist or is empty
    static boolean isMissingOrEmpty(final Path directory) {
        return AtomicFileWriter.isMissingOrEmptyStore(directory);
    }

    /// Creates the storage task executor.
    ///
    /// @return storage task executor
    private StorageTaskExecutor ensureStorageTaskExecutor() {
        if (this.nodeRole == NodeRole.BACKUP_READER) {
            return this.backup.taskExecutor.get();
        }
        return StorageTaskExecutor.create(this.clusterStorageManager,
                this.nodeConfig.operations().storageCheckCloseTimeout());
    }

    /// Creates the node maintenance scheduler.
    ///
    /// @return maintenance scheduler
    private NodeMaintenanceScheduler ensureNodeMaintenanceScheduler() {
        return NodeMaintenanceScheduler.create(this.nodeConfig.operations());
    }

    /// Creates the storage limit gate.
    ///
    /// @return limit gate
    private StorageLimitGate ensureStorageLimitGate() {
        final Long limitBytes = this.nodeConfig.storage().limitBytes();
        if (limitBytes == null) throw new NodeException("PERUNCS_STORAGE_LIMIT_GB must be configured");
        return StorageLimitGate.create(limitBytes);
    }

    /// Returns the configured storage root, defaulting to a node-local directory.
    Path storageParentPath() {
        return this.nodeConfig.storage().root().toAbsolutePath().normalize();
    }

    /// Returns the configured root supplier.
    ///
    /// @return root supplier
    private Supplier<Object> ensureRootSupplier() {
        throw new MissingFoundationPartException(Supplier.class, "Missing root supplier");
    }

    /// Creates the embedded storage foundation.
    ///
    /// @return embedded storage foundation
    private EmbeddedStorageFoundation<?> ensureEmbeddedStorageFoundation() {
        return EmbeddedStorageFoundation.New();
    }

    /// Creates the storage node health check.
    ///
    /// @return storage health check
    private StorageNodeHealthCheck ensureStorageNodeHealthCheck() {
        return StorageNodeHealthCheck.create(
                this.clusterStorageManager,
                this.replication.transport.get().health(
                        () -> this.clusterStorageManager.isRunning() && !this.clusterStorageManager.isStartingUp(),
                        this.replication.applier.get()
                ),
                () -> this.getNodeMaintenanceScheduler().failure() == null
        );
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
    /// Roles are fixed at startup: standalone and writer nodes use the writer
    /// manager; readers use the reader manager. There is no role transition.
    ///
    /// @return storage node manager
    private StorageNodeManager ensureStorageNodeManager() {
        final boolean replicationEnabled = this.hasReplicationMark();
        return StorageNodeManager.create(new StorageNodeManager.Configuration(
                this.getStorageTaskExecutor(),
                this.replication.applier.get(),
                this.getStorageNodeHealthCheck(),
                this.getStorageUsageGauge()::readUsedDiskSpaceBytes,
                this.replication.positionProvider.get(),
                replicationEnabled,
                this.nodeRole,
                this.graphCoordinator));
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
                this.replication.transport.get(),
                this.replication.positionProvider.get(),
                new BackupRestorePolicy.RestoreActions(this::storageParentPath, this::deleteDirectory),
                this.nodeRole.canWrite());
    }

    /// Reports whether this node may manufacture a fresh Store root.
    ///
    /// Standalone nodes own their local image. In a replicated cluster only
    /// the writer may create a root; readers require a matching Store-mark
    /// seed, and a backup reader may restore only a user-uploaded Store.
    ///
    /// @return `true` for standalone and writer roles
    boolean mayCreateRoot() {
        return this.nodeRole.canWrite();
    }

    /// Reports whether this node replicates through the Aeron transport.
    ///
    /// Nodes without replication keep the Store-only seed flow: their Store
    /// image alone is the state. Replicated readers additionally need their
    /// durable Store mark to address history.
    ///
    /// @return `true` when the configured replication transport is Aeron
    boolean hasReplicationMark() {
        return this.replication.transport.get().replicationMark() != null;
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

    private StorageTaskExecutor getStorageTaskExecutor() {
        return this.storageTaskExecutor.get();
    }

    NodeMaintenanceScheduler getNodeMaintenanceScheduler() {
        return this.maintenanceScheduler.get();
    }

    StorageLimitGate getStorageLimitGate() {
        return this.storageLimitGate.get();
    }

    Supplier<Object> getRootSupplier() {
        return this.rootSupplier.get();
    }

    EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation() {
        return this.embeddedStorageFoundation.get();
    }

    private StorageNodeHealthCheck getStorageNodeHealthCheck() {
        return this.healthCheck.get();
    }

    NodeConfig getNodeConfig() {
        return this.nodeConfig;
    }

    StorageUsageGauge getStorageUsageGauge() {
        return this.storageUsageGauge.get();
    }

    StorageNodeManager getStorageNodeManager() {
        return this.storageNodeManager.get();
    }

}
