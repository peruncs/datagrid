package peruncs.cluster.node;

import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.Unpersistable;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.serializer.reference.Swizzling;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.exceptions.StorageException;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.api.*;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReaderWriteRejectedException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.errors.WrongRoleException;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;
import peruncs.cluster.node.backup.BackupNodeManager;
import peruncs.cluster.node.backup.StorageBackupTaskExecutor;
import peruncs.cluster.node.store.ClusterStorageManagers;
import peruncs.cluster.node.store.DistributedStorage;
import peruncs.cluster.node.store.NodeClose;
import peruncs.cluster.node.store.StorageUsageGauge;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.index.ClusterStoreIndexes;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

/// Runs the lifecycle of one assembled cluster node.
///
/// {@link NodeCollaborators} wires and caches every collaborator; this owner
/// decides when they start, in which order, and tears them down in reverse
/// dependency order. It owns the running state: the started/closed flags,
/// the close stages with their per-stage completion, and the close retry
/// semantics that keep resource references after a failed teardown.
///
/// The node is single-use: start it at most once and close it when its work
/// is complete.
///
/// @since 1.0
public final class NodeLifecycle implements AutoCloseable, Unpersistable {
    private static final System.Logger LOGGER = System.getLogger(NodeLifecycle.class.getName());

    /// Creates a builder for one node lifecycle.
    ///
    /// @return an empty builder
    public static Builder create() {
        return new Builder();
    }

    /// Starts a configured node behind the public application entry point.
    ///
    /// @param rootSupplier creates the Store root when needed
    /// @param foundation optional Store tuning
    /// @param settings optional node settings; the process environment is used when `null`
    /// @param <T> root type
    /// @return started node handle
    public static <T> ClusterNode<T> startClusterNode(
            final Supplier<? extends T> rootSupplier,
            final EmbeddedStorageFoundation<?> foundation,
            final NodeConfig config
    ) {
        return ClusterNodeHandle.start(rootSupplier, foundation, config);
    }

    /// Builds a lifecycle from its Store and settings inputs.
    public static final class Builder {
        private Supplier<Object> rootSupplier;
        private EmbeddedStorageFoundation<?> embeddedStorageFoundation;
        private NodeConfig nodeConfig;

        /// Sets the root supplier.
        ///
        /// @param value creates the Store root when needed
        /// @return this builder
        public Builder setRootSupplier(final Supplier<Object> value) {
            this.rootSupplier = value;
            return this;
        }

        /// Sets Store tuning while the node retains ownership of its live file provider.
        ///
        /// @param value Store tuning
        /// @return this builder
        public Builder setEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> value) {
            this.embeddedStorageFoundation = value;
            return this;
        }

        /// Sets the node configuration source.
        ///
        /// @param value node settings
        /// @return this builder
        public Builder setNodeConfig(final NodeConfig value) {
            this.nodeConfig = value;
            return this;
        }

        /// Builds the single-use lifecycle.
        ///
        /// @return configured node lifecycle
        public NodeLifecycle build() {
            return new NodeLifecycle(
                    new NodeCollaborators(this.rootSupplier, this.embeddedStorageFoundation, this.nodeConfig));
        }
    }

    private final NodeCollaborators assembly;
    private volatile boolean started;
    private volatile boolean closed;
    private volatile boolean closing;
    /* The thread that owns the in-flight close attempt; re-entry from it is a
     * no-op, so the facade's shutdown() inside a close cannot recurse. */
    private Thread closer;
    private volatile Throwable closeFailure;
    private volatile WriterProcessLock writerProcessLock;
    /** Set once the replication transport close completed; gates releasing the writer lock. */
    private volatile boolean transportClosed;
    /** Set once the embedded Store shutdown completed; gates releasing the writer lock. */
    private volatile boolean storeClosed;

    /// Adopts a writer process lock acquired by the caller, so that this
    /// lifecycle releases it as the last close stage.
    ///
    /// @param lock the lock to own
    void adoptWriterLock(final WriterProcessLock lock) {
        this.writerProcessLock = lock;
    }
    /* The complete-close trigger installed on the facade: {@link NodeClose}
     * binds the full teardown and the admission probe the facade consults on
     * every persistence entry. */
    private final NodeClose nodeCloseTrigger =
            new NodeClose() {
                @Override
                public boolean close() {
                    return NodeLifecycle.this.closeNode();
                }

                @Override
                public void checkOpen() {
                    NodeLifecycle.this.ensureOpen();
                }

                @Override
                public void awaitAppIdle(final Duration timeout) {
                    final var manager = NodeLifecycle.this.assembly.clusterStorageManager;
                    if (manager != null) ClusterStorageManagers.awaitApplicationSections(manager, timeout);
                }
            };

    NodeLifecycle(final NodeCollaborators assembly) {
        this.assembly = assembly;
    }

    /// Returns the role fixed when the lifecycle was created.
    ///
    /// @return configured node role
    public NodeRole nodeRole() {
        return this.assembly.nodeRole;
    }

    /// Starts storage and returns its application-facing manager.
    ///
    /// @return started manager
    /// @throws NodeException if startup fails
    public synchronized ClusterStorageManager<?> startStorageManager() throws NodeException {
        this.ensureOpen();
        /* This is the generic lifecycle entry used by development nodes and
         * tests as well as production roles. Role-specific public handles
         * enforce their production-mode and role guards below; applying
         * those guards here would make the supported non-production Store
         * lifecycle unreachable. */
        if (this.assembly.clusterStorageManager == null) {
            this.start();
        }
        final ClusterStorageManager<?> manager = this.assembly.clusterStorageManager;
        if (manager == null) {
            throw new NodeException("cluster foundation did not produce a storage manager");
        }
        return manager;
    }

    /// Returns the storage-node controls, starting the node if needed.
    ///
    /// @return storage-node controls
    /// @throws NodeException if startup fails
    public synchronized StorageNodeControl storageNodeManager() throws NodeException {
        this.ensureOpen();
        /* The role managers exist only for production nodes: a dev node
         * starts without the replication collaborators the manager wraps, so
         * manufacturing one here would wrap half-built resources. */
        if (!this.assembly.getNodeConfig().productionMode()) {
            throw new WrongRoleException(
                    "a development node owns no storage node manager; status and checks are production-only");
        }
        if (this.assembly.nodeRole == NodeRole.BACKUP_READER) {
            throw new WrongRoleException(
                    "node role '%s' is not a storage node".formatted(this.assembly.nodeRole.configName()));
        }
        if (this.assembly.clusterStorageManager == null) {
            this.start();
        }

        return this.assembly.getStorageNodeManager();
    }

    /// Returns backup-node controls, starting the node if needed.
    ///
    /// @return backup-node controls
    /// @throws NodeException if startup fails
    public synchronized BackupNodeManager backupNodeManager() throws NodeException {
        this.ensureOpen();
        if (!this.assembly.getNodeConfig().productionMode()) {
            throw new WrongRoleException(
                    "a development node owns no backup node manager; status and checks are production-only");
        }
        if (this.assembly.nodeRole != NodeRole.BACKUP_READER) {
            throw new WrongRoleException(
                    "node role '%s' is not a backup node".formatted(this.assembly.nodeRole.configName()));
        }
        if (this.assembly.clusterStorageManager == null) {
            this.start();
        }

        return this.assembly.backup.nodeManager.get();
    }

    /// Starts the node in its configured role.
    ///
    /// @throws NodeException if startup fails
    /// @throws ReseedRequiredException if local recovery evidence cannot be reconciled and the node
    ///                                 must be reseeded from a compatible backup or Store image
    private synchronized void start() throws NodeException {
        this.ensureOpen();
        if (this.started) {
            throw new IllegalStateException("cluster node has already started");
        }
        this.started = true;
        try {
            /* The role is captured once at assembly time and never
             * re-parsed: a custom settings source cannot split one startup
             * across roles by returning different values later. */
            if (!this.assembly.getNodeConfig().productionMode()) {
                this.startDevNode();
            } else if (this.assembly.nodeRole == NodeRole.BACKUP_READER) {
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
            throw new NodeException("Failed to start node", failure);
        }
    }

    /// Starts a node that restores and serves backups.
    ///
    /// @throws NodeException if startup fails
    private void startBackupNode() throws NodeException {
        LOGGER.log(INFO, "Starting backup cluster node");
        final NodeConfig config = this.assembly.getNodeConfig();

        this.assembly.replication.positionProvider.get().init();

        final var storageParentPath = this.assembly.storageParentPath();
        final var storageRootPath = storageParentPath.resolve("storage");
        /* A crash while a Store replacement was installing must be repaired before anything
         * decides whether a Store exists. */
        AtomicFileWriter.recoverInterruptedReplacement(storageRootPath);

        /* A user upload installs a different image, so the node starts from
         * the latest writer position and must publish a starter backup;
         * without an upload the node resumes or restores like a reader. */
        boolean installedUserUpload = false;
        boolean mustPublishStarterBackup = false;

        // don't send messages generated by starting the storage and storing the empty root
        this.assembly.replication.distributionIgnored.set(true);

        final var backend = this.assembly.backup.backend.get();

        // user uploaded a new storage
        if (backend.hasUserUploadedStorage()) {
            LOGGER.log(INFO, "Restoring user uploaded storage");

            installedUserUpload = true;
            // since the storage is now different from before,
            // the storage nodes also need the exact same storage
            mustPublishStarterBackup = true;
            /* Restore stages and validates a private archive copy before it
             * replaces the existing Store image. */
            backend.restoreUserUploadedStorage(storageParentPath);
        } else if (this.assembly.createBackupRestorePolicy().restoreLatestBackupIfRequired(storageRootPath, () -> backend)) {
            LOGGER.log(INFO, "Restored the newest compatible storage backup");
        } else {
            LOGGER.log(INFO, "Starting with local storage");
        }
        /* A backup node owns no authoritative image of its own. It may
         * manufacture a root only from a user-uploaded Store it then
         * publishes as the starter backup for other nodes; without that
         * upload, without a restored compatible backup, and without local
         * Store files, creating an independent root would diverge the
         * cluster permanently — every later delta references object ids
         * the invented image never contained. Fail closed before opening
         * storage, like a reader without a seed. */
        if (!installedUserUpload && NodeCollaborators.isMissingOrEmpty(storageRootPath)) {
            throw new ReseedRequiredException(
                    "backup node has no local Store image at %s, no compatible backup, and no user upload; seed the Store directory with the writer's Store and replication mark before starting"
                            .formatted(storageRootPath));
        }
        final var embeddedStorageManager = this.prepareEmbeddedStorage(storageRootPath).start();
        this.assembly.embeddedStorageManager = embeddedStorageManager;
        if (this.assembly.hasReplicationMark()) this.requireStoredMark(embeddedStorageManager, storageRootPath);
        this.initializeRoot(embeddedStorageManager, installedUserUpload);
        /* Same one-time policy scan as storage nodes: a seeded or uploaded
         * image with an external index registration is rejected before the
         * backup node serves or publishes anything. */
        this.validateStoreRoots(embeddedStorageManager, config);

        this.assembly.replication.distributionIgnored.set(false);
        this.queueWriterDictionary(embeddedStorageManager);

        final var maintenance = this.assembly.getNodeMaintenanceScheduler();

        /* The manager's shutdown() triggers this lifecycle's complete close,
         * so a DI container or try-with-resources owning the StorageManager
         * tears the whole node down in the sequencer's order. */
        this.assembly.clusterStorageManager = ClusterStorageManagers.readOnly(embeddedStorageManager,
                this.nodeCloseTrigger, this.assembly.graphCoordinator,
                this.assembly.replication.transport.get().replicationMark());

        this.assembly.replication.applier.get().start();
        /* Eagerly create the manager so misconfiguration fails at startup.
         * The assembly owns its lifecycle; embedders borrow it through
         * backupNodeManager(). */
        this.assembly.backup.nodeManager.get();

        this.assembly.getStorageUsageGauge().measureNow();
        this.scheduleStoreMaintenance(maintenance, config, BACKUP_NODE_GC_MINUTES);
        maintenance.schedule(
                "StorageBackup",
                this.assembly.backup.taskExecutor.get().createScheduledWork(),
                config.backup().interval()
        );

        // storage nodes need an initial backup to start from
        if (mustPublishStarterBackup) {
            LOGGER.log(INFO, "Uploading starter backup for storage nodes");
            /* This is a bootstrap barrier. The storage nodes must not observe
                     * uploaded storage until its Store mark and backup are durable. Keep
             * the source upload until that boundary is safely published so a
             * failed bootstrap can retry from the same image. */
            awaitStarterBackup(
                    this.assembly.backup.taskExecutor.get().runBackup(BackupSlot.SCHEDULED),
                    config.backup().closeTimeout().toMillis(), backend::deleteUserUploadedStorage);
        }

        maintenance.start();
    }

    /// Default garbage-collection and cache-check interval of a backup-reader node, in minutes.
    private static final int BACKUP_NODE_GC_MINUTES = 30;
    /// Failed retention runs after which the node reports degraded: an Archive that silently stops
    /// shrinking must not wait for the generic threshold.
    private static final int RETENTION_FAILURE_THRESHOLD = 1;
    /// Default garbage-collection and cache-check interval of a writer or reader node, in minutes.
    private static final int STORAGE_NODE_GC_MINUTES = 60;

    /// Scans the opened Store's roots once against the index policy.
    private void validateStoreRoots(final EmbeddedStorageManager store, final NodeConfig config) {
        ClusterStoreIndexes.validateStorageRoots(
                store, config.limits().maxValidatedIndexObjects(),
                this.assembly.getEmbeddedStorageFoundation().getConnectionFoundation().getTypeHandlerManager());
    }

    /// Schedules the periodic Store garbage collection with its cache check, and the usage gauge.
    ///
    /// @param maintenance        scheduler that runs the tasks
    /// @param config             node configuration, for the configured GC interval
    /// @param defaultGcMinutes   interval used when `PERUNCS_GC_INTERVAL_MINUTES` is not set
    private void scheduleStoreMaintenance(final NodeMaintenanceScheduler maintenance, final NodeConfig config,
                                          final int defaultGcMinutes) {
        final StorageConnection store = this.assembly.clusterStorageManager;
        maintenance.schedule("StoreGcAndCacheCheck", () ->
        {
            LOGGER.log(INFO, "Issuing GC and CC");
            store.issueFullCacheCheck();
            store.issueFullGarbageCollection();
        }, NodeCollaborators.maintenanceInterval(config.storage().gcInterval(), defaultGcMinutes));
        maintenance.schedule("StorageUsageGauge", this.assembly.getStorageUsageGauge()::measureNow,
                StorageUsageGauge.REFRESH_INTERVAL);
    }

    /// Deletes an uploaded Store image only after its starter backup is published.
    ///
    /// @param backup starter-backup result
    /// @param timeoutMillis maximum startup wait
    /// @param deleteUserUpload removes the source upload after success
    /// @throws NodeException when the backup fails, is cancelled, or exceeds its wait budget
    static void awaitStarterBackup(
            final CompletableFuture<?> backup, final long timeoutMillis, final Runnable deleteUserUpload)
            throws NodeException {
        try {
            backup.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException timeout) {
            /* Cancelling only stops waiting for the result; a running backup may still finish. The
             * guarantee that matters is that the uploaded Store is kept for the next start. */
            backup.cancel(false);
            throw new NodeException("Starter backup timed out; uploaded storage was kept", timeout);
        } catch (final CancellationException cancelled) {
            throw new NodeException("Starter backup was cancelled; uploaded storage was kept", cancelled);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new NodeException("Interrupted while creating the starter backup; uploaded storage was kept", interrupted);
        } catch (final ExecutionException failure) {
            final Throwable cause = failure.getCause() == null ? failure : failure.getCause();
            if (cause instanceof Error error) throw error;
            throw new NodeException("Starter backup failed; uploaded storage was kept", cause);
        }
        deleteUserUpload.run();
    }

    /// Starts a standalone Store or a replicated storage node.
    ///
    /// @throws NodeException if startup fails
    private void startStorageNode() throws NodeException {
        LOGGER.log(INFO, "Starting storage cluster node");
        final NodeConfig config = this.assembly.getNodeConfig();

        final var storageParentPath = this.assembly.storageParentPath();
        final var storageRootPath = storageParentPath.resolve("storage");
        /* A crash while a Store replacement was installing must be repaired before anything
         * decides whether a Store exists. */
        AtomicFileWriter.recoverInterruptedReplacement(storageRootPath);
        final boolean writer = this.assembly.nodeRole.canWrite();

        /* The lock protects the Store path, so acquire it before backup
         * selection, Store opening, or the first Aeron runtime startup. */
        if (writer) {
            this.writerProcessLock = WriterProcessLock.acquire(storageRootPath);
        } else {
            this.assembly.replication.positionProvider.get().init();
        }

        // don't send messages generated by starting the storage and storing the empty root
        this.assembly.replication.distributionIgnored.set(true);

        final boolean restored = this.assembly.createBackupRestorePolicy()
                .restoreLatestBackupIfRequired(storageRootPath, this.assembly.backup.backend);
        if (restored) {
            LOGGER.log(INFO, "Restored the newest compatible storage backup");
        } else {
            LOGGER.log(INFO, Files.exists(storageRootPath)
                    ? "Resuming existing local storage and Store mark"
                    : "Starting with local storage");
        }
        /* A reader owns no authoritative image: without a restored backup
         * or existing local Store files it could only manufacture an
         * independent root that later deltas cannot resolve against.
         * Fail fast before opening storage instead of diverging. */
        if (!this.assembly.mayCreateRoot() && NodeCollaborators.isMissingOrEmpty(storageRootPath)) {
            throw new ReseedRequiredException(
                    "node role '%s' has no local Store image or compatible backup at %s; seed the Store directory with its replication mark before starting"
                            .formatted(this.assembly.nodeRole.configName(), storageRootPath));
        }
        final var replicationTransport = this.assembly.replication.transport.get();
        final var embeddedStorageFoundation = this.prepareEmbeddedStorage(storageRootPath);
        DistributedStorage.configureWriting(
                embeddedStorageFoundation,
                this.assembly.replication.outbox,
                replicationTransport.persistenceTargetFactory(
                        this.assembly.replication.outbox,
                        () -> !this.assembly.replication.distributionIgnored.get(),
                        /* The writer's storage connection does not exist
                         * during wiring (root creation runs first); the
                         * supplier is resolved at write time and skips
                         * validation while it is absent. */
                        () -> this.assembly.clusterStorageManager
                )
        );

        final EmbeddedStorageManager embeddedStorageManager;
        try {
            embeddedStorageManager = embeddedStorageFoundation.start();
        } catch (final ReaderWriteRejectedException missingReaderSeed) {
            if (!this.assembly.nodeRole.isWriter() && this.assembly.hasReplicationMark()) {
                throw new ReseedRequiredException(
                        "reader Store has no committed replication mark; restore a compatible backup or seed the Store directory with the writer's Store mark",
                        missingReaderSeed);
            }
            throw missingReaderSeed;
        }
        this.assembly.embeddedStorageManager = embeddedStorageManager;
        if (!this.assembly.nodeRole.isWriter() && this.assembly.hasReplicationMark()) {
            this.requireStoredMark(embeddedStorageManager, storageRootPath);
        }
        this.initializeRoot(embeddedStorageManager, this.assembly.mayCreateRoot());
        /* One-time policy scan at Store start: a freshly deserialized or
         * seeded image containing an external index is rejected before
         * the node serves or publishes anything. */
        this.validateStoreRoots(embeddedStorageManager, config);

        final var limitGate = this.assembly.getStorageLimitGate();
        limitGate.updateUsage(this.assembly.getStorageUsageGauge().measureNow());

        this.assembly.replication.distributionIgnored.set(false);
        this.queueWriterDictionary(embeddedStorageManager);

        final var maintenance = this.assembly.getNodeMaintenanceScheduler();

        /* Reader roles reproduce the writer's history through the internal
         * raw Store import path and must never persist a locally originated
         * write. The application-facing read-only view rejects every write;
         * only the node-owned merger receives the raw manager. */
        /* The manager's shutdown() triggers this lifecycle's complete close —
         * see the backup node path above. */
        this.assembly.clusterStorageManager = writer
                ? ClusterStorageManagers.guarding(
                        embeddedStorageManager,
                        limitGate::limitReached,
                        this.nodeCloseTrigger,
                        this.assembly.graphCoordinator,
                        replicationTransport.replicationMark(),
                        replicationTransport::prepareReplicationCommit,
                        replicationTransport::cancelReplicationCommit)
                : ClusterStorageManagers.readOnly(
                        embeddedStorageManager,
                        this.nodeCloseTrigger,
                        this.assembly.graphCoordinator,
                        replicationTransport.replicationMark());

        if (writer) {
            replicationTransport.ensureWriterMark(this.assembly.clusterStorageManager);
            this.assembly.replication.positionProvider.get().init();
        }

        this.assembly.replication.applier.get().start();

        /* Eagerly create the manager so misconfiguration fails at startup.
         * The assembly owns its lifecycle; embedders borrow it through
         * storageNodeManager(). */
        this.assembly.getStorageNodeManager();

        this.scheduleStoreMaintenance(maintenance, config, STORAGE_NODE_GC_MINUTES);
        maintenance.schedule(
                "StorageLimitChecker",
                limitGate.createScheduledWork(this.assembly.getStorageUsageGauge()::readUsedDiskSpaceBytes),
                Objects.requireNonNull(config.storage().limitCheckInterval(),
                        "PERUNCS_STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES is required")
        );
        if (writer && this.assembly.hasReplicationMark()) {
            maintenance.schedule("PendingReplicationCommit", replicationTransport::retryPendingCommit,
                    replicationTransport.pendingCommitRetryInterval());
        }
        if (writer && this.assembly.hasReplicationMark()) {
            maintenance.schedule("AeronArchiveRetention", () -> {
                try {
                    final var result = replicationTransport.maintainRetention();
                    switch (result.status()) {
                        case DELETED -> LOGGER.log(INFO, "Aeron Archive retention: %s".formatted(result.detail()));
                        /* History is intentionally kept while a reader replays it; an Archive that keeps
                         * growing because of a lagging reader must not stay silent. */
                        case DEFERRED_ACTIVE_REPLAY -> LOGGER.log(System.Logger.Level.WARNING,
                                "Aeron Archive retention deferred: %s".formatted(result.detail()));
                        case NOTHING_TO_DELETE -> LOGGER.log(System.Logger.Level.DEBUG,
                                "Aeron Archive retention: %s".formatted(result.detail()));
                    }
                } catch (final ReplicationPositionUnavailableException unavailable) {
                    LOGGER.log(System.Logger.Level.DEBUG,
                            "Aeron Archive retention waits for the first durable writer position", unavailable);
                }
            }, config.aeron().retentionInterval(), RETENTION_FAILURE_THRESHOLD);
        }

        maintenance.start();
    }

    /// Queues the complete persisted dictionary for the first post-restart
    /// transaction. This covers types introduced by a rejected transaction whose
    /// incremental export was consumed before the writer crashed.
    private void queueWriterDictionary(final EmbeddedStorageManager storage) {
        if (this.assembly.nodeRole != NodeRole.WRITER) {
            return;
        }
        final String dictionary = PersistenceTypeDictionaryAssembler.New().assemble(storage.typeDictionary());
        this.assembly.replication.outbox.stageSnapshot(dictionary);
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
        final var foundation = this.assembly.getEmbeddedStorageFoundation();
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
        this.assembly.replication.transport.get().registerPersistentRoots(foundation);
        foundation.setExceptionHandler((throwable, channel) ->
        {
            try {
                StorageExceptionHandler.defaultHandleException(throwable, channel);
            } catch (final StorageException exception) {
                /* A node is embedded in an application and must not call
                 * System.exit. Log the fatal error and rethrow so the
                 * application supervisor decides on termination. */
                LOGGER.log(ERROR, "Shutting down application due to fatal error", exception);
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
        final Object existing = storage.root();
        if (existing != null) {
            /* The cluster contract stores every root as a Lazy reference.
             * Anything else on disk is an incompatible image: reject it
             * clearly instead of silently migrating or deleting it. */
            if (!(existing instanceof Lazy)) {
                throw new ReseedRequiredException(
                        "node role '%s' opened a Store whose root is not a Lazy reference; reseed from an image written by this node version"
                                .formatted(this.assembly.nodeRole.configName()));
            }
            return;
        }
        if (!allowCreate) {
            throw new ReseedRequiredException(
                    "node role '%s' opened a Store without a root; seed the Store directory with its replication mark before starting"
                            .formatted(this.assembly.nodeRole.configName()));
        }
        LOGGER.log(System.Logger.Level.DEBUG, "Setting and storing new root from root supplier");
        final Object root = this.assembly.getRootSupplier().get();
        storage.setRoot(root instanceof Lazy ? root : Lazy.Reference(root));
        storage.storeRoot();
    }

    /// Fails closed when a replicated reader Store has no durable mark.
    ///
    /// @param storageRootPath Store directory known to hold files
    /// @throws ReseedRequiredException when no committed boundary exists
    private void requireStoredMark(final StorageConnection storage, final Path storageRootPath) {
        final ReplicationMark mark = this.assembly.replication.transport.get().replicationMark();
        final boolean markStored = mark != null && !Swizzling.isNotFoundId(
                storage.persistenceManager().objectRegistry().lookupObjectId(mark));
        if (!markStored || mark.sequence() < 0L || mark.recordingId() < 0L || mark.prepareStartPosition() < 0L) {
            throw new ReseedRequiredException(
                    "node role '%s' has Store files at %s but no committed replication mark; restore a compatible backup or seed the Store directory with the writer's Store mark before starting"
                            .formatted(this.assembly.nodeRole.configName(), storageRootPath));
        }
    }

    /// Starts the local development node.
    ///
    /// @throws NodeException if startup fails
    private void startDevNode() throws NodeException {
        LOGGER.log(INFO, "Starting dev cluster node");
        this.rejectProductionSettingsInDevMode();
        final var storage = this.prepareEmbeddedStorage(this.assembly.storageParentPath().resolve("storage")).start();
        this.assembly.embeddedStorageManager = storage;
        this.initializeRoot(storage, true);

        this.assembly.clusterStorageManager = ClusterStorageManagers.guarding(
                storage,
                () -> false,
                this.nodeCloseTrigger,
                this.assembly.graphCoordinator,
                null,
                ignored -> {
                },
                ignored -> {
                }
        );
    }

    private void rejectProductionSettingsInDevMode() {
        if (this.assembly.getNodeConfig().replicationTransport() != NodeConfig.ReplicationTransport.NONE ||
            this.assembly.hasReplicationMark()) {
            throw new NodeException("replication role or transport requires " +
                    "PERUNCS_PROD_MODE=true");
        }
    }


    /// Stops the node and all resources it created.
    ///
    /// The node monitor protects the lifecycle flags and is taken only
    /// briefly here, so close waits for an in-flight start to finish
    /// (start's whole body is synchronized on the same monitor) while the
    /// long resource waits run outside it — waiting on the monitor
    /// itself would freeze every node API and can deadlock against the
    /// maintenance scheduler termination join. Individual stages may briefly take
    /// the node monitor themselves; each is a flag flip, never a wait.
    ///
    /// Close is idempotent and safe under concurrent callers: re-entry from
    /// the closing thread itself returns, a concurrent caller waits for the
    /// running attempt and then returns (or rethrows that attempt's recorded
    /// failure), and a retry after a failed close re-runs only the stages
    /// that still owe work while preserving the failed attempt's observed
    /// outcome.
    @Override
    public void close() {
        this.closeNode();
    }

    /// Runs the complete node teardown exactly once, sharing it with the
    /// storage facade's shutdown.
    ///
    /// Called by the manager's `shutdown()` trigger and by `ClusterNode.close()`.
    ///
    /// @return `true` when this call performed the teardown, `false` after
    /// observing another caller's successful close
    boolean closeNode() {
        /* A synchronous close from inside an application graph section would
         * join node-owned workers that themselves need the boundary: reject
         * it on BOTH entry points (facade shutdown and ClusterNode.close)
         * before any teardown begins. */
        if (this.assembly.graphCoordinator.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "cannot close the node from inside a graph section; unwind the section first");
        }
        synchronized (this) {
            if (this.closed) {
                return false;
            }
            /* A concurrent close attempt waits and observes its outcome;
             * re-entry from the same thread (for example via the facade's
             * shutdown within a close stage) is a no-op. */
            boolean joinedInFlight = false;
            while (this.closing) {
                if (this.closer == Thread.currentThread()) {
                    return false;
                }
                joinedInFlight = true;
                try {
                    this.wait();
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new NodeException("Interrupted while waiting for node close", interrupted);
                }
            }
            if (this.closed) {
                return false;
            }
            if (joinedInFlight && this.closeFailure != null) {
                /* The attempt this caller joined failed: observe its recorded
                 * failure rather than starting an independent retry. The
                 * failure stays recorded so a later, genuinely new close
                 * retries only the stages still owing work. The cast below
                 * is write-site-driven today; guard it so a future checked
                 * throwable can never turn this into a ClassCastException. */
                if (this.closeFailure instanceof Error error) throw error;
                if (this.closeFailure instanceof RuntimeException runtime) throw runtime;
                throw new IllegalStateException("node close failed", this.closeFailure);
            }
            this.closing = true;
            this.closer = Thread.currentThread();
        }
        Throwable failure = null;
        try {
            final var collaborators = this.assembly;
            final boolean storageManagerClosed = collaborators.storageNodeManager.isInitialized();
            final boolean backupManagerClosed = collaborators.backup.nodeManager.isInitialized();
            final StorageBackupTaskExecutor backupTaskExecutor = collaborators.backup.taskExecutor.isInitialized()
                    ? collaborators.backup.taskExecutor.get() : null;

            final CloseSequencer sequencer = new CloseSequencer();
            final AtomicBoolean appDrained = new AtomicBoolean();
            /* The whole close graph is owned here, in FINAL's dependency
             * order: stop maintenance, bound background work, stop the
             * replication reader/publisher, close the managers and their
             * collaborators, and shut the Store down last. Readiness is
             * evaluated at run time, so a retried close skips completed
             * stages, and the Store stage waits for a still-running backup
             * instead of shutting down underneath its export. */
            sequencer
                    .add(CloseSequencer.stage("application sections",
                            () -> true,
                            () -> {
                                this.nodeCloseTrigger.awaitAppIdle(Duration.ofMillis(
                                        collaborators.getNodeConfig().timeouts().graphDrain().toMillis()));
                                appDrained.set(true);
                            }))
                    /* 1. Stop new maintenance work before anything it uses. */
                    .add(CloseSequencer.stage("maintenance scheduler",
                            collaborators.maintenanceScheduler::isInitialized,
                            () -> collaborators.maintenanceScheduler.get().close()))
                    /* 2. Cancel or boundedly await background backup work. */
                    .add(CloseSequencer.stage("backup task executor",
                            collaborators.backup.taskExecutor::isInitialized,
                            () -> collaborators.backup.taskExecutor.get().close()))
                    .add(CloseSequencer.stage("storage task executor",
                            () -> collaborators.storageTaskExecutor.isInitialized() &&
                                  collaborators.storageTaskExecutor.get() != backupTaskExecutor,
                            () -> collaborators.storageTaskExecutor.get().close()))
                    /* 3. Stop the replication reader/publisher before the
                     * Store: the reader must not deliver into a Store that
                     * is shutting down, and pending imports must finish while
                     * the Store remains open. */
                    .add(CloseSequencer.stage("replication transport",
                            afterAppDrain(appDrained, collaborators.replication.transport::isInitialized),
                            () -> {
                                collaborators.replication.transport.get().close();
                                this.transportClosed = true;
                            }))
                    .add(CloseSequencer.stage("position provider",
                            afterAppDrain(appDrained, collaborators.replication.positionProvider::isInitialized),
                            () -> collaborators.replication.positionProvider.get().close()))
                    .add(CloseSequencer.stage("replication retention",
                            afterAppDrain(appDrained, collaborators.replication.retention::isInitialized),
                            () -> collaborators.replication.retention.get().close()))
                    /* 4. Close the managers. A started node manager owns its
                     * collaborators: closing it cascades to the applier,
                     * publisher, tasks, and health check. Those collaborators
                     * are disposed individually only when their manager never
                     * started — a close after partial construction. Every
                     * implementation is idempotent and tracks per-collaborator
                     * completion, so a retried close finishes the remainder. */
                    .add(CloseSequencer.stage("storage node manager",
                            afterAppDrain(appDrained, () -> storageManagerClosed),
                            () -> collaborators.storageNodeManager.get().close()))
                    .add(CloseSequencer.stage("backup node manager",
                            afterAppDrain(appDrained, () -> backupManagerClosed),
                            () -> collaborators.backup.nodeManager.get().close()))
                    .add(CloseSequencer.stage("health check",
                            afterAppDrain(appDrained, () -> !storageManagerClosed && collaborators.healthCheck.isInitialized()),
                            () -> collaborators.healthCheck.get().close()))
                    .add(CloseSequencer.stage("data client",
                            afterAppDrain(appDrained, () -> !storageManagerClosed && !backupManagerClosed && collaborators.replication.applier.isInitialized()),
                            () -> collaborators.replication.applier.get().dispose()))
                    .add(CloseSequencer.stage("data merger",
                            afterAppDrain(appDrained, collaborators.replication.merger::isInitialized),
                            () -> collaborators.replication.merger.get().dispose()))
                    .add(CloseSequencer.stage("graph drain",
                            appDrained::get,
                            collaborators.graphCoordinator::drain))
                    /* 5. Close the Store last. A backup that outlived its
                     * executor budget must block this stage instead of losing
                     * the race to a shutdown Store: a running backup fails the
                     * stage, and a retry re-runs it instead of skipping
                     * silently — a skipped stage is no proof the Store closed.
                     * The raw embedded manager is shut down here — never
                     * through the facade, whose shutdown() now triggers this
                     * whole close back into this sequencer. A startup failure
                     * may leave the raw Store started but unwrapped: without
                     * the facade this stage owns the raw manager directly. */
                    .add(CloseSequencer.stage("embedded storage",
                            afterAppDrain(appDrained, () -> collaborators.embeddedStorageManager != null),
                            () ->
                            {
                                if (!collaborators.graphCoordinator.isDrained()) {
                                    throw new IllegalStateException(
                                            "Store close deferred: graph sections have not drained");
                                }
                                if (collaborators.maintenanceScheduler.isInitialized() &&
                                    !collaborators.maintenanceScheduler.get().isStopped()) {
                                    throw new IllegalStateException(
                                            "Store close deferred: maintenance workers are still running");
                                }
                                if (backupTaskExecutor != null && backupTaskExecutor.isBackupExecuting()) {
                                    throw new IllegalStateException("Store close deferred: backup still running");
                                }
                                /* Store returns false when the shutdown was
                                 * interrupted mid-drain: that's not success,
                                 * so the close must retry instead of clearing
                                 * references over a partially shut Store. */
                                if (!collaborators.embeddedStorageManager.shutdown()) {
                                    throw new IllegalStateException(
                                            "Store did not complete shutdown; close must be retried");
                                }
                                this.storeClosed = true;
                            }))
                    /* The lock fences a second writer: it is released only once the
                     * transport and the Store are really closed, never after a
                     * failed or deferred earlier stage. */
                    .add(CloseSequencer.stage("writer process lock",
                            () -> this.writerProcessLock != null && appDrained.get()
                                  && (!collaborators.replication.transport.isInitialized() || this.transportClosed)
                                  && (collaborators.embeddedStorageManager == null || this.storeClosed),
                            () -> {
                                this.writerProcessLock.close();
                                this.writerProcessLock = null;
                            }));
            sequencer.run("Failed to close node");
        } catch (final RuntimeException | Error closeFailure) {
            failure = closeFailure;
            throw closeFailure;
        } finally {
            synchronized (this) {
                if (failure == null) {
                    this.assembly.clusterStorageManager = null;
                    this.assembly.embeddedStorageManager = null;
                    this.closeFailure = null;
                    this.closed = true;
                } else {
                    /* Keep resource references so a later close can retry a
                     * stage such as deferred backup-client disposal. Other
                     * APIs remain unavailable while teardown is incomplete.
                     * Waiters from this attempt observe the recorded failure
                     * after the flag clears below. */
                    this.closeFailure = failure;
                }
                this.closing = false;
                this.closer = null;
                this.notifyAll();
            }
        }
        return true;
    }

    private static BooleanSupplier afterAppDrain(
            final AtomicBoolean drained, final BooleanSupplier readiness) {
        return () -> drained.get() && readiness.getAsBoolean();
    }

    private void ensureOpen() {
        if (this.closed || this.closing || this.closeFailure != null) {
            throw new IllegalStateException("cluster node is closed");
        }
    }
}
