package peruncs.cluster.node;

import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.Unpersistable;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.exceptions.StorageException;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.errors.WrongRoleException;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.api.NodeSettingsSource;
import peruncs.cluster.api.NodeSettingsSource.Env.EnvKeys;
import peruncs.cluster.node.backup.BackupNodeControl;
import peruncs.cluster.node.backup.StorageBackupTaskExecutor;
import peruncs.cluster.node.store.ClusterStorageManagers;
import peruncs.cluster.node.store.DistributedStorage;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.index.ClusterStoreIndexes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

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
final class NodeLifecycle implements NodeAssembly, Unpersistable {
    /* Logger name deliberately stays on the public assembly type so log
     * configuration keyed to NodeAssembly keeps working after the split. */
    private static final System.Logger LOGGER = System.getLogger(NodeAssembly.class.getName());

    private final NodeCollaborators assembly;
    private volatile boolean started;
    private volatile boolean closed;
    private volatile boolean closing;
    /* The thread that owns the in-flight close attempt; re-entry from it is a
     * no-op, so the facade's shutdown() inside a close cannot recurse. */
    private Thread closer;
    private volatile Throwable closeFailure;

    NodeLifecycle(final NodeCollaborators assembly) {
        this.assembly = assembly;
    }

    @Override
    public NodeRole nodeRole() {
        return this.assembly.nodeRole;
    }

    @Override
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

    @Override
    public synchronized StorageNodeControl storageNodeManager() throws NodeException {
        this.ensureOpen();
        /* The role managers exist only for production nodes: a dev node
         * starts without the replication collaborators the manager wraps, so
         * manufacturing one here would wrap half-built resources. */
        if (!this.assembly.getNodeSettingsSource().isProdMode()) {
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

    @Override
    public synchronized BackupNodeControl backupNodeManager() throws NodeException {
        this.ensureOpen();
        if (!this.assembly.getNodeSettingsSource().isProdMode()) {
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

        return this.assembly.getBackupNodeManager();
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
            if (!this.assembly.getNodeSettingsSource().isProdMode()) {
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
            throw new NodeException("Cluster node startup failed", failure);
        }
    }

    /// Starts a node that restores and serves backups.
    ///
    /// @throws NodeException if startup fails
    private void startBackupNode() throws NodeException {
        LOGGER.log(System.Logger.Level.INFO, "Starting backup cluster node");
        final var props = this.assembly.getNodeSettingsSource();

        this.assembly.getReplicationPositionProvider().init();

        final var storageParentPath = this.assembly.storageParentPath();
        final var storageRootPath = storageParentPath.resolve("storage");

        /* A user upload installs a different image, so the node starts from
         * the latest writer position and must publish a starter backup;
         * without an upload the node resumes or restores like a reader. */
        boolean installedUserUpload = false;
        boolean mustPublishStarterBackup = false;

        // don't send messages generated by starting the storage and storing the empty root
        this.assembly.getReplicationPublisher().ignoreDistribution(true);

        final var backend = this.assembly.getStorageBackupBackend();

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
            this.assembly.deleteDirectory(storageRootPath);
            backend.restoreUserUploadedStorage(storageParentPath);
            backend.deleteUserUploadedStorage();
        } else if (this.assembly.createBackupRestorePolicy().restoreLatestBackupIfRequired(storageRootPath, backend)) {
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
        if (!installedUserUpload && NodeCollaborators.isMissingOrEmpty(storageRootPath)) {
            throw new ReseedRequiredException(
                    "backup node has no local Store image at %s, no compatible backup, and no user upload; seed the Store directory with the writer's Store and its replication cursor before starting"
                            .formatted(storageRootPath));
        }
        /* Same lost-cursor gate as replicated readers: existing Store
         * files without their durable offset cursor cannot be resumed
         * safely, because the backup node can no longer address the
         * history those files represent. */
        if (!installedUserUpload && this.assembly.usesAeronReplication() && !NodeCollaborators.isMissingOrEmpty(storageRootPath)) {
            this.requireStoredCursorForExistingStore(storageRootPath);
        }

        if (installedUserUpload) {
            final ReplicationCursor cursor;
            try {
                cursor = this.assembly.getReplicationPositionProvider().latest();
            } catch (final ReplicationPositionUnavailableException failure) {
                throw new NodeException(
                        "Cannot bootstrap uploaded storage: replication transport does not expose a writer latest position",
                        failure);
            }
            LOGGER.log(System.Logger.Level.DEBUG, "Set starting replication cursor to: %s".formatted(cursor));
            this.assembly.getDurableCursorFile().set(cursor);
        }

        final var embeddedStorageManager = this.prepareEmbeddedStorage(storageRootPath).start();
        this.assembly.embeddedStorageManager = embeddedStorageManager;
        this.initializeRoot(embeddedStorageManager, installedUserUpload);
        /* Same one-time policy scan as storage nodes: a seeded or uploaded
         * image with an external index registration is rejected before the
         * backup node serves or publishes anything. */
        ClusterStoreIndexes.validateStorageRoots(embeddedStorageManager);

        this.assembly.getReplicationPublisher().ignoreDistribution(false);
        this.queueWriterDictionary(embeddedStorageManager);

        final var maintenance = this.assembly.getNodeMaintenanceScheduler();

        /* The manager's shutdown() triggers this lifecycle's complete close,
         * so a DI container or try-with-resources owning the StorageManager
         * tears the whole node down in the sequencer's order. */
        this.assembly.clusterStorageManager = ClusterStorageManagers.readOnly(embeddedStorageManager,
                this::closeNode, this.assembly.graphCoordinator);

        this.assembly.getReplicationApplier().start();
        /* Eagerly create the manager so misconfiguration fails at startup.
         * The assembly owns its lifecycle; embedders borrow it through
         * backupNodeManager(). */
        this.assembly.getBackupNodeManager();

        final StorageConnection gcConnection = this.assembly.clusterStorageManager;
        maintenance.schedule("GcWorkaround", () ->
        {
            LOGGER.log(System.Logger.Level.INFO, "Issuing GC and CC");
            gcConnection.issueFullCacheCheck();
            gcConnection.issueFullGarbageCollection();
        }, NodeCollaborators.maintenanceInterval(props.gcIntervalMinutes(), EnvKeys.GC_INTERVAL_MINUTES, 30));
        maintenance.schedule(
                "StorageBackup",
                this.assembly.getStorageBackupTaskExecutor().createScheduledWork(),
                NodeCollaborators.maintenanceInterval(props.backupIntervalMinutes(), EnvKeys.BACKUP_INTERVAL_MINUTES, 120)
        );

        // storage nodes need an initial backup to start from
        if (mustPublishStarterBackup) {
            LOGGER.log(System.Logger.Level.INFO, "Uploading starter backup for storage nodes");
            /* This is a bootstrap barrier.  The storage nodes must not observe the
             * uploaded-storage state until the archive is durable. */
            this.assembly.getStorageBackupManager().createStorageBackup(false);
        }

        maintenance.start();
    }

    /// Starts a node that publishes storage data.
    ///
    /// @throws NodeException if startup fails
    private void startStorageNode() throws NodeException {
        LOGGER.log(System.Logger.Level.INFO, "Starting storage cluster node");
        final var props = this.assembly.getNodeSettingsSource();

        final var storageParentPath = this.assembly.storageParentPath();
        final var storageRootPath = storageParentPath.resolve("storage");

        /* The position provider is initialized before restore selection:
         * both startup paths resolve the backup identity the same way, and
         * an uninitialized provider would silently degrade the identity on
         * every writer start. */
        this.assembly.getReplicationPositionProvider().init();

        // don't send messages generated by starting the storage and storing the empty root
        this.assembly.getReplicationPublisher().ignoreDistribution(true);

        final var backend = this.assembly.getStorageBackupBackend();

        final boolean restored = this.assembly.createBackupRestorePolicy()
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
        if (!restored && !this.assembly.mayCreateRoot() && NodeCollaborators.isMissingOrEmpty(storageRootPath)) {
            throw new ReseedRequiredException(
                    "node role '%s' has no local Store image and no backup seed at %s; restore a compatible backup or seed the Store directory with its replication cursor before starting"
                            .formatted(NodeRole.of(props).configName(), storageRootPath));
        }
        /* Lost-cursor gate, replicated readers only: Store files without
         * their durable offset cursor cannot be resumed safely, because
         * the reader can no longer address the history those files
         * represent. Nodes without replication keep the Store-only seed
         * flow above. */
        if (!restored && !this.assembly.mayCreateRoot() && this.assembly.usesAeronReplication() &&
            !NodeCollaborators.isMissingOrEmpty(storageRootPath)) {
            this.requireStoredCursorForExistingStore(storageRootPath);
        }

        final var dataDistributor = this.assembly.getReplicationPublisher();
        /* Pre-start root gate: the authoritative check in initializeRoot
         * runs after the Store is opened, when rejecting a reader already
         * leaves Store files behind. Re-check immediately before creating
         * the Store so a rejected reader leaves no fresh image. */
        if (!this.assembly.mayCreateRoot() && NodeCollaborators.isMissingOrEmpty(storageRootPath)) {
            throw new ReseedRequiredException(
                    "node role '%s' has no local Store image at %s; restore a compatible backup or seed the Store directory with its replication cursor before starting"
                            .formatted(NodeRole.of(props).configName(), storageRootPath));
        }
        final var embeddedStorageFoundation = this.prepareEmbeddedStorage(storageRootPath);
        DistributedStorage.configureWriting(
                embeddedStorageFoundation,
                dataDistributor,
                this.assembly.getClusterReplicationTransport().persistenceTargetFactory(
                        props.replicationStreamName(),
                        dataDistributor,
                        /* The writer's storage connection does not exist
                         * during wiring (root creation runs first); the
                         * supplier is resolved at write time and skips
                         * validation while it is absent. */
                        () -> this.assembly.clusterStorageManager
                )
        );

        final var embeddedStorageManager = embeddedStorageFoundation.start();
        this.assembly.embeddedStorageManager = embeddedStorageManager;
        this.initializeRoot(embeddedStorageManager, this.assembly.mayCreateRoot());
        /* One-time policy scan at Store start: a freshly deserialized or
         * seeded image containing an external index is rejected before
         * the node serves or publishes anything. */
        ClusterStoreIndexes.validateStorageRoots(embeddedStorageManager);

        this.assembly.getReplicationPublisher().ignoreDistribution(false);
        this.queueWriterDictionary(embeddedStorageManager);

        final var maintenance = this.assembly.getNodeMaintenanceScheduler();
        final var limitGate = this.assembly.getStorageLimitGate();

        /* Reader roles reproduce the writer's history through the internal
         * raw Store import path and must never persist a locally originated
         * write. The application-facing read-only view rejects every write;
         * only the node-owned merger receives the raw manager. */
        final boolean writer = NodeRole.of(props) == NodeRole.WRITER;
        /* The manager's shutdown() triggers this lifecycle's complete close —
         * see the backup node path above. */
        this.assembly.clusterStorageManager = writer
                ? ClusterStorageManagers.guarding(
                        embeddedStorageManager,
                        limitGate::limitReached,
                        this::closeNode,
                        this.assembly.graphCoordinator)
                : ClusterStorageManagers.readOnly(
                        embeddedStorageManager,
                        this::closeNode,
                        this.assembly.graphCoordinator);

        this.assembly.getReplicationApplier().start();

        /* Eagerly create the manager so misconfiguration fails at startup.
         * The assembly owns its lifecycle; embedders borrow it through
         * storageNodeManager(). */
        this.assembly.getStorageNodeManager();

        final StorageConnection gcConnection = this.assembly.clusterStorageManager;
        maintenance.schedule("GcWorkaround", () ->
        {
            LOGGER.log(System.Logger.Level.INFO, "Issuing GC and CC");
            gcConnection.issueFullCacheCheck();
            gcConnection.issueFullGarbageCollection();
        }, NodeCollaborators.maintenanceInterval(props.gcIntervalMinutes(), EnvKeys.GC_INTERVAL_MINUTES, 60));
        maintenance.schedule(
                "StorageLimitChecker",
                limitGate.createScheduledWork(this.assembly.getStorageUsageGauge()),
                Duration.ofMinutes(NodeCollaborators.requiredPositive(
                        props.storageLimitCheckerIntervalMinutes(),
                        EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES
                ))
        );

        maintenance.start();
    }

    /// Queues the complete persisted dictionary for the first post-restart
    /// transaction. This covers types introduced by a rejected transaction whose
    /// incremental export was consumed before the writer crashed.
    private void queueWriterDictionary(final EmbeddedStorageManager storage) {
        final NodeSettingsSource props = this.assembly.getNodeSettingsSource();
        if (NodeRole.of(props) != NodeRole.WRITER) {
            return;
        }
        final String dictionary = PersistenceTypeDictionaryAssembler.New().assemble(storage.typeDictionary());
        this.assembly.getReplicationPublisher().queueTypeDictionaryForNextTransaction(dictionary);
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
                    "node role '%s' opened a Store without a root; seed the Store directory with its replication cursor before starting"
                            .formatted(this.assembly.nodeRole.configName()));
        }
        LOGGER.log(System.Logger.Level.DEBUG, "Setting and storing new root from root supplier");
        final Object root = this.assembly.getRootSupplier().get();
        storage.setRoot(root instanceof Lazy ? root : Lazy.Reference(root));
        storage.storeRoot();
    }

    /// Fails closed when existing Store files lost their durable offset cursor.
    ///
    /// @param storageRootPath Store directory known to hold files
    /// @throws ReseedRequiredException when no usable stored cursor exists
    private void requireStoredCursorForExistingStore(final Path storageRootPath) {
        RuntimeException cursorFailure = null;
        ReplicationCursor stored = null;
        try {
            stored = this.assembly.getDurableCursorFile().get();
        } catch (final RuntimeException failure) {
            cursorFailure = failure;
        }
        if (stored == null || stored.logicalSequence() < 0) {
            throw new ReseedRequiredException(
                    "node role '%s' has Store files at %s but no durable replication cursor; restore a compatible backup or seed the Store directory with its replication cursor before starting"
                            .formatted(this.assembly.nodeRole.configName(), storageRootPath),
                    cursorFailure);
        }
    }

    /// Starts the local development node.
    ///
    /// @throws NodeException if startup fails
    private void startDevNode() throws NodeException {
        LOGGER.log(System.Logger.Level.INFO, "Starting dev cluster node");
        final var storage = this.assembly.getEmbeddedStorageFoundation().start();
        this.assembly.embeddedStorageManager = storage;
        this.initializeRoot(storage, true);

        this.assembly.clusterStorageManager = ClusterStorageManagers.guarding(
                storage,
                peruncs.cluster.node.store.StorageSizeValidation.notReached(),
                this::closeNode,
                this.assembly.graphCoordinator
        );
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
    /// Called by the manager's `shutdown()` trigger and by [ClusterNode#close].
    ///
    /// @return `true` when this call performed the teardown, `false` after
    /// observing another caller's successful close
    boolean closeNode() {
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
                 * retries only the stages still owing work. */
                if (this.closeFailure instanceof Error error) throw error;
                throw (RuntimeException) this.closeFailure;
            }
            this.closing = true;
            this.closer = Thread.currentThread();
        }
        Throwable failure = null;
        try {
            final var collaborators = this.assembly;
            final boolean storageManagerClosed = collaborators.storageNodeManager.isInitialized();
            final boolean backupManagerClosed = collaborators.backupNodeManager.isInitialized();
            final StorageBackupTaskExecutor backupTaskExecutor = collaborators.storageBackupTaskExecutor.isInitialized()
                    ? collaborators.storageBackupTaskExecutor.get() : null;

            final CloseSequencer sequencer = new CloseSequencer();
            /* The whole close graph is owned here, in FINAL's dependency
             * order: stop maintenance, bound background work, stop the
             * replication reader/publisher, close the managers and their
             * collaborators, and shut the Store down last. Readiness is
             * evaluated at run time, so a retried close skips completed
             * stages, and the Store stage waits for a still-running backup
             * instead of shutting down underneath its export. */
            sequencer
                    /* 1. Stop new maintenance work before anything it uses. */
                    .add(CloseSequencer.stage("maintenance scheduler",
                            collaborators.maintenanceScheduler::isInitialized,
                            () -> collaborators.maintenanceScheduler.get().close()))
                    /* 2. Cancel or boundedly await background backup work. */
                    .add(CloseSequencer.stage("backup task executor",
                            collaborators.storageBackupTaskExecutor::isInitialized,
                            () -> collaborators.storageBackupTaskExecutor.get().close()))
                    .add(CloseSequencer.stage("storage task executor",
                            () -> collaborators.storageTaskExecutor.isInitialized() &&
                                  collaborators.storageTaskExecutor.get() != backupTaskExecutor,
                            () -> collaborators.storageTaskExecutor.get().close()))
                    /* 3. Stop the replication reader/publisher before the
                     * Store: the reader must not deliver into a Store that
                     * is shutting down, and the final cursor force needs the
                     * Store still open. */
                    .add(CloseSequencer.stage("replication transport",
                            collaborators.replicationTransport::isInitialized,
                            () -> collaborators.replicationTransport.get().close()))
                    .add(CloseSequencer.stage("position provider",
                            collaborators.positionProvider::isInitialized,
                            () -> collaborators.positionProvider.get().close()))
                    .add(CloseSequencer.stage("replication retention",
                            collaborators.replicationRetention::isInitialized,
                            () -> collaborators.replicationRetention.get().close()))
                    /* 4. Close the managers. A started node manager owns its
                     * collaborators: closing it cascades to the applier,
                     * publisher, tasks, and health check. Those collaborators
                     * are disposed individually only when their manager never
                     * started — a close after partial construction. Every
                     * implementation is idempotent and tracks per-collaborator
                     * completion, so a retried close finishes the remainder. */
                    .add(CloseSequencer.stage("storage node manager",
                            () -> storageManagerClosed,
                            () -> collaborators.storageNodeManager.get().close()))
                    .add(CloseSequencer.stage("backup node manager",
                            () -> backupManagerClosed,
                            () -> collaborators.backupNodeManager.get().close()))
                    .add(CloseSequencer.stage("data distributor",
                            () -> !storageManagerClosed && collaborators.dataDistributor.isInitialized(),
                            () -> collaborators.dataDistributor.get().dispose()))
                    .add(CloseSequencer.stage("health check",
                            () -> !storageManagerClosed && collaborators.healthCheck.isInitialized(),
                            () -> collaborators.healthCheck.get().close()))
                    .add(CloseSequencer.stage("data client",
                            () -> !storageManagerClosed && !backupManagerClosed && collaborators.dataClient.isInitialized(),
                            () -> collaborators.dataClient.get().dispose()))
                    .add(CloseSequencer.stage("data merger",
                            collaborators.dataMerger::isInitialized,
                            () -> collaborators.dataMerger.get().dispose()))
                    .add(CloseSequencer.stage("applied listener",
                            collaborators.commitAppliedListener::isInitialized,
                            () -> collaborators.commitAppliedListener.get().close()))
                    .add(CloseSequencer.stage("stored cursor manager",
                            () -> !collaborators.commitAppliedListener.isInitialized() && collaborators.durableCursorFile != null,
                            collaborators::closeDurableCursorFile))
                    /* 5. Close the Store last. A backup that outlived its
                     * executor budget must block this stage instead of losing
                     * the race to a shutdown Store. The raw embedded manager
                     * is shut down here — never through the facade, whose
                     * shutdown() now triggers this whole close back into this
                     * sequencer. A startup failure may leave the raw Store
                     * started but unwrapped: without the facade this stage
                     * owns the raw manager directly. */
                    .add(CloseSequencer.stage("embedded storage",
                            () -> collaborators.embeddedStorageManager != null && (
                                    backupTaskExecutor == null || !backupTaskExecutor.isRunningBackup()),
                            () -> collaborators.embeddedStorageManager.shutdown()));
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
        return failure == null;
    }

    private void ensureOpen() {
        if (this.closed || this.closing || this.closeFailure != null) {
            throw new IllegalStateException("cluster node is closed");
        }
    }
}
