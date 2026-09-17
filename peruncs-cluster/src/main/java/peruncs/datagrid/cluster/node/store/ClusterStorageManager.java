package peruncs.datagrid.cluster.node.store;

import org.eclipse.serializer.afs.types.AFile;
import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.collections.types.XGettingEnum;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.*;
import org.eclipse.serializer.persistence.types.PersistenceStorer.Creator;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.types.*;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.exceptions.ReaderWriteRejectedException;
import peruncs.datagrid.cluster.node.exceptions.StorageLimitReachedException;
import peruncs.datagrid.cluster.storage.types.RejectingPersistenceTarget;
import peruncs.datagrid.cluster.storage.types.StorageGraphCoordinator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.eclipse.serializer.util.X.notNull;

/// This storage manager adds cluster shutdown and write gating to Store.
///
/// The manager adapts Store's API for application access while it
/// runs the node's shutdown callback, rejects writes past the configured
/// storage limit on a writer, rejects every application write on a reader,
/// and wraps the raw persistence target so a fluent binary write cannot
/// bypass the gate. Store import is deliberately unavailable through this
/// application-facing wrapper; reader roots are available only through a
/// coordinated read closure. Node-owned bootstrap and replication code use
/// their internal storage connection instead.
///
/// @param <T> root type
public interface ClusterStorageManager<T> extends StorageManager {
        /// Creates a manager with size validation and shutdown handling.
    ///
    /// @param <T>                   root type
    /// @param delegate              Store manager
    /// @param storageSizeValidation size validation policy
    /// @param shutdownCallback      shutdown callback
    /// @return cluster storage manager
    static <T> ClusterStorageManager<T> New(
            final StorageManager delegate,
            final StorageSizeValidation storageSizeValidation,
            final ShutdownCallback shutdownCallback
    ) {
        return New(delegate, storageSizeValidation, shutdownCallback, new StorageGraphCoordinator());
    }

        /// Creates a manager sharing the Store graph coordinator with replication.
    ///
    /// @param <T>              root type
    /// @param delegate         Store manager
    /// @param storageSizeValidation size validation policy
    /// @param shutdownCallback shutdown callback
    /// @param graphCoordinator graph coordinator shared with replication
    /// @return cluster storage manager
    static <T> ClusterStorageManager<T> New(
            final StorageManager delegate,
            final StorageSizeValidation storageSizeValidation,
            final ShutdownCallback shutdownCallback,
            final StorageGraphCoordinator graphCoordinator
    ) {
        return new Default<>(notNull(delegate), notNull(storageSizeValidation), notNull(shutdownCallback),
                notNull(graphCoordinator));
    }

        /// Creates a read-only manager for reader roles.
    ///
    /// Reads, maintenance, and restore keep working; every application write
    /// entry point — `store`, `storeAll`, `storeRoot`, `setRoot`, storers,
    /// raw persistence target, and public import methods —
    /// fails with [ReaderWriteRejectedException] so a reader can never
    /// persist an unreplicated local divergence.
    ///
    /// @param <T>              root type
    /// @param delegate         Store manager
    /// @param shutdownCallback shutdown callback
    /// @return read-only cluster storage manager
    static <T> ClusterStorageManager<T> ReadOnly(final StorageManager delegate, final ShutdownCallback shutdownCallback) {
        return ReadOnly(delegate, shutdownCallback, new StorageGraphCoordinator());
    }

        /// Creates a read-only manager sharing the Store graph coordinator.
    ///
    /// @param <T>              root type
    /// @param delegate         Store manager
    /// @param shutdownCallback shutdown callback
    /// @param graphCoordinator graph coordinator shared with replication
    /// @return read-only cluster storage manager
    static <T> ClusterStorageManager<T> ReadOnly(
            final StorageManager delegate,
            final ShutdownCallback shutdownCallback,
            final StorageGraphCoordinator graphCoordinator) {
        return new ReadOnly<>(notNull(delegate), notNull(shutdownCallback), notNull(graphCoordinator));
    }

    @Override
    @SuppressWarnings("unchecked")
    @Deprecated(forRemoval = true)
    Lazy<T> root();

        /// Reads the current root while excluding replication materialization.
    ///
    /// The inherited [#root()] method exposes Store's live lazy reference and
    /// cannot hold a lock across the caller's subsequent object-graph access,
    /// so it throws on readers: application code must use this closure, which
    /// covers the complete traversal with the coordinator's read side. The
    /// action must copy what it needs into an immutable or detached result and
    /// must not return any live graph object. Only a direct root return can be
    /// detected here; callers must also avoid returning nested mutable objects.
    ///
    /// @param <R>    result type
    /// @param action graph read; it receives the materialized root, or `null`
    /// @return action result, never the live graph
    <R> R readRoot(Function<? super T, ? extends R> action);

        /// Returns the graph coordinator guarding this Store.
    ///
    /// Use it to guard traversals that cannot go through [#readRoot], such as
    /// multi-call queries: `coordinator.read(() -> ...)`.
    ///
    /// @return graph coordinator for this Store
    StorageGraphCoordinator graphCoordinator();

    @Override
    ClusterStorageManager<T> start() throws NodeLibraryException;

        /// Runs node-specific work immediately before Store shuts down.
    interface ShutdownCallback {
                /// Creates a callback that does nothing.
        ///
        /// @return no-op callback
        static ShutdownCallback NoOp() {
            return new NoOp();
        }

                /// Runs node-specific shutdown work.
        void onShutdown();

                /// A callback for applications that need no shutdown action.
        final class NoOp implements ShutdownCallback {
            private NoOp() {
            }

            @Override
            public void onShutdown() {
                // no-op
            }
        }
    }

        /// Reports whether the configured storage limit has been reached.
    interface StorageSizeValidation {
                /// Reports whether another Store write must be rejected.
        ///
        /// @return `true` when the limit is reached
        boolean isStorageLimitReached();
    }


        /// Adds the write gate, the raw-target gate, and shutdown handling to Store.
    ///
    /// @param <T> root type
    class Default<T> implements ClusterStorageManager<T> {
        private static final System.Logger LOGGER = System.getLogger(Default.class.getName());
        private final StorageSizeValidation storageSizeValidation;
        private final StorageManager delegate;
        private final ShutdownCallback shutdownCallback;
        private final StorageGraphCoordinator graphCoordinator;
        private boolean callbackCompleted;
        private boolean storeShutdownCompleted;

        private Default(
                final StorageManager delegate,
                final StorageSizeValidation storageSizeValidation,
                final ShutdownCallback shutdownCallback,
                final StorageGraphCoordinator graphCoordinator
        ) {
            this.delegate = delegate;
            this.storageSizeValidation = storageSizeValidation;
            this.shutdownCallback = shutdownCallback;
            this.graphCoordinator = graphCoordinator;
        }

        /* The limit gates only the write entry points (store, storeAll,
         * storeRoot, and Storer.commit). Reads, maintenance, registration,
         * and restore must keep working on a full disk so the node can
         * drain, back up, or recover instead of failing every operation. */
        void validateState() throws StorageLimitReachedException {
            if (this.storageSizeValidation.isStorageLimitReached()) {
                throw new StorageLimitReachedException(
                        "Can not store more objects in storage as the storage limit has been reached"
                );
            }
        }

        void rejectApplicationImport() {
            throw new UnsupportedOperationException(
                    "Store imports are reserved for the node-owned replication and bootstrap paths");
        }

                /// Wraps the raw persistence target with this manager's write gate.
        ///
        /// @param raw unwrapped target
        /// @return gated target
        PersistenceTarget<Binary> gateTarget(final PersistenceTarget<Binary> raw) {
            return new GatedPersistenceTarget(raw, this::validateState);
        }

        @Override
        public void checkAcceptingTasks() {
            this.delegate.checkAcceptingTasks();
        }

        @Override
        public StorageConfiguration configuration() {
            return this.delegate.configuration();
        }

        @Override
        public StorageConnection createConnection() {
            /* A raw delegate connection would bypass this manager's write
             * gates. The cluster manager is itself a valid StorageConnection;
             * returning it keeps all connection-scoped calls on the guarded
             * boundary. Replication and bootstrap imports intentionally use
             * the node-owned embedded connection, never this application
             * facade. */
            return this;
        }

        @Override
        public StorageRawFileStatistics createStorageStatistics() {
            return this.delegate.createStorageStatistics();
        }

        @Override
        public Database database() {
            return this.delegate.database();
        }

        @Override
        public void exportChannels(final StorageLiveFileProvider fileProvider, final boolean performGarbageCollection) {
            this.delegate.exportChannels(fileProvider, performGarbageCollection);
        }

        @Override
        public StorageEntityTypeExportStatistics exportTypes(
                final StorageEntityTypeExportFileProvider exportFileProvider,
                final Predicate<? super StorageEntityTypeHandler> isExportType
        ) {
            return this.delegate.exportTypes(exportFileProvider, isExportType);
        }

        @Override
        public void importData(final XGettingEnum<ByteBuffer> importData) {
            this.rejectApplicationImport();
        }

        @Override
        public void importFiles(final XGettingEnum<AFile> importFiles) {
            this.rejectApplicationImport();
        }

        @Override
        public long initializationTime() {
            return this.delegate.initializationTime();
        }

        @Override
        public boolean isAcceptingTasks() {
            return this.delegate.isAcceptingTasks();
        }

        @Override
        public boolean isActive() {
            return this.delegate.isActive();
        }

        @Override
        public boolean isRunning() {
            return this.delegate.isRunning();
        }

        @Override
        public boolean isShuttingDown() {
            return this.delegate.isShuttingDown();
        }

        @Override
        public boolean isStartingUp() {
            return this.delegate.isStartingUp();
        }

        @Override
        public boolean issueCacheCheck(final long nanoTimeBudget, final StorageEntityCacheEvaluator entityEvaluator) {
            return this.delegate.issueCacheCheck(nanoTimeBudget, entityEvaluator);
        }

        @Override
        public boolean issueFileCheck(final long nanoTimeBudget) {
            return this.delegate.issueFileCheck(nanoTimeBudget);
        }

        @Override
        public void issueFullBackup(
                final StorageLiveFileProvider targetFileProvider,
                final PersistenceTypeDictionaryExporter typeDictionaryExporter
        ) {
            this.delegate.issueFullBackup(targetFileProvider, typeDictionaryExporter);
        }

        @Override
        public boolean issueGarbageCollection(final long nanoTimeBudget) {
            return this.delegate.issueGarbageCollection(nanoTimeBudget);
        }

        @Override
        public void issueTransactionsLogCleanup() {
            this.delegate.issueTransactionsLogCleanup();
        }

        @Override
        public boolean issueStorageFlush() {
            return this.delegate.issueStorageFlush();
        }

        @Override
        public StorageIntegrityCheckResult issueIntegrityCheck(final long nanoTimeBudget) {
            return this.delegate.issueIntegrityCheck(nanoTimeBudget);
        }

        @Override
        public long operationModeTime() {
            return this.delegate.operationModeTime();
        }

        @Override
        public PersistenceManager<Binary> persistenceManager() {
            return new BinaryPersistenceManagerAdapter(this.delegate.persistenceManager());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object setRoot(final Object newRoot) {
            this.validateState();
            return this.delegate.setRoot(newRoot);
        }

        @Override
        public synchronized boolean shutdown() {
            if (this.callbackCompleted && this.storeShutdownCompleted) {
                return false;
            }
            LOGGER.log(System.Logger.Level.INFO, "Shutting down ClusterStorageManager");
            Throwable failure = null;
            boolean result = false;
            if (!this.callbackCompleted) {
                try {
                    this.shutdownCallback.onShutdown();
                    this.callbackCompleted = true;
                } catch (final Throwable callbackFailure) {
                    failure = callbackFailure;
                }
            }
            if (!this.storeShutdownCompleted) {
                try {
                    result = this.delegate.shutdown();
                    this.storeShutdownCompleted = true;
                } catch (final Throwable shutdownFailure) {
                    if (failure == null) failure = shutdownFailure;
                    else if (failure != shutdownFailure) failure.addSuppressed(shutdownFailure);
                }
            }
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException runtime) throw runtime;
            if (failure != null) throw new IllegalStateException("failed to shut down cluster storage", failure);
            return result;
        }

        @Override
        public ClusterStorageManager<T> start() {
            this.delegate.start();
            return this;
        }

        @Override
        public long store(final Object instance) {
            this.validateState();
            return this.delegate.store(instance);
        }

        @Override
        public long[] storeAll(final Object... instances) {
            this.validateState();
            return this.delegate.storeAll(instances);
        }

        @Override
        public void storeAll(final Iterable<?> instances) {
            this.validateState();
            this.delegate.storeAll(instances);
        }

        @Override
        public long storeRoot() {
            this.validateState();
            return this.delegate.storeRoot();
        }

        @Override
        public StorageTypeDictionary typeDictionary() {
            return this.delegate.typeDictionary();
        }

        @Override
        public PersistenceRootsView viewRoots() {
            return this.delegate.viewRoots();
        }

        @Override
        public Storer createEagerStorer() {
            return new ClusterStorerAdapter(this.delegate.createEagerStorer());
        }

        @Override
        public Storer createLazyStorer() {
            return new ClusterStorerAdapter(this.delegate.createLazyStorer());
        }

        @Override
        public Storer createStorer() {
            return new ClusterStorerAdapter(this.delegate.createStorer());
        }

        @Override
        public void accessUsageMarks(final Consumer<? super XGettingEnum<Object>> logic) {
            this.delegate.accessUsageMarks(logic);
        }

        @Override
        public boolean isUsed() {
            return this.delegate.isUsed();
        }

        @Override
        public int markUnused() {
            return this.delegate.markUnused();
        }

        @Override
        public int markUsedFor(final Object instance) {
            return this.delegate.markUsedFor(instance);
        }

        @Override
        public int unmarkUsedFor(final Object instance) {
            return this.delegate.unmarkUsedFor(instance);
        }

        @Override
        public Lazy<T> root() {
            /* Writers own their image and mutate it through store(); returning
             * the live reference preserves the Store write flow. Readers
             * override this to throw: a live reference would escape the
             * coordinator read lock and observe a half-materialized batch. */
            return this.delegate.root();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R readRoot(final Function<? super T, ? extends R> action) {
            notNull(action);
            return this.graphCoordinator.read(() -> {
                final Object raw = this.delegate.root();
                final T value = raw instanceof Lazy<?> lazy ? (T) lazy.get() : (T) raw;
                final R result = action.apply(value);
                /* Even readRoot(root -> root) would hand a mutable graph object
                 * to traversal after the lock is released. Reject direct
                 * escapes; callers must copy what they need inside the closure. */
                if (result != null && (result == value || result == raw)) {
                    throw new IllegalStateException(
                            "readRoot action must not return the live root; copy the needed state inside the closure");
                }
                return result;
            });
        }

        @Override
        public StorageGraphCoordinator graphCoordinator() {
            return this.graphCoordinator;
        }

        @Override
        public List<StorageAdjacencyDataExporter.AdjacencyFiles> exportAdjacencyData(final Path workingDir) {
            return this.delegate.exportAdjacencyData(workingDir);
        }

                /// Adapts the cluster manager to Store's binary persistence manager.
        private final class BinaryPersistenceManagerAdapter implements PersistenceManager<Binary> {
            private final PersistenceManager<Binary> delegate;

            private BinaryPersistenceManagerAdapter(final PersistenceManager<Binary> delegate) {
                this.delegate = delegate;
            }

            @Override
            public long ensureObjectId(final Object object) {
                return this.delegate.ensureObjectId(object);
            }

            @Override
            public <U> long ensureObjectId(
                    final U object,
                    final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
                    final PersistenceTypeHandler<Binary, U> optionalHandler
            ) {
                return this.delegate.ensureObjectId(object, objectIdRequestor, optionalHandler);
            }

            @Override
            public <U> long ensureObjectIdGuaranteedRegister(
                    final U object,
                    final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
                    final PersistenceTypeHandler<Binary, U> optionalHandler
            ) {
                return this.delegate.ensureObjectIdGuaranteedRegister(object, objectIdRequestor, optionalHandler);
            }

            @Override
            public void consolidate() {
                this.delegate.consolidate();
            }

            @Override
            public boolean registerLocalRegistry(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
                return this.delegate.registerLocalRegistry(localRegistry);
            }

            @Override
            public void mergeEntries(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
                this.delegate.mergeEntries(localRegistry);
            }

            @Override
            public long lookupObjectId(final Object object) {
                return this.delegate.lookupObjectId(object);
            }

            @Override
            public Object lookupObject(final long objectId) {
                return this.delegate.lookupObject(objectId);
            }

            @Override
            public Object get() {
                return this.delegate.get();
            }

            @Override
            public Object getObject(final long objectId) {
                return this.delegate.getObject(objectId);
            }

            @Override
            public <C extends Consumer<Object>> C collect(final C collector, final long... objectIds) {
                return this.delegate.collect(collector, objectIds);
            }

            @Override
            public <C extends Consumer<Object>> C collect(final C collector, final Set_long objectIds) {
                return this.delegate.collect(collector, objectIds);
            }

            @Override
            public long store(final Object instance) {
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.store(instance);
            }

            @Override
            public long[] storeAll(final Object... instances) {
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.storeAll(instances);
            }

            @Override
            public void storeAll(final Iterable<?> instances) {
                ClusterStorageManager.Default.this.validateState();
                this.delegate.storeAll(instances);
            }

            @Override
            public ByteOrder getTargetByteOrder() {
                return this.delegate.getTargetByteOrder();
            }

            @Override
            public PersistenceStorer createLazyStorer() {
                return new ClusterPersistenceStorerAdapter(this.delegate.createLazyStorer());
            }

            @Override
            public PersistenceStorer createStorer() {
                return new ClusterPersistenceStorerAdapter(this.delegate.createStorer());
            }

            @Override
            public PersistenceStorer createEagerStorer() {
                return new ClusterPersistenceStorerAdapter(this.delegate.createEagerStorer());
            }

            @Override
            public PersistenceStorer createStorer(final Creator<Binary> storerCreator) {
                return new ClusterPersistenceStorerAdapter(this.delegate.createStorer(storerCreator));
            }

            @Override
            public PersistenceLoader createLoader() {
                return this.delegate.createLoader();
            }

            @Override
            public PersistenceRegisterer createRegisterer() {
                return new ClusterPersistenceRegistererAdapter(this.delegate.createRegisterer());
            }

            @Override
            public void updateMetadata(
                    final PersistenceTypeDictionary typeDictionary,
                    final long highestTypeId,
                    final long highestObjectId
            ) {
                ClusterStorageManager.Default.this.validateState();
                this.delegate.updateMetadata(typeDictionary, highestTypeId, highestObjectId);
            }

            @Override
            public PersistenceObjectRegistry objectRegistry() {
                return this.delegate.objectRegistry();
            }

            @Override
            public Object objectRegistryMonitor() {
                return this.delegate.objectRegistryMonitor();
            }

            @Override
            public PersistenceTypeDictionary typeDictionary() {
                return this.delegate.typeDictionary();
            }

            @Override
            public PersistenceRootsView viewRoots() {
                return ClusterStorageManager.Default.this.viewRoots();
            }

            @Override
            public long currentObjectId() {
                return this.delegate.currentObjectId();
            }

            @Override
            public PersistenceManager<Binary> updateCurrentObjectId(final long currentObjectId) {
                ClusterStorageManager.Default.this.validateState();
                this.delegate.updateCurrentObjectId(currentObjectId);
                return this;
            }

            @Override
            public PersistenceSource<Binary> source() {
                return this.delegate.source();
            }

            @Override
            public PersistenceTarget<Binary> target() {
                return ClusterStorageManager.Default.this.gateTarget(this.delegate.target());
            }

            @Override
            public void close() {
                this.delegate.close();
            }
        }

                /// Registers binary types through the cluster manager boundary.
        private record ClusterPersistenceRegistererAdapter(PersistenceRegisterer delegate)
                implements PersistenceRegisterer {
            @Override
            public <U> long apply(final U instance) {
                return this.delegate.apply(instance);
            }

            @Override
            public long register(final Object instance) {
                return this.delegate.register(instance);
            }

            @Override
            public long[] registerAll(final Object... instances) {
                return this.delegate.registerAll(instances);
            }
        }

                /// Stores binary entities while applying the cluster's size rules.
        private final class ClusterPersistenceStorerAdapter extends ClusterStorerAdapter implements PersistenceStorer {
            private final PersistenceStorer delegate;

            private ClusterPersistenceStorerAdapter(final PersistenceStorer delegate) {
                super(delegate);
                this.delegate = delegate;
            }

            @Override
            public PersistenceStorer reinitialize() {
                this.delegate.reinitialize();
                return this;
            }

            @Override
            public PersistenceStorer reinitialize(final long initialCapacity) {
                this.delegate.reinitialize(initialCapacity);
                return this;
            }

            @Override
            public PersistenceStorer ensureCapacity(final long desiredCapacity) {
                this.delegate.ensureCapacity(desiredCapacity);
                return this;
            }
        }

                /// Delegates Store storer operations while preserving cluster checks.
        private class ClusterStorerAdapter implements Storer {
            private final Storer storer;

            private ClusterStorerAdapter(final Storer storer) {
                this.storer = storer;
            }

            @Override
            public long store(final Object instance) {
                return this.storer.store(instance);
            }

            @Override
            public long store(final Object instance, final long objectId) {
                return this.storer.store(instance, objectId);
            }

            @Override
            public long[] storeAll(final Object... instances) {
                return this.storer.storeAll(instances);
            }

            @Override
            public void storeAll(final Iterable<?> instances) {
                this.storer.storeAll(instances);
            }

            @Override
            public Object commit() {
                Default.this.validateState();
                return this.storer.commit();
            }

            @Override
            public void clear() {
                this.storer.clear();
            }

            @Override
            public boolean skipMapped(final Object instance, final long objectId) {
                return this.storer.skipMapped(instance, objectId);
            }

            @Override
            public boolean skip(final Object instance) {
                return this.storer.skip(instance);
            }

            @Override
            public boolean skipNulled(final Object instance) {
                return this.storer.skipNulled(instance);
            }

            @Override
            public long size() {
                return this.storer.size();
            }

            @Override
            public long currentCapacity() {
                return this.storer.currentCapacity();
            }

            @Override
            public long maximumCapacity() {
                return this.storer.maximumCapacity();
            }

            @Override
            public Storer reinitialize() {
                this.storer.reinitialize();
                return this;
            }

            @Override
            public Storer reinitialize(final long initialCapacity) {
                this.storer.reinitialize(initialCapacity);
                return this;
            }

            @Override
            public Storer ensureCapacity(final long desiredCapacity) {
                this.storer.ensureCapacity(desiredCapacity);
                return this;
            }

            @Override
            public void registerCommitListener(final PersistenceCommitListener listener) {
                this.storer.registerCommitListener(listener);
            }

            @Override
            public boolean isEmpty() {
                return this.storer.isEmpty();
            }

            @Override
            public void registerRegistrationListener(final PersistenceObjectRegistrationListener listener) {
                this.storer.registerRegistrationListener(listener);
            }
        }
    }

        /// Rejects application writes while keeping reads, maintenance, and
    /// restore working. Replication uses the unwrapped Store connection owned
    /// by the node; exposing import through this application-facing view would
    /// let a reader manufacture an unreplicated local image.
    ///
    /// The write gate always throws [ReaderWriteRejectedException], which
    /// flows through the inherited `store`, `storeAll`, `storeRoot`,
    /// `setRoot`, storer `commit`, raw-target `write`, and import paths.
    ///
    /// Object-ID assignment (`createRegisterer`, `ensureObjectId*`,
    /// `lookupObjectId`) is deliberately ungated: it only hands out in-memory
    /// identifiers from the local registry and persists nothing by itself. A
    /// divergent Store image can only be persisted through a gated write
    /// entry point, so ID assignment stays read-safe while every durable
    /// mutation is rejected.
    ///
    /// @param <T> root type
    final class ReadOnly<T> extends Default<T> {
        private ReadOnly(final StorageManager delegate, final ShutdownCallback shutdownCallback,
                         final StorageGraphCoordinator graphCoordinator) {
            super(delegate, () -> false, shutdownCallback, graphCoordinator);
        }

        @Override
        void validateState() {
            throw new ReaderWriteRejectedException(
                    "node role is read-only; application writes are rejected because they would diverge from the writer");
        }

        @Override
        void rejectApplicationImport() {
            throw new ReaderWriteRejectedException(
                    "node role is read-only; application imports are reserved for the node-owned paths");
        }

        @Override
        PersistenceTarget<Binary> gateTarget(final PersistenceTarget<Binary> raw) {
            return RejectingPersistenceTarget.New(raw);
        }

        @Override
        @Deprecated(forRemoval = true)
        public Lazy<T> root() {
            /* The merger applies batches on the coordinator write side; a live
             * reference returned here would be traversed after the read lock is
             * gone. Application readers must use readRoot(...) or
             * graphCoordinator().read(...). */
            throw new UnsupportedOperationException(
                    "use readRoot(...) for a coherent graph read; root() cannot retain the coordinator read lock");
        }

        @Override
        public PersistenceRootsView viewRoots() {
            throw new UnsupportedOperationException(
                    "use readRoot(...) for a coherent graph read; viewRoots() exposes live roots");
        }

    }

        /// Validates the write gate before every raw-target write.
    ///
    /// The persistence manager's raw target otherwise bypasses the storer
    /// `commit` gate, so a fluent binary write would escape storage-limit
    /// enforcement on writers and application-write rejection on readers.
    final class GatedPersistenceTarget implements PersistenceTarget<Binary> {
        private final PersistenceTarget<Binary> delegate;
        private final Runnable writeGate;

        private GatedPersistenceTarget(final PersistenceTarget<Binary> delegate, final Runnable writeGate) {
            this.delegate = delegate;
            this.writeGate = writeGate;
        }

        @Override
        public boolean isWritable() {
            return this.delegate.isWritable();
        }

        @Override
        public void write(final Binary data) {
            this.writeGate.run();
            this.delegate.write(data);
        }

        @Override
        public void prepareTarget() {
            this.delegate.prepareTarget();
        }

        @Override
        public void closeTarget() {
            this.delegate.closeTarget();
        }
    }
}
