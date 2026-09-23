package peruncs.datagrid.cluster.node.store;

import org.eclipse.serializer.afs.types.AFile;
import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.collections.types.XGettingEnum;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.*;
import org.eclipse.serializer.persistence.types.PersistenceStorer.Creator;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.types.*;
import peruncs.datagrid.cluster.errors.StorageLimitReachedException;
import peruncs.datagrid.cluster.storage.StorageGraphCoordinator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.eclipse.serializer.util.X.notNull;

/// Store facade with the write gate, the raw-target gate, and coordinated reads.
///
/// [start] and [shutdown] delegate to the owned [ClusterStoreLifecycle];
/// every other method is a Store operation, forwarded as-is, gated on the
/// configured size limit when it persists application data, or wrapped so a
/// fluent write cannot bypass the gate.
///
/// @param <T> root type
class GuardingStorageManager<T> implements ClusterStorageManager<T> {
    private final StorageSizeValidation storageSizeValidation;
    private final StorageManager delegate;
    private final ClusterStoreLifecycle lifecycle;
    private final StorageGraphCoordinator graphCoordinator;
    private final LazyConstant<PersistenceManager<Binary>> persistenceManager;

    GuardingStorageManager(
            final StorageManager delegate,
            final StorageSizeValidation storageSizeValidation,
            final ShutdownCallback shutdownCallback,
            final StorageGraphCoordinator graphCoordinator
    ) {
        this.delegate = delegate;
        this.storageSizeValidation = storageSizeValidation;
        this.lifecycle = new ClusterStoreLifecycle(shutdownCallback);
        this.graphCoordinator = graphCoordinator;
        /* One adapter is enough for the manager's lifetime. Each call used
         * to build a new wrapper over the same shared delegate, so closing
         * one borrower's adapter closed Store's persistence manager for
         * everyone. */
        this.persistenceManager = LazyConstant.of(
                () -> new BinaryPersistenceManagerAdapter(delegate.persistenceManager()));
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
        return this.persistenceManager.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object setRoot(final Object newRoot) {
        this.validateState();
        return this.delegate.setRoot(newRoot);
    }

    @Override
    public boolean shutdown() {
        return this.lifecycle.shutdown(this.delegate);
    }

    @Override
    public ClusterStorageManager<T> start() {
        this.lifecycle.start(this.delegate);
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
    @SuppressWarnings("unchecked") // fixing the inherited type variable to this Store's root type is sound for typed managers
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
            GuardingStorageManager.this.validateState();
            return this.delegate.store(instance);
        }

        @Override
        public long[] storeAll(final Object... instances) {
            GuardingStorageManager.this.validateState();
            return this.delegate.storeAll(instances);
        }

        @Override
        public void storeAll(final Iterable<?> instances) {
            GuardingStorageManager.this.validateState();
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
            GuardingStorageManager.this.validateState();
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
            return GuardingStorageManager.this.viewRoots();
        }

        @Override
        public long currentObjectId() {
            return this.delegate.currentObjectId();
        }

        @Override
        public PersistenceManager<Binary> updateCurrentObjectId(final long currentObjectId) {
            GuardingStorageManager.this.validateState();
            this.delegate.updateCurrentObjectId(currentObjectId);
            return this;
        }

        @Override
        public PersistenceSource<Binary> source() {
            return this.delegate.source();
        }

        @Override
        public PersistenceTarget<Binary> target() {
            return GuardingStorageManager.this.gateTarget(this.delegate.target());
        }

        @Override
        public void close() {
            /* This adapter is a borrowed view of the Store's shared
             * persistence manager. The owning storage manager alone ends
             * that lifecycle. */
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
            GuardingStorageManager.this.validateState();
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

    /// Validates the write gate before every raw-target write.
    ///
    /// The persistence manager's raw target otherwise bypasses the storer
    /// `commit` gate, so a fluent binary write would escape storage-limit
    /// enforcement on writers and application-write rejection on readers.
    static final class GatedPersistenceTarget implements PersistenceTarget<Binary> {
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
