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
import peruncs.datagrid.cluster.node.exceptions.StorageLimitReachedException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.eclipse.serializer.util.X.notNull;

/// This storage manager adds cluster shutdown and size checks to Store.
///
/// The wrapper keeps the Store API visible while adding the node's shutdown
/// callback. The full implementation also validates storage size before writes
/// and routes graph updates through the cluster lock.
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
        return new Default<>(notNull(delegate), notNull(storageSizeValidation), notNull(shutdownCallback));
    }

        /// Runs the node callback and always gives the Store a chance to shut down.
    private static boolean shutdownWithCallback(
            final StorageManager delegate, final ShutdownCallback shutdownCallback) {
        Throwable failure = null;
        boolean result = false;
        try {
            shutdownCallback.onShutdown();
        } catch (final Throwable callbackFailure) {
            failure = callbackFailure;
        }
        try {
            result = delegate.shutdown();
        } catch (final Throwable shutdownFailure) {
            if (failure == null) failure = shutdownFailure;
            else if (failure != shutdownFailure) failure.addSuppressed(shutdownFailure);
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
        return result;
    }

        /// Creates a manager that only adds shutdown handling.
    ///
    /// @param <T>              root type
    /// @param delegate         Store manager
    /// @param shutdownCallback shutdown callback
    /// @return cluster storage manager
    static <T> ClusterStorageManager<T> Wrapper(final StorageManager delegate, final ShutdownCallback shutdownCallback) {
        return new Wrapper<>(notNull(delegate), notNull(shutdownCallback));
    }

    @Override
    @SuppressWarnings("unchecked")
    Lazy<T> root();

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

        /// Adds the shutdown callback while delegating every Store operation.
    ///
    /// @param <T> root type
    class Wrapper<T> implements ClusterStorageManager<T> {
        private static final System.Logger LOGGER = System.getLogger(Wrapper.class.getName());

        private final StorageManager delegate;
        private final ShutdownCallback shutdownCallback;

        private Wrapper(final StorageManager delegate, final ShutdownCallback shutdownCallback) {
            this.delegate = delegate;
            this.shutdownCallback = shutdownCallback;
        }

        @Override
        public StorageConfiguration configuration() {
            return this.delegate.configuration();
        }

        @Override
        public StorageTypeDictionary typeDictionary() {
            return this.delegate.typeDictionary();
        }

        @Override
        public boolean shutdown() {
            LOGGER.log(System.Logger.Level.INFO, "Shutting down ClusterStorageManager");
            return ClusterStorageManager.shutdownWithCallback(this.delegate, this.shutdownCallback);
        }

        @Override
        public StorageConnection createConnection() {
            return this.delegate.createConnection();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object setRoot(final Object newRoot) {
            return this.delegate.setRoot(newRoot);
        }

        @Override
        public long storeRoot() {
            return this.delegate.storeRoot();
        }

        @Override
        public PersistenceRootsView viewRoots() {
            return this.delegate.viewRoots();
        }

        @Override
        public Database database() {
            return this.delegate.database();
        }

        @Override
        public boolean isAcceptingTasks() {
            return this.delegate.isAcceptingTasks();
        }

        @Override
        public boolean isRunning() {
            return this.delegate.isRunning();
        }

        @Override
        public boolean isStartingUp() {
            return this.delegate.isStartingUp();
        }

        @Override
        public boolean isShuttingDown() {
            return this.delegate.isShuttingDown();
        }

        @Override
        public void checkAcceptingTasks() {
            this.delegate.checkAcceptingTasks();
        }

        @Override
        public long initializationTime() {
            return this.delegate.initializationTime();
        }

        @Override
        public long operationModeTime() {
            return this.delegate.operationModeTime();
        }

        @Override
        public boolean isActive() {
            return this.delegate.isActive();
        }

        @Override
        public boolean issueGarbageCollection(final long nanoTimeBudget) {
            return this.delegate.issueGarbageCollection(nanoTimeBudget);
        }

        @Override
        public boolean issueFileCheck(final long nanoTimeBudget) {
            return this.delegate.issueFileCheck(nanoTimeBudget);
        }

        @Override
        public boolean issueCacheCheck(final long nanoTimeBudget, final StorageEntityCacheEvaluator entityEvaluator) {
            return this.delegate.issueCacheCheck(nanoTimeBudget, entityEvaluator);
        }

        @Override
        public void issueFullBackup(
                final StorageLiveFileProvider targetFileProvider,
                final PersistenceTypeDictionaryExporter typeDictionaryExporter
        ) {
            this.delegate.issueFullBackup(targetFileProvider, typeDictionaryExporter);
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
        public StorageRawFileStatistics createStorageStatistics() {
            return this.delegate.createStorageStatistics();
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
        public void importFiles(final XGettingEnum<AFile> importFiles) {
            this.delegate.importFiles(importFiles);
        }

        @Override
        public void importData(final XGettingEnum<ByteBuffer> importData) {
            this.delegate.importData(importData);
        }

        @Override
        public PersistenceManager<Binary> persistenceManager() {
            return this.delegate.persistenceManager();
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
        public boolean isUsed() {
            return this.delegate.isUsed();
        }

        @Override
        public int markUnused() {
            return this.delegate.markUnused();
        }

        @Override
        public void accessUsageMarks(final Consumer<? super XGettingEnum<Object>> logic) {
            this.delegate.accessUsageMarks(logic);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Lazy<T> root() throws NodeLibraryException {
            return this.delegate.root();
        }

        @Override
        public ClusterStorageManager<T> start() throws NodeLibraryException {
            this.delegate.start();
            return this;
        }

        @Override
        public List<StorageAdjacencyDataExporter.AdjacencyFiles> exportAdjacencyData(final Path workingDir) {
            return this.delegate.exportAdjacencyData(workingDir);
        }
    }

        /// Adds size validation and distributed graph-write behavior to Store.
    ///
    /// @param <T> root type
    class Default<T> implements ClusterStorageManager<T> {
        private static final System.Logger LOGGER = System.getLogger(Default.class.getName());
        private final StorageSizeValidation storageSizeValidation;
        private final StorageManager delegate;
        private final ShutdownCallback shutdownCallback;

        private Default(
                final StorageManager delegate,
                final StorageSizeValidation storageSizeValidation,
                final ShutdownCallback shutdownCallback
        ) {
            this.delegate = delegate;
            this.storageSizeValidation = storageSizeValidation;
            this.shutdownCallback = shutdownCallback;
        }

        private void validateState() throws StorageLimitReachedException {
            if (this.storageSizeValidation.isStorageLimitReached()) {
                throw new StorageLimitReachedException(
                        "Can not store more objects in storage as the storage limit has been reached"
                );
            }
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
            return this.delegate.createConnection();
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
            this.validateState();
            this.delegate.importData(importData);
        }

        @Override
        public void importFiles(final XGettingEnum<AFile> importFiles) {
            this.validateState();
            this.delegate.importFiles(importFiles);
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
        public boolean shutdown() {
            LOGGER.log(System.Logger.Level.INFO, "Shutting down ClusterStorageManager");
            return ClusterStorageManager.shutdownWithCallback(this.delegate, this.shutdownCallback);
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
        @SuppressWarnings("unchecked")
        public Lazy<T> root() {
            return this.delegate.root();
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
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.ensureObjectId(object);
            }

            @Override
            public <U> long ensureObjectId(
                    final U object,
                    final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
                    final PersistenceTypeHandler<Binary, U> optionalHandler
            ) {
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.ensureObjectId(object, objectIdRequestor, optionalHandler);
            }

            @Override
            public <U> long ensureObjectIdGuaranteedRegister(
                    final U object,
                    final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
                    final PersistenceTypeHandler<Binary, U> optionalHandler
            ) {
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.ensureObjectIdGuaranteedRegister(object, objectIdRequestor, optionalHandler);
            }

            @Override
            public void consolidate() {
                ClusterStorageManager.Default.this.validateState();
                this.delegate.consolidate();
            }

            @Override
            public boolean registerLocalRegistry(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.registerLocalRegistry(localRegistry);
            }

            @Override
            public void mergeEntries(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
                ClusterStorageManager.Default.this.validateState();
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
                return this.delegate.viewRoots();
            }

            @Override
            public long currentObjectId() {
                return this.delegate.currentObjectId();
            }

            @Override
            public PersistenceManager<Binary> updateCurrentObjectId(final long currentObjectId) {
                return this.delegate.updateCurrentObjectId(currentObjectId);
            }

            @Override
            public PersistenceSource<Binary> source() {
                return this.delegate.source();
            }

            @Override
            public PersistenceTarget<Binary> target() {
                return this.delegate.target();
            }

            @Override
            public void close() {
                this.delegate.close();
            }
        }

                /// Registers binary types through the cluster manager boundary.
        private final class ClusterPersistenceRegistererAdapter implements PersistenceRegisterer {
            private final PersistenceRegisterer delegate;

            private ClusterPersistenceRegistererAdapter(final PersistenceRegisterer delegate) {
                this.delegate = delegate;
            }

            @Override
            public <U> long apply(final U instance) {
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.apply(instance);
            }

            @Override
            public long register(final Object instance) {
                ClusterStorageManager.Default.this.validateState();
                return this.delegate.register(instance);
            }

            @Override
            public long[] registerAll(final Object... instances) {
                ClusterStorageManager.Default.this.validateState();
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
                return this.storer.reinitialize();
            }

            @Override
            public Storer reinitialize(final long initialCapacity) {
                return this.storer.reinitialize(initialCapacity);
            }

            @Override
            public Storer ensureCapacity(final long desiredCapacity) {
                return this.storer.ensureCapacity(desiredCapacity);
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
}
