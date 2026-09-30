package peruncs.cluster.node.store;

import org.eclipse.serializer.afs.types.AFile;
import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.collections.types.XGettingEnum;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.*;
import org.eclipse.serializer.persistence.types.PersistenceStorer.Creator;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.serializer.reference.Swizzling;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.api.GraphBoundary;
import peruncs.cluster.errors.*;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.*;


/// Store facade with the write gates, the raw-target gate, and the shared
/// graph-boundary adapter.
///
/// [#start] is an idempotent admission check (the node lifecycle owns real
/// startup); [#shutdown] triggers the owning node's complete, ordered close
/// through the installed [NodeClose]. Every other method is a Store
/// operation, forwarded as-is, gated on admission and size limits when it
/// persists application data, or wrapped so a fluent write cannot bypass the
/// gate. Persistence failures with an uncertain durable outcome latch graph
/// invalidity: later coordinated sections and direct writes fail closed.
/// A retry-safe [WriteRejectedException] leaves the graph valid; a locally
/// accepted commit waiting for its marker also preserves the graph while
/// replication admission stays suspended. Close drains admitted app sections.
///
/// @param <T> root type
class GuardingStorageManager<T> implements ClusterStorageManager<T> {
    private final BooleanSupplier storageLimitReached;
    private final StorageManager delegate;
    private final NodeClose nodeClose;
    private final StorageGraphCoordinator graphCoordinator;
    private final GraphBoundary graphBoundary;
    private final LazyConstant<PersistenceManager<Binary>> persistenceManager;
    private final ApplicationSections appSections;
    private final ReplicationMark replicationMark;
    private final Consumer<ReplicationMark> prepareReplicationCommit;
    private final Consumer<ReplicationMark> cancelReplicationCommit;
    /* Set when this manager's shutdown() triggered the node close: reads are
     * then served by failing fast instead of resurrecting a closed Store. */
    private volatile boolean closed;

    GuardingStorageManager(
            final StorageManager delegate,
            final BooleanSupplier storageLimitReached,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator,
            final ReplicationMark replicationMark,
            final Consumer<ReplicationMark> prepareReplicationCommit,
            final Consumer<ReplicationMark> cancelReplicationCommit
    ) {
        this.delegate = delegate;
        this.storageLimitReached = storageLimitReached;
        this.nodeClose = nodeClose;
        this.graphCoordinator = graphCoordinator;
        this.replicationMark = replicationMark;
        this.prepareReplicationCommit = prepareReplicationCommit;
        this.cancelReplicationCommit = cancelReplicationCommit;
        this.appSections = new ApplicationSections(nodeClose);
        this.graphBoundary = this.newApplicationBoundary();
        /* One adapter is enough for the manager's lifetime. Each call used
         * to build a new wrapper over the same shared delegate, so closing
         * one borrower's adapter closed Store's persistence manager for
         * everyone. */
        this.persistenceManager = LazyConstant.of(
                () -> new BinaryPersistenceManagerAdapter(delegate.persistenceManager()));
        /* One adapter per manager lifetime: the raw Database lookup and the
         * guarded view are identity-stable, so no wrapper or delegate lookup
         * is needed per database() call. */
        this.delegateDatabase = delegate.database();
        this.guardedDatabase = this.new GuardedDatabase();
    }

    /* Cached once: identical to re-resolving on every accessor. */
    private final Database delegateDatabase;
    private final Database guardedDatabase;

    /* The write gates cover every write entry point (store, storeAll,
     * storeRoot, setRoot, and Storer.commit): a latched graph invalidity
     * rejects first — a torn graph can never be persisted further — and the
     * storage limit gates persistence so reads, maintenance, registration,
     * and restore keep working on a full disk. */
    void validateState() throws StorageLimitReachedException, GraphInvalidatedException {
        /* Lifecycle admission first: a close in progress or completed closes
         * the writes immediately, before any persistence entry can reach a
         * half-torn Store. */
        this.ensureOpen();
        this.ensureGraphValid();
        if (this.storageLimitReached.getAsBoolean()) {
            throw new StorageLimitReachedException(
                    "Can not store more objects in storage as the storage limit has been reached"
            );
        }
    }

    /// Fails closed with the latched graph invalidity, if any.
    ///
    /// A failed update section may leave the graph partially applied; no
    /// fresh write — and no read that would escape the coordinated
    /// boundary — may run until the node reloads or reseeds.
    void ensureGraphValid() {
        final GraphInvalidatedException failure = this.graphCoordinator.graphFailure();
        if (failure != null) {
            throw failure;
        }
    }

    void rejectApplicationImport() {
        throw new UnsupportedOperationException(
                "Store imports are reserved for the node-owned replication and bootstrap paths");
    }

    boolean isReadOnly() {
        return false;
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
        this.ensureOpen();
        this.ensureGraphValid();
        return this;
    }

    @Override
    public StorageRawFileStatistics createStorageStatistics() {
        return this.delegate.createStorageStatistics();
    }

    @Override
    public Database database() {
        /* The raw Database would hand back the unguarded Store manager,
         * bypassing admission, graph coordination, and reader rejection.
         * Surface a Database whose operations all route through this facade. */
        this.ensureOpen();
        this.ensureGraphValid();
        return this.guardedDatabase;
    }

    /// A [Database] view over the guarded manager: every operation routes
    /// through this facade, so import rejection, the boundary, and the node's
    /// lifecycle/validity admission all still apply.
    private final class GuardedDatabase implements Database {
        @Override
        public String databaseName() {
            return GuardingStorageManager.this.delegateDatabase.databaseName();
        }

        @Override
        public String toIdentifyingString() {
            return GuardingStorageManager.this.delegateDatabase.toIdentifyingString();
        }

        @Override
        public StorageManager storage() {
            /* The facade itself: never the raw embedded manager. */
            return GuardingStorageManager.this;
        }

        @Override
        public StorageManager setStorage(final StorageManager storage) {
            throw new UnsupportedOperationException(
                    "a cluster node's Store is owned by its node lifecycle; setStorage is reserved");
        }

        @Override
        public boolean hasStorage() {
            return GuardingStorageManager.this.isRunning();
        }

        @Override
        public Database guaranteeNoActiveStorage() {
            throw new UnsupportedOperationException(
                    "a cluster node's Store lifecycle is owned by the node, not the application Database");
        }

        @Override
        public StorageManager guaranteeActiveStorage() {
            GuardingStorageManager.this.ensureOpen();
            GuardingStorageManager.this.ensureGraphValid();
            return GuardingStorageManager.this;
        }

        @Override
        public Object getObject(final long objectId) {
            /* Object retrieval reads the managed graph: it joins the same
             * coordinated read section as every other read, so a failed or
             * draining Store never serves identifiers. */
            return GuardingStorageManager.this.read(
                    () -> GuardingStorageManager.this.delegateDatabase.getObject(objectId));
        }

        @Override
        public long store(final Object instance) {
            return GuardingStorageManager.this.store(instance);
        }

        @Override
        public long[] storeAll(final Object... instances) {
            return GuardingStorageManager.this.storeAll(instances);
        }

        @Override
        public void storeAll(final Iterable<?> instances) {
            GuardingStorageManager.this.storeAll(instances);
        }

        @Override
        public Storer createLazyStorer() {
            return GuardingStorageManager.this.createLazyStorer();
        }

        @Override
        public Storer createStorer() {
            return GuardingStorageManager.this.createStorer();
        }

        @Override
        public Storer createEagerStorer() {
            return GuardingStorageManager.this.createEagerStorer();
        }
    }

    @Override
    public void exportChannels(final StorageLiveFileProvider fileProvider, final boolean performGarbageCollection) {
        this.read(() -> this.delegate.exportChannels(fileProvider, performGarbageCollection));
    }

    @Override
    public StorageEntityTypeExportStatistics exportTypes(
            final StorageEntityTypeExportFileProvider exportFileProvider,
            final Predicate<? super StorageEntityTypeHandler> isExportType
    ) {
        return this.read(() -> this.delegate.exportTypes(exportFileProvider, isExportType));
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
        this.read(() -> this.delegate.issueFullBackup(targetFileProvider, typeDictionaryExporter));
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
        /* A borrowed binary-level adapter for fluent Store flows. Writes
         * through it stay on this manager's gates; graph reads escaping a
         * coordinated boundary remain the caller's responsibility — read and
         * write sections are joined through [#graphBoundary()]. Accessors
         * reject on a closed or invalidated store. */
        this.ensureOpen();
        this.ensureGraphValid();
        return this.persistenceManager.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object setRoot(final Object newRoot) {
        this.validateState();
        /* Cluster roots are always `Lazy` references: a plain replacement
         * would silently break that documented shape for every later start.
         * Reject before mutating, so a wrong shape does not invalidate the
         * graph either. */
        if (!(newRoot instanceof Lazy)) {
            throw new IllegalArgumentException(
                    "cluster Store roots are Lazy references; wrap the new root with Lazy.Reference(...)");
        }
        /* In-memory replacement only (durable persistence stays with a later
         * storeRoot): it runs the exclusive section for ordering but does not
         * latch the graph on an ordinary delegate failure — a failed swap is
         * not an uncertain durable write. */
        return this.writeSection(() -> this.delegate.setRoot(newRoot), false);
    }

    @Override
    public boolean shutdown() {
        /* A graph section cannot own teardown: closing joins workers that may
         * hold this thread's lock, so a synchronous close from inside a
         * section would deadlock. Reject it — the caller invalidates dirty
         * state itself, unwinds the section, then closes through the owning
         * node. */
        if (this.graphCoordinator.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "cannot close the node from inside a graph section; unwind the section first");
        }
        /* The lifecycle owns ALL join/retry semantics: wait for an in-flight
         * close, propagate its recorded failure, retry the stages that owe
         * work. The facade flag is only the admission fast path — a failed
         * close leaves it unset so the retry succeeds. */
        final boolean performed = this.nodeClose.close();
        this.closed = true;
        return performed;
    }

    @Override
    public ClusterStorageManager<T> start() {
        /* The node lifecycle owns real startup: this is an idempotent
         * admission check, never a resurrection of a closed Store — closed
         * and invalidated states are rejected, like every other entry. */
        this.ensureOpen();
        this.ensureGraphValid();
        return this;
    }

    /// Fails closed when this manager is no longer live.
    ///
    /// A node close — whether routed through this facade's shutdown or
    /// directly through the owning lifecycle — leaves the raw Store shut
    /// down underneath; reads and writes on a dead Store must not resurrect
    /// or re-enter it.
    private void ensureOpen() {
        /* An outer facade section is admitted and counted before it takes the
         * coordinator; nested calls on that thread must finish if close starts
         * while the section is active. Calls outside that section consult the
         * lifecycle's closed/closing/failed-close state here, including a
         * close initiated directly through ClusterNode. */
        if (!this.graphCoordinator.isHeldByCurrentThread()) this.nodeClose.checkOpen();
        if (this.closed || !this.delegate.isRunning()) {
            throw new IllegalStateException("cluster storage manager is closed");
        }
    }

    /* A delegated persistence failure is conservatively uncertain — bytes may
     * already be written locally or offered for replication. Latch the graph
     * so every later coordinated section fails closed until the node reloads
     * or reseeds. Called only from writeExclusive sections on the caller
     * thread that owns exclusivity; admission gate rejections never reach it,
     * so a failed size/role check cannot poison a healthy graph. */
    private void reportPersistenceFailure(final Throwable failure) {
        this.graphCoordinator.invalidate(failure);
    }

    /// Returns whether this cause chain proves rejection before local persistence.
    ///
    /// @param failure delegated failure
    /// @return `true` only when retry is safe
    static boolean isCleanRejection(final Throwable failure) {
        if (failure == null) return false;
        Throwable slow = failure;
        Throwable fast = failure;
        boolean rejected = false;
        boolean rejectedRecordedAbort = false;
        boolean sawAbortCause = false;
        int depth = 0;
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (depth++ == 16) return false;
            if (current instanceof Error) return false;
            if (current instanceof WriteRejectedException rejection) {
                rejected = true;
                rejectedRecordedAbort |= rejection.hasRecordedAbort();
            } else if (current instanceof ReplicationException) {
                if (!(rejected && rejectedRecordedAbort &&
                      current instanceof ReplicationUnavailableException && !sawAbortCause)) {
                    return false;
                }
                sawAbortCause = true;
            }
            slow = slow == null ? null : slow.getCause();
            fast = fast == null || fast.getCause() == null ? null : fast.getCause().getCause();
            if (slow != null && slow == fast) return false;
        }
        return rejected;
    }

    /// Returns whether this cause chain identifies a locally accepted commit awaiting its marker.
    ///
    /// @param failure delegated failure
    /// @return `true` when the chain reports a locally accepted pending commit
    static boolean isPendingCommit(final Throwable failure) {
        Throwable slow = failure;
        Throwable fast = failure;
        boolean pending = false;
        int depth = 0;
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (depth++ == 16 || current instanceof Error) return false;
            if (current instanceof ReplicationPendingException) {
                pending = true;
            } else if (current instanceof ReplicationException &&
                       !(pending && current instanceof ReplicationUnavailableException)) {
                return false;
            }
            slow = slow == null ? null : slow.getCause();
            fast = fast == null || fast.getCause() == null ? null : fast.getCause().getCause();
            if (slow != null && slow == fast) return false;
        }
        return pending;
    }

    /// Runs one persistence step under the shared exclusive section:
    /// admission after lock acquisition, latch only on the delegate's failure.
    private <R> R persist(final Supplier<R> step) {
        return this.writeSection(step, true);
    }

    private void persist(final Runnable step) {
        this.writeSection(step, true);
    }

    /// Runs a result-producing write after admission and graph validation.
    ///
    /// @param action write performed under exclusive graph access
    /// @param latchPersistenceFailure whether an uncertain action failure invalidates the graph
    /// @param <R> result type
    private <R> R writeSection(final Supplier<R> action, final boolean latchPersistenceFailure) {
        return this.appSection(() -> {
            this.validateState();
            return this.graphCoordinator.writeExclusive(() -> {
                this.validateState();
                try {
                    return action.get();
                } catch (final RuntimeException | Error failure) {
                    this.latchPersistenceFailure(failure, latchPersistenceFailure);
                    throw failure;
                }
            });
        });
    }

    /// Runs a void graph-boundary write without wrapping it in an allocated supplier.
    ///
    /// @param action write performed under exclusive graph access
    private void writeSection(final Runnable action) {
        this.writeSection(action, false);
    }

    private void writeSection(final Runnable action, final boolean latchPersistenceFailure) {
        this.appSection(() -> {
            this.validateState();
            this.graphCoordinator.writeExclusive(() -> {
                this.validateState();
                try {
                    action.run();
                } catch (final RuntimeException | Error failure) {
                    this.latchPersistenceFailure(failure, latchPersistenceFailure);
                    throw failure;
                }
            });
        });
    }

    private void latchPersistenceFailure(final Throwable failure, final boolean enabled) {
        if (enabled && !isPendingCommit(failure) && !isCleanRejection(failure)) {
            this.reportPersistenceFailure(failure);
        }
    }

    private <R> R appSection(final Supplier<R> action) {
        final boolean outermost = this.enterAppSection();
        try {
            return action.get();
        } finally {
            this.exitAppSection(outermost);
        }
    }

    private void appSection(final Runnable action) {
        final boolean outermost = this.enterAppSection();
        try {
            action.run();
        } finally {
            this.exitAppSection(outermost);
        }
    }

    private boolean enterAppSection() {
        this.ensureOpen();
        final boolean outermost = !this.graphCoordinator.isHeldByCurrentThread();
        if (outermost) this.appSections.enter();
        return outermost;
    }

    private void exitAppSection(final boolean outermost) {
        if (outermost) this.appSections.exit();
    }

    boolean awaitAppIdle(final Duration timeout) {
        return this.appSections.awaitIdle(timeout);
    }

    @Override
    public long store(final Object instance) {
        return this.persist(() -> {
            final ClusterStorerAdapter storer = new ClusterStorerAdapter(this.delegate.createStorer());
            final long objectId = storer.store(instance);
            storer.commitWithinWriteSection();
            return objectId;
        });
    }

    @Override
    public long[] storeAll(final Object... instances) {
        return this.persist(() -> {
            final ClusterStorerAdapter storer = new ClusterStorerAdapter(this.delegate.createStorer());
            final long[] objectIds = storer.storeAll(instances);
            storer.commitWithinWriteSection();
            return objectIds;
        });
    }

    @Override
    public void storeAll(final Iterable<?> instances) {
        this.persist(() -> {
            final ClusterStorerAdapter storer = new ClusterStorerAdapter(this.delegate.createStorer());
            storer.storeAll(instances);
            storer.commitWithinWriteSection();
        });
    }

    @Override
    public long storeRoot() {
        return this.persist(() -> {
            final PersistenceRootReferencing rootReference = this.delegate.viewRoots().rootReference();
            final Object root = rootReference.get();
            final ClusterStorerAdapter storer = new ClusterStorerAdapter(this.delegate.createStorer());
            storer.store(rootReference);
            final long rootId = root == null ? Swizzling.nullId() : storer.store(root);
            storer.commitWithinWriteSection();
            return rootId;
        });
    }

    @Override
    public StorageTypeDictionary typeDictionary() {
        return this.read(this.delegate::typeDictionary);
    }

    @Override
    public PersistenceRootsView viewRoots() {
        /* Live root accessors outside a coordinated read are only tolerable
         * while the graph is provably valid and the store is open. Note that
         * callers still must not traverse the returned view past a
         * coordinated boundary. */
        return this.read(() -> {
            final PersistenceRootsView roots = this.delegate.viewRoots();
            if (this.replicationMark == null) return roots;
            return new PersistenceRootsView() {
                @Override
                public PersistenceRootReferencing rootReference() {
                    return roots.rootReference();
                }

                @Override
                public <C extends BiConsumer<String, Object>> C iterateEntries(final C iterator) {
                    roots.iterateEntries((identifier, root) -> {
                        if (!ReplicationMark.ROOT_ID.equals(identifier)) iterator.accept(identifier, root);
                    });
                    return iterator;
                }
            };
        });
    }

    @Override
    public Storer createEagerStorer() {
        this.ensureOpen();
        this.ensureGraphValid();
        return new ClusterStorerAdapter(this.delegate.createEagerStorer());
    }

    @Override
    public Storer createLazyStorer() {
        this.ensureOpen();
        this.ensureGraphValid();
        return new ClusterStorerAdapter(this.delegate.createLazyStorer());
    }

    @Override
    public Storer createStorer() {
        this.ensureOpen();
        this.ensureGraphValid();
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
        /* The live root stays accessible to both roles — readers traverse it
         * inside `graphBoundary().read(...)`, writers under the documented
         * write contract — but never after the node closed or the graph was
         * invalidated. */
        this.ensureOpen();
        this.ensureGraphValid();
        return this.delegate.root();
    }

    @Override
    public GraphBoundary graphBoundary() {
        return this.graphBoundary;
    }

    /// The cached, role-aware application boundary over the shared coordinator.
    ///
    /// Reads join the coordinator's read side; writes run on its exclusive
    /// side only after this manager's admission (limit, role, invalidity) —
    /// replication materialization never passes through that admission, so
    /// reader nodes still apply replicated writes while application writes
    /// are rejected before their callback executes.
    private GraphBoundary newApplicationBoundary() {
        return new GraphBoundary() {
            @Override
            public void read(final Runnable action) {
                /* No live traversal on a closed node: fail fast before taking
                 * the read side instead of crashing against a shut-down Store
                 * mid-walk. */
                GuardingStorageManager.this.read(action);
            }

            @Override
            public <R> R read(final Supplier<R> action) {
                return GuardingStorageManager.this.read(action);
            }

            @Override
            public void write(final Runnable action) {
                GuardingStorageManager.this.writeSection(action);
            }

            @Override
            public <R> R write(final Supplier<R> action) {
                return GuardingStorageManager.this.writeSection(action, false);
            }

            @Override
            public void invalidate(final Throwable cause) {
                /* A potentially dirty application failure reaching the
                 * boundary must occupy the same latch the replication paths
                 * use, preserving its first cause. */
                GuardingStorageManager.this.graphCoordinator.invalidate(cause);
            }
        };
    }

    @Override
    public List<StorageAdjacencyDataExporter.AdjacencyFiles> exportAdjacencyData(final Path workingDir) {
        /* Whole-graph export joins the coordinated read side: it must never
         * observe a half-applied batch or a torn graph. */
        return this.read(() -> this.delegate.exportAdjacencyData(workingDir));
    }

    private <R> R read(final Supplier<R> action) {
        return this.appSection(() -> this.graphCoordinator.read(action));
    }

    private void read(final Runnable action) {
        this.read(() -> {
            action.run();
            return null;
        });
    }

    /// Adapts the cluster manager to Store's binary persistence manager.
    private final class BinaryPersistenceManagerAdapter implements PersistenceManager<Binary> {
        private final PersistenceManager<Binary> delegate;

        private BinaryPersistenceManagerAdapter(final PersistenceManager<Binary> delegate) {
            this.delegate = delegate;
        }

        @Override
        public long ensureObjectId(final Object object) {
            return GuardingStorageManager.this.isReadOnly() ? this.lookupRegisteredId(object)
                    : this.delegate.ensureObjectId(object);
        }

        @Override
        public <U> long ensureObjectId(
                final U object,
                final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
                final PersistenceTypeHandler<Binary, U> optionalHandler
        ) {
            return GuardingStorageManager.this.isReadOnly() ? this.lookupRegisteredId(object)
                    : this.delegate.ensureObjectId(object, objectIdRequestor, optionalHandler);
        }

        @Override
        public <U> long ensureObjectIdGuaranteedRegister(
                final U object,
                final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
                final PersistenceTypeHandler<Binary, U> optionalHandler
        ) {
            return GuardingStorageManager.this.isReadOnly() ? this.lookupRegisteredId(object)
                    : this.delegate.ensureObjectIdGuaranteedRegister(object, objectIdRequestor, optionalHandler);
        }

        @Override
        public void consolidate() {
            this.rejectReaderMutation();
            this.delegate.consolidate();
        }

        @Override
        public boolean registerLocalRegistry(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
            this.rejectReaderMutation();
            return this.delegate.registerLocalRegistry(localRegistry);
        }

        @Override
        public void mergeEntries(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
            this.rejectReaderMutation();
            this.delegate.mergeEntries(localRegistry);
        }

        @Override
        public long lookupObjectId(final Object object) {
            return GuardingStorageManager.this.read(() -> this.delegate.lookupObjectId(object));
        }

        @Override
        public Object lookupObject(final long objectId) {
            return GuardingStorageManager.this.read(() -> this.delegate.lookupObject(objectId));
        }

        @Override
        public Object get() {
            return GuardingStorageManager.this.read(this.delegate::get);
        }

        @Override
        public Object getObject(final long objectId) {
            return GuardingStorageManager.this.read(() -> this.delegate.getObject(objectId));
        }

        @Override
        public <C extends Consumer<Object>> C collect(final C collector, final long... objectIds) {
            return GuardingStorageManager.this.read(() -> this.delegate.collect(collector, objectIds));
        }

        @Override
        public <C extends Consumer<Object>> C collect(final C collector, final Set_long objectIds) {
            return GuardingStorageManager.this.read(() -> this.delegate.collect(collector, objectIds));
        }

        @Override
        public long store(final Object instance) {
            /* Exclusive section like the facade's own store(): adapter writes
             * must not race application write sections, and a delegate failure
             * invalidates through the same latch. */
            return GuardingStorageManager.this.persist(() -> {
                final ClusterPersistenceStorerAdapter storer =
                        new ClusterPersistenceStorerAdapter(this.delegate.createStorer());
                final long objectId = storer.store(instance);
                storer.commitWithinWriteSection();
                return objectId;
            });
        }

        @Override
        public long[] storeAll(final Object... instances) {
            return GuardingStorageManager.this.persist(() -> {
                final ClusterPersistenceStorerAdapter storer =
                        new ClusterPersistenceStorerAdapter(this.delegate.createStorer());
                final long[] objectIds = storer.storeAll(instances);
                storer.commitWithinWriteSection();
                return objectIds;
            });
        }

        @Override
        public void storeAll(final Iterable<?> instances) {
            GuardingStorageManager.this.persist(() -> {
                final ClusterPersistenceStorerAdapter storer =
                        new ClusterPersistenceStorerAdapter(this.delegate.createStorer());
                storer.storeAll(instances);
                storer.commitWithinWriteSection();
            });
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
            GuardingStorageManager.this.ensureGraphValid();
            return this.delegate.createLoader();
        }

        @Override
        public PersistenceRegisterer createRegisterer() {
            this.rejectReaderMutation();
            return this.delegate.createRegisterer();
        }

        @Override
        public void updateMetadata(
                final PersistenceTypeDictionary typeDictionary,
                final long highestTypeId,
                final long highestObjectId
        ) {
            if (GuardingStorageManager.this.replicationMark != null) {
                throw new UnsupportedOperationException("replicated Store metadata is node-owned");
            }
            GuardingStorageManager.this.persist(() ->
                    this.delegate.updateMetadata(typeDictionary, highestTypeId, highestObjectId));
        }

        @Override
        public PersistenceObjectRegistry objectRegistry() {
            this.rejectReaderMutation();
            return this.delegate.objectRegistry();
        }

        @Override
        public Object objectRegistryMonitor() {
            return this.delegate.objectRegistryMonitor();
        }

        @Override
        public PersistenceTypeDictionary typeDictionary() {
            return GuardingStorageManager.this.read(this.delegate::typeDictionary);
        }

        private long lookupRegisteredId(final Object object) {
            return GuardingStorageManager.this.read(() -> {
                final long objectId = this.delegate.lookupObjectId(object);
                if (Swizzling.isNotFoundId(objectId)) {
                    throw new ReaderWriteRejectedException(
                            "node role is read-only; assigning a new object id would diverge from the writer");
                }
                return objectId;
            });
        }

        private void rejectReaderMutation() {
            if (GuardingStorageManager.this.isReadOnly()) {
                throw new ReaderWriteRejectedException(
                        "node role is read-only; application registry changes are rejected");
            }
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
            if (GuardingStorageManager.this.replicationMark != null) {
                throw new UnsupportedOperationException("replicated Store object ids are node-owned");
            }
            GuardingStorageManager.this.persist(() ->
                    this.delegate.updateCurrentObjectId(currentObjectId));
            return this;
        }

        @Override
        public PersistenceSource<Binary> source() {
            return this.delegate.source();
        }

        @Override
        public PersistenceTarget<Binary> target() {
            if (GuardingStorageManager.this.replicationMark != null) {
                throw new UnsupportedOperationException("replicated Store commits must use the storage manager");
            }
            return GuardingStorageManager.this.gateTarget(this.delegate.target());
        }

        @Override
        public void close() {
            /* This adapter is a borrowed view of the Store's shared
             * persistence manager. The owning storage manager alone ends
             * that lifecycle. */
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
            /* A cached storer must not keep accepting registrations after the
             * graph is latched or the node closed — buffering into a visible
             * write would otherwise still corrupt a later commit. */
            GuardingStorageManager.this.validateState();
            return this.storer.store(instance);
        }

        @Override
        public long store(final Object instance, final long objectId) {
            GuardingStorageManager.this.validateState();
            return this.storer.store(instance, objectId);
        }

        @Override
        public long[] storeAll(final Object... instances) {
            GuardingStorageManager.this.validateState();
            return this.storer.storeAll(instances);
        }

        @Override
        public void storeAll(final Iterable<?> instances) {
            GuardingStorageManager.this.validateState();
            this.storer.storeAll(instances);
        }

        @Override
        public Object commit() {
            /* Same exclusive persist section as store()/storeRoot():
             * admission before and after the lock, latch only on a delegate
             * persistence failure. */
            return GuardingStorageManager.this.persist(this::commitWithinWriteSection);
        }

        Object commitWithinWriteSection() {
            if (GuardingStorageManager.this.replicationMark != null) {
                final ReplicationMark mark = GuardingStorageManager.this.replicationMark;
                try {
                    GuardingStorageManager.this.prepareReplicationCommit.accept(mark);
                    this.storer.store(mark);
                    return this.storer.commit();
                } finally {
                    GuardingStorageManager.this.cancelReplicationCommit.accept(mark);
                }
            }
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
    /// Non-static because a delegate I/O failure must latch the owning
    /// manager's graph invalidity.
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
            /* The admission gate stays OUTSIDE the delegate failure catch:
             * a gate rejection is a policy refusal, never graph damage. The
             * delegate write itself runs the exclusive section, so a raw
             * fluent write cannot race an application boundary write. */
            this.writeGate.run();
            GuardingStorageManager.this.persist(() -> this.delegate.write(data));
        }

        @Override
        public void prepareTarget() {
            this.delegate.prepareTarget();
        }

        /// A borrowed view never owns the live target: closing it must be a
        /// no-op so an application cannot shut the shared Store down through
        /// the persistence-manager adapter.
        @Override
        public void closeTarget() {
        }
    }
}
