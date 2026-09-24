package peruncs.datagrid.cluster.storage.binary;

import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryProvider;
import org.eclipse.serializer.typing.Disposable;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.errors.CorruptReplicationDataException;
import peruncs.datagrid.cluster.errors.ReplicationUnavailableException;
import peruncs.datagrid.cluster.storage.StorageGraphCoordinator;

import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.eclipse.serializer.util.X.notNull;

/// Applies committed Store binary data on a reader node.
///
/// Incoming buffers are copied to owned native memory and queued immediately;
/// the Store import itself is deferred into the drained batch so replay work,
/// graph exclusion, and index refresh are coalesced while each Store
/// transaction boundary remains intact. Object-graph materialization runs on a
/// bounded single-thread executor. Providers must call this merger only after
/// their transport-specific commit validation has completed.
///
/// # Delivery threading
///
/// Data admission ([#receiveData(Binary)], [#receiveDataOwned(Binary)]) and
/// materialization scheduling assume a single delivery thread per merger: the
/// transport delivers committed transactions in order, and the merger's
/// queue/import hand-off relies on that ordering. Concurrent data admissions
/// are not part of the contract. Type-dictionary delivery may share the same
/// delivery thread.
///
/// # Composition
///
/// The default implementation splits its state by ownership: [ApplyQueue]
/// holds the queue, the byte counters, and the admission conditions,
/// [ApplyWorker] runs the coalescing batch loop, and this class keeps
/// lifecycle (executors, disposal, terminal failure latch) plus the
/// type-dictionary merge — visible explicitly to the other two through the
/// [MergerLifecycle] seam. A standalone lifecycle class would own no state
/// beyond the references to the other collaborators, so it stays folded
/// into the facade.
///
/// # Locking
///
/// The implementation holds two locks with one explicit order: queue draining
/// runs inside the materialization {@code LockedExecutor} and then takes the
/// queue lock in [ApplyQueue]; no path acquires them in the reverse order.
/// The coalescing-delay condition belongs to the queue lock — a condition
/// must bind to its guarding lock, which {@code LockedExecutor} does not
/// expose, so the queue lock stays explicit — and lifecycle uses only
/// volatiles plus the executor's own thread safety. Store and graph callbacks
/// run after both locks have been released. A single merged lock is
/// deliberately not used: draining must release the queue admission lock
/// before a blocking graph update, or a slow Store would stall the Aeron
/// polling thread and look like transport loss.
public final class StorageBinaryDataMerger implements StorageBinaryDataReceiver, Disposable, MergerLifecycle {
        /// Immutable configuration for one bounded binary merger.
    ///
    /// Every limit lives here so operators can tune coalescing, disposal
    /// grace, and index-validation bounds without code changes; the constants
    /// on [StorageBinaryDataMerger] hold the safe defaults.
    ///
    /// The coordinator is optional. When present, post-materialization graph
    /// scans join its read/write boundary; when absent, the merger preserves
    /// the direct-scan behavior used by standalone storage clients.
    ///
    /// @param foundation                persistence foundation
    /// @param storage                   Store connection
    /// @param objectGraphUpdateHandler  graph update handler
    /// @param cachingTimeoutMs          maximum wait for a cached batch
    /// @param cachedBytesLimit          queued payload bytes that trigger a backpressure wait
    /// @param maxCachedBytes            hard cap on queued payload bytes
    /// @param applyTimeoutMs            maximum wait for one materialization batch
    /// @param disposeOrderlyTimeoutMs   orderly worker termination window during disposal
    /// @param disposeInterruptTimeoutMs interrupt-based worker termination window during disposal
    /// @param maxValidatedIndexObjects  bound on index-relevant objects visited by one root scan
    /// @param graphCoordinator          per-Store graph coordinator, or `null`
    public record Configuration(
            BinaryPersistenceFoundation<?> foundation,
            StorageConnection storage,
            ObjectGraphUpdateHandler objectGraphUpdateHandler,
            long cachingTimeoutMs,
            long cachedBytesLimit,
            long maxCachedBytes,
            long applyTimeoutMs,
            long disposeOrderlyTimeoutMs,
            long disposeInterruptTimeoutMs,
            int maxValidatedIndexObjects,
            StorageGraphCoordinator graphCoordinator
    ) {
        /// Validates the merger collaborators and limits.
        public Configuration {
            Objects.requireNonNull(foundation, "foundation");
            Objects.requireNonNull(storage, "storage");
            Objects.requireNonNull(objectGraphUpdateHandler, "objectGraphUpdateHandler");
            if (cachingTimeoutMs < 0L) throw new IllegalArgumentException("cachingTimeoutMs must not be negative");
            if (cachedBytesLimit <= 0L) throw new IllegalArgumentException("cachedBytesLimit must be positive");
            if (maxCachedBytes <= 0L) throw new IllegalArgumentException("maxCachedBytes must be positive");
            if (applyTimeoutMs <= 0L) throw new IllegalArgumentException("applyTimeoutMs must be positive");
            if (disposeOrderlyTimeoutMs <= 0L) {
                throw new IllegalArgumentException("disposeOrderlyTimeoutMs must be positive");
            }
            if (disposeInterruptTimeoutMs <= 0L) {
                throw new IllegalArgumentException("disposeInterruptTimeoutMs must be positive");
            }
            if (maxValidatedIndexObjects <= 0) {
                throw new IllegalArgumentException("maxValidatedIndexObjects must be positive");
            }
        }

        /// Creates a configuration with every documented default.
        ///
        /// @param foundation               persistence foundation
        /// @param storage                  Store connection
        /// @param objectGraphUpdateHandler graph update handler
        /// @param graphCoordinator         per-Store graph coordinator, or `null`
        /// @return configuration using the default limits
        public static Configuration create(
                final BinaryPersistenceFoundation<?> foundation,
                final StorageConnection storage,
                final ObjectGraphUpdateHandler objectGraphUpdateHandler,
                final StorageGraphCoordinator graphCoordinator) {
            return new Configuration(
                    foundation,
                    storage,
                    objectGraphUpdateHandler,
                    CACHING_TIMEOUT_MS,
                    CACHING_BYTES_LIMIT,
                    MAX_CACHED_BYTES,
                    APPLY_TIMEOUT_MS,
                    DISPOSE_ORDERLY_TIMEOUT_MS,
                    DISPOSE_INTERRUPT_TIMEOUT_MS,
                    MAX_VALIDATED_INDEX_OBJECTS,
                    graphCoordinator
            );
        }
    }

        /// Creates a merger with bounded deferred materialization.
    ///
    /// @param configuration immutable merger configuration
    /// @return binary merger
    public static StorageBinaryDataMerger create(final Configuration configuration) {
        return new StorageBinaryDataMerger(notNull(configuration));
    }

    private static final System.Logger LOGGER = System.getLogger(StorageBinaryDataMerger.class.getName());
    /// Default cache timeout in milliseconds.
    static final long CACHING_TIMEOUT_MS = 10_000L;
    /// Default queued payload bytes that trigger a backpressure wait.
    static final long CACHING_BYTES_LIMIT = 64L << 20;
    /// Default hard cap on queued payload bytes.
    static final long MAX_CACHED_BYTES = 1L << 30;
    /// Default maximum wait for one materialization batch, in milliseconds.
    static final long APPLY_TIMEOUT_MS = 60_000L;
    /// Default orderly worker termination window during disposal, in milliseconds.
    static final long DISPOSE_ORDERLY_TIMEOUT_MS = 30_000L;
    /// Default interrupt-based worker termination window during disposal, in milliseconds.
    static final long DISPOSE_INTERRUPT_TIMEOUT_MS = 5_000L;
    /// Default bound on index-relevant objects visited by one root scan.
    static final int MAX_VALIDATED_INDEX_OBJECTS = 4096;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(Thread.ofVirtual()
            .name("eclipse-datagrid-store-materializer", 0L)
            .factory());
    /* A Store callback can ignore interruption forever. Keep timeout
     * detection off that worker so health and acknowledgement paths fail
     * closed even when the callback itself never returns. */
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(runnable ->
            Thread.ofPlatform()
                    .daemon()
                    .name("eclipse-datagrid-store-watchdog")
                    .unstarted(runnable));
    private final LockedExecutor materialization = LockedExecutor.New();
    private final BinaryPersistenceFoundation<?> foundation;
    private final StorageConnection storage;
    private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
    private final StorageGraphCoordinator graphCoordinator;
    private final long maxCachedBytes;
    private final long applyTimeoutMs;
    private final long disposeOrderlyTimeoutMs;
    private final long disposeInterruptTimeoutMs;
    /* The worker's own structured-scope deadline covers the caller's whole
     * retry budget, so a slow-but-progressing batch is absorbed by the
     * caller's retries instead of being declared terminal by the worker on
     * the first per-attempt expiry. */
    private final long materializationBudgetMs;
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private final ApplyQueue queue;
    private final ApplyWorker worker;
    /* Cached so admissions allocate no submission lambda on the hot path. */
    private final Supplier<Future<?>> workerSubmission = this::submitWorker;
    private volatile boolean disposed;
    private volatile Future<?> updateFuture;
    /* One parsing foundation serves every dictionary message: rebuilding a
     * BinaryPersistenceFoundation per message re-created its handler graph
     * inside the materialization lock. A non-caching provider reads the
     * current snapshot through this holder, so consecutive dictionaries
     * never share parsed state. */
    private final BinaryPersistenceFoundation<?> dictionaryFoundation;
    private final Object dictionaryParseLock = new Object();
    private String dictionarySource;

private StorageBinaryDataMerger(final Configuration configuration) {
        this.foundation = configuration.foundation();
        this.storage = configuration.storage();
        this.objectGraphUpdateHandler = configuration.objectGraphUpdateHandler();
        this.graphCoordinator = configuration.graphCoordinator();
        final long cachingTimeoutMs = configuration.cachingTimeoutMs();
        final long cacheBytesLimit = configuration.cachedBytesLimit();
        this.maxCachedBytes = configuration.maxCachedBytes();
        this.applyTimeoutMs = configuration.applyTimeoutMs();
        this.disposeOrderlyTimeoutMs = configuration.disposeOrderlyTimeoutMs();
        this.disposeInterruptTimeoutMs = configuration.disposeInterruptTimeoutMs();
        try {
            this.materializationBudgetMs = Math.multiplyExact(applyTimeoutMs, APPLY_TIMEOUT_RETRIES + 1L);
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException("applyTimeoutMs is too large: %s".formatted(applyTimeoutMs), overflow);
        }
        this.queue = new ApplyQueue(this, cacheBytesLimit, this.maxCachedBytes, this.applyTimeoutMs);
        this.worker = new ApplyWorker(
                this,
                this.queue,
                this.materialization,
                this.watchdog,
                this.foundation,
                this.storage,
                this.objectGraphUpdateHandler,
                cachingTimeoutMs,
                configuration.maxValidatedIndexObjects(),
                this.materializationBudgetMs);
        final BinaryPersistenceFoundation<?> parsingFoundation = BinaryPersistence.Foundation()
                .setClassLoaderProvider(this.foundation.getClassLoaderProvider())
                .setFieldEvaluatorPersister(this.foundation.getFieldEvaluatorPersistable());
        parsingFoundation.setTypeDictionaryProvider(PersistenceTypeDictionaryProvider.New(
                () -> this.dictionarySource,
                parsingFoundation.getTypeDictionaryCompiler()
        ));
        this.dictionaryFoundation = parsingFoundation;
    }

            /// Aeron transfers its assembled direct buffers before this callback starts.
    ///
    /// @return `true` because this merger releases the transferred buffers
    @Override
    public boolean canReceiveDataOwned() {
        return true;
    }

            /// Receives a borrowed binary. Must be called by the single
    /// transport delivery thread; see the class javadoc.
    ///
    /// @param data complete binary to receive
    @Override
    public void receiveData(final Binary data) {
        if (this.failure.get() != null) {
            throw this.failure.get();
        }
        if (this.disposed) {
            /* A disposed receiver must not acknowledge data. Returning normally
             * would let the Aeron assembler advance its cursor even though the Store
             * binary was discarded. */
            throw new ReplicationUnavailableException("Storage binary merger is disposed");
        }
        final ByteBuffer[] sourceBuffers = StorageBinaryBuffers
                .importArray(data);
        /* Copy before queueing because the caller releases the borrowed
         * binary as soon as this callback returns. */
        final ByteBuffer[] ownedBuffers = StorageBinaryDataImporter.copyOwned(sourceBuffers);
        /* scheduleMaterialization owns cleanup on every rejection.  Releasing here
         * as well would double-free buffers when the worker has already drained its
         * queue after a terminal failure. */
        this.scheduleMaterialization(ownedBuffers);
    }

            /// Imports Aeron-owned direct buffers without a second native allocation.
    ///
    /// Must be called by the single transport delivery thread; see the
    /// class javadoc. Every precondition and the buffer extraction itself
    /// run inside the ownership cleanup block: once the assembler hands
    /// the binary over, a validation throw must still release the native
    /// buffers instead of leaking them. A `null` binary fails with
    /// [NullPointerException]; malformed buffers fail with
    /// [CorruptReplicationDataException].
    ///
    /// @param data complete binary whose direct buffers may be transferred
    /// @return always `true`; the receiver owns the buffers after return
    @Override
    public boolean receiveDataOwned(final Binary data) {
        ByteBuffer[] buffers = null;
        try {
            buffers = StorageBinaryBuffers.ownedArray(notNull(data));
            if (this.failure.get() != null) {
                throw this.failure.get();
            }
            if (this.disposed) {
                throw new ReplicationUnavailableException("Storage binary merger is disposed");
            }
            /* Import is deferred to the drained batch so replay amortizes
             * Store commits, fsyncs, materialization, and index refresh. */
        } catch (final RuntimeException | Error failure) {
            /* Ownership has not transferred to the deferred-materialization
             * queue on any of these paths. The Aeron callback contract still
             * requires this acceptor to release the transferred native
             * buffers. When extraction itself failed there is no normalized
             * array yet, so fall back to freeing whatever direct buffers the
             * binary still exposes. */
            try {
                if (buffers != null) StorageBinaryDataImporter.release(buffers);
                else StorageBinaryBuffers.releaseDirect(data);
            } catch (final RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        /* scheduleMaterialization now owns the buffers, including any cleanup when
         * executor submission or backpressure fails after queue admission. */
        this.scheduleMaterialization(buffers);
        return true;
    }

    private void scheduleMaterialization(final ByteBuffer[] ownedBuffers) {
        if (this.failure.get() != null) {
            StorageBinaryDataImporter.release(ownedBuffers);
            throw this.failure.get();
        }
        if (this.disposed) {
            StorageBinaryDataImporter.release(ownedBuffers);
            throw new ReplicationUnavailableException("Storage binary merger is disposed");
        }

        final long incomingBytes;
        try {
            long total = 0L;
            for (final ByteBuffer buffer : ownedBuffers) total = Math.addExact(total, buffer.remaining());
            incomingBytes = total;
        } catch (final ArithmeticException overflow) {
            StorageBinaryDataImporter.release(ownedBuffers);
            throw new IllegalArgumentException("Storage binary length overflows accounting", overflow);
        }
        if (incomingBytes > this.maxCachedBytes) {
            StorageBinaryDataImporter.release(ownedBuffers);
            throw new IllegalArgumentException(
                    "Storage binary of %s bytes exceeds the configured maxCachedBytes limit of %s bytes"
                            .formatted(incomingBytes, this.maxCachedBytes));
        }
        if (this.queue.residentWouldExceed(incomingBytes)) {
            try {
                this.awaitApplied();
            } catch (final RuntimeException | Error failure) {
                /* The buffers have not entered the queue yet, so this method still owns
                 * them when the pre-admission flush fails. Release them exactly once. */
                StorageBinaryDataImporter.release(ownedBuffers);
                throw failure;
            }
        }

        final long residentBytes;
        try {
            residentBytes = this.queue.admit(ownedBuffers, incomingBytes, this.workerSubmission);
        } catch (final RuntimeException | Error failure) {
            /* Admission either rejected before enqueueing or rolled its own
             * enqueue back on submission failure, so the buffers never
             * transferred to the queue on a thrown path. Release exactly once. */
            StorageBinaryDataImporter.release(ownedBuffers);
            throw failure;
        }

        if (this.queue.exceedsSoftLimit(residentBytes)) {
            /* Backpressure: wait only until the worker has drained the
             * queue back under the limit. Waiting for the worker task
             * itself would stall until the queue is fully empty and the
             * worker exited, serializing the delivery thread against the
             * entire backlog on every trigger. The worker is signalled
             * first when it is still in its coalescing delay. The wait is
             * bounded and retried: a hung materializer must fail the
             * merger instead of hanging the delivery thread forever,
             * while a merely slow one is given the retry budget. The
             * metric is payload bytes, not buffer count: a single
             * multi-buffer transaction must not bypass coalescing merely
             * because it carries many small channel buffers. */
            this.queue.awaitQueueDrainedBelowLimit();
        }
    }

    private Future<?> submitWorker() {
        return this.updateFuture = this.executor.submit(this.worker::runMaterializationWorker);
    }

    @Override
    public RuntimeException failure() {
        return this.failure.get();
    }

    /// Test-visible snapshot of the queued byte counter (queued but not yet drained).
    long queuedBytes() {
        return this.queue.queuedBytes();
    }

    /// Test-visible snapshot of the in-flight byte counter (held by the worker's batch).
    long inFlightBytes() {
        return this.queue.inFlightBytes();
    }

        /// Returns the graph coordinator this merger joins for its own Store reads.
    ///
    /// Node-owned read paths adopt the read side through the returned
    /// coordinator:
    ///
    /// ```java
    /// final StorageGraphCoordinator coordinator = merger.graphCoordinator();
    /// if (coordinator != null) coordinator.read(() -> { /* graph access */ });
    /// ```
    ///
    /// A `null` return means the merger was built without a coordinator: its
    /// scans run directly because there is no shared lock to join.
    ///
    /// @return per-Store graph coordinator, or `null` when unwired
    public StorageGraphCoordinator graphCoordinator() {
        return this.graphCoordinator;
    }

            /// Read-phase counterpart of [#readJoined(Supplier)] for scans
    /// that produce a plan for a later write-side mutation.
    private <T> T readJoined(final Supplier<T> read) {
        final StorageGraphCoordinator coordinator = this.graphCoordinator;
        return coordinator == null ? read.get() : coordinator.read(read);
    }

    /// Merges the writer's type dictionary: unknown types gain handlers,
    /// structurally conflicting types fail the merger terminally.
    ///
    /// Newly registered definitions are persisted immediately, before the
    /// transaction's binary is imported, so a restart can still resolve
    /// every imported type id. The first failure is recorded and rethrown
    /// on every later call; a failed merger never accepts more work.
    ///
    /// The remote snapshot is parsed before the materialization lock on a
    /// cached parsing foundation; only the local-conflict plan and the
    /// handler registration run inside the lock.
    ///
    /// @param typeDictionaryData writer's type dictionary snapshot
    @Override
    public void receiveTypeDictionary(final String typeDictionaryData) {
        if (this.failure.get() != null) {
            throw this.failure.get();
        }
        if (this.disposed) {
            throw new ReplicationUnavailableException("Storage binary merger is disposed");
        }
        final ArrayList<PersistenceTypeDefinition> remoteTypes =
                this.parseRemoteTypeDefinitions(Objects.requireNonNull(typeDictionaryData, "typeDictionaryData"));
        /* Dictionary registration mutates the same type handlers the worker
         * reads while materializing. Funnel the merge through the shared
         * materialization mutual exclusion so a delivery-thread merge can
         * never interleave with a worker batch. The worker never waits on
         * the delivery thread, so this wait cannot deadlock. */
        this.materialization.write(() ->
        {
            /* Read phase through the shared read side when a coordinator
             * is wired: conflict detection mutates nothing. The
             * worker cannot interleave — it needs the materialization lock
             * held here — so the plan stays valid until the write phase. */
            final ArrayList<PersistenceTypeDefinition> pending =
                    this.readJoined(() -> planDictionaryMerge(remoteTypes));
            if (pending.isEmpty()) return;
            /* Mutation phase through the update handler like
             * materialization: the materialization lock alone cannot
             * exclude application reads that joined the coordinator's read
             * side, so handler registration runs on the write side. Lock
             * order matches the worker — materialization lock outside,
             * coordinator inside — so the two paths cannot deadlock. */
            this.objectGraphUpdateHandler.objectGraphUpdateAvailable(() ->
                    applyDictionaryMerge(pending));
        });
    }

            /// Parses the writer's dictionary snapshot into remote definitions.
    ///
    /// Runs before the materialization lock on one cached parsing
    /// foundation: only the delivery thread parses, and the dedicated
    /// monitor guards the mutable snapshot holder if a transport ever
    /// retries from another thread. Failures latch the terminal merger
    /// failure.
    ///
    /// @param typeDictionaryData writer's type dictionary snapshot
    /// @return every remote definition, in dictionary iteration order
    private ArrayList<PersistenceTypeDefinition> parseRemoteTypeDefinitions(final String typeDictionaryData) {
        try {
            final PersistenceTypeDictionary remoteTypeDictionary;
            synchronized (this.dictionaryParseLock) {
                this.dictionarySource = typeDictionaryData;
                remoteTypeDictionary = this.dictionaryFoundation
                        .getTypeDictionaryProvider()
                        .provideTypeDictionary();
            }
            final ArrayList<PersistenceTypeDefinition> remoteTypes = new ArrayList<>();
            remoteTypeDictionary.iterateAllTypeDefinitions(remoteTypes::add);
            return remoteTypes;
        } catch (final RuntimeException | Error failure) {
            this.noteFailure("Store type dictionary update failed", failure);
            throw failure;
        }
    }

            /// Filters remote definitions against the local dictionary.
    ///
    /// Read-only: unknown remote types are collected, structurally
    /// conflicting types fail with [CorruptReplicationDataException]. Runs on
    /// the coordinator's read side when one is wired.
    ///
    /// @param remoteTypes parsed remote definitions
    /// @return remote definitions missing locally, in iteration order
    private ArrayList<PersistenceTypeDefinition> planDictionaryMerge(
            final ArrayList<PersistenceTypeDefinition> remoteTypes) {
        try {
            final PersistenceTypeDictionary localTypeDictionary =
                    this.storage.persistenceManager().typeDictionary();

            final ArrayList<PersistenceTypeDefinition> pending = new ArrayList<>();
            for (final PersistenceTypeDefinition remoteType : remoteTypes) {
                final PersistenceTypeDefinition localType =
                        localTypeDictionary.lookupTypeById(remoteType.typeId());
                if (localType == null) {
                    LOGGER.log(Level.DEBUG, "New type: %s".formatted(remoteType.typeName()));
                    pending.add(remoteType);
                } else if (!PersistenceTypeDescription.equalStructure(localType, remoteType)) {
                    throw new CorruptReplicationDataException(
                            "Remote type definition conflicts with local definition: %s <> %s"
                                    .formatted(localType, remoteType));
                }
            }
            return pending;
        } catch (final RuntimeException | Error failure) {
            this.noteFailure("Store type dictionary update failed", failure);
            throw failure;
        }
    }

            /// Registers planned remote definitions and persists them immediately.
    ///
    /// Runs on the update handler (the coordinator's write side), before
    /// the transaction's binary is imported, so a restart can still
    /// resolve every imported type id. Each registration is re-checked
    /// under the write side: application code may have registered the
    /// same type between the read-phase plan and this mutation, and a
    /// genuine structural conflict still fails terminally.
    ///
    /// @param pending remote definitions missing locally
    private void applyDictionaryMerge(final ArrayList<PersistenceTypeDefinition> pending) {
        try {
            final PersistenceTypeDictionary localTypeDictionary =
                    this.storage.persistenceManager().typeDictionary();
            for (final PersistenceTypeDefinition remoteType : pending) {
                final PersistenceTypeDefinition localType =
                        localTypeDictionary.lookupTypeById(remoteType.typeId());
                if (localType == null) {
                    this.foundation.getTypeHandlerManager().ensureTypeHandler(remoteType);
                } else if (!PersistenceTypeDescription.equalStructure(localType, remoteType)) {
                    throw new CorruptReplicationDataException(
                            "Remote type definition conflicts with local definition: %s <> %s"
                                    .formatted(localType, remoteType));
                }
            }
            /* A reader may import data without performing a local Store operation.
             * Persist newly registered remote definitions now, before the transaction's
             * binary is imported, so a restart can resolve every imported type id. */
            this.foundation.getTypeHandlerManager().exportPendingTypeDictionaryChanges();
        } catch (final RuntimeException | Error failure) {
            this.noteFailure("Store type dictionary update failed", failure);
            throw failure;
        }
    }

    /* MergerLifecycle seam methods are public only because Java interface
     * members cannot be narrower; the seam type itself is package-private. */
    @Override
    public boolean isDisposed() {
        return this.disposed;
    }

    @Override
    public void latchFailure(final RuntimeException failure) {
        this.failure.compareAndSet(null, failure);
    }

    @Override
    public void onMaterializationBudgetExpired(final long startedNanos) {
        if (this.queue.batchActiveSinceNanos() != startedNanos) return;
        final ReplicationUnavailableException terminal = new ReplicationUnavailableException(
                "Timed out while applying Store data after %s ms; the Store callback is still running"
                        .formatted(this.materializationBudgetMs));
        if (this.failure.compareAndSet(null, terminal)) {
            LOGGER.log(Level.ERROR, terminal.getMessage(), terminal);
            this.queue.signalAllWaiters();
        }
    }

    /// Shuts the materialization worker down, freeing native buffers only
    /// once the worker truly owns nothing.
    ///
    /// The worker gets an orderly window first, then an interrupt window;
    /// a timeout stays retryable instead of latching the merger into a
    /// state where later cleanups silently do nothing. Buffers are
    /// released only after termination is confirmed, never speculatively.
    @Override
    public void dispose() {
        /* A timeout is retryable: the worker may still own native buffers.  Do
         * not let the first failed attempt make every later cleanup a no-op.
         * Lifecycle needs no monitor: disposed is volatile, worker scheduling is
         * only touched under the queue lock, and the executor is thread-safe. */
        if (this.disposed && this.executor.isTerminated()) {
            this.queue.releaseAll();
            return;
        }
        this.disposed = true;
        this.executor.shutdown();
        boolean terminated = false;
        try {
            // if any external processes like Kubernetes shuts us down, it will wait for the externally set
            // grace period and then kill the process. But any other case we will await the task orderly like this.
            terminated = this.executor.awaitTermination(this.disposeOrderlyTimeoutMs, TimeUnit.MILLISECONDS);
            if (!terminated) {
                LOGGER.log(Level.WARNING, "Timed out waiting for storage graph updates; interrupting remaining work");
                this.executor.shutdownNow();
                terminated = this.executor.awaitTermination(this.disposeInterruptTimeoutMs, TimeUnit.MILLISECONDS);
                if (!terminated) {
                    throw new ReplicationUnavailableException(
                            "Storage graph update worker did not terminate; native buffers remain owned by it");
                }
            }
        } catch (final InterruptedException e) {
            this.executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new ReplicationUnavailableException("Interrupted while waiting for storage graph updates", e);
        } finally {
            this.watchdog.shutdownNow();
            if (terminated || this.executor.isTerminated()) this.queue.releaseAll();
        }
    }

    /// Blocks until the received binary is materialized into the object graph.
    ///
    /// This is the durability boundary behind replication cursors and
    /// acknowledgements: it bypasses the normal coalescing delay, which
    /// would otherwise add the full cache timeout to every commit, and
    /// drains under the worker's own lock. The worker is woken with a flag
    /// plus notification so no wake-up is lost, but never interrupted —
    /// it may be mid-materialization and an interrupt could leave the
    /// graph half-applied. A bounded wait that expires, or any worker
    /// failure, fails the merger terminally; only safely queued buffers
    /// are released, since the timed-out batch may still be owned.
    @Override
    public void awaitApplied() {
        if (this.failure.get() != null) {
            /* A worker can fail between receiveDataOwned() and this boundary.
             * Release buffers accepted after the worker's failure cleanup so the
             * assembler's ownership transfer cannot turn into a native leak. */
            this.queue.releaseAll();
            throw this.failure.get();
        }
        if (this.disposed) {
            throw new ReplicationUnavailableException("Storage binary merger is disposed");
        }
        /*
         * The normal merger deliberately delays materialization to coalesce updates.
         * A replication cursor/ACK, however, is a durability boundary: waiting for
         * the delayed task would add the full cache timeout to every Aeron commit.
         * Drain immediately under the same lock used by the worker. The delayed
         * worker is deliberately not interrupted: it may already be inside Store
         * materialization, and interrupting it can leave the object graph half-applied.
         */
        /* Wake a worker that is in its coalescing delay.  The flag avoids a lost
         * signal when the worker is between checking the flag and awaiting
         * the condition. */
        try {
            while (true) {
                if (this.queue.requestFlushUnlessDrained()) return;
                final Future<?> pending = this.updateFuture;
                if (pending == null) {
                    throw this.recordLifecycleFailure(
                            "Storage data is queued without a materialization worker",
                            new IllegalStateException("missing materialization future"));
                }
                this.awaitMaterialization(pending, "imported Store data materialization");
                final RuntimeException terminal = this.failure.get();
                if (terminal != null) {
                    throw new ExecutionException(terminal);
                }
                /* A completed future may have been replaced while a later
                 * admission scheduled the next worker. Recheck queue and
                 * in-flight ownership under their lock before declaring
                 * the durability boundary complete. */
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            this.noteFailure("Interrupted while waiting for imported Store data materialization", e);
            this.queue.releaseAll();
            throw new ReplicationUnavailableException(
                    "Interrupted while waiting for imported Store data materialization", e);
        } catch (final ExecutionException e) {
            final RuntimeException mergerFailure = this.failure.get();
            if (mergerFailure != null) {
                throw mergerFailure;
            }
            final RuntimeException terminal = this.recordFailure("Failed to materialize imported Store data", e.getCause() == null ? e : e.getCause());
            this.queue.releaseAll();
            throw terminal;
        } catch (final TimeoutException e) {
            final RuntimeException terminal = this.recordLifecycleFailure(
                    "Timed out waiting for imported Store data materialization", e);
            /* Only queued buffers are safe to release here. A callback may
             * still own the in-flight batch whose wait timed out. */
            this.queue.releaseAll();
            throw terminal;
        }
    }

    /// Waits for one materialization future with a bounded retry budget.
    ///
    /// Returns normally when the future completes. The caller handles
    /// interruption and execution failure immediately; only a timeout is
    /// retried, because the worker may simply be slower than the configured
    /// budget while still making progress. When the budget expires the last
    /// [TimeoutException] is rethrown for the caller to record and clean up.
    ///
    /// @param pending   materialization worker future
    /// @param operation operation name for retry diagnostics
    /// @throws InterruptedException if the waiting thread is interrupted
    /// @throws ExecutionException   if the worker failed
    /// @throws TimeoutException     if the retry budget expired
    private void awaitMaterialization(final Future<?> pending, final String operation)
            throws InterruptedException, ExecutionException, TimeoutException {
        int retries = APPLY_TIMEOUT_RETRIES;
        while (true) {
            try {
                pending.get(this.applyTimeoutMs, TimeUnit.MILLISECONDS);
                return;
            } catch (final TimeoutException timeout) {
                final RuntimeException terminal = this.failure.get();
                if (terminal != null) {
                    throw new ExecutionException(terminal);
                }
                if (retries-- <= 0) {
                    throw timeout;
                }
                LOGGER.log(Level.WARNING, "%s did not finish within %s ms; retrying (%s retries left)".formatted(operation, this.applyTimeoutMs, retries + 1));
            }
        }
    }

    /* Records the first failure without returning it, for paths that rethrow
     * the original cause to preserve its type and stack. See [#recordFailure]
     * for paths that throw the canonical first failure. Part of the
     * package-only MergerLifecycle seam; public only because interface
     * members cannot be narrower. */
    @Override
    public void noteFailure(final String message, final Throwable cause) {
        this.failure.compareAndSet(null, new IllegalStateException(message, cause));
    }

    /* Records the first wait/interrupt/timeout failure and returns it; later
     * failures are dropped so concurrent paths cannot overwrite the root
     * cause. */
    @Override
    public RuntimeException recordLifecycleFailure(final String message, final Throwable cause) {
        this.failure.compareAndSet(null, new ReplicationUnavailableException(message, cause));
        return this.failure.get();
    }

    /* Records the first failure and returns it; later failures are dropped so
     * concurrent paths cannot overwrite the root cause. Callers throw the
     * result themselves, which is why this method only records. */
    private RuntimeException recordFailure(final String message, final Throwable cause) {
        this.noteFailure(message, cause);
        return this.failure.get();
    }
}
