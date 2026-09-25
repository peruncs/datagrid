package peruncs.cluster.storage.binary;

import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.storage.index.ClusterIndexMaintenance;

import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/// Runs the merger's single coalescing worker loop.
///
/// One worker thread waits out the coalescing delay, drains the whole queue
/// into reused scratch arrays, replays the Store import, materializes the
/// object graph transaction by transaction, refreshes the index barrier, and
/// releases the native batch exactly once. The loop never allocates on its
/// steady-state path: the drain scratch grows once to the largest batch seen
/// and every error is captured into the owner's terminal failure latch.
final class ApplyWorker {
    private static final System.Logger LOGGER =
            System.getLogger(StorageBinaryDataMerger.class.getName());
    private final MergerLifecycle owner;
    private final ApplyQueue queue;
    private final LockedExecutor materialization;
    private final ScheduledExecutorService watchdog;
    private final BinaryPersistenceFoundation<?> foundation;
    private final StorageConnection storage;
    private final StorageBinaryDataMaterializer dataMaterializer = new StorageBinaryDataMaterializer();
    private final ClusterIndexMaintenance indexMaintenance = new ClusterIndexMaintenance();
    private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
    private final long cachingTimeoutMs;
    private final int maxValidatedIndexObjects;
    /* The worker's own structured-scope deadline covers the caller's whole
     * retry budget, so a slow-but-progressing batch is absorbed by the
     * caller's retries instead of being declared terminal by the worker on
     * the first per-attempt expiry. */
    private final long materializationBudgetMs;
    /* The index refresh scans the whole store, so its bound is a multiple of
     * the materialization budget rather than the same value: a large,
     * progressing rebuild deserves headroom, while a wedged one still fails
     * bounded. */
    static final long INDEX_REFRESH_BUDGET_MULTIPLIER = 10L;
    private final long indexRefreshBudgetMs;
    /* Reused batch drain: only ever touched under the materialization
     * lock, which serializes the worker drain and any await-thread drain,
     * so no further synchronization is needed. The populated prefix holds
     * the batch; slots past it are always `null`. Grows to the largest
     * batch seen and stays there. */
    private final Drain drain = new Drain();
    /* Phase stamps for the batch budget. The materialization budget is
     * batch-proportional and stops at the materialization boundary; the
     * index refresh after it scans the whole store and gets a budget of its
     * own, measured from that same boundary. Both stamps are guarded by
     * `budgetLock` because the watchdog thread reads them: a watchdog that
     * fires exactly when a phase completes must lose, so the cancel and the
     * stamp run inside the same critical section the expiry check reads. */
    private final Object budgetLock = new Object();
    private long materializedAtNanos;
    private long indexRefreshedAtNanos;

    ApplyWorker(
            final MergerLifecycle owner,
            final ApplyQueue queue,
            final LockedExecutor materialization,
            final ScheduledExecutorService watchdog,
            final BinaryPersistenceFoundation<?> foundation,
            final StorageConnection storage,
            final ObjectGraphUpdateHandler objectGraphUpdateHandler,
            final long cachingTimeoutMs,
            final int maxValidatedIndexObjects,
            final long materializationBudgetMs) {
        this.owner = owner;
        this.queue = queue;
        this.materialization = materialization;
        this.watchdog = watchdog;
        this.foundation = foundation;
        this.storage = storage;
        this.objectGraphUpdateHandler = objectGraphUpdateHandler;
        this.cachingTimeoutMs = cachingTimeoutMs;
        this.maxValidatedIndexObjects = maxValidatedIndexObjects;
        this.materializationBudgetMs = materializationBudgetMs;
        try {
            this.indexRefreshBudgetMs = Math.multiplyExact(materializationBudgetMs, INDEX_REFRESH_BUDGET_MULTIPLIER);
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "materializationBudgetMs is too large for the index-refresh bound: %s".formatted(materializationBudgetMs),
                    overflow);
        }
    }

    /// Coalesces and applies queued batches until the queue runs dry.
    ///
    /// A throwable latches the merger's terminal failure unless shutdown
    /// already interrupted the worker — disposal interrupts are released
    /// quietly because the buffers still belong to the merger, not to a
    /// failed batch.
    void runMaterializationWorker() {
        try {
            this.queue.awaitFlushRequestOrTimeout(this.cachingTimeoutMs);
            while (true) {
                if (this.queue.unscheduleWorkerIfEmpty()) {
                    return;
                }
                /* Drain ownership under the queue lock, then release it before
                 * materializing. Store graph updates may block or complete on another
                 * executor; holding queueLock across that wait would stall the
                 * Aeron polling thread and make a slow Store look like transport loss. */
                this.applyData();
            }
        } catch (final Throwable t) {
            if (this.owner.isDisposed() && Thread.currentThread().isInterrupted()) {
                LOGGER.log(Level.DEBUG,
                        "Storage binary merger worker released pending data during disposal");
                this.queue.releaseAll();
                return;
            }
            final RuntimeException normalized = t instanceof RuntimeException runtime
                    ? runtime : new IllegalStateException("Storage binary merger failed", t);
            this.owner.latchFailure(normalized);
            LOGGER.log(Level.ERROR, "Storage binary merger failed", this.owner.failure());
            this.queue.releaseAll();
            if (t instanceof Error error) throw error;
            throw normalized;
        }
    }

    private void applyData() {
        this.materialization.write(() ->
        {
            /* Drain into the reused scratch array: no list, no exact-size
             * copy, no fork, and no per-batch Duration or scope — steady
             * state allocates nothing on the heap on this path. The
             * scratch grows once to the largest batch seen. */
            final int pending = this.queue.drainInto(this.drain);
            if (pending == 0) return;
            final int pendingTransactions = this.drain.transactionCount;
            final long startedNanos = this.drain.startedNanos;
            final long batchBytes = this.drain.batchBytes;

            /* Materialize synchronously on this thread: no structured
             * child can retain the drained buffers past the finally below,
             * so ownership is always reclaimed — on success, on failure,
             * and when the batch overruns its budget. A bounded overall
             * timeout is kept as elapsed detection rather than preemption:
             * interrupting a wedged Store mid-materialization could leave
             * the graph half-applied, so an overrun only marks the merger
             * failed after the buffers are already freed. A handler that
             * never returns still pins this worker; dispose() bounds that
             * wait and reports the pinned buffers instead of freeing them
             * underneath the Store. */
            synchronized (this.budgetLock) {
                this.materializedAtNanos = 0L;
                this.indexRefreshedAtNanos = 0L;
            }
            final ScheduledFuture<?> materializationWatchdog = this.watchdog.schedule(
                    () -> this.materializationBudgetExpired(startedNanos),
                    this.materializationBudgetMs,
                    TimeUnit.MILLISECONDS);
            try {
                this.objectGraphUpdateHandler.objectGraphUpdateAvailable(() ->
                {
                    /* One coordinator write section covers import,
                     * materialization, validation, and index refresh. Application reads joining the read
                     * side observe either the pre-batch or the post-batch
                     * boundary — never a materialized graph with stale
                     * search views. */
                    int transactionOffset = 0;
                    for (int index = 0; index < pendingTransactions; index++) {
                        final int transactionLength = this.drain.transactionLengths[index];
                        this.drain.ensureViews(transactionLength);
                        StorageBinaryDataImporter.importDirect(
                                this.storage, this.drain.buffers, transactionOffset, transactionLength,
                                this.drain.views);
                        transactionOffset += transactionLength;
                    }
                    /* Reader-side index maintenance follows the import: imports
                     * materialize entities without the map API, so no
                     * index group observes them and both search views
                     * freeze at the first query. Retirement runs before
                     * the swap, not after: closing the Lucene writer over
                     * the commit point it references deletes nothing,
                     * while closing it after the swap deletes the
                     * replicated files it never created — including the
                     * commit point — and the next reopen wipes the rest.
                     * The swap then lands in a view-less index and the
                     * next query reopens over the current files. Runs
                     * only for non-empty batches: applyData returns early
                     * when idle. */
                    this.indexMaintenance.beforeApply(
                            this.storage, this.drain.buffers, pending,
                            this.maxValidatedIndexObjects);
                    transactionOffset = 0;
                    for (int index = 0; index < pendingTransactions; index++) {
                        final int transactionLength = this.drain.transactionLengths[index];
                        /* Serializer's loader treats one source response as
                         * one version per object. Preserve Store commit
                         * boundaries during graph application too: replay
                         * batches commonly contain consecutive versions of
                         * the same object. Index views are retired first so
                         * GigaMap's reloaded index state cannot retain a
                         * view over the pre-import files. */
                        this.dataMaterializer.materialize(
                                this.foundation, this.storage, this.drain.buffers,
                                transactionOffset, transactionLength);
                        transactionOffset += transactionLength;
                    }
                    /* Materialization ends here: its batch-proportional budget
                     * is spent, and the index refresh below — a whole-store
                     * scan plus the vector rebuild — gets a separately bounded
                     * phase so a large progressing rebuild is never charged
                     * against the materialization budget. Stamp and cancel
                     * under the budget lock so a concurrent expiry loses the
                     * race deterministically instead of latching a completed
                     * phase. */
                    synchronized (this.budgetLock) {
                        this.materializedAtNanos = System.nanoTime();
                        materializationWatchdog.cancel(false);
                    }
                    final ScheduledFuture<?> refreshWatchdog = this.watchdog.schedule(
                            () -> this.refreshBudgetExpired(startedNanos),
                            this.indexRefreshBudgetMs,
                            TimeUnit.MILLISECONDS);
                    try {
                        /* Reader-side index enforcement plus the vector
                         * rebuild share one root-graph traversal: a writer that
                         * smuggled an external Lucene directory or a
                         * non-persisted vector index past registration fails
                         * this reader closed instead of diverging it, and any
                         * vector graph cleared above is rebuilt eagerly in the
                         * same section. The scan visits index metadata only,
                         * never entity payload, and the rebuild is skipped
                         * entirely when the store has no vector indices. The
                         * eager rebuild keeps the deadlock-avoidance invariant
                         * documented on the maintenance entry point: a lazy
                         * rebuild on the next query would race the following
                         * batch's bulk materialization. */
                        this.indexMaintenance.afterApply(this.storage, this.maxValidatedIndexObjects);
                    } finally {
                        synchronized (this.budgetLock) {
                            this.indexRefreshedAtNanos = System.nanoTime();
                            refreshWatchdog.cancel(false);
                        }
                    }
                });
            } catch (final RuntimeException | Error failure) {
                /* A genuine failure says failed; only an overrun says timed
                 * out. The two are never conflated into one message. */
                /* A failed batch must not leave its half-planned index
                 * scratch populated: the pinned entries retain the affected
                 * indexes and their reachable graphs across the latched
                 * terminal failure. */
                this.indexMaintenance.resetScratch();
                if (failure instanceof RuntimeException runtime) {
                    this.owner.noteFailure("Store graph update failed", runtime);
                }
                throw failure;
            } finally {
                materializationWatchdog.cancel(false);
                try {
                    StorageBinaryDataImporter.release(this.drain.buffers, pending);
                } finally {
                    this.drain.clear();
                    this.queue.completeInFlight(batchBytes);
                }
            }
            this.verifyBatchBudgets(startedNanos);
        });
    }

    /// Latches a materialization-phase overrun unless the phase already ended.
    ///
    /// The stamp check runs under the budget lock, so a watchdog that fires
    /// concurrently with the phase boundary always resolves to one verdict.
    ///
    /// @param startedNanos nanoTime stamp of the batch being applied
    void materializationBudgetExpired(final long startedNanos) {
        synchronized (this.budgetLock) {
            if (this.materializedAtNanos != 0L) return;
        }
        this.owner.onMaterializationBudgetExpired(startedNanos);
    }

    /// Latches an index-refresh-phase overrun unless the phase already ended.
    ///
    /// @param startedNanos        nanoTime stamp of the batch being applied
    /// @param rebuildStartedNanos nanoTime stamp at the start of the index phase
    void refreshBudgetExpired(final long startedNanos) {
        synchronized (this.budgetLock) {
            if (this.indexRefreshedAtNanos != 0L) return;
        }
        this.owner.onRefreshBudgetExpired(startedNanos, this.indexRefreshBudgetMs);
    }

    /// Checks each completed phase against its own budget after the batch returns.
    ///
    /// Materialization is batch-proportional and bounded from batch start to
    /// the materialized stamp; the index refresh, which scans the whole
    /// store, is bounded from that stamp to its own completion. A batch whose
    /// handler never ran the updater (no stamp) falls back to charging the
    /// whole elapsed time against the materialization budget. A batch that
    /// threw before stamping already failed through the caller's catch.
    ///
    /// @param startedNanos nanoTime stamp of the batch being applied
    void verifyBatchBudgets(final long startedNanos) {
        final long materializedAt;
        final long refreshedAt;
        synchronized (this.budgetLock) {
            materializedAt = this.materializedAtNanos;
            refreshedAt = this.indexRefreshedAtNanos;
        }
        final long materializedElapsedMs = TimeUnit.NANOSECONDS.toMillis(
                (materializedAt == 0L ? System.nanoTime() : materializedAt) - startedNanos);
        if (materializedElapsedMs > this.materializationBudgetMs) {
            final ReplicationUnavailableException terminal = new ReplicationUnavailableException(
                    "Timed out while applying Store data: batch took %s ms with a budget of %s ms"
                            .formatted(materializedElapsedMs, this.materializationBudgetMs));
            this.owner.noteFailure(terminal.getMessage(), terminal);
            throw terminal;
        }
        if (materializedAt != 0L && refreshedAt != 0L) {
            /* Bounds the refresh phase only: the vector rebuild scans the
             * whole store, so including the import and materialization time
             * would fail a healthy large import. */
            final long refreshElapsedMs = TimeUnit.NANOSECONDS.toMillis(refreshedAt - materializedAt);
            if (refreshElapsedMs > this.indexRefreshBudgetMs) {
                final ReplicationUnavailableException terminal = new ReplicationUnavailableException(
                        "Timed out while refreshing reader index views: refresh took %s ms with a budget of %s ms"
                                .formatted(refreshElapsedMs, this.indexRefreshBudgetMs));
                this.owner.noteFailure(terminal.getMessage(), terminal);
                throw terminal;
            }
        }
    }

    /// Reused worker-confined storage for one drained batch.
    ///
    /// The populated prefix holds the batch; slots past it are always
    /// `null` or zero. Grows to the largest batch seen and stays there, so
    /// the steady-state apply path allocates nothing.
    static final class Drain {
        ByteBuffer[] buffers = new ByteBuffer[16];
        ByteBuffer[] views = new ByteBuffer[0];
        int[] transactionLengths = new int[16];
        int bufferCount;
        int transactionCount;
        long batchBytes;
        long startedNanos;

        void ensureCapacity(final int buffersNeeded, final int transactionsNeeded) {
            if (buffersNeeded > this.buffers.length) {
                this.buffers = new ByteBuffer[buffersNeeded];
            }
            if (transactionsNeeded > this.transactionLengths.length) {
                this.transactionLengths = new int[transactionsNeeded];
            }
        }

        void ensureViews(final int transactionLength) {
            /* Exact sizing is a Store-API constraint, not an oversight: the
             * importer hands the whole views array to the Store import, so a
             * grow-only array would smuggle a stale null tail into it. */
            if (this.views.length != transactionLength) {
                this.views = new ByteBuffer[transactionLength];
            }
        }

        void clear() {
            Arrays.fill(this.buffers, 0, this.bufferCount, null);
            Arrays.fill(this.transactionLengths, 0, this.transactionCount, 0);
        }
    }
}
