package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.typing.Disposable;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static java.lang.System.Logger.Level.WARNING;
import static org.eclipse.serializer.util.X.notNull;

/// Applies committed Store binary data on a reader node.
///
/// Incoming buffers are imported into the local Store immediately and then
/// coalesced for object-graph updates on a bounded single-thread executor.
/// Providers must call this merger only after their transport-specific commit
/// validation has completed.
///
/// The implementation holds two locks with one explicit order: queue draining
/// runs inside the materialization {@code LockedExecutor} and then takes
/// {@code queueLock}; no path acquires them in the reverse order. The
/// coalescing-delay condition belongs to {@code queueLock} — a condition must
/// bind to its guarding lock, which {@code LockedExecutor} does not expose,
/// so the queue lock stays explicit — and lifecycle uses only volatiles plus
/// the executor's own thread safety. Store and graph callbacks run after both
/// locks have been released. A single merged lock is deliberately not used:
/// draining must release the queue admission lock before a blocking graph
/// update, or a slow Store would stall the Aeron polling thread and look like
/// transport loss.
public interface StorageBinaryDataMerger extends StorageBinaryDataReceiver, Disposable {
        /// Immutable configuration for one bounded binary merger.
    ///
    /// The coordinator is optional. When present, post-materialization graph
    /// scans join its read/write boundary; when absent, the merger preserves
    /// the direct-scan behavior used by standalone storage clients.
    ///
    /// @param foundation               persistence foundation
    /// @param storage                  Store connection
    /// @param objectGraphUpdateHandler graph update handler
    /// @param cachingTimeoutMs         maximum wait for a cached batch
    /// @param cachedBinaryLimit        maximum cached binary count
    /// @param applyTimeoutMs           maximum wait for one materialization batch
    /// @param graphCoordinator         per-Store graph coordinator, or `null`
    record Configuration(
            BinaryPersistenceFoundation<?> foundation,
            StorageConnection storage,
            ObjectGraphUpdateHandler objectGraphUpdateHandler,
            long cachingTimeoutMs,
            long cachedBinaryLimit,
            long applyTimeoutMs,
            StorageGraphCoordinator graphCoordinator
    ) {
        /// Validates the merger collaborators and timing limits.
        public Configuration {
            Objects.requireNonNull(foundation, "foundation");
            Objects.requireNonNull(storage, "storage");
            Objects.requireNonNull(objectGraphUpdateHandler, "objectGraphUpdateHandler");
            if (cachingTimeoutMs < 0L) throw new IllegalArgumentException("cachingTimeoutMs must not be negative");
            if (cachedBinaryLimit <= 0L) throw new IllegalArgumentException("cachedBinaryLimit must be positive");
            if (applyTimeoutMs <= 0L) throw new IllegalArgumentException("applyTimeoutMs must be positive");
        }

        /// Starts a builder for a merger configuration.
        ///
        /// @return empty configuration builder
        public static Builder builder() {
            return new Builder();
        }

        /// Builds a merger configuration without a positional parameter list.
        public static final class Builder {
            private BinaryPersistenceFoundation<?> foundation;
            private StorageConnection storage;
            private ObjectGraphUpdateHandler objectGraphUpdateHandler;
            private long cachingTimeoutMs;
            private long cachedBinaryLimit;
            private long applyTimeoutMs;
            private StorageGraphCoordinator graphCoordinator;

            /// @param value persistence foundation
            /// @return this builder
            public Builder foundation(final BinaryPersistenceFoundation<?> value) {
                this.foundation = value;
                return this;
            }

            /// @param value Store connection
            /// @return this builder
            public Builder storage(final StorageConnection value) {
                this.storage = value;
                return this;
            }

            /// @param value graph update handler
            /// @return this builder
            public Builder objectGraphUpdateHandler(final ObjectGraphUpdateHandler value) {
                this.objectGraphUpdateHandler = value;
                return this;
            }

            /// @param value maximum cache wait in milliseconds
            /// @return this builder
            public Builder cachingTimeoutMs(final long value) {
                this.cachingTimeoutMs = value;
                return this;
            }

            /// @param value maximum cached binary count
            /// @return this builder
            public Builder cachedBinaryLimit(final long value) {
                this.cachedBinaryLimit = value;
                return this;
            }

            /// @param value maximum materialization wait in milliseconds
            /// @return this builder
            public Builder applyTimeoutMs(final long value) {
                this.applyTimeoutMs = value;
                return this;
            }

            /// @param value optional Store graph coordinator
            /// @return this builder
            public Builder graphCoordinator(final StorageGraphCoordinator value) {
                this.graphCoordinator = value;
                return this;
            }

            /// @return immutable merger configuration
            public Configuration build() {
                return new Configuration(foundation, storage, objectGraphUpdateHandler,
                        cachingTimeoutMs, cachedBinaryLimit, applyTimeoutMs, graphCoordinator);
            }
        }
    }

        /// Creates a merger with bounded deferred materialization.
    ///
    /// @param configuration immutable merger configuration
    /// @return binary merger
    static StorageBinaryDataMerger New(final Configuration configuration) {
        final Configuration settings = notNull(configuration);
        return new Default(
                notNull(settings.foundation()),
                notNull(settings.storage()),
                notNull(settings.objectGraphUpdateHandler()),
                settings.cachingTimeoutMs(),
                settings.cachedBinaryLimit(),
                settings.applyTimeoutMs(),
                settings.graphCoordinator()
        );
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
    /// scans run directly because there is no shared lock to join. Custom
    /// merger implementations without a coordinator likewise return `null`
    /// and run reads directly.
    ///
    /// @return per-Store graph coordinator, or `null` when unwired
    default StorageGraphCoordinator graphCoordinator() {
        return null;
    }

        /// Returns an asynchronous materialization failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    default RuntimeException failure() {
        return null;
    }

        /// Supplies conservative defaults for deferred object-graph application.
    interface Defaults {
                /// Default cache timeout in milliseconds.
        long CACHING_TIMEOUT_MS = 10_000L;
                /// Default cached binary count.
        long CACHING_LIMIT = 50L;
                /// Default maximum wait for one materialization batch, in milliseconds.
        long APPLY_TIMEOUT_MS = 60_000L;
    }

        /// Applies imported data on one bounded worker and reports failures.
    class Default implements StorageBinaryDataMerger {
        private static final System.Logger LOGGER = System.getLogger(StorageBinaryDataMerger.class.getName());
        private static final long MAX_CACHED_BYTES = 1L << 30;
        /* One slow batch must not brick the reader: a GC pause or a slow disk
         * can exceed the apply timeout while the worker is still progressing.
         * The bounded wait is retried this many times before latching a
         * terminal failure; a genuinely wedged worker still fails after the
         * budget, so shutdown stays bounded. */
        private static final int APPLY_TIMEOUT_RETRIES = 2;
        private final ExecutorService executor = Executors.newSingleThreadExecutor(Thread.ofVirtual()
                .name("eclipse-datagrid-store-materializer", 0L)
                .factory());
        /* Every access runs under queueLock (admission, drain, release, and
         * flush checks), so an ArrayDeque is sufficient and avoids the
         * per-node allocation a concurrent queue pays on every offer. */
        private final ArrayDeque<ByteBuffer> cachedData = new ArrayDeque<>();
        /* Queue admission and object-graph materialization are separate concerns.
         * The worker and awaitApplied() can run concurrently, but a Store update
         * must never materialize two batches at once or callbacks can observe and
         * mutate the graph out of order. */
        private final ReentrantLock queueLock = new ReentrantLock();
        private final Condition flushCondition = this.queueLock.newCondition();
        private final LockedExecutor materialization = LockedExecutor.New();
        private final BinaryPersistenceFoundation<?> foundation;
        private final StorageConnection storage;
        private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
        private final StorageGraphCoordinator graphCoordinator;
        private final long cachingTimeoutMs;
        private final long cacheLimit;
        private final long applyTimeoutMs;
        /* The worker's own structured-scope deadline covers the caller's whole
         * retry budget, so a slow-but-progressing batch is absorbed by the
         * caller's retries instead of being declared terminal by the worker on
         * the first per-attempt expiry. */
        private final long materializationBudgetMs;
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        /* Guarded by queueLock on every mutation; readers snapshot it under the
         * same lock, so no atomics are needed. */
        private long cachedBufferCount;
        private volatile boolean flushRequested;
        private volatile boolean disposed;
        private boolean workerScheduled;
        private long cachedBytes;
        private volatile Future<?> updateFuture;
        /* Reused batch drain: only ever touched under the materialization
         * lock, which serializes the worker drain and any await-thread drain,
         * so no further synchronization is needed. The populated prefix holds
         * the batch; slots past it are always `null`. Grows to the largest
         * batch seen and stays there. */
        private ByteBuffer[] drainBuffers = new ByteBuffer[16];

        private Default(
                final BinaryPersistenceFoundation<?> foundation,
                final StorageConnection storage,
                final ObjectGraphUpdateHandler objectGraphUpdateHandler,
                final long cachingTimeoutMs,
                final long cacheLimit,
                final long applyTimeoutMs,
                final StorageGraphCoordinator graphCoordinator
        ) {
            this.foundation = foundation;
            this.storage = storage;
            this.objectGraphUpdateHandler = objectGraphUpdateHandler;
            this.graphCoordinator = graphCoordinator;
            this.cachingTimeoutMs = cachingTimeoutMs;
            this.cacheLimit = cacheLimit;
            this.applyTimeoutMs = applyTimeoutMs;
            try {
                this.materializationBudgetMs = Math.multiplyExact(applyTimeoutMs, APPLY_TIMEOUT_RETRIES + 1L);
            } catch (final ArithmeticException overflow) {
                throw new IllegalArgumentException("applyTimeoutMs is too large: %s".formatted(applyTimeoutMs), overflow);
            }
        }

                /// Aeron transfers its assembled direct buffers before this callback starts.
        ///
        /// @return `true` because this merger releases the transferred buffers
        @Override
        public boolean canReceiveDataOwned() {
            return true;
        }

        @Override
        public void receiveData(final Binary data) {
            if (this.failure.get() != null) {
                throw new IllegalStateException("Storage binary merger has failed", this.failure.get());
            }
            if (this.disposed) {
                /* A disposed receiver must not acknowledge data. Returning normally
                 * would let the Aeron assembler advance its cursor even though the Store
                 * binary was discarded. */
                throw new IllegalStateException("Storage binary merger is disposed");
            }
            final ByteBuffer[] sourceBuffers = StorageBinaryDataChunker
                    .importArray(data);
            /* Serialize the Store import with deferred materialization: both
             * mutate the same persistence and type-handler state, and importing
             * during a materialization can expose a partially imported
             * transaction. This is the same exclusion the dictionary path uses. */
            final ByteBuffer[] ownedBuffers = this.materialization.write(
                    () -> StorageBinaryDataImporter.importOwned(this.storage, sourceBuffers));
            /* scheduleMaterialization owns cleanup on every rejection.  Releasing here
             * as well would double-free buffers when the worker has already drained its
             * queue after a terminal failure. */
            this.scheduleMaterialization(ownedBuffers);
        }

                /// Imports Aeron-owned direct buffers without a second native allocation.
        ///
        /// Every precondition and the buffer extraction itself run inside the
        /// ownership cleanup block: once the assembler hands the binary over, a
        /// validation throw must still release the native buffers instead of
        /// leaking them. A `null` binary fails with [NullPointerException];
        /// malformed buffers fail with [StorageBinaryDataException].
        @Override
        public boolean receiveDataOwned(final Binary data) {
            ByteBuffer[] buffers = null;
            try {
                buffers = StorageBinaryDataChunker.ownedArray(notNull(data));
                if (this.failure.get() != null) {
                    throw new IllegalStateException("Storage binary merger has failed", this.failure.get());
                }
                if (this.disposed) {
                    throw new IllegalStateException("Storage binary merger is disposed");
                }
                /* Same exclusion as the borrowed-copy path: the Store import must
                 * not interleave with a deferred materialization. */
                final ByteBuffer[] imported = buffers;
                this.materialization.write(() ->
                {
                    StorageBinaryDataImporter.importDirect(this.storage, imported);
                });
            } catch (final RuntimeException | Error failure) {
                /* Ownership has not transferred to the deferred-materialization
                 * queue on any of these paths. The Aeron callback contract still
                 * requires this acceptor to release the transferred native
                 * buffers. When extraction itself failed there is no normalized
                 * array yet, so fall back to freeing whatever direct buffers the
                 * binary still exposes. */
                try {
                    if (buffers != null) StorageBinaryDataImporter.release(buffers);
                    else StorageBinaryDataChunker.releaseDirect(data);
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
                throw new IllegalStateException("Storage binary merger has failed", this.failure.get());
            }
            if (this.disposed) {
                StorageBinaryDataImporter.release(ownedBuffers);
                throw new IllegalStateException("Storage binary merger is disposed");
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
            if (incomingBytes > MAX_CACHED_BYTES) {
                StorageBinaryDataImporter.release(ownedBuffers);
                throw new IllegalArgumentException("Storage binary exceeds the 1 GiB materialization limit");
            }
            final boolean drainFirst;
            this.queueLock.lock();
            try {
                final long projected = Math.addExact(this.cachedBytes, incomingBytes);
                drainFirst = this.cachedBytes > 0 && projected > MAX_CACHED_BYTES;
            } finally {
                this.queueLock.unlock();
            }
            if (drainFirst) {
                try {
                    this.awaitApplied();
                } catch (final RuntimeException | Error failure) {
                    /* The buffers have not entered cachedData yet, so this method still owns
                     * them when the pre-admission flush fails. Release them exactly once. */
                    StorageBinaryDataImporter.release(ownedBuffers);
                    throw failure;
                }
            }

            boolean queued = false;
            long bufferedCount;
            try {
                this.queueLock.lock();
                try {
                    /* The worker can fail between the entry check above and this
                     * ownership hand-off.  Reject before enqueueing so a caller never
                     * loses the native buffers into a dead queue. */
                    if (this.failure.get() != null) {
                        throw new IllegalStateException("Storage binary merger has failed", this.failure.get());
                    }
                    if (this.disposed) {
                        throw new IllegalStateException("Storage binary merger is disposed");
                    }
                    final long projectedBytes = Math.addExact(this.cachedBytes, incomingBytes);
                    if (projectedBytes > MAX_CACHED_BYTES) {
                        throw new IllegalStateException("Storage binary materialization cache is full");
                    }
                    /* Bulk add without a wrapper allocation: Collections.addAll
                     * passes the array straight through to per-element add. */
                    Collections.addAll(this.cachedData, ownedBuffers);
                    this.cachedBufferCount += ownedBuffers.length;
                    this.cachedBytes = projectedBytes;
                    queued = true;
                    bufferedCount = this.cachedBufferCount;
                    if (!this.workerScheduled) {
                        try {
                            this.workerScheduled = true;
                            this.updateFuture = this.executor.submit(this::runMaterializationWorker);
                        } catch (final RuntimeException | Error failure) {
                            /* Submission happens under queueLock, so the worker cannot have
                             * removed these newly queued buffers yet. Buffers
                             * compare by content, so removal must be by identity:
                             * a content-equal stranger must never be dequeued. */
                            for (final ByteBuffer buffer : ownedBuffers) {
                                removeIdentical(this.cachedData, buffer);
                            }
                            this.cachedBytes = Math.subtractExact(this.cachedBytes, incomingBytes);
                            this.cachedBufferCount -= ownedBuffers.length;
                            queued = false;
                            /* A shutdown racing this admission must surface as the
                             * documented disposal refusal, not a raw executor
                             * rejection. */
                            if (failure instanceof RejectedExecutionException && this.disposed) {
                                throw new IllegalStateException("Storage binary merger is disposed", failure);
                            }
                            throw failure;
                        }
                    }
                } finally {
                    this.queueLock.unlock();
                }

                if (bufferedCount > this.cacheLimit) {
                    try {
                        /* The worker is signalled before this wait when it is still in its
                         * coalescing delay. The wait is bounded and retried a small number
                         * of times: a hung materializer must fail the merger instead of
                         * hanging the delivery thread forever, while a merely slow one is
                         * given the retry budget before the reader is failed. */
                        this.awaitMaterialization(this.updateFuture, "import data task");
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting for import data task", e);
                    } catch (final TimeoutException e) {
                        /* The batch is already queued, so the worker still owns it:
                         * release nothing here. Latch the terminal failure so no
                         * further batches are admitted; the worker drains or fails
                         * on its own while dispose() still joins it. */
                        throw this.recordFailure("Timed out waiting for import data task", e);
                    } catch (final ExecutionException e) {
                        final RuntimeException mergerFailure = this.failure.get();
                        if (mergerFailure != null) {
                            throw new IllegalStateException("Storage binary merger has failed", mergerFailure);
                        }
                        throw new IllegalStateException("Storage binary merger task failed", e.getCause());
                    }
                }
            } catch (final RuntimeException | Error failure) {
                if (!queued) {
                    StorageBinaryDataImporter.release(ownedBuffers);
                }
                /* Once queued, the worker or releaseCachedData owns the buffers.  Releasing
                 * them here would race applyDataSafely and double-deallocate native memory. */
                throw failure;
            }
        }

        private void runMaterializationWorker() {
            try {
                this.awaitFlushRequestOrTimeout();
                while (true) {
                    this.queueLock.lock();
                    try {
                        if (this.cachedData.isEmpty()) {
                            this.workerScheduled = false;
                            return;
                        }
                    } finally {
                        this.queueLock.unlock();
                    }
                    /* Drain ownership under the queue lock, then release it before
                     * materializing. Store graph updates may block or complete on another
                     * executor; keeping queueLock across that wait previously stalled the
                     * Aeron polling thread and made a slow Store look like transport loss. */
                    this.applyData();
                }
            } catch (final Throwable t) {
                if (this.disposed && Thread.currentThread().isInterrupted()) {
                    this.releaseCachedData();
                    return;
                }
                final RuntimeException normalized = t instanceof RuntimeException runtime
                        ? runtime : new IllegalStateException("Storage binary merger failed", t);
                this.failure.compareAndSet(null, normalized);
                LOGGER.log(System.Logger.Level.ERROR, "Storage binary merger failed", this.failure.get());
                this.releaseCachedData();
                if (t instanceof Error error) throw error;
                throw normalized;
            }
        }

        @Override
        public RuntimeException failure() {
            return this.failure.get();
        }

        @Override
        public StorageGraphCoordinator graphCoordinator() {
            return this.graphCoordinator;
        }

                /// Read-phase counterpart of [#readJoined(Supplier)] for scans
        /// that produce a plan for a later write-side mutation.
        private <T> T readJoined(final Supplier<T> read) {
            final StorageGraphCoordinator coordinator = this.graphCoordinator;
            return coordinator == null ? read.get() : coordinator.read(read);
        }

        private void applyData() {
            this.materialization.write(() ->
            {
                /* Drain into the reused scratch array: no list, no exact-size
                 * copy, no fork, and no per-batch Duration or scope — steady
                 * state allocates nothing on the heap on this path. The
                 * scratch grows once to the largest batch seen. */
                this.queueLock.lock();
                final int pending;
                try {
                    final long count = this.cachedBufferCount;
                    if (count > Integer.MAX_VALUE) {
                        throw new IllegalStateException("Storage binary materialization batch exceeds array capacity");
                    }
                    pending = (int) count;
                    if (pending == 0) return;
                    if (pending > this.drainBuffers.length) {
                        this.drainBuffers = new ByteBuffer[pending];
                    }
                    for (int index = 0; index < pending; index++) {
                        final ByteBuffer next = this.cachedData.poll();
                        if (next == null) {
                            throw new IllegalStateException("Storage binary materialization queue shrank during drain");
                        }
                        this.cachedBufferCount--;
                        this.cachedBytes = Math.subtractExact(this.cachedBytes, next.remaining());
                        this.drainBuffers[index] = next;
                    }
                } finally {
                    this.queueLock.unlock();
                }

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
                final long startedNanos = System.nanoTime();
                try {
                    this.objectGraphUpdateHandler.objectGraphUpdateAvailable(() ->
                    {
                        /* One coordinator write section covers the batch end
                         * to end: materialization, validation, and index
                         * refresh. Application reads joining the read side
                         * observe either the pre-batch or the post-batch
                         * boundary — never a materialized graph with stale
                         * search views. */
                        /* Reader-side index maintenance FIRST: imports
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
                        ClusterStoreIndexes.refreshImportedIndexes(this.storage);
                        StorageBinaryDataMaterializer.materialize(this.storage, this.drainBuffers, pending);
                        /* Reader-side index enforcement: a writer that smuggled
                         * an external Lucene directory or a non-persisted
                         * vector index past registration fails this reader
                         * closed instead of diverging it. The scan visits
                         * index metadata only, never entity payload. */
                        ClusterStoreIndexes.validateStorageRoots(this.storage);
                    });
                } catch (final RuntimeException | Error failure) {
                    /* A genuine failure says failed; only an overrun says timed
                     * out. The two are never conflated into one message. */
                    if (failure instanceof RuntimeException runtime) {
                        this.noteFailure("Store graph update failed", runtime);
                    }
                    throw failure;
                } finally {
                    StorageBinaryDataImporter.release(this.drainBuffers, pending);
                    Arrays.fill(this.drainBuffers, 0, pending, null);
                }
                final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
                if (elapsedMs > this.materializationBudgetMs) {
                    final IllegalStateException terminal = new IllegalStateException(
                            "Timed out while applying Store data: batch took %s ms with a budget of %s ms"
                                    .formatted(elapsedMs, this.materializationBudgetMs));
                    this.noteFailure(terminal.getMessage(), terminal);
                    throw terminal;
                }
            });
        }

                /// Removes one queued buffer by identity. Buffers compare by content,
        /// so [ArrayDeque#remove] could dequeue a content-equal stranger.
        private static void removeIdentical(final ArrayDeque<ByteBuffer> queue, final ByteBuffer buffer) {
            final var cursor = queue.iterator();
            while (cursor.hasNext()) {
                if (cursor.next() == buffer) {
                    cursor.remove();
                    return;
                }
            }
        }

        private void applyDataSafely() {
            final boolean hasData;
            this.queueLock.lock();
            try {
                hasData = !this.cachedData.isEmpty();
            } finally {
                this.queueLock.unlock();
            }
            if (hasData) this.applyData();
        }

        private void awaitFlushRequestOrTimeout() {
            if (this.cachingTimeoutMs <= 0L) return;
            this.queueLock.lock();
            try {
                if (this.flushRequested) {
                    this.flushRequested = false;
                    return;
                }
                long remaining = TimeUnit.MILLISECONDS.toNanos(this.cachingTimeoutMs);
                try {
                    while (!this.flushRequested && remaining > 0L) {
                        remaining = this.flushCondition.awaitNanos(remaining);
                    }
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Storage graph update worker was interrupted", interrupted);
                }
                this.flushRequested = false;
            } finally {
                this.queueLock.unlock();
            }
        }

        private void releaseCachedData() {
            this.queueLock.lock();
            try {
                final var pending = new ArrayList<ByteBuffer>();
                ByteBuffer buffer;
                while ((buffer = this.cachedData.poll()) != null) {
                    this.cachedBufferCount--;
                    pending.add(buffer);
                }
                this.cachedBufferCount = 0L;
                this.cachedBytes = 0L;
                /* Failure- and shutdown-path cleanup only: every queued buffer
                 * is distinctly owned, so the unconditional release frees
                 * exactly what the queue holds. Never runs while a batch is
                 * mid-materialization — an in-flight batch lives in the drain
                 * scratch, not in this queue. */
                StorageBinaryDataImporter.release(pending.toArray(ByteBuffer[]::new));
            } finally {
                this.queueLock.unlock();
            }
        }

        /// Merges the writer's type dictionary: unknown types gain handlers,
        /// structurally conflicting types fail the merger terminally.
        ///
        /// Newly registered definitions are persisted immediately, before the
        /// transaction's binary is imported, so a restart can still resolve
        /// every imported type id. The first failure is recorded and rethrown
        /// on every later call; a failed merger never accepts more work.
        ///
        /// @param typeDictionaryData writer's type dictionary snapshot
        @Override
        public void receiveTypeDictionary(final String typeDictionaryData) {
            if (this.failure.get() != null) {
                throw new IllegalStateException("Storage binary merger has failed", this.failure.get());
            }
            if (this.disposed) {
                throw new IllegalStateException("Storage binary merger is disposed");
            }
            /* Dictionary registration mutates the same type handlers the worker
             * reads while materializing. Funnel the merge through the shared
             * materialization mutual exclusion so a delivery-thread merge can
             * never interleave with a worker batch. The worker never waits on
             * the delivery thread, so this wait cannot deadlock. */
            this.materialization.write(() ->
            {
                /* Read phase through the shared read side when a coordinator
                 * is wired: parsing and conflict detection mutate nothing. The
                 * worker cannot interleave — it needs the materialization lock
                 * held here — so the plan stays valid until the write phase. */
                final ArrayList<PersistenceTypeDefinition> pending =
                        this.readJoined(() -> planDictionaryMerge(typeDictionaryData));
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

                /// Parses the writer's dictionary snapshot and plans handler registrations.
        ///
        /// Read-only: unknown remote types are collected, structurally
        /// conflicting types fail with [StorageBinaryDataException]. Runs on
        /// the coordinator's read side when one is wired.
        ///
        /// @param typeDictionaryData writer's type dictionary snapshot
        /// @return remote definitions missing locally, in iteration order
        private ArrayList<PersistenceTypeDefinition> planDictionaryMerge(final String typeDictionaryData) {
            try {
                final PersistenceTypeDictionary remoteTypeDictionary = BinaryPersistence.Foundation()
                        .setClassLoaderProvider(this.foundation.getClassLoaderProvider())
                        .setFieldEvaluatorPersister(this.foundation.getFieldEvaluatorPersistable())
                        .setTypeDictionaryLoader(() -> typeDictionaryData)
                        .getTypeDictionaryProvider()
                        .provideTypeDictionary();
                final PersistenceTypeDictionary localTypeDictionary =
                        this.storage.persistenceManager().typeDictionary();

                final ArrayList<PersistenceTypeDefinition> pending = new ArrayList<>();
                remoteTypeDictionary.iterateAllTypeDefinitions(remoteType ->
                {
                    final PersistenceTypeDefinition localType =
                            localTypeDictionary.lookupTypeById(remoteType.typeId());
                    if (localType == null) {
                        LOGGER.log(System.Logger.Level.DEBUG, "New type: %s".formatted(remoteType.typeName()));
                        pending.add(remoteType);
                    } else if (!PersistenceTypeDescription.equalStructure(localType, remoteType)) {
                        throw new StorageBinaryDataException(
                                "Remote type definition conflicts with local definition: %s <> %s"
                                        .formatted(localType, remoteType));
                    }
                });
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
                        throw new StorageBinaryDataException(
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
             * Lifecycle needs no monitor: disposed is volatile, workerScheduled is
             * only touched under queueLock, and the executor is thread-safe. */
            if (this.disposed && this.executor.isTerminated()) {
                this.releaseCachedData();
                return;
            }
            this.disposed = true;
            this.executor.shutdown();
            boolean terminated = false;
            try {
                // if any external processes like Kubernetes shuts us down, it will wait for the externally set
                // grace period and then kill the process. But any other case we will await the task orderly like this.
                terminated = this.executor.awaitTermination(30, TimeUnit.SECONDS);
                if (!terminated) {
                    LOGGER.log(WARNING, "Timed out waiting for storage graph updates; interrupting remaining work");
                    this.executor.shutdownNow();
                    terminated = this.executor.awaitTermination(5, TimeUnit.SECONDS);
                    if (!terminated) {
                        throw new IllegalStateException(
                                "Storage graph update worker did not terminate; native buffers remain owned by it");
                    }
                }
            } catch (final InterruptedException e) {
                this.executor.shutdownNow();
                Thread.currentThread().interrupt();
                throw new StorageBinaryDataException("Interrupted while waiting for storage graph updates", e);
            } finally {
                if (terminated || this.executor.isTerminated()) this.releaseCachedData();
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
                this.releaseCachedData();
                throw new IllegalStateException("Storage binary merger has failed", this.failure.get());
            }
            if (this.disposed) {
                throw new IllegalStateException("Storage binary merger is disposed");
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
            this.queueLock.lock();
            try {
                this.flushRequested = true;
                this.flushCondition.signalAll();
            } finally {
                this.queueLock.unlock();
            }
            this.applyDataSafely();
            final Future<?> pending = this.updateFuture;
            if (pending == null) return;
            try {
                this.awaitMaterialization(pending, "imported Store data materialization");
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                this.noteFailure("Interrupted while waiting for imported Store data materialization", e);
                this.releaseCachedData();
                throw new StorageBinaryDataException("Interrupted while waiting for imported Store data materialization", e);
            } catch (final ExecutionException e) {
                final RuntimeException mergerFailure = this.failure.get();
                if (mergerFailure != null) {
                    throw new IllegalStateException("Storage binary merger has failed", mergerFailure);
                }
                final RuntimeException terminal = this.recordFailure("Failed to materialize imported Store data", e.getCause() == null ? e : e.getCause());
                this.releaseCachedData();
                throw terminal;
            } catch (final TimeoutException e) {
                final RuntimeException terminal = this.recordFailure("Timed out waiting for imported Store data materialization", e);
                /* Only queued buffers are safe to release here. The callback represented by
                 * pending may still own the batch whose wait timed out. */
                this.releaseCachedData();
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
                    if (retries-- <= 0) {
                        throw timeout;
                    }
                    LOGGER.log(WARNING, "%s did not finish within %s ms; retrying (%s retries left)".formatted(operation, this.applyTimeoutMs, retries + 1));
                }
            }
        }

        /* Records the first failure without returning it, for paths that rethrow
         * the original cause to preserve its type and stack. See [#recordFailure]
         * for paths that throw the canonical first failure. */
        private void noteFailure(final String message, final Throwable cause) {
            this.failure.compareAndSet(null, new IllegalStateException(message, cause));
        }

        /* Records the first failure and returns it; later failures are dropped so
         * concurrent paths cannot overwrite the root cause. Callers throw the
         * result themselves, which is why this method only records. */
        private RuntimeException recordFailure(final String message, final Throwable cause) {
            this.noteFailure(message, cause);
            return this.failure.get();
        }
    }
}
