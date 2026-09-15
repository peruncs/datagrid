package peruncs.datagrid.cluster.node.replication;

import org.eclipse.serializer.collections.types.XEnum;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.typing.Disposable;
import org.eclipse.serializer.util.X;
import org.eclipse.serializer.util.logging.Logging;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import peruncs.datagrid.cluster.node.exceptions.IncompatibleTypeDictionaryException;
import peruncs.datagrid.cluster.node.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.storage.types.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/// Applies committed Store binary data on a reader node.
///
/// Incoming buffers are imported into the local Store immediately and then
/// coalesced for object-graph updates on a bounded single-thread executor.
/// Providers must call this merger only after their transport-specific commit
/// validation has completed.
public interface ClusterStorageBinaryDataMerger extends StorageBinaryDataMerger, Disposable {
        /// Creates a merger with bounded deferred materialization.
    ///
    /// @param foundation               persistence foundation
    /// @param storage                  Store connection
    /// @param objectGraphUpdateHandler graph update handler
    /// @param cachingTimeoutMs         maximum wait for a cached batch
    /// @param cachedBinaryLimit        maximum cached binary count
    /// @return binary merger
    static ClusterStorageBinaryDataMerger New(
            final BinaryPersistenceFoundation<?> foundation,
            final StorageConnection storage,
            final ObjectGraphUpdateHandler objectGraphUpdateHandler,
            final long cachingTimeoutMs,
            final long cachedBinaryLimit
    ) {
        return new Default(
                notNull(foundation),
                notNull(storage),
                notNull(objectGraphUpdateHandler),
                notNegative(cachingTimeoutMs),
                positive(cachedBinaryLimit)
        );
    }

        /// Returns an asynchronous materialization failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    default RuntimeException failure() {
        return null;
    }

        /// Wait until the latest accepted import has completed object-graph materialization.
    default void awaitApplied() {
    }

        /// Supplies conservative defaults for deferred object-graph application.
    interface Defaults {
                /// Default cache timeout in milliseconds.
        long CACHING_TIMEOUT_MS = 10_000L;
                /// Default cached binary count.
        long CACHING_LIMIT = 50L;
    }

        /// Applies imported data on one bounded worker and reports failures.
    class Default implements ClusterStorageBinaryDataMerger {
        private static final Logger LOG = Logging.getLogger(ClusterStorageBinaryDataMerger.class);
        private static final long MAX_CACHED_BYTES = 1L << 30;
        private static final long APPLY_TIMEOUT_SECONDS = 60L;
        private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable ->
        {
            final Thread thread = new Thread(runnable, "eclipse-datagrid-store-materializer");
            thread.setDaemon(true);
            return thread;
        });
        private final ConcurrentLinkedQueue<ByteBuffer> cachedData = new ConcurrentLinkedQueue<>();
        private final Object applyLock = new Object();
        /* Queue admission and object-graph materialization are separate concerns.
         * The worker and awaitApplied() can run concurrently, but a Store update
         * must never materialize two batches at once or callbacks can observe and
         * mutate the graph out of order. */
        private final Object materializationLock = new Object();
        private final Object flushMonitor = new Object();
        private final BinaryPersistenceFoundation<?> foundation;
        private final StorageConnection storage;
        private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
        private final long cachingTimeoutMs;
        private final long cacheLimit;
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        private final AtomicLong cachedBufferCount = new AtomicLong();
        private volatile boolean flushRequested;
        private volatile boolean disposed;
        private boolean workerScheduled;
        private long cachedBytes;
        private volatile Future<?> updateFuture = CompletableFuture.completedFuture(null);

        private Default(
                final BinaryPersistenceFoundation<?> foundation,
                final StorageConnection storage,
                final ObjectGraphUpdateHandler objectGraphUpdateHandler,
                final long cachingTimeoutMs,
                final long cacheLimit
        ) {
            this.foundation = foundation;
            this.storage = storage;
            this.objectGraphUpdateHandler = objectGraphUpdateHandler;
            this.cachingTimeoutMs = cachingTimeoutMs;
            this.cacheLimit = cacheLimit;
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
            final ByteBuffer[] ownedBuffers = StorageBinaryDataImporter.importOwned(this.storage, sourceBuffers);
            /* scheduleMaterialization owns cleanup on every rejection.  Releasing here
             * as well would double-free buffers when the worker has already drained its
             * queue after a terminal failure. */
            this.scheduleMaterialization(ownedBuffers);
        }

                /// Imports Aeron-owned direct buffers without a second native allocation.
        @Override
        public boolean receiveDataOwned(final Binary data) {
            final ByteBuffer[] buffers = StorageBinaryDataChunker.ownedArray(
                    org.eclipse.serializer.util.X.notNull(data));
            if (this.failure.get() != null) {
                StorageBinaryDataImporter.release(buffers);
                throw new IllegalStateException("Storage binary merger has failed", this.failure.get());
            }
            if (this.disposed) {
                StorageBinaryDataImporter.release(buffers);
                throw new IllegalStateException("Storage binary merger is disposed");
            }
            StorageBinaryDataImporter.importDirect(this.storage, buffers);
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
            boolean drainFirst;
            synchronized (this.applyLock) {
                final long projected = Math.addExact(this.cachedBytes, incomingBytes);
                drainFirst = this.cachedBytes > 0 && projected > MAX_CACHED_BYTES;
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
            try {
                synchronized (this.applyLock) {
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
                    this.cachedData.addAll(Arrays.asList(ownedBuffers));
                    this.cachedBufferCount.addAndGet(ownedBuffers.length);
                    this.cachedBytes = projectedBytes;
                    queued = true;
                    if (!this.workerScheduled) {
                        try {
                            this.workerScheduled = true;
                            this.updateFuture = this.executor.submit(this::runMaterializationWorker);
                        } catch (final RuntimeException | Error failure) {
                            /* Submission happens under applyLock, so the worker cannot have
                             * removed these newly queued buffers yet. */
                            for (final ByteBuffer buffer : ownedBuffers) {
                                this.cachedData.removeIf(candidate -> candidate == buffer);
                            }
                            this.cachedBytes = Math.subtractExact(this.cachedBytes, incomingBytes);
                            this.cachedBufferCount.addAndGet(-ownedBuffers.length);
                            queued = false;
                            throw failure;
                        }
                    }
                }

                if (this.cachedBufferCount.get() > this.cacheLimit) {
                    try {
                        /* The worker is signalled before this wait when it is still in its
                         * coalescing delay. Waiting on the future once avoids a timed polling
                         * loop that can spin after interruption and gives backpressure one
                         * unambiguous completion point. */
                        this.updateFuture.get();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting for import data task", e);
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
                    synchronized (this.applyLock) {
                        if (this.cachedData.isEmpty()) {
                            this.workerScheduled = false;
                            return;
                        }
                    }
                    /* Drain ownership under the queue lock, then release it before
                     * materializing. Store graph updates may block or complete on another
                     * executor; keeping applyLock across that wait previously stalled the
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
                LOG.error("Storage binary merger failed", this.failure.get());
                this.releaseCachedData();
                if (t instanceof Error error) throw error;
                throw normalized;
            }
        }

        @Override
        public RuntimeException failure() {
            return this.failure.get();
        }

        private void applyData() {
            synchronized (this.materializationLock) {
                final XEnum<ByteBuffer> data = X.Enum();
                final AtomicBoolean released = new AtomicBoolean();
                final Runnable release = () ->
                {
                    if (released.compareAndSet(false, true)) {
                        for (final ByteBuffer buffer : data) {
                            XMemory.deallocateDirectByteBuffer(buffer);
                        }
                    }
                };

                synchronized (this.applyLock) {
                    ByteBuffer next;
                    while ((next = this.cachedData.poll()) != null) {
                        this.cachedBufferCount.decrementAndGet();
                        this.cachedBytes = Math.subtractExact(this.cachedBytes, next.remaining());
                        data.add(next);
                    }
                }

                try {
                    this.objectGraphUpdateHandler.objectGraphUpdateAvailable(() ->
                    {
                        try {
                            StorageBinaryDataMaterializer.materialize(this.storage, data.toArray(ByteBuffer.class));
                        } finally {
                            release.run();
                        }
                    }).toCompletableFuture().get(APPLY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    /* The update callback may still be running after interruption;
                     * its finally block remains the sole owner of these buffers. */
                    if (!(this.disposed && Thread.currentThread().isInterrupted())) {
                        this.fail("Interrupted while applying Store data", interrupted);
                    }
                    throw new IllegalStateException("Interrupted while applying Store data", interrupted);
                } catch (final ExecutionException failure) {
                    release.run();
                    final IllegalStateException terminal = new IllegalStateException(
                            "Timed out or failed while applying Store data",
                            failure.getCause() == null ? failure : failure.getCause());
                    this.fail(terminal.getMessage(), terminal);
                    throw terminal;
                } catch (final TimeoutException failure) {
                    /* Future.get timeout does not cancel the callback. Do not free
                     * direct buffers while materialization may still be using them. */
                    final IllegalStateException terminal = new IllegalStateException(
                            "Timed out while applying Store data", failure);
                    this.fail(terminal.getMessage(), terminal);
                    throw terminal;
                } catch (final RuntimeException | Error failure) {
                    release.run();
                    if (failure instanceof RuntimeException runtime) {
                        this.fail("Store graph update failed", runtime);
                    }
                    throw failure;
                }
            }
        }

        private void applyDataSafely() {
            final boolean hasData;
            synchronized (this.applyLock) {
                hasData = !this.cachedData.isEmpty();
            }
            if (hasData) this.applyData();
        }

        private void awaitFlushRequestOrTimeout() {
            if (this.cachingTimeoutMs <= 0L) return;
            synchronized (this.flushMonitor) {
                if (this.flushRequested) {
                    this.flushRequested = false;
                    return;
                }
                final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.cachingTimeoutMs);
                try {
                    while (!this.flushRequested) {
                        final long remaining = deadline - System.nanoTime();
                        if (remaining <= 0L) break;
                        TimeUnit.NANOSECONDS.timedWait(this.flushMonitor, remaining);
                    }
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Storage graph update worker was interrupted", interrupted);
                }
                this.flushRequested = false;
            }
        }

        private void releaseCachedData() {
            synchronized (this.applyLock) {
                ByteBuffer buffer;
                while ((buffer = this.cachedData.poll()) != null) {
                    this.cachedBufferCount.decrementAndGet();
                    XMemory.deallocateDirectByteBuffer(buffer);
                }
                this.cachedBufferCount.set(0L);
                this.cachedBytes = 0L;
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
            try {
                final PersistenceTypeDictionary remoteTypeDictionary = BinaryPersistence.Foundation()
                        .setClassLoaderProvider(this.foundation.getClassLoaderProvider())
                        .setFieldEvaluatorPersister(this.foundation.getFieldEvaluatorPersistable())
                        .setTypeDictionaryLoader(() -> typeDictionaryData)
                        .getTypeDictionaryProvider()
                        .provideTypeDictionary();
                final PersistenceTypeDictionary localTypeDictionary = this.storage.persistenceManager().typeDictionary();

                remoteTypeDictionary.iterateAllTypeDefinitions(remoteType ->
                {
                    final PersistenceTypeDefinition localType = localTypeDictionary.lookupTypeById(remoteType.typeId());
                    if (localType == null) {
                        LOG.debug("New type: {}", remoteType.typeName());
                        this.foundation.getTypeHandlerManager().ensureTypeHandler(remoteType);

                    } else if (!PersistenceTypeDescription.equalStructure(localType, remoteType)) {
                        throw new IncompatibleTypeDictionaryException(
                                "Remote type definition conflicts with local definition: %s <> %s".formatted(localType, remoteType)
                        );
                    }
                });
                /* A reader may import data without performing a local Store operation.
                 * Persist newly registered remote definitions now, before the transaction's
                 * binary is imported, so a restart can resolve every imported type id. */
                this.foundation.getTypeHandlerManager().exportPendingTypeDictionaryChanges();
            } catch (final RuntimeException failure) {
                this.fail("Store type dictionary update failed", failure);
                throw failure;
            } catch (final Error failure) {
                this.fail("Store type dictionary update failed", failure);
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
             * not let the first failed attempt make every later cleanup a no-op. */
            synchronized (this) {
                if (this.disposed && this.executor.isTerminated()) {
                    this.releaseCachedData();
                    return;
                }
                this.disposed = true;
                this.executor.shutdown();
            }
            boolean terminated = false;
            try {
                // if any external processes like Kubernetes shuts us down, it will wait for the externally set
                // grace period and then kill the process. But any other case we will await the task orderly like this.
                terminated = this.executor.awaitTermination(30, TimeUnit.SECONDS);
                if (!terminated) {
                    LOG.warn("Timed out waiting for storage graph updates; interrupting remaining work");
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
                throw new NodelibraryException(e);
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
             * notification when the worker is between checking the flag and entering
             * wait(). */
            synchronized (this.flushMonitor) {
                this.flushRequested = true;
                this.flushMonitor.notifyAll();
            }
            this.applyDataSafely();
            final Future<?> pending = this.updateFuture;
            try {
                pending.get(APPLY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                this.fail("Interrupted while waiting for imported Store data materialization", e);
                this.releaseCachedData();
                throw new NodelibraryException(e);
            } catch (final ExecutionException e) {
                final RuntimeException mergerFailure = this.failure.get();
                if (mergerFailure != null) {
                    throw new IllegalStateException("Storage binary merger has failed", mergerFailure);
                }
                final RuntimeException terminal = this.fail(
                        "Failed to materialize imported Store data", e.getCause() == null ? e : e.getCause());
                this.releaseCachedData();
                throw terminal;
            } catch (final TimeoutException e) {
                final RuntimeException terminal = this.fail(
                        "Timed out waiting for imported Store data materialization", e);
                /* Only queued buffers are safe to release here. The callback represented by
                 * pending may still own the batch whose wait timed out. */
                this.releaseCachedData();
                throw terminal;
            }
        }

        private RuntimeException fail(final String message, final Throwable cause) {
            final RuntimeException terminal = new IllegalStateException(message, cause);
            this.failure.compareAndSet(null, terminal);
            return this.failure.get();
        }
    }
}
