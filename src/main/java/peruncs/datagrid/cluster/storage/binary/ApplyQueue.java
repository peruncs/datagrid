package peruncs.datagrid.cluster.storage.binary;

import peruncs.datagrid.cluster.errors.ReplicationUnavailableException;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/// Owns the merger's admitted-but-not-yet-applied native buffers and their
/// byte accounting.
///
/// Every counter mutation happens under one explicit lock, and the resident
/// memory invariant holds on every public path:
/// `residentBytes = queuedBytes + inFlightBytes`, capped by the configured
/// hard limit. Producers gain admission through [#admit], the worker drains
/// through [#drainInto] and reports completion through [#completeInFlight],
/// so every admitted byte moves exactly once from queued to in-flight to
/// done. The coalescing flag, its flush condition, and the drained
/// backpressure condition all bind to the same lock.
final class ApplyQueue {
    /* Queue admission and object-graph materialization are separate concerns.
     * The worker and awaitApplied() can run concurrently, but a Store update
     * must never materialize two batches at once or callbacks can observe and
     * mutate the graph out of order. */
    private final ReentrantLock queueLock = new ReentrantLock();
    private final Condition flushCondition = this.queueLock.newCondition();
    /* Backpressure signal: the worker posts it after every drained batch,
     * so an over-limit delivery waits only until the queue drops back under
     * the limit — not until the worker exits, which would serialize the
     * delivery thread against the whole backlog on every trigger. */
    private final Condition drainedCondition = this.queueLock.newCondition();
    /* Every access runs under queueLock (admission, drain, release, and
     * flush checks), so an ArrayDeque is sufficient and avoids the
     * per-node allocation a concurrent queue pays on every offer. */
    private final ArrayDeque<ByteBuffer> cachedData = new ArrayDeque<>();
    private final ArrayDeque<Integer> cachedTransactionLengths = new ArrayDeque<>();
    private final MergerLifecycle owner;
    private final long cacheBytesLimit;
    private final long maxCachedBytes;
    private final long applyTimeoutMs;
    /* Guarded by queueLock on every mutation; readers snapshot it under the
     * same lock, so no atomics are needed. */
    private long cachedBufferCount;
    private volatile boolean flushRequested;
    private boolean workerScheduled;
    private long cachedBytes;
    /* Native memory transferred from the queue to the active Store batch.
     * Guarded by queueLock with cachedBytes so the configured hard cap is
     * a cap on all merger-owned memory, not just the visible queue. */
    private long inFlightBytes;
    /* Set when the worker enters a batch drain and cleared when the batch
     * completes: backpressure waits against an over-limit queue must
     * keep the previous fail-fast semantics of the completed-future wait,
     * where a wedged materialization batch failed the delivery within the
     * bounded, retried apply budget instead of parking on a queue that had
     * already been pulled from. */
    private volatile long batchActiveSinceNanos;

    ApplyQueue(
            final MergerLifecycle owner,
            final long cacheBytesLimit,
            final long maxCachedBytes,
            final long applyTimeoutMs) {
        this.owner = owner;
        this.cacheBytesLimit = cacheBytesLimit;
        this.maxCachedBytes = maxCachedBytes;
        this.applyTimeoutMs = applyTimeoutMs;
    }

    /// Reports whether admitting `incomingBytes` would breach the hard cap.
    ///
    /// Used for the pre-admission drain decision: a producer first flushes
    /// the pipeline instead of piling onto an already saturated merger.
    ///
    /// @param incomingBytes payload bytes about to be admitted
    /// @return `true` when the projected resident bytes exceed the hard cap
    boolean residentWouldExceed(final long incomingBytes) {
        this.queueLock.lock();
        try {
            final long projected = Math.addExact(
                    Math.addExact(this.cachedBytes, this.inFlightBytes), incomingBytes);
            return projected > this.maxCachedBytes;
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Admits owned buffers and returns the resident byte total afterwards.
    ///
    /// Runs the terminal-state re-check, the hard-cap check, the enqueue,
    /// and the first worker submission atomically under the queue lock, so
    /// a caller never loses the native buffers into a dead queue. When the
    /// resident bytes cross the soft limit, the coalescing worker is flushed
    /// with the flag set plus one signal before this method returns — that
    /// sequence must stay under the lock or a waiting worker can miss the
    /// wake-up. On any thrown failure the buffers have not been queued (or
    /// have already been rolled back), so the caller releases them exactly
    /// once.
    ///
    /// @param ownedBuffers  native buffers whose ownership transfers to the queue
    /// @param incomingBytes total payload bytes of `ownedBuffers`
    /// @param workerSubmission schedules the worker when none is running
    /// @return resident bytes (queued plus in-flight) after this admission
    long admit(
            final ByteBuffer[] ownedBuffers,
            final long incomingBytes,
            final Supplier<Future<?>> workerSubmission) {
        this.queueLock.lock();
        try {
            /* The worker can fail between the entry checks and this
             * ownership hand-off.  Reject before enqueueing so a caller never
             * loses the native buffers into a dead queue. */
            if (this.owner.failure() != null) {
                throw new IllegalStateException("Storage binary merger has failed", this.owner.failure());
            }
            if (this.owner.isDisposed()) {
                throw new ReplicationUnavailableException("Storage binary merger is disposed");
            }
            final long queuedAfterAdmission = Math.addExact(this.cachedBytes, incomingBytes);
            final long residentAfterAdmission = Math.addExact(queuedAfterAdmission, this.inFlightBytes);
            if (residentAfterAdmission > this.maxCachedBytes) {
                throw new ReplicationUnavailableException(
                        "Storage binary materialization cache is full: %s queued plus %s in-flight bytes with a %s byte limit"
                                .formatted(this.cachedBytes, this.inFlightBytes, this.maxCachedBytes));
            }
            /* Bulk add without a wrapper allocation: Collections.addAll
             * passes the array straight through to per-element add. */
            Collections.addAll(this.cachedData, ownedBuffers);
            this.cachedTransactionLengths.addLast(ownedBuffers.length);
            this.cachedBufferCount += ownedBuffers.length;
            this.cachedBytes = queuedAfterAdmission;
            if (!this.workerScheduled) {
                try {
                    this.workerScheduled = true;
                    workerSubmission.get();
                } catch (final RuntimeException | Error failure) {
                    /* Submission happens under queueLock, so the worker cannot have
                     * removed these newly queued buffers yet. Buffers
                     * compare by content, so removal must be by identity:
                     * a content-equal stranger must never be dequeued. */
                    for (final ByteBuffer buffer : ownedBuffers) {
                        removeIdentical(this.cachedData, buffer);
                    }
                    this.cachedTransactionLengths.removeLast();
                    this.cachedBytes = Math.subtractExact(this.cachedBytes, incomingBytes);
                    this.cachedBufferCount -= ownedBuffers.length;
                    /* A shutdown racing this admission must surface as the
                     * documented disposal refusal, not a raw executor
                     * rejection. */
                    if (failure instanceof RejectedExecutionException && this.owner.isDisposed()) {
                        throw new ReplicationUnavailableException("Storage binary merger is disposed", failure);
                    }
                    throw failure;
                }
            }
            if (residentAfterAdmission > this.cacheBytesLimit) {
                this.flushRequested = true;
                this.flushCondition.signal();
            }
            return residentAfterAdmission;
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Reports the resident bytes still cross the soft backpressure limit.
    ///
    /// @param residentBytes resident bytes measured at admission time
    /// @return `true` when the producer must wait for the queue to drain
    boolean exceedsSoftLimit(final long residentBytes) {
        return residentBytes > this.cacheBytesLimit;
    }

    /// Wakes a worker parked in its coalescing delay unless nothing is left.
    ///
    /// The flag avoids a lost signal when the worker is between checking the
    /// flag and awaiting the condition. Used by the durability boundary
    /// ({@code awaitApplied}), which needs a signal-all because several
    /// threads may observe the boundary.
    ///
    /// @return `true` when queue and in-flight batch are both empty
    boolean requestFlushUnlessDrained() {
        this.queueLock.lock();
        try {
            if (this.cachedData.isEmpty() && this.inFlightBytes == 0L) return true;
            this.flushRequested = true;
            this.flushCondition.signalAll();
            return false;
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Ends the worker's scheduling claim when the queue ran dry.
    ///
    /// @return `true` when the queue is empty and the worker should exit
    boolean unscheduleWorkerIfEmpty() {
        this.queueLock.lock();
        try {
            if (this.cachedData.isEmpty()) {
                this.workerScheduled = false;
                return true;
            }
            return false;
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Waits out the coalescing delay, returning early on a flush request.
    ///
    /// @param cachingTimeoutMs maximum coalescing wait, or `0` to skip it
    void awaitFlushRequestOrTimeout(final long cachingTimeoutMs) {
        if (cachingTimeoutMs <= 0L) return;
        this.queueLock.lock();
        try {
            if (this.flushRequested) {
                this.flushRequested = false;
                return;
            }
            long remaining = TimeUnit.MILLISECONDS.toNanos(cachingTimeoutMs);
            try {
                while (!this.flushRequested && remaining > 0L) {
                    remaining = this.flushCondition.awaitNanos(remaining);
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new ReplicationUnavailableException(
                        "Storage graph update worker was interrupted", interrupted);
            }
            this.flushRequested = false;
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Transfers the whole queue into the worker's drain scratch.
    ///
    /// Moves every queued buffer's bytes from the queued counter to the
    /// in-flight counter under the queue lock — the queued-to-in-flight leg
    /// of the exactly-once ownership hand-off — and stamps the batch-active
    /// marker the watchdog compares against. Waiters parked on the byte
    /// limit are signalled before the lock is released because the batch
    /// just left the queue.
    ///
    /// @param drain worker-confined scratch, grown in place when too small
    /// @return drained buffer count, or `0` when the queue is empty
    int drainInto(final ApplyWorker.Drain drain) {
        this.queueLock.lock();
        try {
            final long count = this.cachedBufferCount;
            if (count > Integer.MAX_VALUE) {
                throw new IllegalStateException("Storage binary materialization batch exceeds array capacity");
            }
            final int pending = (int) count;
            if (pending == 0) return 0;
            final int pendingTransactions = this.cachedTransactionLengths.size();
            drain.ensureCapacity(pending, pendingTransactions);
            drain.bufferCount = pending;
            drain.transactionCount = pendingTransactions;
            drain.batchBytes = 0L;
            int countedBuffers = 0;
            for (int index = 0; index < pendingTransactions; index++) {
                final int transactionLength = this.cachedTransactionLengths.removeFirst();
                drain.transactionLengths[index] = transactionLength;
                countedBuffers = Math.addExact(countedBuffers, transactionLength);
            }
            if (countedBuffers != pending) {
                throw new IllegalStateException("Store transaction boundaries do not match queued buffers");
            }
            for (int index = 0; index < pending; index++) {
                final ByteBuffer next = this.cachedData.poll();
                if (next == null) {
                    throw new IllegalStateException("Storage binary materialization queue shrank during drain");
                }
                this.cachedBufferCount--;
                this.cachedBytes = Math.subtractExact(this.cachedBytes, next.remaining());
                drain.batchBytes = Math.addExact(drain.batchBytes, next.remaining());
                drain.buffers[index] = next;
            }
            this.inFlightBytes = Math.addExact(this.inFlightBytes, drain.batchBytes);
            drain.startedNanos = System.nanoTime();
            this.batchActiveSinceNanos = drain.startedNanos;
            /* The batch just left the queue: a waiter parked on the
             * byte limit may now fit again. Signalling while nobody
             * waits costs an uncontended signal only. */
            this.drainedCondition.signalAll();
            return pending;
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Completes the in-flight leg after a batch released its native memory.
    ///
    /// The batch and its native memory are both released: waiters may now
    /// re-evaluate the complete cap.
    ///
    /// @param batchBytes bytes the completed batch accounted for
    void completeInFlight(final long batchBytes) {
        this.queueLock.lock();
        try {
            this.inFlightBytes = Math.subtractExact(this.inFlightBytes, batchBytes);
            this.batchActiveSinceNanos = 0L;
            this.drainedCondition.signalAll();
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Bounded backpressure wait until the queue drops back at or under the
    /// configured byte limit.
    ///
    /// The worker signals [drainedCondition] after every drained batch, so
    /// this returns as soon as enough bytes left the queue — not when the
    /// queue is empty. A latched worker failure fails immediately, and an
    /// over-limit queue that the worker cannot drain within the bounded,
    /// retried budget latches the lifecycle failure exactly like the
    /// previous future-based wait did.
    void awaitQueueDrainedBelowLimit() {
        this.queueLock.lock();
        try {
            int retries = MergerLifecycle.APPLY_TIMEOUT_RETRIES;
            long remaining = TimeUnit.MILLISECONDS.toNanos(this.applyTimeoutMs);
            /* Same wait-set as before the deferred import: an over-limit
             * queue blocks its producer until the worker is idle AND the
             * queue is back under the limit. The batch-active check keeps
             * the bounded wait on a wedged materialization (fail-closed
             * within the retry budget); the queue check applies the byte
             * backpressure. Both signals arrive via drainedCondition. */
            while (Math.addExact(this.cachedBytes, this.inFlightBytes) > this.cacheBytesLimit) {
                if (this.owner.failure() != null) {
                    throw new IllegalStateException("Storage binary merger has failed", this.owner.failure());
                }
                if (this.owner.isDisposed()) {
                    throw new ReplicationUnavailableException("Storage binary merger is disposed");
                }
                if (remaining <= 0L) {
                    /* Slices of one apply timeout with the shared retry
                     * budget: a slow-but-progressing batch finishes within
                     * the budget (the completed-future wait behaved the
                     * same), a wedged worker fails after it. */
                    if (retries-- <= 0) {
                        throw this.owner.recordLifecycleFailure("Timed out waiting for import data task",
                                new TimeoutException(
                                        "queue stayed over the %s byte limit for %d slices"
                                                .formatted(this.cacheBytesLimit, MergerLifecycle.APPLY_TIMEOUT_RETRIES + 1)));
                    }
                    remaining = TimeUnit.MILLISECONDS.toNanos(this.applyTimeoutMs);
                }
                try {
                    remaining = this.drainedCondition.awaitNanos(remaining);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new ReplicationUnavailableException(
                            "Interrupted while waiting for import data task", interrupted);
                }
            }
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Releases every still-queued buffer on the failure or shutdown path.
    ///
    /// Never runs while a batch is mid-materialization — an in-flight batch
    /// lives in the drain scratch, not in this queue.
    void releaseAll() {
        this.queueLock.lock();
        try {
            final var pending = new ArrayList<ByteBuffer>();
            ByteBuffer buffer;
            while ((buffer = this.cachedData.poll()) != null) {
                this.cachedBufferCount--;
                pending.add(buffer);
            }
            this.cachedBufferCount = 0L;
            this.cachedTransactionLengths.clear();
            this.cachedBytes = 0L;
            /* Failure- and shutdown-path cleanup only: every queued buffer
             * is distinctly owned, so the unconditional release frees
             * exactly what the queue holds. */
            StorageBinaryDataImporter.release(pending.toArray(ByteBuffer[]::new));
        } finally {
            this.queueLock.unlock();
        }
    }

    /// Returns the nanoTime stamp of the batch the worker is applying, or `0`.
    ///
    /// @return batch-active marker compared by the materialization watchdog
    long batchActiveSinceNanos() {
        return this.batchActiveSinceNanos;
    }

    /// Wakes every waiter after a terminal materialization-budget expiry.
    void signalAllWaiters() {
        this.queueLock.lock();
        try {
            this.drainedCondition.signalAll();
            this.flushCondition.signalAll();
        } finally {
            this.queueLock.unlock();
        }
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
}
