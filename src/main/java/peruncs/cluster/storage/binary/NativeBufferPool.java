package peruncs.cluster.storage.binary;

import peruncs.cluster.errors.CorruptReplicationDataException;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.concurrent.locks.ReentrantLock;

/// Reuses a bounded number of power-of-two native buffers between transactions.
final class NativeBufferPool implements AutoCloseable {
    private static final int BUCKET_COUNT = 31;
    private static final int PER_BUCKET_LIMIT = 2;
    private static final boolean CHECKED = Boolean.getBoolean("peruncs.pool.checked");
    private final ArrayDeque<ByteBuffer>[] buckets;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxRetainedBytes;
    private final IdentityHashMap<ByteBuffer, NativeMemory.Allocation> allocations = new IdentityHashMap<>();
    private final IdentityHashMap<ByteBuffer, BufferState> checkedBuffers;
    private long retainedBytes;
    private int checkedRetainedBuffers;
    private boolean closed;

    /// Creates a pool with the given byte ceiling and fixed per-bucket count.
    ///
    /// @param maxRetainedBytes maximum native capacity retained after release
    NativeBufferPool(final long maxRetainedBytes) {
        this(maxRetainedBytes, CHECKED);
    }

    @SuppressWarnings("unchecked")
    NativeBufferPool(final long maxRetainedBytes, final boolean checked) {
        if (maxRetainedBytes < 0L) throw new IllegalArgumentException("maxRetainedBytes must not be negative");
        this.maxRetainedBytes = maxRetainedBytes;
        this.checkedBuffers = checked ? new IdentityHashMap<>() : null;
        this.buckets = (ArrayDeque<ByteBuffer>[]) new ArrayDeque<?>[BUCKET_COUNT];
        for (int i = 0; i < this.buckets.length; i++) this.buckets[i] = new ArrayDeque<>(PER_BUCKET_LIMIT);
    }

    /// Acquires a cleared buffer whose capacity is at least `requiredCapacity`.
    ///
    /// @param requiredCapacity minimum capacity in bytes
    /// @return writable native buffer
    ByteBuffer acquire(final int requiredCapacity) {
        if (requiredCapacity <= 0) throw new IllegalArgumentException("requiredCapacity must be positive");
        final int bucket = acquireBucketIndex(requiredCapacity);
        if (bucket >= 0) {
            this.lock.lock();
            try {
                if (this.closed) throw new IllegalStateException("native buffer pool is closed");
                final ByteBuffer reused = this.buckets[bucket].pollLast();
                if (reused != null) {
                    this.retainedBytes -= reused.capacity();
                    if (this.checkedBuffers != null) this.checkedRetainedBuffers--;
                    this.lease(reused);
                    return reused.clear();
                }
            } finally {
                this.lock.unlock();
            }
        } else {
            this.lock.lock();
            try {
                if (this.closed) throw new IllegalStateException("native buffer pool is closed");
            } finally {
                this.lock.unlock();
            }
        }
        final NativeMemory.Allocation allocation =
                NativeMemory.allocateScoped(bucket < 0 ? requiredCapacity : 1 << bucket);
        final ByteBuffer allocated = allocation.buffer();
        final boolean closedAfterAllocation;
        this.lock.lock();
        try {
            closedAfterAllocation = this.closed;
            if (!closedAfterAllocation) {
                this.allocations.put(allocated, allocation);
                if (this.checkedBuffers != null) this.checkedBuffers.put(allocated, new BufferState());
            }
        } finally {
            this.lock.unlock();
        }
        if (closedAfterAllocation) {
            allocation.close();
            throw new IllegalStateException("native buffer pool is closed");
        }
        return allocated;
    }

    /// Retains a compatible buffer within the pool limits or frees it.
    ///
    /// @param buffer owned native buffer to return
    void release(final ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() == 0) return;
        if (!buffer.isDirect()) throw new IllegalArgumentException("native buffer pool only accepts direct buffers");
        final int bucket = releaseBucketIndex(buffer.capacity());
        boolean retained = false;
        NativeMemory.Allocation freed = null;
        this.lock.lock();
        try {
            this.retire(buffer);
            if (bucket >= 0) {
                final long nextBytes = this.retainedBytes + buffer.capacity();
                if (!this.closed && this.buckets[bucket].size() < PER_BUCKET_LIMIT &&
                    nextBytes <= this.maxRetainedBytes &&
                    (this.checkedBuffers == null || this.checkedRetainedBuffers == 0)) {
                    this.buckets[bucket].addLast(buffer.clear());
                    this.retainedBytes = nextBytes;
                    if (this.checkedBuffers != null) this.checkedRetainedBuffers++;
                    retained = true;
                }
            }
            if (!retained) {
                if (this.checkedBuffers != null) this.checkedBuffers.remove(buffer);
                freed = this.allocations.remove(buffer);
            }
        } finally {
            this.lock.unlock();
        }
        if (!retained) {
            if (freed == null) throw new IllegalStateException("native buffer allocation is missing");
            freed.close();
        }
    }

    /// Captures and checks a buffer lease only when the checked test mode is enabled.
    long generation(final ByteBuffer buffer) {
        if (this.checkedBuffers == null || buffer.capacity() == 0) return 0L;
        this.lock.lock();
        try {
            final BufferState state = this.checkedBuffers.get(buffer);
            if (state == null || !state.leased) {
                throw new IllegalStateException("native buffer is not owned by this pool");
            }
            return state.generation;
        } finally {
            this.lock.unlock();
        }
    }

    /// Fails when a buffer was released or reused since queue admission.
    void checkGeneration(final ByteBuffer buffer, final long expected) {
        if (this.checkedBuffers == null || buffer.capacity() == 0) return;
        this.lock.lock();
        try {
            final BufferState state = this.checkedBuffers.get(buffer);
            if (state == null || !state.leased || state.generation != expected) {
                throw new CorruptReplicationDataException("native buffer reused while owned");
            }
        } finally {
            this.lock.unlock();
        }
    }

    /// Reports whether test-only lease validation is active.
    boolean checked() {
        return this.checkedBuffers != null;
    }

    private void lease(final ByteBuffer buffer) {
        if (this.checkedBuffers == null) return;
        final BufferState state = this.checkedBuffers.get(buffer);
        if (state == null || state.leased) throw new IllegalStateException("native buffer pool lease state is invalid");
        state.leased = true;
    }

    private void retire(final ByteBuffer buffer) {
        if (this.checkedBuffers == null) return;
        final BufferState state = this.checkedBuffers.get(buffer);
        if (state == null || !state.leased) throw new IllegalStateException("native buffer pool lease state is invalid");
        state.generation = Math.incrementExact(state.generation);
        state.leased = false;
        buffer.clear();
        poison(buffer);
    }

    private static void poison(final ByteBuffer buffer) {
        final int capacity = buffer.capacity();
        final int checkedBytes = capacity <= 4 * 1024 ? capacity : 64;
        for (int index = 0; index < checkedBytes; index++) buffer.put(index, (byte) 0xDE);
        if (capacity > 4 * 1024) {
            for (int index = capacity - 64; index < capacity; index++) buffer.put(index, (byte) 0xDE);
        }
    }

    /// Mutable generation and lease state for one checked-mode buffer.
    private static final class BufferState {
        private long generation;
        private boolean leased = true;
    }

    /// Frees all retained native buffers; later releases are freed immediately.
    @Override
    public void close() {
        RuntimeException failure = null;
        this.lock.lock();
        try {
            if (this.closed) return;
            this.closed = true;
            for (final ArrayDeque<ByteBuffer> bucket : this.buckets) {
                ByteBuffer buffer;
                while ((buffer = bucket.pollLast()) != null) {
                    try {
                        final NativeMemory.Allocation allocation = this.allocations.remove(buffer);
                        if (allocation == null) throw new IllegalStateException("native buffer allocation is missing");
                        allocation.close();
                    } catch (final RuntimeException cleanupFailure) {
                        if (failure == null) failure = cleanupFailure;
                        else failure.addSuppressed(cleanupFailure);
                    } finally {
                        if (this.checkedBuffers != null) this.checkedBuffers.remove(buffer);
                    }
                }
            }
            this.retainedBytes = 0L;
            this.checkedRetainedBuffers = 0;
        } finally {
            this.lock.unlock();
        }
        if (failure != null) throw failure;
    }

    private static int acquireBucketIndex(final int requiredCapacity) {
        if (requiredCapacity <= 0 || requiredCapacity > (1 << 30)) return -1;
        return 32 - Integer.numberOfLeadingZeros(requiredCapacity - 1);
    }

    private static int releaseBucketIndex(final int capacity) {
        if (capacity <= 0 || capacity > (1 << 30) || Integer.bitCount(capacity) != 1) return -1;
        return Integer.numberOfTrailingZeros(capacity);
    }
}
