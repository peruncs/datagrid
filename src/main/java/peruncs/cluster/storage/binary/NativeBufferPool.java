package peruncs.cluster.storage.binary;

import org.eclipse.serializer.memory.XMemory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.locks.ReentrantLock;

/// Reuses a bounded number of power-of-two native buffers between transactions.
final class NativeBufferPool implements AutoCloseable {
    private static final int BUCKET_COUNT = 31;
    private static final int PER_BUCKET_LIMIT = 2;
    // ponytail: cap retention at 32 MiB; raise it only if profiling shows larger buffers recur.
    static final long MAX_RETAINED_BYTES = 32L << 20;

    private final ArrayDeque<ByteBuffer>[] buckets;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxRetainedBytes;
    private long retainedBytes;
    private boolean closed;

    /// Creates a pool with the given byte ceiling and fixed per-bucket count.
    ///
    /// @param maxRetainedBytes maximum native capacity retained after release
    @SuppressWarnings("unchecked")
    NativeBufferPool(final long maxRetainedBytes) {
        if (maxRetainedBytes < 0L) throw new IllegalArgumentException("maxRetainedBytes must not be negative");
        this.maxRetainedBytes = maxRetainedBytes;
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
                final ByteBuffer reused = this.buckets[bucket].pollLast();
                if (reused != null) {
                    this.retainedBytes -= reused.capacity();
                    return reused.clear();
                }
            } finally {
                this.lock.unlock();
            }
        }
        return XMemory.allocateDirectNative(bucket < 0 ? requiredCapacity : 1 << bucket);
    }

    /// Retains a compatible buffer within the pool limits or frees it.
    ///
    /// @param buffer owned native buffer to return
    void release(final ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() == 0) return;
        if (!buffer.isDirect()) throw new IllegalArgumentException("native buffer pool only accepts direct buffers");
        final int bucket = releaseBucketIndex(buffer.capacity());
        boolean retained = false;
        if (bucket >= 0) {
            this.lock.lock();
            try {
                final long nextBytes = this.retainedBytes + buffer.capacity();
                if (!this.closed && this.buckets[bucket].size() < PER_BUCKET_LIMIT &&
                    nextBytes <= this.maxRetainedBytes) {
                    this.buckets[bucket].addLast(buffer.clear());
                    this.retainedBytes = nextBytes;
                    retained = true;
                }
            } finally {
                this.lock.unlock();
            }
        }
        if (!retained) XMemory.deallocateDirectByteBuffer(buffer);
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
                        XMemory.deallocateDirectByteBuffer(buffer);
                    } catch (final RuntimeException cleanupFailure) {
                        if (failure == null) failure = cleanupFailure;
                        else failure.addSuppressed(cleanupFailure);
                    }
                }
            }
            this.retainedBytes = 0L;
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
