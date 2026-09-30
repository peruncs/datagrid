package peruncs.cluster.storage.binary;

import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;
import java.util.Map;

/// Owns direct native-order buffers through closeable shared arenas.
///
/// This package is not exported by the module; the public methods serve only
/// internal storage packages. A future allocator change stays at this boundary.
public final class NativeMemory {
    private static final Map<ByteBuffer, Allocation> ALLOCATIONS = new IdentityHashMap<>();

    private NativeMemory() {
    }

    /// Allocates one direct native-order buffer.
    public static ByteBuffer allocateDirect(final int capacity) {
        if (capacity == 0) return ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());
        final Allocation allocation = allocateScoped(capacity);
        synchronized (ALLOCATIONS) {
            ALLOCATIONS.put(allocation.buffer(), allocation);
        }
        return allocation.buffer();
    }

    /// Releases a buffer allocated by [#allocateDirect(int)].
    public static void releaseDirect(final ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() == 0) return;
        final Allocation allocation;
        synchronized (ALLOCATIONS) {
            allocation = ALLOCATIONS.remove(buffer);
        }
        if (allocation == null) throw new IllegalArgumentException("buffer was not allocated by NativeMemory");
        allocation.close();
    }

    /// Allocates a buffer with an explicit lifetime for owners that already track buffers.
    ///
    /// @param capacity positive buffer capacity
    /// @return the buffer and its shared arena
    static Allocation allocateScoped(final int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        final Arena arena = Arena.ofShared();
        try {
            return new Allocation(arena,
                    arena.allocate(capacity, Long.BYTES).asByteBuffer().order(ByteOrder.nativeOrder()));
        } catch (final RuntimeException | Error failure) {
            arena.close();
            throw failure;
        }
    }

    /// One direct buffer and the arena that controls its lifetime.
    static final class Allocation implements AutoCloseable {
        private final Arena arena;
        private final ByteBuffer buffer;

        private Allocation(final Arena arena, final ByteBuffer buffer) {
            this.arena = arena;
            this.buffer = buffer;
        }

        ByteBuffer buffer() {
            return this.buffer;
        }

        @Override
        public void close() {
            this.arena.close();
        }
    }
}
