package peruncs.cluster.storage.binary;

import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

/// Allocates direct native-order buffers whose lifetime the owner controls explicitly.
///
/// Every buffer comes with a closeable [Allocation]. The owner keeps the handle and closes it
/// when the buffer is no longer used; no registry maps buffers back to their allocations, so
/// owners never contend with each other.
public final class NativeMemory {
    private NativeMemory() {
    }

    /// Allocates one native-order buffer backed by its own shared arena.
    ///
    /// @param capacity positive buffer capacity in bytes
    /// @return the buffer and the handle that frees it
    /// @throws IllegalArgumentException when the capacity is not positive
    public static Allocation allocate(final int capacity) {
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
    ///
    /// Closing is idempotent. The buffer must not be touched after the handle is closed.
    public static final class Allocation implements AutoCloseable {
        private final Arena arena;
        private final ByteBuffer buffer;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Allocation(final Arena arena, final ByteBuffer buffer) {
            this.arena = arena;
            this.buffer = buffer;
        }

        /// Returns the native buffer.
        ///
        /// @return the buffer, valid until [#close()]
        public ByteBuffer buffer() {
            return this.buffer;
        }

        /// Frees the native memory; later calls do nothing.
        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) this.arena.close();
        }
    }
}
