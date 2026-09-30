package peruncs.cluster.storage.binary;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/// Measures steady-state pool reuse against fresh arena allocation.
@State(Scope.Thread)
public class NativeBufferPoolBenchmark {
    @Param({"64", "4096", "65536"})
    public int capacity;

    private NativeBufferPool pool;

    @Setup(Level.Trial)
    public void setup() {
        this.pool = new NativeBufferPool(1L << 20);
        this.pool.release(this.pool.acquire(this.capacity));
    }

    @Benchmark
    public int pooledAcquireRelease() {
        final var buffer = this.pool.acquire(this.capacity);
        this.pool.release(buffer);
        return buffer.capacity();
    }

    @Benchmark
    public int scopedArenaAllocateRelease() {
        try (NativeMemory.Allocation allocation = NativeMemory.allocateScoped(this.capacity)) {
            return allocation.buffer().capacity();
        }
    }

    @Benchmark
    public int managedArenaAllocateRelease() {
        final var buffer = NativeMemory.allocateDirect(this.capacity);
        NativeMemory.releaseDirect(buffer);
        return buffer.capacity();
    }

    @TearDown(Level.Trial)
    public void close() {
        this.pool.close();
    }
}
