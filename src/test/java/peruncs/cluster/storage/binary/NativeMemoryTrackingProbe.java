package peruncs.cluster.storage.binary;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

/// Child process that holds PerunCS native buffers until the NMT test releases them.
public final class NativeMemoryTrackingProbe {
    private static final int[] CAPACITIES = {
            16 << 20, 16 << 20, 8 << 20, 8 << 20,
            4 << 20, 4 << 20, 2 << 20, 2 << 20
    };

    private NativeMemoryTrackingProbe() {
    }

    /// Holds a bounded set of buffers while the parent records NMT snapshots.
    ///
    /// @param args unused launcher arguments
    public static void main(final String[] args) throws Exception {
        final NativeBufferPool pool = new NativeBufferPool(60L << 20, false);
        final ByteBuffer[] buffers = new ByteBuffer[CAPACITIES.length];
        for (int index = 0; index < buffers.length; index++) {
            buffers[index] = pool.acquire(CAPACITIES[index]);
            buffers[index].put(0, (byte) index);
            buffers[index].put(buffers[index].capacity() - 1, (byte) index);
        }
        System.out.println("ALLOCATED");
        System.out.flush();

        final BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        if (!"release".equals(input.readLine())) throw new IllegalStateException("expected release command");
        for (final ByteBuffer buffer : buffers) pool.release(buffer);
        pool.close();
        System.out.println("RELEASED");
        System.out.flush();
        if (!"exit".equals(input.readLine())) throw new IllegalStateException("expected exit command");
    }

}
