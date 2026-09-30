package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.CorruptReplicationDataException;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class NativeBufferPoolTest {
    @Test
    void roundsUpReusesAndBoundsNativeBuffers() {
        try (final NativeBufferPool pool = new NativeBufferPool(8L, false)) {
            final ByteBuffer first = pool.acquire(3);
            final ByteBuffer second = pool.acquire(3);
            final ByteBuffer evicted = pool.acquire(3);
            assertEquals(4, first.capacity());

            first.position(2);
            first.limit(3);
            pool.release(first);
            pool.release(second);
            pool.release(evicted);

            final ByteBuffer reusedSecond = pool.acquire(3);
            final ByteBuffer reusedFirst = pool.acquire(3);
            final ByteBuffer fresh = pool.acquire(3);
            assertSame(second, reusedSecond, "the most recently returned buffer is reused first");
            assertSame(first, reusedFirst);
            assertNotSame(evicted, fresh, "the per-bucket cap frees excess buffers");
            assertEquals(0, reusedFirst.position());
            assertEquals(reusedFirst.capacity(), reusedFirst.limit());
            reusedFirst.put(0, (byte) 42);

            pool.release(reusedSecond);
            pool.release(reusedFirst);
            pool.release(fresh);
            assertDoesNotThrow(pool::close);
        }
    }

    @Test
    void totalRetentionCapEvictsOversizedBucketSet() {
        try (final NativeBufferPool pool = new NativeBufferPool(4L, false)) {
            final ByteBuffer retained = pool.acquire(3);
            final ByteBuffer evicted = pool.acquire(3);
            pool.release(retained);
            pool.release(evicted);

            assertSame(retained, pool.acquire(3));
            final ByteBuffer fresh = pool.acquire(3);
            assertNotSame(evicted, fresh);
            pool.release(retained);
            pool.release(fresh);
        }
    }

    @Test
    void checkedModePoisonsReleasedBuffersAndRejectsStaleLeases() {
        try (final NativeBufferPool pool = new NativeBufferPool(8_192L, true)) {
            final ByteBuffer buffer = pool.acquire(8_192);
            buffer.put(0, (byte) 1);
            buffer.put(4_096, (byte) 2);
            buffer.put(8_191, (byte) 3);
            final long generation = pool.generation(buffer);

            pool.release(buffer);

            assertEquals((byte) 0xDE, buffer.get(0));
            assertEquals((byte) 2, buffer.get(4_096));
            assertEquals((byte) 0xDE, buffer.get(8_191));
            assertThrows(IllegalStateException.class, () -> pool.release(buffer));

            final ByteBuffer reused = pool.acquire(8_192);
            assertSame(buffer, reused);
            final ApplyWorker.Drain drain = new ApplyWorker.Drain(true);
            drain.buffers[0] = reused;
            drain.generations[0] = generation;
            assertThrows(CorruptReplicationDataException.class,
                    () -> drain.checkOwnership(pool, 0, 1));
            drain.generations[0] = pool.generation(reused);
            assertDoesNotThrow(() -> drain.checkOwnership(pool, 0, 1));
            pool.release(reused);
        }
    }

    @Test
    void checkedModeRetainsOnlyOneBufferAcrossAllSizeClasses() {
        try (final NativeBufferPool pool = new NativeBufferPool(1_024L, true)) {
            final ByteBuffer retained = pool.acquire(256);
            final ByteBuffer discarded = pool.acquire(512);

            pool.release(retained);
            pool.release(discarded);

            assertThrows(IllegalStateException.class, () -> pool.generation(discarded));
            final ByteBuffer reused = pool.acquire(256);
            assertSame(retained, reused);
            pool.release(reused);
        }
    }
}
