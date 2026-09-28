package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class NativeBufferPoolTest {
    @Test
    void roundsUpReusesAndBoundsNativeBuffers() {
        try (final NativeBufferPool pool = new NativeBufferPool(8L)) {
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
        try (final NativeBufferPool pool = new NativeBufferPool(4L)) {
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
}
