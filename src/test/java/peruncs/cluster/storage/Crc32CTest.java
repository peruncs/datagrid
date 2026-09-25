package peruncs.cluster.storage;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the shared allocation-free CRC32C helper.
class Crc32cTest {
    @Test
    void contextReusesDirectBufferAcrossIndependentRanges() {
        final ByteBuffer direct = ByteBuffer.allocateDirect(6);
        direct.put(new byte[]{1, 2, 3, 4, 5, 6});
        final UnsafeBuffer buffer = new UnsafeBuffer(direct);
        final Crc32C.Context context = new Crc32C.Context();
        assertEquals(Crc32C.compute(new byte[]{2, 3, 4}), context.compute(buffer, 1, 3));
        assertEquals(Crc32C.compute(new byte[]{5, 6}), context.compute(buffer, 4, 2));
        assertThrows(IllegalArgumentException.class, () -> context.compute(buffer, 5, 2));
    }

        /// A caller-owned accumulator must start each message from zero.
    @Test
    void accumulatorResetsBetweenMessages() {
        final var first = Crc32C.accumulator();
        first.update(new byte[]{1, 2, 3});

        final var second = Crc32C.accumulator();
        second.update(new byte[]{4, 5});

        assertEquals(Crc32C.compute(new byte[]{4, 5}), (int) second.getValue());
    }

        /// The reuse overload resets a dirty accumulator instead of continuing it.
    @Test
    void reuseOverloadResetsADirtyAccumulator() {
        final byte[] bytes = new byte[]{9, 8, 7, 6, 5};
        final CRC32C dirty = Crc32C.accumulator();
        dirty.update(new byte[]{1, 2, 3});

        assertEquals(Crc32C.compute(bytes), Crc32C.compute(bytes, 0, bytes.length, dirty));
        assertEquals(Crc32C.compute(bytes, 1, 3), Crc32C.compute(bytes, 1, 3, dirty));
    }

        /// The whole-array overload matches the explicit range overload.
    @Test
    void wholeArrayMatchesExplicitRange() {
        final byte[] bytes = new byte[]{4, 5, 6};
        assertEquals(Crc32C.compute(bytes, 0, bytes.length), Crc32C.compute(bytes));
        assertEquals(Crc32C.compute(bytes, 1, 2, Crc32C.accumulator()), Crc32C.compute(bytes, 1, 2));
    }

        /// Out-of-range slices and a null reuse accumulator are rejected.
    @Test
    void rejectsInvalidRangeAndNullReuse() {
        final byte[] bytes = new byte[]{1, 2, 3};
        assertThrows(IllegalArgumentException.class, () -> Crc32C.compute(bytes, -1, 2));
        assertThrows(IllegalArgumentException.class, () -> Crc32C.compute(bytes, 0, 4));
        assertThrows(IllegalArgumentException.class, () -> Crc32C.compute(bytes, 2, 2));
        assertThrows(NullPointerException.class, () -> Crc32C.compute(bytes, 0, 3, null));
    }

        /// Null input is rejected consistently instead of failing while reading its length.
    @Test
    void rejectsNullInput() {
        assertThrows(NullPointerException.class, () -> Crc32C.compute(null));
    }
}
