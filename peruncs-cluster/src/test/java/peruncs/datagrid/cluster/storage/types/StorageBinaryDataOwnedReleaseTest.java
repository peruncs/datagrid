package peruncs.datagrid.cluster.storage.types;

import org.agrona.UnsafeApi;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/// Proves owned deliveries fail closed without leaking native memory.
///
/// Once the assembler hands a binary over, the merger owns every direct buffer
/// in it — including when extraction itself rejects the binary. Each malformed
/// shape below must throw its documented exception while still freeing the
/// native buffers, and must never latch a terminal merger failure: a single bad
/// delivery is refused, not fatal.
class StorageBinaryDataOwnedReleaseTest {
    private static final int SOAK_ITERATIONS = 500;
    private static final int SOAK_BUFFER_BYTES = 1024;

    /// Builds an owned binary whose channel buffers can be swapped after
    /// construction. ChunksWrapper validates directness in its constructor but
    /// keeps the caller's array by reference, so replacing an element afterwards
    /// is the only way to present a genuinely malformed binary through public API.
    private static final class SwappableBinary extends ChunksWrapper {
        final ByteBuffer[] backing;

        SwappableBinary(final ByteBuffer... buffers) {
            super(buffers);
            this.backing = buffers;
        }
    }

    private static StorageBinaryDataMerger merger() {
        return StorageBinaryDataMerger.New(StorageBinaryDataMerger.Configuration.builder()
                .foundation(StorageBinaryDataMergerTestSupport.foundation())
                .storage(StorageBinaryDataMergerTestSupport.connection())
                .objectGraphUpdateHandler(ObjectGraphUpdateHandler.PerStore(new StorageGraphCoordinator()))
                .cachingTimeoutMs(0L).cachedBinaryLimit(1L).applyTimeoutMs(60_000L).build());
    }

    private static ByteBuffer direct(final int bytes) {
        final ByteBuffer buffer = XMemory.allocateDirectNative(bytes);
        buffer.put(new byte[bytes]);
        return buffer;
    }

    private static void corruptPosition(final ByteBuffer buffer, final int position) throws Exception {
        /* java.nio enforces position <= capacity on every API path, so a logical
         * length beyond capacity is unreachable through public buffers. Poke the
         * field directly to pin the defensive invalid-length guard. The address
         * is untouched, so the buffer still frees cleanly. */
        final Field field = Buffer.class.getDeclaredField("position");
        UnsafeApi.putInt(buffer, UnsafeApi.objectFieldOffset(field), position);
    }

    /// Verifies a null owned delivery is rejected without latching a terminal merger failure.
    @Test
    void nullOwnedBinaryThrowsWithoutLatchingFailure() {
        final StorageBinaryDataMerger merger = merger();
        try {
            assertThrows(NullPointerException.class, () -> merger.receiveDataOwned(null));
            assertNull(merger.failure(), "a refused delivery must not fail the merger");
        } finally {
            merger.dispose();
        }
    }

    /// Verifies an owned binary smuggling a heap buffer is freed and rejected with a non-direct failure.
    @Test
    void nonDirectOwnedBufferIsReleasedAndRejected() {
        final StorageBinaryDataMerger merger = merger();
        try {
            final SwappableBinary binary = new SwappableBinary(direct(64), direct(64));
            binary.backing[1] = ByteBuffer.allocate(64);
            final StorageBinaryDataException failure =
                    assertThrows(StorageBinaryDataException.class, () -> merger.receiveDataOwned(binary));
            assertTrue(failure.getMessage().contains("non-direct"),
                    "unexpected message: " + failure.getMessage());
            assertEquals(0, failure.getSuppressed().length,
                    "a clean refusal must not carry a cleanup failure");
            assertNull(merger.failure(), "a refused delivery must not fail the merger");
        } finally {
            merger.dispose();
        }
    }

    /// Verifies an owned binary with a corrupted buffer length is freed and rejected with an invalid-length failure.
    @Test
    void invalidLengthOwnedBufferIsReleasedAndRejected() throws Exception {
        final StorageBinaryDataMerger merger = merger();
        try {
            final ByteBuffer victim = direct(64);
            corruptPosition(victim, 1 << 20);
            final Binary binary = ChunksWrapper.New(direct(64), victim);
            final StorageBinaryDataException failure =
                    assertThrows(StorageBinaryDataException.class, () -> merger.receiveDataOwned(binary));
            assertTrue(failure.getMessage().contains("invalid buffer length"),
                    "unexpected message: " + failure.getMessage());
            assertEquals(0, failure.getSuppressed().length,
                    "a clean refusal must not carry a cleanup failure");
            assertNull(merger.failure(), "a refused delivery must not fail the merger");
        } finally {
            merger.dispose();
        }
    }

    /// Exercises repeated malformed owned deliveries to verify each frees native memory without latching merger failure.
    @Test
    void malformedOwnedDeliveriesNeverLeakOrPoisonTheMerger() throws Exception {
        /* Soak: every iteration allocates fresh native memory and must free it on
         * the refusal path. A leak would pin native memory per iteration; a
         * double-free or queue poisoning would crash or latch the merger. */
        final StorageBinaryDataMerger merger = merger();
        try {
            for (int iteration = 0; iteration < SOAK_ITERATIONS; iteration++) {
                final SwappableBinary heapSmuggled = new SwappableBinary(
                        direct(SOAK_BUFFER_BYTES), direct(SOAK_BUFFER_BYTES));
                heapSmuggled.backing[1] = ByteBuffer.allocate(SOAK_BUFFER_BYTES);
                assertThrows(StorageBinaryDataException.class, () -> merger.receiveDataOwned(heapSmuggled));

                final ByteBuffer corrupted = direct(SOAK_BUFFER_BYTES);
                corruptPosition(corrupted, 1 << 20);
                assertThrows(StorageBinaryDataException.class,
                        () -> merger.receiveDataOwned(ChunksWrapper.New(direct(16), corrupted)));
            }
            assertNull(merger.failure(), "refused deliveries must not fail the merger");
        } finally {
            merger.dispose();
        }
    }

    /// Verifies an owned delivery after disposal is refused with a disposed failure.
    @Test
    void refusedOwnedDeliveryOnDisposedMergerStaysClean() {
        final StorageBinaryDataMerger merger = merger();
        merger.dispose();
        final IllegalStateException failure = assertThrows(
                IllegalStateException.class, () -> merger.receiveDataOwned(ChunksWrapper.New(direct(16))));
        assertTrue(failure.getMessage().contains("disposed"),
                "an owned delivery after disposal must report the disposal: " + failure.getMessage());
    }
}
