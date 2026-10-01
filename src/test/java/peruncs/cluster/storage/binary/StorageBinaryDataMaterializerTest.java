package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.CorruptReplicationDataException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the materializer refuses malformed transaction buffers before touching the Store graph.
class StorageBinaryDataMaterializerTest {
    private static ByteBuffer entity(final long declaredLength) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(24).order(ByteOrder.nativeOrder());
        buffer.putLong(declaredLength).putLong(7L).putLong(1L);
        buffer.flip();
        return buffer;
    }

    private static void materialize(final ByteBuffer... buffers) {
        new StorageBinaryDataMaterializer().materialize(StorageBinaryDataMergerTestSupport.foundation(),
                StorageBinaryDataMergerTestSupport.connection(), buffers, 0, buffers.length);
    }

    @Test
    void aHeapBufferIsRejected() {
        assertThrows(CorruptReplicationDataException.class, () -> materialize(ByteBuffer.allocate(24)));
    }

    @Test
    void aBufferNotAtPositionZeroIsRejected() {
        final ByteBuffer buffer = entity(24L);
        buffer.position(8);
        assertThrows(CorruptReplicationDataException.class, () -> materialize(buffer));
    }

    @Test
    void anEntityLengthLargerThanItsBufferIsRejected() {
        assertThrows(RuntimeException.class, () -> materialize(entity(4_096L)));
    }

    @Test
    void anEntityLengthSmallerThanItsHeaderIsRejected() {
        assertThrows(RuntimeException.class, () -> materialize(entity(8L)));
    }

    @Test
    void aSliceBeyondTheArrayIsRejectedAsAnArgumentError() {
        final ByteBuffer[] buffers = {entity(24L)};
        assertThrows(IllegalArgumentException.class, () -> new StorageBinaryDataMaterializer().materialize(
                StorageBinaryDataMergerTestSupport.foundation(), StorageBinaryDataMergerTestSupport.connection(),
                buffers, 1, 1));
    }

    @Test
    void anEmptyBufferIsSkipped() {
        final ByteBuffer empty = ByteBuffer.allocateDirect(0);
        assertDoesNotThrow(() -> materialize(empty));
    }
}
