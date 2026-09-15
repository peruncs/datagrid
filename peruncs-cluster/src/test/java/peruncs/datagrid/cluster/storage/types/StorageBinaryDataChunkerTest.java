package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksBuffer;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.util.BufferSizeProviderIncremental;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Tests storage binary data chunker behavior.
class StorageBinaryDataChunkerTest {
        /// The receiver fast path must collect exactly one array without a list copy.
    @Test
    void bufferArrayPreservesAllChannelBuffers() {
        final ByteBuffer first = XMemory.toDirectByteBuffer(new byte[]{1});
        final ByteBuffer second = XMemory.toDirectByteBuffer(new byte[]{2});
        final var binary = ChunksWrapper.New(first, second);
        final ByteBuffer[] buffers = StorageBinaryDataChunker.bufferArray(binary);
        assertEquals(2, buffers.length);
        assertEquals(first, buffers[0]);
        assertEquals(second, buffers[1]);
        assertEquals(0, first.position());
        assertEquals(0, second.position());
    }

        /// Import views normalize Serializer's position-as-length representation without mutating it.
    @Test
    void importArrayNormalizesChunksWrapperBuffers() {
        final ByteBuffer source = XMemory.allocateDirectNative(8);
        try {
            source.put(new byte[]{1, 2, 3});
            final var binary = ChunksWrapper.New(source);
            final ByteBuffer[] normalized = StorageBinaryDataChunker.importArray(binary);
            assertEquals(1, normalized.length);
            assertEquals(0, normalized[0].position());
            assertEquals(3, normalized[0].remaining());
            assertEquals(3, source.position());
        } finally {
            XMemory.deallocateDirectByteBuffer(source);
        }
    }

        /// Owned import returns the original buffers normalized in place.
    @Test
    void ownedArrayReturnsOriginalsNormalized() {
        final ByteBuffer source = XMemory.allocateDirectNative(8);
        try {
            source.put(new byte[]{1, 2, 3});
            final var binary = ChunksWrapper.New(source);
            final ByteBuffer[] owned = StorageBinaryDataChunker.ownedArray(binary);
            assertEquals(1, owned.length);
            assertSame(source, owned[0]);
            assertEquals(0, owned[0].position());
            assertEquals(3, owned[0].limit());
        } finally {
            XMemory.deallocateDirectByteBuffer(source);
        }
    }

        /// Owned import preserves channel order with per-buffer logical lengths.
    @Test
    void ownedArrayPreservesChannelOrderWithPerBufferLengths() {
        final ByteBuffer first = XMemory.allocateDirectNative(8);
        final ByteBuffer second = XMemory.allocateDirectNative(8);
        try {
            first.put(new byte[]{1, 2, 3});
            second.put(new byte[]{4, 5});
            final var binary = ChunksWrapper.New(first, second);
            final ByteBuffer[] owned = StorageBinaryDataChunker.ownedArray(binary);
            assertEquals(2, owned.length);
            assertSame(first, owned[0]);
            assertSame(second, owned[1]);
            assertEquals(0, owned[0].position());
            assertEquals(3, owned[0].limit());
            assertEquals(0, owned[1].position());
            assertEquals(2, owned[1].limit());
        } finally {
            XMemory.deallocateDirectByteBuffer(first);
            XMemory.deallocateDirectByteBuffer(second);
        }
    }

        /// Owned import rejects a missing binary instead of failing mid-iteration.
    @Test
    void ownedArrayRejectsNullBinary() {
        assertThrows(NullPointerException.class,
                () -> StorageBinaryDataChunker.ownedArray(null));
    }
}
