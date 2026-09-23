package peruncs.datagrid.cluster.storage.binary;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/// Tests the storage binary buffer views: borrowed duplicates normalize
/// without mutating the source, and owned extraction returns the original
/// direct buffers normalized in place.
class StorageBinaryBuffersTest {
        /// Borrowed views normalize Serializer's position-as-length representation
        /// without mutating the source or leaking the intermediate duplicate.
    @Test
    void importArrayNormalizesChunksWrapperBuffers() {
        final ByteBuffer source = XMemory.allocateDirectNative(8);
        try {
            source.put(new byte[]{1, 2, 3});
            final var binary = ChunksWrapper.New(source);
            final ByteBuffer[] normalized = StorageBinaryBuffers.importArray(binary);
            assertEquals(1, normalized.length);
            assertEquals(0, normalized[0].position());
            assertEquals(3, normalized[0].remaining());
            assertEquals(3, source.position(), "the source buffer must not be mutated");
        } finally {
            XMemory.deallocateDirectByteBuffer(source);
        }
    }

        /// Import views preserve channel order for a multi-buffer binary.
    @Test
    void importArrayPreservesChannelOrder() {
        final ByteBuffer first = XMemory.allocateDirectNative(8);
        final ByteBuffer second = XMemory.allocateDirectNative(8);
        try {
            first.put(new byte[]{1, 2, 3});
            second.put(new byte[]{4, 5});
            final ByteBuffer[] normalized = StorageBinaryBuffers.importArray(ChunksWrapper.New(first, second));
            assertEquals(2, normalized.length);
            assertEquals(3, normalized[0].remaining());
            assertEquals(2, normalized[1].remaining());
            assertEquals(4, normalized[1].get(0));
            assertEquals(5, normalized[1].get(1));
        } finally {
            XMemory.deallocateDirectByteBuffer(first);
            XMemory.deallocateDirectByteBuffer(second);
        }
    }

        /// Owned import returns the original buffers normalized in place.
    @Test
    void ownedArrayReturnsOriginalsNormalized() {
        final ByteBuffer source = XMemory.allocateDirectNative(8);
        try {
            source.put(new byte[]{1, 2, 3});
            final var binary = ChunksWrapper.New(source);
            final ByteBuffer[] owned = StorageBinaryBuffers.ownedArray(binary);
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
            final ByteBuffer[] owned = StorageBinaryBuffers.ownedArray(binary);
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

        /// Owned import preserves every channel buffer by identity and leaves
        /// the sources at position zero afterwards — successors of the
        /// removed `bufferArray` assertions. The upstream wrapper owns the
        /// limit representation, so only identity, position, and the logical
        /// length are pinned.
    @Test
    void ownedArrayPreservesAllChannelBuffersByIdentity() {
        final ByteBuffer first = XMemory.allocateDirectNative(8);
        final ByteBuffer second = XMemory.allocateDirectNative(8);
        try {
            first.put(new byte[]{1});
            second.put(new byte[]{2});
            final var binary = ChunksWrapper.New(first, second);
            final ByteBuffer[] owned = StorageBinaryBuffers.ownedArray(binary);
            assertEquals(2, owned.length);
            assertSame(first, owned[0]);
            assertSame(second, owned[1]);
            assertEquals(0, first.position(), "normalization must leave the source at position zero");
            assertEquals(0, second.position(), "normalization must leave the source at position zero");
            assertEquals(1, owned[0].remaining());
            assertEquals(1, owned[1].remaining());
        } finally {
            XMemory.deallocateDirectByteBuffer(first);
            XMemory.deallocateDirectByteBuffer(second);
        }
    }

        /// Normalization is not destructive for the borrowed path: repeated
        /// imports of the same wrapper produce equivalent, independent views.
    @Test
    void importArrayIsRepeatableWithoutSourceMutation() {
        final ByteBuffer source = XMemory.allocateDirectNative(8);
        try {
            source.put(new byte[]{1, 2, 3});
            final var binary = ChunksWrapper.New(source);
            final int sourcePosition = source.position();
            final ByteBuffer[] firstPass = StorageBinaryBuffers.importArray(binary);
            final ByteBuffer[] secondPass = StorageBinaryBuffers.importArray(binary);
            assertEquals(firstPass.length, secondPass.length);
            assertEquals(firstPass[0].remaining(), secondPass[0].remaining());
            assertEquals(sourcePosition, source.position(), "borrowed views must not move the source");
        } finally {
            XMemory.deallocateDirectByteBuffer(source);
        }
    }

        /// Owned import rejects a missing binary instead of failing mid-iteration.
    @Test
    void ownedArrayRejectsNullBinary() {
        assertThrows(NullPointerException.class,
                () -> StorageBinaryBuffers.ownedArray(null));
    }

        /// Borrowed import rejects a missing binary instead of failing mid-iteration.
    @Test
    void importArrayRejectsNullBinary() {
        assertThrows(NullPointerException.class,
                () -> StorageBinaryBuffers.importArray(null));
    }
}
