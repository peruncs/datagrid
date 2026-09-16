package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Exposes Store binary buffers to the replication transport without mutating them.
interface StorageBinaryDataChunker {

    /// Collects duplicate source views in channel order.
    ///
    /// @param data source Store binary
    /// @return duplicate views in channel order
    static List<ByteBuffer> buffers(final Binary data) {
        Objects.requireNonNull(data, "data");
        final List<ByteBuffer> buffers = new ArrayList<>();
        data.iterateChannelChunks(chunk ->
        {
            if (chunk == null)
                throw new StorageBinaryDataException("binary contains a null channel");
            for (final ByteBuffer buffer : chunk.buffers()) {
                if (buffer == null)
                    throw new StorageBinaryDataException("binary contains a null channel buffer");
                buffers.add(buffer.duplicate());
            }
        });
        return buffers;
    }

    /// Returns duplicate source views in channel order.
    ///
    /// @param data source Store binary
    /// @return duplicate views in channel order
    static ByteBuffer[] bufferArray(final Binary data) {
        return buffers(data).toArray(ByteBuffer[]::new);
    }

    /// Returns the original direct buffers of an owned binary and normalizes them
    /// in place for Store import. This method is only for a caller that has
    /// already taken ownership of the binary and therefore may transfer release
    /// responsibility for the returned buffers.
    ///
    /// @param data owned binary
    /// @return original direct buffers, positioned at zero
    static ByteBuffer[] ownedArray(final Binary data) {
        Objects.requireNonNull(data, "data");
        /* One pass with one list: each buffer is validated and normalized inline,
         * so no boxed length list and no second loop. Normalizing before a later
         * buffer fails is unobservable: the caller discards the whole binary on
         * failure and the assembler releases its native storage regardless. */
        final List<ByteBuffer> buffers = new ArrayList<>();
        data.iterateChannelChunks(channel ->
        {
            if (channel == null) throw new StorageBinaryDataException("binary contains a null channel");
            for (final ByteBuffer buffer : channel.buffers()) {
                if (buffer == null || !buffer.isDirect()) {
                    throw new StorageBinaryDataException("owned binary contains a non-direct buffer");
                }
                final int logicalLength = logicalLength(data, buffer);
                if (logicalLength < 0 || logicalLength > buffer.capacity()) {
                    throw new StorageBinaryDataException("owned binary contains an invalid buffer length");
                }
                buffer.clear();
                buffer.limit(logicalLength);
                buffers.add(buffer);
            }
        });
        return buffers.toArray(ByteBuffer[]::new);
    }

    /// Returns import-ready duplicate views with position zero. Serializer's
    /// [ChunksWrapper] stores its logical length in the source position;
    /// ordinary binaries expose the remaining bytes instead.
    ///
    /// @param data source Store binary
    /// @return import-ready duplicate views
    static ByteBuffer[] importArray(final Binary data) {
        Objects.requireNonNull(data, "data");
        final ByteBuffer[] source = bufferArray(data);
        final ByteBuffer[] result = new ByteBuffer[source.length];
        final boolean wrapped = data instanceof ChunksWrapper;
        for (int index = 0; index < source.length; index++) {
            final ByteBuffer buffer = source[index];
            if (wrapped) {
                final int logicalLength = buffer.position();
                if (logicalLength < 0 || logicalLength > buffer.capacity()) {
                    throw new StorageBinaryDataException("invalid wrapped binary buffer length");
                }
                buffer.clear();
                buffer.limit(logicalLength);
            }
            result[index] = buffer.slice();
        }
        return result;
    }

    /// Returns the logical payload length of one channel buffer.
    ///
    /// Serializer's [ChunksWrapper] stores its logical length in the source
    /// position; ordinary binaries expose the remaining bytes instead. This is
    /// the single type-test for that distinction; callers must use this
    /// helper instead of branching on the binary type themselves.
    ///
    /// @param data   source Store binary
    /// @param buffer one channel buffer of that binary
    /// @return logical payload length in bytes
    private static int logicalLength(final Binary data, final ByteBuffer buffer) {
        return data instanceof ChunksWrapper ? buffer.position() : buffer.remaining();
    }
}
