package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;

/// Exposes Store binary buffers to the replication transport without mutating them.
final class StorageBinaryDataChunker {
        /* The merger calls the array collectors once per transaction. Reuse one
         * collection scratch list per thread instead of allocating (and growing)
         * a new list for every batch. Only the returned arrays escape; the
         * scratch is always cleared before reuse. Virtual threads each hold
         * their own short-lived list, which dies with the thread. */
    private static final ThreadLocal<ArrayList<ByteBuffer>> SCRATCH =
            ThreadLocal.withInitial(ArrayList::new);

    private StorageBinaryDataChunker() {
    }

    /// Returns duplicate source views in channel order.
    ///
    /// @param data source Store binary
    /// @return duplicate views in channel order
    static ByteBuffer[] bufferArray(final Binary data) {
        Objects.requireNonNull(data, "data");
        final ArrayList<ByteBuffer> scratch = scratch();
        try {
            data.iterateChannelChunks(chunk ->
            {
                if (chunk == null)
                    throw new StorageBinaryDataException("binary contains a null channel");
                for (final ByteBuffer buffer : chunk.buffers()) {
                    if (buffer == null)
                        throw new StorageBinaryDataException("binary contains a null channel buffer");
                    scratch.add(buffer.duplicate());
                }
            });
            return scratch.toArray(ByteBuffer[]::new);
        } finally {
            scratch.clear();
        }
    }

    /// Returns the original direct buffers of an owned binary and normalizes them
    /// in place for Store import. This method is only for a caller that has
    /// already taken ownership of the binary and therefore may transfer release
    /// responsibility for the returned buffers.
    ///
    /// @param data owned binary
    /// @return original direct buffers, positioned at zero
    public static ByteBuffer[] ownedArray(final Binary data) {
        Objects.requireNonNull(data, "data");
        /* One pass with one reused scratch list: each buffer is validated and
         * normalized inline, so no boxed length list and no second loop.
         * Normalizing before a later buffer fails is unobservable: the caller
         * discards the whole binary on failure and releases its native storage
         * through the ownership cleanup block instead. */
        final ArrayList<ByteBuffer> scratch = scratch();
        try {
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
                    scratch.add(buffer);
                }
            });
            return scratch.toArray(ByteBuffer[]::new);
        } finally {
            /* The returned array (or the caller's best-effort release) owns the
             * buffers from here; the scratch only held references. */
            scratch.clear();
        }
    }

        /// Best-effort release of every direct buffer still reachable from a binary
    /// whose owned extraction failed before producing a normalized array.
    ///
    /// Extraction validates inline, so a late malformed buffer leaves earlier
    /// direct buffers with no owner but the caller. This fallback frees exactly
    /// those buffers — direct, non-empty views only — and never throws, so it
    /// cannot mask the original validation failure.
    ///
    /// @param data binary whose extraction failed, or `null`
    public static void releaseDirect(final Binary data) {
        if (data == null) return;
        try {
            data.iterateChannelChunks(channel ->
            {
                if (channel == null) return;
                final ByteBuffer[] buffers;
                try {
                    buffers = channel.buffers();
                } catch (final RuntimeException ignored) {
                    return;
                }
                if (buffers == null) return;
                for (final ByteBuffer buffer : buffers) {
                    if (buffer != null && buffer.isDirect() && buffer.capacity() > 0) {
                        try {
                            XMemory.deallocateDirectByteBuffer(buffer);
                        } catch (final RuntimeException ignored) {
                            /* One unfreeable buffer must not stop the rest. */
                        }
                    }
                }
            });
        } catch (final RuntimeException ignored) {
            /* Best-effort cleanup stays silent by contract. */
        }
    }

    private static ArrayList<ByteBuffer> scratch() {
        final ArrayList<ByteBuffer> scratch = SCRATCH.get();
        scratch.clear();
        return scratch;
    }

    /// Returns import-ready duplicate views with position zero. Serializer's
    /// [ChunksWrapper] stores its logical length in the source position;
    /// ordinary binaries expose the remaining bytes instead.
    ///
    /// @param data source Store binary
    /// @return import-ready duplicate views
    public static ByteBuffer[] importArray(final Binary data) {
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
