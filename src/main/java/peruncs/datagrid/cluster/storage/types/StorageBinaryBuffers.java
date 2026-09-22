package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;

/// Exposes Store binary buffers to the replication transport without mutating
/// the source binary.
///
/// The class only normalizes and enumerates channel buffers; it never splits
/// or chunks a binary. Borrowed views are duplicates at position zero, and
/// owned extraction hands the original direct buffers back to a caller that
/// already owns the binary.
final class StorageBinaryBuffers {
    /* Lexically scoped scratch storage cannot leak from a platform-thread pool
     * or be retained by a parked virtual thread. The public operation binds one
     * list and its recursive body consumes that binding. */
    private static final ScopedValue<ArrayList<ByteBuffer>> SCRATCH = ScopedValue.newInstance();

    private StorageBinaryBuffers() {
    }

    /// Returns import-ready duplicate views with position zero in channel order.
    ///
    /// Serializer's [ChunksWrapper] stores its logical length in the source
    /// position; ordinary binaries expose the remaining bytes instead. The
    /// source buffers are never mutated.
    ///
    /// @param data source Store binary
    /// @return import-ready duplicate views
    static ByteBuffer[] importArray(final Binary data) {
        Objects.requireNonNull(data, "data");
        if (!SCRATCH.isBound()) {
            return ScopedValue.where(SCRATCH, new ArrayList<>()).call(() -> importArray(data));
        }
        final boolean wrapped = data instanceof ChunksWrapper;
        final ArrayList<ByteBuffer> scratch = scratch();
        try {
            data.iterateChannelChunks(chunk ->
            {
                if (chunk == null) throw new StorageBinaryDataException("binary contains a null channel");
                for (final ByteBuffer source : chunk.buffers()) {
                    if (source == null) {
                        throw new StorageBinaryDataException("binary contains a null channel buffer");
                    }
                    final ByteBuffer view = source.duplicate();
                    if (wrapped) {
                        final int logicalLength = logicalLength(data, view);
                        if (logicalLength < 0 || logicalLength > view.capacity()) {
                            throw new StorageBinaryDataException("invalid wrapped binary buffer length");
                        }
                        view.clear();
                        view.limit(logicalLength);
                    }
                    scratch.add(view.slice());
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
    static ByteBuffer[] ownedArray(final Binary data) {
        Objects.requireNonNull(data, "data");
        if (!SCRATCH.isBound()) {
            return ScopedValue.where(SCRATCH, new ArrayList<>()).call(() -> ownedArray(data));
        }
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
    static void releaseDirect(final Binary data) {
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
