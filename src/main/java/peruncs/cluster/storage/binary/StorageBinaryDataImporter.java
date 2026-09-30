package peruncs.cluster.storage.binary;

import org.eclipse.serializer.collections.types.XGettingEnum;
import org.eclipse.serializer.util.X;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.eclipse.serializer.util.X.notNull;

/// Copies incoming Store binary buffers into owned native memory and imports them.
///
/// Non-empty slots own distinct native memory. Empty slots are distinct views
/// of one shared zero-capacity buffer: Store's identity set keeps every slot.
public final class StorageBinaryDataImporter {
    private static final ByteBuffer EMPTY_BACKING = ByteBuffer.allocateDirect(0);
    private StorageBinaryDataImporter() {
    }

    private static XGettingEnum<ByteBuffer> asBufferEnum(final ByteBuffer[] buffers) {
        return buffers.length == 1 ? X.Constant(buffers[0]) : X.Enum(buffers);
    }

    /// Copies the source buffers into distinctly owned native buffers without
    /// importing them. Used by the merger's deferred-import path: the borrowed
    /// binary is released by the transport as soon as its buffers are safely
    /// copied, and the ordered Store imports run later inside one drained batch.
    ///
    /// @param sourceBuffers normalized source buffers
    /// @return distinctly owned native copies
    static ByteBuffer[] copyOwned(final ByteBuffer[] sourceBuffers, final NativeBufferPool pool) {
        notNull(sourceBuffers);
        notNull(pool);
        final ByteBuffer[] ownedBuffers = new ByteBuffer[sourceBuffers.length];
        try {
            for (int i = 0; i < sourceBuffers.length; i++) {
                final ByteBuffer source = notNull(sourceBuffers[i]);
                if (source.position() != 0) {
                    throw new IllegalArgumentException(
                            "import buffers must be normalized to position zero; got %s".formatted(source.position()));
                }
                final int sourceLength = source.remaining();
                if (sourceLength == 0) {
                    ownedBuffers[i] = EMPTY_BACKING.duplicate();
                    continue;
                }
                final ByteBuffer owned = pool.acquire(sourceLength);
                ownedBuffers[i] = owned;
                owned.put(0, source, 0, sourceLength);
                owned.limit(sourceLength);
            }
            return ownedBuffers;
        } catch (final RuntimeException | Error failure) {
            releaseAfterFailure(ownedBuffers, pool, failure);
            throw failure;
        }
    }

    /// Imports already-direct buffers without allocating a second native copy.
    ///
    /// @param storage destination Store connection
    /// @param buffers buffers offered by the transport
    /// @return `true` when all buffers were direct and ownership was imported
    /// @throws RuntimeException if import fails; the caller retains ownership and
    ///                          must release the buffers
    static boolean importDirect(final StorageConnection storage, final ByteBuffer[] buffers) {
        return importDirect(storage, buffers, buffers.length);
    }

    /// Imports the populated prefix of a reusable direct-buffer array.
    ///
    /// The import collection is reused by the calling worker thread, avoiding
    /// an exact-size array allocation for every differently sized replay batch.
    ///
    /// @param storage destination Store connection
    /// @param buffers reusable buffer array
    /// @param length populated prefix length
    /// @return `true` when the prefix was imported
    static boolean importDirect(final StorageConnection storage, final ByteBuffer[] buffers, final int length) {
        return importDirect(storage, buffers, 0, length);
    }

    /// Imports one transaction slice from a reusable batch array.
    static boolean importDirect(final StorageConnection storage, final ByteBuffer[] buffers,
                                final int offset, final int length) {
        return importDirect(storage, buffers, offset, length, null);
    }

    /// Imports a slice using worker-owned views when available.
    static boolean importDirect(final StorageConnection storage, final ByteBuffer[] buffers,
                                final int offset, final int length, final ByteBuffer[] reusableViews) {
        notNull(storage);
        notNull(buffers);
        if (offset < 0 || length < 0 || offset > buffers.length - length) {
            throw new IllegalArgumentException(
                    "import range out of bounds: offset=%s, length=%s".formatted(offset, length));
        }
        final int end = offset + length;
        for (int index = offset; index < end; index++) {
            final ByteBuffer buffer = buffers[index];
            if (buffer == null || !buffer.isDirect()) return false;
            if (buffer.position() != 0) {
                throw new IllegalArgumentException("import buffers must be normalized to position zero");
            }
        }
        final ByteBuffer[] imported;
        if (offset == 0 && length == buffers.length) {
            imported = buffers;
        } else {
            imported = reusableViews != null && reusableViews.length == length
                    ? reusableViews : new ByteBuffer[length];
            System.arraycopy(buffers, offset, imported, 0, length);
        }
        try {
            storage.importData(asBufferEnum(imported));
        } finally {
            if (imported == reusableViews) Arrays.fill(imported, null);
        }
        return true;
    }

    private static void releaseAfterFailure(
            final ByteBuffer[] buffers, final NativeBufferPool pool, final Throwable failure) {
        try {
            release(buffers, pool);
        } catch (final Throwable cleanupFailure) {
            if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
        }
    }

    /// Returns owned buffers to their required reader pool.
    ///
    /// @param buffers buffers to release
    /// @param pool pool that allocated the buffers
    static void release(final ByteBuffer[] buffers, final NativeBufferPool pool) {
        if (buffers == null) return;
        release(buffers, buffers.length, pool);
    }

    /// Releases the first `length` slots of a scratch array.
    ///
    /// Scratch arrays are usually larger than the batch they hold; only the
    /// populated prefix is owned. Slots past `length` are untouched and keep
    /// whatever value (normally `null`) the caller left there.
    ///
    /// @param buffers scratch array holding owned buffers in its prefix
    /// @param length  number of populated prefix slots
    static void release(final ByteBuffer[] buffers, final int length, final NativeBufferPool pool) {
        if (buffers == null) return;
        notNull(pool);
        if (length < 0 || length > buffers.length) {
            throw new IllegalArgumentException("release length out of range: %s".formatted(length));
        }
        RuntimeException failure = null;
        for (int index = 0; index < length; index++) {
            final ByteBuffer buffer = buffers[index];
            if (buffer != null && buffer.capacity() != 0) {
                try {
                    pool.release(buffer);
                } catch (final RuntimeException cleanupFailure) {
                    if (failure == null) failure = cleanupFailure;
                    else failure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (failure != null) throw failure;
    }
}
