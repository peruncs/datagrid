package peruncs.cluster.storage.binary;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.util.X;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.util.X.notNull;

/// Copies incoming Store binary buffers into owned native memory and imports them.
///
/// Non-empty slots own distinct native memory. Empty slots are distinct views
/// of one shared zero-capacity buffer: Store's identity set keeps every slot.
public final class StorageBinaryDataImporter {
    private static final ByteBuffer EMPTY_BACKING = ByteBuffer.allocateDirect(0);
    private StorageBinaryDataImporter() {
    }

        /// Imports a snapshot of the source buffers and returns the native buffers that
    /// remain owned by the caller for subsequent object-graph materialization.
    /// Source positions are never changed.
    ///
    /// @param storage       destination Store connection
    /// @param sourceBuffers source buffers
    /// @return imported native buffers, owned by the caller
    static ByteBuffer[] importOwned(
            final StorageConnection storage,
            final ByteBuffer[] sourceBuffers
    ) {
        notNull(storage);
        notNull(sourceBuffers);
        final ByteBuffer[] ownedBuffers = copyBuffers(sourceBuffers);
        try {
            return importAndReset(storage, ownedBuffers);
        } catch (final RuntimeException | Error failure) {
            releaseAfterFailure(ownedBuffers, failure);
            throw failure;
        }
    }

    private static ByteBuffer[] importAndReset(final StorageConnection storage, final ByteBuffer[] importedBuffers) {
        /* The pinned Store import task reads buffer addresses and limits, and
         * its file-copy path slices a duplicate. It does not change these
         * owned views before the synchronous importData call returns. */
        storage.importData(X.Enum(importedBuffers));
        return importedBuffers;
    }

    /// Copies the source buffers into distinctly owned native buffers without
    /// importing them. Used by the merger's deferred-import path: the borrowed
    /// binary is released by the transport as soon as its buffers are safely
    /// copied, and the ordered Store imports run later inside one drained batch.
    ///
    /// @param sourceBuffers normalized source buffers
    /// @return distinctly owned native copies
    static ByteBuffer[] copyOwned(final ByteBuffer[] sourceBuffers) {
        notNull(sourceBuffers);
        return copyBuffers(sourceBuffers);
    }

    private static ByteBuffer[] copyBuffers(final ByteBuffer[] sourceBuffers) {
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
                final ByteBuffer owned = XMemory.allocateDirectNative(sourceLength);
                ownedBuffers[i] = owned;
                owned.put(0, source, 0, sourceLength);
            }
            return ownedBuffers;
        } catch (final RuntimeException | Error failure) {
            releaseAfterFailure(ownedBuffers, failure);
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
            storage.importData(X.Enum(imported));
        } finally {
            if (imported == reusableViews) java.util.Arrays.fill(imported, null);
        }
        return true;
    }

    private static void releaseAfterFailure(final ByteBuffer[] buffers, final Throwable failure) {
        try {
            release(buffers);
        } catch (final Throwable cleanupFailure) {
            if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
        }
    }

        /// Releases native buffers returned by [#importOwned(StorageConnection, ByteBuffer\[\])].
    ///
    /// Every non-empty owned slot is deallocated. Empty views have no native
    /// ownership; the shared backing remains available for later imports.
    ///
    /// @param buffers buffers to release
    static void release(final ByteBuffer[] buffers) {
        if (buffers == null) return;
        release(buffers, buffers.length);
    }

        /// Releases the first `length` slots of a scratch array.
    ///
    /// Scratch arrays are usually larger than the batch they hold; only the
    /// populated prefix is owned. Slots past `length` are untouched and keep
    /// whatever value (normally `null`) the caller left there.
    ///
    /// @param buffers scratch array holding owned buffers in its prefix
    /// @param length  number of populated prefix slots
    static void release(final ByteBuffer[] buffers, final int length) {
        if (buffers == null) return;
        if (length < 0 || length > buffers.length) {
            throw new IllegalArgumentException("release length out of range: %s".formatted(length));
        }
        RuntimeException failure = null;
        for (int index = 0; index < length; index++) {
            final ByteBuffer buffer = buffers[index];
            if (buffer != null && buffer.capacity() != 0) {
                try {
                    XMemory.deallocateDirectByteBuffer(buffer);
                } catch (final RuntimeException cleanupFailure) {
                    if (failure == null) failure = cleanupFailure;
                    else failure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (failure != null) throw failure;
    }
}
