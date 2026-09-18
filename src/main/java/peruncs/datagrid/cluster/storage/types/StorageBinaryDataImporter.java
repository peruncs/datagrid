package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.util.X.notNull;

/// Copies incoming Store binary buffers into owned native memory and imports them.
///
/// Every slot of a returned array is distinctly owned native memory — including
/// empty slots, which each get their own fresh empty direct buffer instead of
/// sharing one static instance. The caller must therefore release every slot
/// exactly once through [#release(ByteBuffer[])] (or the ranged overload);
/// slots are never shared, duplicated, or retained anywhere else.
final class StorageBinaryDataImporter {
    private StorageBinaryDataImporter() {
    }

        /// Imports a snapshot of the source buffers and returns the native buffers that
    /// remain owned by the caller for subsequent object-graph materialization.
    /// Source positions are never changed.
    ///
    /// @param storage       destination Store connection
    /// @param sourceBuffers source buffers
    /// @return imported native buffers, owned by the caller
    public static ByteBuffer[] importOwned(
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
        /* Storage.importData consumes the supplied views synchronously and does not
         * retain them. Reset the owned buffers afterwards because their positions are
         * needed by the deferred materializer. */
        storage.importData(org.eclipse.serializer.util.X.Enum(importedBuffers));
        for (final ByteBuffer imported : importedBuffers) imported.position(0);
        return importedBuffers;
    }

    private static ByteBuffer[] copyBuffers(final ByteBuffer[] sourceBuffers) {
        final ByteBuffer[] ownedBuffers = new ByteBuffer[sourceBuffers.length];
        try {
            for (int i = 0; i < sourceBuffers.length; i++) {
                final ByteBuffer source = notNull(sourceBuffers[i]).duplicate();
                if (source.position() != 0) {
                    throw new IllegalArgumentException(
                            "import buffers must be normalized to position zero; got %s".formatted(source.position()));
                }
                final int sourceLength = source.remaining();
                if (sourceLength == 0) {
                    /* A fresh empty per slot: each slot stays independently
                     * owned so the unconditional release below frees exactly
                     * what this slot owns — never a shared static buffer, and
                     * never a duplicate that would double-free one address. */
                    ownedBuffers[i] = ByteBuffer.allocateDirect(0);
                    continue;
                }
                final ByteBuffer owned = XMemory.allocateDirectNative(sourceLength);
                ownedBuffers[i] = owned;
                owned.put(source).flip();
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
    public static boolean importDirect(final StorageConnection storage, final ByteBuffer[] buffers) {
        notNull(storage);
        notNull(buffers);
        for (final ByteBuffer buffer : buffers) {
            if (buffer == null || !buffer.isDirect()) return false;
            if (buffer.position() != 0) {
                throw new IllegalArgumentException("import buffers must be normalized to position zero");
            }
        }
        importAndReset(storage, buffers);
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
    /// Every non-null slot is deallocated unconditionally — including empty
    /// buffers, which are independently owned since the shared static empty
    /// was removed. Slots must be distinctly owned: passing two views of one
    /// native address would free it twice.
    ///
    /// @param buffers buffers to release
    public static void release(final ByteBuffer[] buffers) {
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
    public static void release(final ByteBuffer[] buffers, final int length) {
        if (buffers == null) return;
        if (length < 0 || length > buffers.length) {
            throw new IllegalArgumentException("release length out of range: %s".formatted(length));
        }
        RuntimeException failure = null;
        for (int index = 0; index < length; index++) {
            final ByteBuffer buffer = buffers[index];
            if (buffer != null) {
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
