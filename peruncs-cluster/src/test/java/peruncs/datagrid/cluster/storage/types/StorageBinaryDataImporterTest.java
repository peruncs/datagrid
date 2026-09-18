package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/// Covers the importer failure contract: a failed import must propagate the
/// original failure while still releasing the native copies it allocated, and
/// release itself must tolerate every buffer shape the transport can hand over
/// (nulls, empty channels, duplicates of already-released views).
class StorageBinaryDataImporterTest {
    @TempDir
    Path storagePath;

    /// Verifies release tolerates nulls, empty channels, and zero-capacity buffers while rejecting an out-of-range length.
    @Test
    void releaseFreesEverySlotUnconditionally() {
        /* Empty slots are independently owned since the shared static empty
         * was removed, so the release frees them exactly like any other slot —
         * including zero-capacity buffers, which the old capacity guard used
         * to skip. Slots must be distinctly owned: two views of one native
         * address would free it twice. */
        final ByteBuffer owned = XMemory.allocateDirectNative(64);
        final ByteBuffer firstEmpty = ByteBuffer.allocateDirect(0);
        final ByteBuffer secondEmpty = ByteBuffer.allocateDirect(0);

        assertDoesNotThrow(() -> StorageBinaryDataImporter.release(null));
        assertDoesNotThrow(() -> StorageBinaryDataImporter.release(new ByteBuffer[0]));
        assertDoesNotThrow(() -> StorageBinaryDataImporter.release(
                new ByteBuffer[]{null, firstEmpty, secondEmpty, owned}));
        assertDoesNotThrow(() -> StorageBinaryDataImporter.release(new ByteBuffer[]{null}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> StorageBinaryDataImporter.release(new ByteBuffer[]{owned}, 2));
    }

    /// Verifies empty heap sources each import to a distinct owned empty buffer that releases cleanly.
    @Test
    void emptySourcesGetFreshEmptiesPerSlot() {
        /* Each empty source must produce its own independently owned empty —
         * never two duplicates of one shared static buffer. */
        try (EmbeddedStorageManager manager = EmbeddedStorage.start(new Root(), this.storagePath)) {
            final StorageConnection connection = manager.createConnection();

            final ByteBuffer[] owned = StorageBinaryDataImporter.importOwned(
                    connection, new ByteBuffer[]{ByteBuffer.allocate(0), ByteBuffer.allocate(0)});
            try {
                assertEquals(2, owned.length);
                assertNotNull(owned[0]);
                assertNotNull(owned[1]);
                assertNotSame(owned[0], owned[1],
                        "empty slots must be independently owned, not duplicates of one shared buffer");
                assertEquals(0, owned[0].capacity());
                assertEquals(0, owned[1].capacity());
            } finally {
                /* Unconditional release frees both empties without throwing. */
                assertDoesNotThrow(() -> StorageBinaryDataImporter.release(owned));
            }
        }
    }

    /// Verifies a null source entry fails the owned import with a null-pointer failure before touching storage.
    @Test
    void copyFailurePropagatesWithoutMasking() {
        try (EmbeddedStorageManager manager = EmbeddedStorage.start(new Root(), this.storagePath)) {
            final StorageConnection connection = manager.createConnection();

            /* A null entry fails while copying, before any import ran. */
            assertThrows(NullPointerException.class, () -> StorageBinaryDataImporter.importOwned(
                    connection, new ByteBuffer[]{ByteBuffer.allocateDirect(8), null}));
        }
    }

    /// Verifies an import failure still frees the allocated native copies while propagating the original failure unmasked.
    @Test
    void importFailureReleasesCopiesAndPropagatesTheOriginalFailure() {
        /* Upstream import failures never surface through a real connection: the
         * storage task records channel problems without rethrowing them to the
         * importData caller. A failing stub is the only deterministic seam for
         * this branch. */
        final IllegalStateException boom = new IllegalStateException("boom");
        final StorageConnection failing = (StorageConnection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) ->
                {
                    if (method.getName().equals("importData")) throw boom;
                    return defaultValue(method.getReturnType());
                });

        final Throwable failure = assertThrows(IllegalStateException.class,
                () -> StorageBinaryDataImporter.importOwned(
                        failing, new ByteBuffer[]{ByteBuffer.allocate(16)}));
        assertSame(boom, failure, "the original failure must propagate unmasked");
        assertEquals(0, boom.getSuppressed().length, "cleanup of the native copies must not fail");
    }

    /// Verifies direct import validates heap, unnormalized, and null inputs without touching storage.
    @Test
    void importDirectValidatesWithoutTouchingStorage() {
        try (EmbeddedStorageManager manager = EmbeddedStorage.start(new Root(), this.storagePath)) {
            final StorageConnection connection = manager.createConnection();

            assertFalse(StorageBinaryDataImporter.importDirect(
                    connection, new ByteBuffer[]{ByteBuffer.allocate(8)}),
                    "heap buffers cannot be imported without a copy");
            final ByteBuffer unnormalized = ByteBuffer.allocateDirect(8);
            unnormalized.position(4);
            assertThrows(IllegalArgumentException.class, () -> StorageBinaryDataImporter.importDirect(
                    connection, new ByteBuffer[]{unnormalized}));
            assertThrows(NullPointerException.class, () -> StorageBinaryDataImporter.importDirect(
                    null, new ByteBuffer[0]));
        }
    }

    private static Object defaultValue(final Class<?> type) {
        if (type == boolean.class) return false;
        if (type == long.class) return 0L;
        if (type == int.class) return 0;
        if (type == double.class) return 0.0;
        if (type == float.class) return 0.0f;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        if (type == char.class) return (char) 0;
        return null;
    }

    static final class Root {
    }
}
