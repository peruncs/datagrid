package peruncs.datagrid.cluster.nodelibrary.replication;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.types.Crc32c;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests replication cursor store behavior. */
class ReplicationCursorStoreTest {
    /** Verifies atomic round-tripping of an opaque provider position. */
    @Test
    void roundTripsOpaqueProviderPositionAtomically() throws Exception {
        final var path = Files.createTempFile("datagrid-replication", ".cursor");
        final var expected = new ReplicationCursor("aeron", UUID.randomUUID(), 17, new byte[]{4, 5, 6});
        ReplicationCursorStore.write(path, expected);
        assertEquals(expected, ReplicationCursorStore.read(path));
        Files.deleteIfExists(path);
    }

    /** Verifies detection of corrupt cursor before using provider bytes. */
    @Test
    void detectsCorruptCursorBeforeUsingProviderBytes() throws Exception {
        final var path = Files.createTempFile("datagrid-replication", ".cursor");
        ReplicationCursorStore.write(path, new ReplicationCursor("aeron", null, 3, new byte[]{1}));
        final byte[] bytes = Files.readAllBytes(path);
        bytes[10] ^= 1;
        Files.write(path, bytes);
        assertThrows(java.io.IOException.class, () -> ReplicationCursorStore.read(path));
        Files.deleteIfExists(path);
    }

    /** Verifies that a valid checksum cannot hide an appended cursor payload. */
    @Test
    void rejectsTrailingCursorBytes() throws Exception {
        final var path = Files.createTempFile("datagrid-replication", ".cursor");
        ReplicationCursorStore.write(path, new ReplicationCursor("aeron", null, 3, new byte[]{1}));
        final byte[] original = Files.readAllBytes(path);
        final byte[] extended = Arrays.copyOf(original, original.length + 1);
        System.arraycopy(original, 0, extended, 0, original.length - Integer.BYTES);
        final int crc = Crc32c.compute(extended, 0, extended.length - Integer.BYTES);
        ByteBuffer.wrap(extended).putInt(extended.length - Integer.BYTES, crc);
        Files.write(path, extended);

        assertThrows(java.io.IOException.class, () -> ReplicationCursorStore.read(path));
        Files.deleteIfExists(path);
    }
}
