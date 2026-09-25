package peruncs.cluster.node.replication;

import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.Crc32C;
import peruncs.cluster.storage.ReplicationCursor;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Tests replication cursor store behavior.
class ReplicationCursorStoreTest {
        /// Verifies atomic round-tripping of an opaque provider position.
    @Test
    void roundTripsOpaqueProviderPositionAtomically() throws Exception {
        final var path = Files.createTempFile("datagrid-replication", ".cursor");
        final var expected = new ReplicationCursor("aeron", UUID.randomUUID(), 17, "040506");
        ReplicationCursorStore.write(path, expected);
        assertEquals(expected, ReplicationCursorStore.read(path));
        Files.deleteIfExists(path);
    }

        /// Verifies detection of corrupt cursor before using provider bytes.
    @Test
    void detectsCorruptCursorBeforeUsingProviderBytes() throws Exception {
        final var path = Files.createTempFile("datagrid-replication", ".cursor");
        ReplicationCursorStore.write(path, new ReplicationCursor("aeron", null, 3, "01"));
        final byte[] bytes = Files.readAllBytes(path);
        bytes[10] ^= 1;
        Files.write(path, bytes);
        assertThrows(java.io.IOException.class, () -> ReplicationCursorStore.read(path));
        Files.deleteIfExists(path);
    }

        /// Verifies that a valid checksum cannot hide an appended cursor payload.
    @Test
    void rejectsTrailingCursorBytes() throws Exception {
        final var path = Files.createTempFile("datagrid-replication", ".cursor");
        ReplicationCursorStore.write(path, new ReplicationCursor("aeron", null, 3, "01"));
        final byte[] original = Files.readAllBytes(path);
        final byte[] extended = Arrays.copyOf(original, original.length + 1);
        System.arraycopy(original, 0, extended, 0, original.length - Integer.BYTES);
        final int crc = Crc32C.compute(extended, 0, extended.length - Integer.BYTES);
        ByteBuffer.wrap(extended).putInt(extended.length - Integer.BYTES, crc);
        Files.write(path, extended);

        assertThrows(java.io.IOException.class, () -> ReplicationCursorStore.read(path));
        Files.deleteIfExists(path);
    }

        /// Verifies flags and UTF-8 are validated before cursor construction.
    @Test
    void rejectsUnknownFlagsAndMalformedUtf8() throws Exception {
        final byte[] flags = ReplicationCursorStore.encode(
                new ReplicationCursor("aeron", null, 3, "01"));
        ByteBuffer.wrap(flags).putShort(6, (short) 1);
        ByteBuffer.wrap(flags).putInt(flags.length - Integer.BYTES,
                Crc32C.compute(flags, 0, flags.length - Integer.BYTES));
        assertThrows(java.io.IOException.class, () -> ReplicationCursorStore.decode(flags));

        final byte[] malformed = ReplicationCursorStore.encode(
                new ReplicationCursor("aeron", null, 3, "01"));
        malformed[12] = (byte) 0xc3;
        malformed[13] = 0x28;
        ByteBuffer.wrap(malformed).putInt(malformed.length - Integer.BYTES,
                Crc32C.compute(malformed, 0, malformed.length - Integer.BYTES));
        assertThrows(java.io.IOException.class, () -> ReplicationCursorStore.decode(malformed));
    }
}
