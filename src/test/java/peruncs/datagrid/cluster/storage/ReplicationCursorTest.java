package peruncs.datagrid.cluster.storage;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Tests replication cursor behavior.
class ReplicationCursorTest {
        /// Verifies the position is stored as immutable hex and identity is value based.
    @Test
    void providerPositionIsImmutableHexAndIdentityIsValueBased() {
        final UUID generation = UUID.randomUUID();
        final ReplicationCursor cursor = ReplicationCursor.of("aeron", generation, 8, new byte[]{1, 2, 3});

        assertEquals("010203", cursor.providerPosition());
        assertTrue(cursor.hasProviderPosition());
        assertEquals(cursor, new ReplicationCursor("aeron", generation, 8, "010203"));
        assertEquals(cursor.hashCode(), new ReplicationCursor("aeron", generation, 8, "010203").hashCode());
    }

        /// Verifies empty and null positions normalize to the absent marker.
    @Test
    void emptyPositionMeansAbsent() {
        assertEquals("", new ReplicationCursor("aeron", null, -1, "").providerPosition());
        assertEquals("", ReplicationCursor.of("aeron", null, -1, null).providerPosition());
        assertEquals("", ReplicationCursor.of("aeron", null, -1, new byte[0]).providerPosition());
        assertFalse(new ReplicationCursor("aeron", null, -1, "").hasProviderPosition());
    }

        /// Verifies uppercase hex is accepted but normalized to lowercase.
    @Test
    void uppercaseHexIsNormalizedToLowercase() {
        final ReplicationCursor cursor = new ReplicationCursor("aeron", null, -1, "AB12");
        assertEquals("ab12", cursor.providerPosition());
        assertArrayEquals(new byte[]{(byte) 0xAB, 0x12}, cursor.providerPositionBytes());
        assertEquals(new ReplicationCursor("aeron", null, -1, "ab12"), cursor);
    }

        /// Verifies rejection of invalid sequence, transport, and position.
    @Test
    void rejectsInvalidSequenceTransportAndPosition() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationCursor("", null, -1, ""));
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationCursor("aeron", null, -2, ""));
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationCursor("aeron", null, -1, "zz"));
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationCursor("aeron", null, -1, "abc"));
    }
}
