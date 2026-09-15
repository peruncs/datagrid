package peruncs.datagrid.cluster.nodelibrary.replication;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests replication cursor behavior. */
class ReplicationCursorTest {
    /** Verifies provider position is defensive and identity is value based. */
    @Test
    void providerPositionIsDefensiveAndIdentityIsValueBased() {
        final byte[] position = {1, 2, 3};
        final UUID generation = UUID.randomUUID();
        final ReplicationCursor cursor = new ReplicationCursor("aeron", generation, 8, position);
        position[0] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, cursor.providerPosition());
        final byte[] returned = cursor.providerPosition();
        returned[1] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, cursor.providerPosition());
    }

    /** Verifies rejection of invalid sequence and transport. */
    @Test
    void rejectsInvalidSequenceAndTransport() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationCursor("", null, -1, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationCursor("aeron", null, -2, new byte[0]));
    }
}
