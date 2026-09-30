package peruncs.cluster.storage;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationPositionTest {
    @Test
    void noneIsAnExplicitUnresolvedPosition() {
        assertEquals(new ReplicationPosition(null, null, -1L, -1L, -1L, -1L, 0L, null),
                ReplicationPosition.NONE);
        assertFalse(ReplicationPosition.NONE.isResolved());
    }

    @Test
    void resolvedPositionRequiresARecordedBoundaryAndFence() {
        final UUID id = UUID.randomUUID();
        final ReplicationPosition position = new ReplicationPosition(id, id, 3L, 4L, 5L, 6L, 7L, id);

        assertEquals(5L, position.sequence());
        assertEquals(6L, position.prepareStartPosition());
        assertEquals(7L, position.fencingToken());
        assertTrue(position.isResolved());
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationPosition(id, id, 3L, 4L, 5L, -1L, 7L, id));
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationPosition(id, id, 3L, 4L, 5L, 6L, 0L, id));
    }
}
