package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.ReplicationPosition;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the neutral client's best-effort lifecycle reporting.
class ReplicationApplierTest {
        /// The neutral default reports a resolved boundary with an unknown
        /// transport position instead of failing or fabricating one.
    @Test
    void noOpClientReportsBestEffortStopResult() {
        final ReplicationApplier client = ReplicationApplier.noOp();
        assertSame(ReplicationPosition.NONE, client.position(),
                "a non-replicated client reports the none position");

        final ReplicationApplier.StopResult result = client.stopResult();
        assertEquals(ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY, result.outcome());
        assertEquals(-1L, result.sequence());
        assertEquals(-1L, result.position(), "the fallback cannot know a transport position");
        assertFalse(result.hasSequence());
        assertFalse(result.hasPosition());
        assertNull(client.failure());
    }

        /// The default currentSequence delegates to the published position.
    @Test
    void defaultCurrentSequenceDelegatesToPosition() {
        final UUID id = UUID.randomUUID();
        final ReplicationPosition position = new ReplicationPosition(id, id, 1L, 1L, 41L, 1L, 1L, id);
        final ReplicationApplier client = new ReplicationApplier() {
            @Override
            public void start() {
            }

            @Override
            public void stopAtLatestMessage() {
            }

            @Override
            public ReplicationPosition position() {
                return position;
            }

            @Override
            public boolean isRunning() {
                return false;
            }

            @Override
            public RuntimeException failure() {
                return null;
            }

            @Override
            public void resume() {
            }

            @Override
            public void dispose() {
            }
        };

        assertEquals(41L, client.currentSequence());
        final ReplicationApplier.StopResult result = client.stopResult();
        assertTrue(result.hasSequence());
        assertTrue(result.hasPosition());
    }

        /// A client that cannot produce a position still yields a best-effort stop
        /// result instead of a null dereference.
    @Test
    void nullPositionNormalizesToNoneInStopResult() {
        final ReplicationApplier client = new ReplicationApplier() {
            @Override
            public void start() {
            }

            @Override
            public void stopAtLatestMessage() {
            }

            @Override
            public ReplicationPosition position() {
                return null;
            }

            @Override
            public boolean isRunning() {
                return false;
            }

            @Override
            public RuntimeException failure() {
                return null;
            }

            @Override
            public void resume() {
            }

            @Override
            public void dispose() {
            }
        };

        assertEquals(-1L, client.stopResult().sequence());
    }
}
