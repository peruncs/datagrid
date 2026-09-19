package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the neutral client's best-effort lifecycle reporting.
class StorageBinaryDataClientTest {
        /// The neutral default reports a resolved boundary with an unknown
        /// transport position instead of failing or fabricating one.
    @Test
    void noOpClientReportsBestEffortStopResult() {
        final StorageBinaryDataClient client = StorageBinaryDataClient.NoOp(null);
        assertSame(ReplicationCursor.NONE, client.cursor(),
                "a null starting cursor must normalize to the none cursor");

        final StorageBinaryDataClient.StopResult result = client.stopResult();
        assertEquals(StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY, result.outcome());
        assertEquals(-1L, result.sequence());
        assertEquals(-1L, result.position(), "the fallback cannot know a transport position");
        assertNull(client.failure());
    }

        /// The default currentSequence delegates to the published cursor.
    @Test
    void defaultCurrentSequenceDelegatesToCursor() {
        final ReplicationCursor cursor = new ReplicationCursor("test", null, 41L, "");
        final StorageBinaryDataClient client = new StorageBinaryDataClient() {
            @Override
            public void start() {
            }

            @Override
            public void stopAtLatestMessage() {
            }

            @Override
            public ReplicationCursor cursor() {
                return cursor;
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
    }

        /// A client that cannot produce a cursor still yields a best-effort stop
        /// result instead of a null dereference.
    @Test
    void nullCursorNormalizesToNoneInStopResult() {
        final StorageBinaryDataClient client = new StorageBinaryDataClient() {
            @Override
            public void start() {
            }

            @Override
            public void stopAtLatestMessage() {
            }

            @Override
            public ReplicationCursor cursor() {
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
