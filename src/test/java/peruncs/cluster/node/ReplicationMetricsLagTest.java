package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.replication.ReplicationMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Verifies the -1-when-unknown lag rule owned by [ReplicationMetrics].
class ReplicationMetricsLagTest {
    private static StorageNodeControl manager(final long current, final long latest) {
        return new StorageNodeControl() {
            @Override
            public void startStorageChecks() {
            }

            @Override
            public boolean isRunningStorageChecks() {
                return false;
            }

            @Override
            public boolean isReady() throws NodeException {
                return true;
            }

            @Override
            public boolean isHealthy() {
                return true;
            }

            @Override
            public long readStorageSizeBytes() throws NodeException {
                return 0L;
            }

            @Override
            public long currentSequence() {
                return current;
            }

            @Override
            public long latestSequence() {
                return latest;
            }

            @Override
            public ReplicationState replicationState() {
                return ReplicationState.LIVE;
            }
        };
    }

        /// Unknown boundaries never render as a healthy zero lag.
    @Test
    void unknownBoundariesReportUnknownLag() {
        assertEquals(-1L, manager(5L, -1L).replicationMetrics().lagTransactions());
        assertEquals(-1L, manager(-1L, 9L).replicationMetrics().lagTransactions());
        assertEquals(-1L, manager(-1L, -1L).replicationMetrics().lagTransactions());
    }

        /// Known boundaries report their non-negative distance.
    @Test
    void knownBoundariesReportDistance() {
        assertEquals(4L, manager(5L, 9L).replicationMetrics().lagTransactions());
        assertEquals(0L, manager(9L, 9L).replicationMetrics().lagTransactions());
    }

        /// A current sequence ahead of the observed writer clamps to zero, never negative.
    @Test
    void aheadReaderClampsToZero() {
        assertEquals(0L, manager(12L, 9L).replicationMetrics().lagTransactions());
    }

        /// The snapshot carries the raw boundaries through untouched.
    @Test
    void snapshotPreservesRawSequences() {
        final ReplicationMetrics metrics = manager(5L, 9L).replicationMetrics();

        assertEquals(5L, metrics.currentSequence());
        assertEquals(9L, metrics.latestSequence());
    }
}
