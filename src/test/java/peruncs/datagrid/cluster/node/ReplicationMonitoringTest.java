package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.api.ReplicationState;

import static org.junit.jupiter.api.Assertions.*;

/// Tests replication monitoring behavior.
class ReplicationMonitoringTest {
        /// Verifies that Aeron state, lag, and readiness are exposed as raw values.
    @Test
    void exposesAeronTransportStateLagAndReadinessAsRawValues() throws Exception {
        final StorageNodeManager manager = new StorageNodeManager() {

            public long currentSequence() {
                return 7;
            }

            public long latestSequence() {
                return 10;
            }

            public ReplicationState replicationState() {
                return ReplicationState.REPLAYING;
            }

            public void startStorageChecks() {
            }

            public boolean isRunningStorageChecks() {
                return false;
            }

            public boolean isReady() {
                return false;
            }

            public boolean isHealthy() {
                return true;
            }

            public long readStorageSizeBytes() {
                return 123;
            }

            public void close() {
            }
        };

        final var metrics = manager.replicationMetrics();
        assertEquals(7, metrics.currentSequence());
        assertEquals(10, metrics.latestSequence());
        assertEquals(3, metrics.lagTransactions());
        assertEquals(ReplicationState.REPLAYING, metrics.state());
        assertFalse(metrics.ready());
        assertTrue(metrics.healthy());
        assertEquals(123, manager.readStorageSizeBytes());
    }

        /// An unknown writer boundary must report unknown lag, never a healthy zero.
    @Test
    void unknownWriterBoundaryReportsUnknownLag() throws Exception {
        final StorageNodeManager manager = new StorageNodeManager() {

            public long currentSequence() {
                return 7;
            }

            public ReplicationState replicationState() {
                return ReplicationState.REPLAYING;
            }

            public void startStorageChecks() {
            }

            public boolean isRunningStorageChecks() {
                return false;
            }

            public boolean isReady() {
                return true;
            }

            public boolean isHealthy() {
                return true;
            }

            public long readStorageSizeBytes() {
                return 123;
            }

            public void close() {
            }
        };

        final var metrics = manager.replicationMetrics();
        assertEquals(7, metrics.currentSequence());
        assertEquals(-1, metrics.latestSequence());
        assertEquals(-1, metrics.lagTransactions());
    }
}
