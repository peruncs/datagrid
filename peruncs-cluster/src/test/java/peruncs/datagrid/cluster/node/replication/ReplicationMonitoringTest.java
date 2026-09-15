package peruncs.datagrid.cluster.node.replication;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.StorageNodeManager;
import peruncs.datagrid.cluster.node.http.ClusterRestRequestController;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests replication monitoring behavior.
class ReplicationMonitoringTest {
        /// Verifies that Aeron transport state, lag, and readiness are exposed as raw values.
    @Test
    void exposesAeronTransportStateLagAndReadinessAsPrometheusMetrics() throws Exception {
        final StorageNodeManager manager = new StorageNodeManager() {
            public boolean isDistributor() {
                return false;
            }

            public long getCurrentSequence() {
                return 7;
            }

            public long getLatestSequence() {
                return 10;
            }

            public String getReplicationTransport() {
                return "aeron";
            }

            public ReplicationHealth.State getReplicationState() {
                return ReplicationHealth.State.REPLAYING;
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

        final ClusterRestRequestController controller = ClusterRestRequestController.StorageNode(manager);
        final var metrics = controller.getReplicationMetrics();
        assertEquals(7, metrics.currentSequence());
        assertEquals(10, metrics.latestSequence());
        assertEquals(3, metrics.lagTransactions());
        assertEquals("aeron", metrics.transport());
        assertEquals(ReplicationHealth.State.REPLAYING, metrics.state());
        assertFalse(metrics.ready());
        assertTrue(metrics.healthy());
        assertEquals(123, controller.getStorageBytes());
        controller.close();
    }

        /// An unknown writer boundary must report unknown lag, never a healthy zero.
    @Test
    void unknownWriterBoundaryReportsUnknownLag() throws Exception {
        final StorageNodeManager manager = new StorageNodeManager() {
            public boolean isDistributor() {
                return false;
            }

            public long getCurrentSequence() {
                return 7;
            }

            public long getLatestSequence() {
                return -1;
            }

            public String getReplicationTransport() {
                return "aeron";
            }

            public ReplicationHealth.State getReplicationState() {
                return ReplicationHealth.State.REPLAYING;
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

        final ClusterRestRequestController controller = ClusterRestRequestController.StorageNode(manager);
        try {
            final var metrics = controller.getReplicationMetrics();
            assertEquals(7, metrics.currentSequence());
            assertEquals(-1, metrics.latestSequence());
            assertEquals(-1, metrics.lagTransactions());
        } finally {
            controller.close();
        }
    }
}
