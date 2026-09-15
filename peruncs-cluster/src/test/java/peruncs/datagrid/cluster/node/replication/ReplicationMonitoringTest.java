package peruncs.datagrid.cluster.node.replication;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.StorageNodeManager;
import peruncs.datagrid.cluster.node.http.ClusterRestRequestController;

import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests replication monitoring behavior.
class ReplicationMonitoringTest {
        /// Verifies that Aeron transport state, lag, and readiness are exposed as Prometheus metrics.
    @Test
    void exposesAeronTransportStateLagAndReadinessAsPrometheusMetrics() throws Exception {
        final StorageNodeManager manager = new StorageNodeManager() {
            public boolean isDistributor() {
                return false;
            }

            public long getCurrentMessageIndex() {
                return 7;
            }

            public long getLatestMessageIndex() {
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

        final ClusterRestRequestController controller = ClusterRestRequestController.StorageNode(
                manager, NodeLibraryPropertiesProvider.Env());
        final String metrics = controller.getReplicationMetrics();
        assertTrue(metrics.contains("cluster_replication_current_sequence{transport=\"aeron\"} 7"));
        assertTrue(metrics.contains("cluster_replication_latest_sequence{transport=\"aeron\"} 10"));
        assertTrue(metrics.contains("cluster_replication_lag_transactions{transport=\"aeron\"} 3"));
        assertTrue(metrics.contains("state=\"replaying\""));
        assertTrue(metrics.contains("cluster_replication_ready{transport=\"aeron\"} 0"));
        assertTrue(metrics.contains("cluster_replication_healthy{transport=\"aeron\"} 1"));
        controller.close();
    }
}
