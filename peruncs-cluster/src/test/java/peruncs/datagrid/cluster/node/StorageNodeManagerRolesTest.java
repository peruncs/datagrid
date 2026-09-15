package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.exceptions.HttpResponseException;
import peruncs.datagrid.cluster.node.http.ClusterRestRequestController;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the reader/promotable role split is structural, not a runtime flag.
class StorageNodeManagerRolesTest {
        /// A fixed-role reader never distributes and exposes no promotion type.
    @Test
    void readerNeverDistributes() {
        final StorageNodeManager manager = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron", StorageNodeManager.Role.READER);

        assertFalse(manager.isDistributor());
        assertFalse(manager instanceof PromotableStorageNodeManager);
    }

        /// A fixed writer reports itself as the distributor and derives its
        /// health from the distributor instead of a reader health check.
    @Test
    void writerManagerIsDistributor() throws Exception {
        final StorageNodeManager manager = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron",
                StorageNodeManager.Role.DISTRIBUTOR);

        assertTrue(manager.isDistributor());
        assertTrue(manager.isReady(), "a distributor must not depend on a reader health check");
        assertTrue(manager.isHealthy(), "a distributor must not depend on a reader health check");
    }

        /// Aeron roles are fixed at transport creation, so the promotable
    /// factory refuses aeron wiring instead of failing at promotion time.
    @Test
    void promotableFactoryRefusesAeron() {
        assertThrows(IllegalArgumentException.class, () -> PromotableStorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron"));
    }

        /// Finishing without starting fails even before the reader is consulted.
    @Test
    void promotableFinishWithoutStartFails() {
        final PromotableStorageNodeManager manager = PromotableStorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "neutral");

        assertFalse(manager.isDistributor());
        assertThrows(HttpResponseException.class, manager::finishDistributionSwitch);
    }

        /// Promotion endpoints on a fixed-role reader report not-a-distributor.
    @Test
    void controllerRejectsPromotionOnFixedReader() {
        final StorageNodeManager manager = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron", StorageNodeManager.Role.READER);
        final ClusterRestRequestController controller = ClusterRestRequestController.StorageNode(manager);

        final HttpResponseException start = assertThrows(
                HttpResponseException.class, controller::postActivateDistributorStart);
        final HttpResponseException finish = assertThrows(
                HttpResponseException.class, controller::postActivateDistributorFinish);
        assertEquals(400, start.statusCode());
        assertEquals(400, finish.statusCode());
    }

    @SuppressWarnings("unchecked")
    private static <T> T stub(final Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> {
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return null;
                });
    }
}
