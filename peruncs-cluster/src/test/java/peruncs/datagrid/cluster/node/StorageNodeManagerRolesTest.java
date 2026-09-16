package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the fixed reader/distributor role split: the role is chosen at
/// creation and never changes, so an unsupported transition is
/// unrepresentable.
class StorageNodeManagerRolesTest {
        /// A fixed-role reader never distributes.
    @Test
    void readerNeverDistributes() {
        final StorageNodeManager manager = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron", StorageNodeManager.Role.READER);

        assertFalse(manager.isDistributor());
        assertEquals("aeron", manager.getReplicationTransport());
    }

        /// A fixed writer reports itself as the distributor and derives its
        /// health from the distributor instead of a reader health check.
    @Test
    void writerManagerIsDistributor() {
        final StorageNodeManager manager = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron",
                StorageNodeManager.Role.DISTRIBUTOR);

        assertTrue(manager.isDistributor());
        assertTrue(manager.isReady(), "a distributor must not depend on a reader health check");
        assertTrue(manager.isHealthy(), "a distributor must not depend on a reader health check");
    }

        /// The reader reports its current sequence from the replication client.
    @Test
    void readerReportsClientSequence() {
        final StorageBinaryDataClient client = stub(StorageBinaryDataClient.class);
        final StorageNodeManager manager = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                client, stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "none",
                StorageNodeManager.Role.READER);

        assertFalse(manager.isDistributor());
        assertEquals(0L, manager.getCurrentSequence());
    }

        /// The distributor flag reflects the fixed role.
    @Test
    void distributorFlagReflectsFixedRole() {
        final StorageNodeManager reader = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron", StorageNodeManager.Role.READER);
        final StorageNodeManager writer = StorageNodeManager.New(
                stub(StorageBinaryDataDistributor.class), stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class), stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class), stub(ReplicationPositionProvider.class), "aeron",
                StorageNodeManager.Role.DISTRIBUTOR);

        assertFalse(reader.isDistributor());
        assertTrue(writer.isDistributor());
    }

    @SuppressWarnings("unchecked")
    private static <T> T stub(final Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> {
                    if (method.getName().equals("cursor")) {
                        return new ReplicationCursor("test", null, 0L, "");
                    }
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return null;
                });
    }
}
