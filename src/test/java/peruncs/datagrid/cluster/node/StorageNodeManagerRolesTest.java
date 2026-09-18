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
        final StorageNodeManager manager = manager(StorageNodeManager.Role.READER, "aeron");

        assertFalse(manager.isDistributor());
        assertEquals("aeron", manager.getReplicationTransport());
    }

        /// A fixed writer reports itself as the distributor and derives its
        /// health from the distributor instead of a reader health check.
    @Test
    void writerManagerIsDistributor() {
        final StorageNodeManager manager = manager(StorageNodeManager.Role.DISTRIBUTOR, "aeron");

        assertTrue(manager.isDistributor());
        assertTrue(manager.isReady(), "a distributor must not depend on a reader health check");
        assertTrue(manager.isHealthy(), "a distributor must not depend on a reader health check");
    }

        /// The reader reports its current sequence from the replication client.
    @Test
    void readerReportsClientSequence() {
        final StorageBinaryDataClient client = stub(StorageBinaryDataClient.class);
        final StorageNodeManager manager = StorageNodeManager.New(StorageNodeManager.Configuration.builder()
                .dataDistributor(stub(StorageBinaryDataDistributor.class))
                .storageTaskExecutor(stub(StorageTaskExecutor.class))
                .dataClient(client)
                .healthCheck(stub(StorageNodeHealthCheck.class))
                .storageDiskSpaceReader(stub(StorageDiskSpaceReader.class))
                .positionProvider(stub(ReplicationPositionProvider.class))
                .replicationTransport("none")
                .role(StorageNodeManager.Role.READER)
                .build());

        assertFalse(manager.isDistributor());
        assertEquals(0L, manager.getCurrentSequence());
    }

        /// The distributor flag reflects the fixed role.
    @Test
    void distributorFlagReflectsFixedRole() {
        final StorageNodeManager reader = manager(StorageNodeManager.Role.READER, "aeron");
        final StorageNodeManager writer = manager(StorageNodeManager.Role.DISTRIBUTOR, "aeron");

        assertFalse(reader.isDistributor());
        assertTrue(writer.isDistributor());
    }

    private static StorageNodeManager manager(final StorageNodeManager.Role role, final String transport) {
        return StorageNodeManager.New(StorageNodeManager.Configuration.builder()
                .dataDistributor(stub(StorageBinaryDataDistributor.class))
                .storageTaskExecutor(stub(StorageTaskExecutor.class))
                .dataClient(stub(StorageBinaryDataClient.class))
                .healthCheck(stub(StorageNodeHealthCheck.class))
                .storageDiskSpaceReader(stub(StorageDiskSpaceReader.class))
                .positionProvider(stub(ReplicationPositionProvider.class))
                .replicationTransport(transport)
                .role(role)
                .build());
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
