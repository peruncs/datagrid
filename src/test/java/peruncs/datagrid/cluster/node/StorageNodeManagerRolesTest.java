package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the fixed reader/writer role split: the role is chosen at
/// creation and never changes, so an unsupported transition is
/// unrepresentable.
class StorageNodeManagerRolesTest {
        /// A fixed-role reader never distributes.
    @Test
    void readerNeverDistributes() {
        final StorageNodeManager manager = manager(StorageNodeManager.Role.READER, "aeron");

        assertFalse(manager.isWriter());
        assertEquals("aeron", manager.replicationTransport());
    }

        /// A fixed writer reports itself as the writer and derives its
        /// health from the distributor instead of a reader health check.
    @Test
    void writerManagerIsWriter() {
        final StorageNodeManager manager = manager(StorageNodeManager.Role.WRITER, "aeron");

        assertTrue(manager.isWriter());
        assertTrue(manager.isReady(), "a writer must not depend on a reader health check");
        assertTrue(manager.isHealthy(), "a writer must not depend on a reader health check");
    }

        /// The reader reports its current sequence from the replication client.
    @Test
    void readerReportsClientSequence() {
        final StorageBinaryDataClient client = stub(StorageBinaryDataClient.class);
        final StorageNodeManager manager = StorageNodeManager.New(new StorageNodeManager.Configuration(
                stub(StorageBinaryDataDistributor.class),
                stub(StorageTaskExecutor.class),
                client,
                stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class),
                stub(ReplicationPositionProvider.class),
                "none",
                StorageNodeManager.Role.READER));

        assertFalse(manager.isWriter());
        assertEquals(0L, manager.currentSequence());
    }

        /// The writer flag reflects the fixed role.
    @Test
    void writerFlagReflectsFixedRole() {
        final StorageNodeManager reader = manager(StorageNodeManager.Role.READER, "aeron");
        final StorageNodeManager writer = manager(StorageNodeManager.Role.WRITER, "aeron");

        assertFalse(reader.isWriter());
        assertTrue(writer.isWriter());
    }

    private static StorageNodeManager manager(final StorageNodeManager.Role role, final String transport) {
        return StorageNodeManager.New(new StorageNodeManager.Configuration(
                stub(StorageBinaryDataDistributor.class),
                stub(StorageTaskExecutor.class),
                stub(StorageBinaryDataClient.class),
                stub(StorageNodeHealthCheck.class),
                stub(StorageDiskSpaceReader.class),
                stub(ReplicationPositionProvider.class),
                transport,
                role));
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
