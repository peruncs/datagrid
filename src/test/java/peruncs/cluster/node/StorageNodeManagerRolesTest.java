package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.StorageNodeHealthCheck;
import peruncs.cluster.node.store.StorageTaskExecutor;
import peruncs.cluster.node.store.StorageUsageGauge;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.ReplicationPublisher;

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
        /* The transport is no longer a monitoring surface: metrics compose
         * from the state, sequence, and readiness components only. */
        assertFalse(manager.isRunningStorageChecks());
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
        final ReplicationApplier client = stub(ReplicationApplier.class);
        final StorageNodeManager manager = StorageNodeManager.create(new StorageNodeManager.Configuration(
                stub(ReplicationPublisher.class),
                stub(StorageTaskExecutor.class),
                client,
                stub(StorageNodeHealthCheck.class),
                stub(StorageUsageGauge.class),
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
        return StorageNodeManager.create(new StorageNodeManager.Configuration(
                stub(ReplicationPublisher.class),
                stub(StorageTaskExecutor.class),
                stub(ReplicationApplier.class),
                stub(StorageNodeHealthCheck.class),
                stub(StorageUsageGauge.class),
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
