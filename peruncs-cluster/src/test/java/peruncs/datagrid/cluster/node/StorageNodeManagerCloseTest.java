package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies close-once disposal: a failed close is never retried, and
/// promotion-released reader resources are not disposed again.
class StorageNodeManagerCloseTest {
    private static final class CountingHandler implements InvocationHandler {
        final AtomicInteger disposeCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        volatile RuntimeException disposeFailure;

        @Override
        public Object invoke(final Object proxy, final java.lang.reflect.Method method, final Object[] args) {
            switch (method.getName()) {
                case "dispose" -> {
                    this.disposeCalls.incrementAndGet();
                    if (this.disposeFailure != null) throw this.disposeFailure;
                    return null;
                }
                case "close" -> {
                    this.closeCalls.incrementAndGet();
                    return null;
                }
                case "cursor" -> {
                    return new ReplicationCursor("test", UUID.randomUUID(), 5L, "");
                }
                case "stopResult" -> {
                    return new StorageBinaryDataClient.StopResult(
                            StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY, 5L, -1L);
                }
                default -> {
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return null;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T tracked(final Class<T> type, final CountingHandler handler) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static StorageNodeManager reader(
            final CountingHandler distributor, final CountingHandler client,
            final CountingHandler health, final CountingHandler position) {
        return StorageNodeManager.New(
                tracked(StorageBinaryDataDistributor.class, distributor),
                tracked(StorageTaskExecutor.class, new CountingHandler()),
                tracked(StorageBinaryDataClient.class, client),
                tracked(StorageNodeHealthCheck.class, health),
                tracked(StorageDiskSpaceReader.class, new CountingHandler()),
                tracked(ReplicationPositionProvider.class, position),
                "aeron");
    }

        /// A close that fails mid-way still releases everything exactly once;
        /// a retry is a no-op instead of re-disposing.
    @Test
    void failedCloseIsNotRetried() {
        final CountingHandler distributor = new CountingHandler();
        final CountingHandler client = new CountingHandler();
        final CountingHandler health = new CountingHandler();
        final CountingHandler position = new CountingHandler();
        distributor.disposeFailure = new IllegalStateException("distributor close failed");
        final StorageNodeManager manager = reader(distributor, client, health, position);

        assertThrows(NodeLibraryException.class, manager::close);
        manager.close();

        assertEquals(1, distributor.disposeCalls.get(), "distributor re-disposed on retry");
        assertEquals(1, client.disposeCalls.get(), "data client re-disposed on retry");
        assertEquals(1, health.closeCalls.get(), "health check re-closed on retry");
        assertEquals(1, position.closeCalls.get(), "position provider re-closed on retry");
    }

        /// Reader resources released by promotion are skipped by a later close.
    @Test
    void promotionThenCloseSkipsReaderResources() throws Exception {
        final CountingHandler distributor = new CountingHandler();
        final CountingHandler client = new CountingHandler();
        final CountingHandler health = new CountingHandler();
        final CountingHandler position = new CountingHandler();
        final PromotableStorageNodeManager manager = PromotableStorageNodeManager.New(
                tracked(StorageBinaryDataDistributor.class, distributor),
                tracked(StorageTaskExecutor.class, new CountingHandler()),
                tracked(StorageBinaryDataClient.class, client),
                tracked(StorageNodeHealthCheck.class, health),
                tracked(StorageDiskSpaceReader.class, new CountingHandler()),
                tracked(ReplicationPositionProvider.class, position),
                "neutral");

        manager.switchToDistribution();
        assertTrue(manager.finishDistributionSwitch());
        manager.close();

        assertEquals(1, client.disposeCalls.get(), "data client disposed twice across promotion and close");
        assertEquals(1, health.closeCalls.get(), "health check closed twice across promotion and close");
        assertEquals(1, distributor.disposeCalls.get(), "distributor not disposed on close");
        assertEquals(1, position.closeCalls.get(), "position provider not closed on close");
    }
}
