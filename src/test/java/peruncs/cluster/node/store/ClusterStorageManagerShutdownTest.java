package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


/// Verifies that partial shutdown can be retried without repeating completed work.
class ClusterStorageManagerShutdownTest {
    /// Verifies a failing shutdown callback can be retried without shutting the underlying Store twice.
    @Test
    void callbackFailureCanBeRetriedWithoutShuttingStoreTwice() {
        final AtomicInteger callbacks = new AtomicInteger();
        final AtomicInteger storeShutdowns = new AtomicInteger();
        final StorageManager store = (StorageManager) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{StorageManager.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("shutdown")) {
                        storeShutdowns.incrementAndGet();
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        final ClusterStorageManager<Object> manager = ClusterStorageManager.create(store, () -> true, () -> {
            if (callbacks.incrementAndGet() == 1) throw new IllegalStateException("retry me");
        });
        assertThrows(IllegalStateException.class, manager::shutdown);
        assertEquals(1, storeShutdowns.get());
        assertFalse(manager.shutdown());
        assertFalse(manager.shutdown());
        assertEquals(2, callbacks.get());
        assertEquals(1, storeShutdowns.get());
    }
}
