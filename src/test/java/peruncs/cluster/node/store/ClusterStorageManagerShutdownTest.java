package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import org.junit.jupiter.api.Test;
import peruncs.cluster.api.ClusterStorageManager;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that the manager's shutdown() triggers the owning node close,
/// retries a failed close without repeating completed work, and never
/// resurrects a closed Store through start().
class ClusterStorageManagerShutdownTest {
    /// A failing node-close trigger can be retried without double-closing.
    @Test
    void closeFailureCanBeRetriedWithoutRepeatingCompletedWork() {
        final AtomicInteger closeCalls = new AtomicInteger();
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
        /* Stand-in for the owning lifecycle's close: fails once, then
         * performs teardown; each later call only observes it. */
        final NodeClose nodeClose = () ->
        {
            final int call = closeCalls.incrementAndGet();
            if (call == 1) throw new IllegalStateException("retry me");
            return call == 2;
        };
        final ClusterStorageManager<Object> manager = ClusterStorageManagers.guarding(
                store, StorageSizeValidation.notReached(), nodeClose,
                new peruncs.cluster.storage.StorageGraphCoordinator());

        assertThrows(IllegalStateException.class, manager::shutdown);
        assertEquals(0, storeShutdowns.get(), "a failed node close must not route to the raw Store twice");
        assertTrue(manager.shutdown(), "the retry performs the teardown");
        assertFalse(manager.shutdown(), "a later shutdown only observes the completed close");
        assertEquals(2, closeCalls.get(), "observed closes never re-enter the lifecycle");
        assertEquals(0, storeShutdowns.get(),
                "store shutdown belongs to the owning lifecycle, not the facade");
    }

    /// A manager that shut its node down rejects resurrection attempts.
    @Test
    void closedManagerRejectsStart() {
        final StorageManager store = (StorageManager) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{StorageManager.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("shutdown")) return true;
                    throw new UnsupportedOperationException(method.getName());
                });
        final AtomicInteger closeCalls = new AtomicInteger();
        final NodeClose nodeClose = () -> closeCalls.incrementAndGet() == 1;
        final ClusterStorageManager<Object> manager = ClusterStorageManagers.guarding(
                store, StorageSizeValidation.notReached(), nodeClose,
                new peruncs.cluster.storage.StorageGraphCoordinator());
        assertTrue(manager.shutdown());
        assertFalse(manager.shutdown(), "a second shutdown observes the completed close");
        assertThrows(IllegalStateException.class, manager::start,
                "start() must never resurrect a closed Store");
    }
}
