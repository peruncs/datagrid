package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import org.junit.jupiter.api.Test;
import peruncs.cluster.api.ClusterStorageManager;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        final NodeClose nodeClose = new NodeClose() {
            @Override
            public boolean close() {
                final int call = closeCalls.incrementAndGet();
                if (call == 1) throw new IllegalStateException("retry me");
                return call == 2;
            }

            @Override
            public void checkOpen() {
            }
        };
        final ClusterStorageManager<Object> manager = ClusterStorageManagers.guarding(
                store, StorageSizeValidation.notReached(), nodeClose,
                new peruncs.cluster.storage.StorageGraphCoordinator());

        assertThrows(IllegalStateException.class, manager::shutdown);
        assertEquals(0, storeShutdowns.get(), "a failed node close must not route to the raw Store twice");
        assertTrue(manager.shutdown(), "the retry performs the teardown");
        assertFalse(manager.shutdown(), "a later shutdown only observes the completed close");
        assertEquals(3, closeCalls.get(),
                "an observed close still runs the lifecycle trigger, which answers without re-closing");
        assertEquals(0, storeShutdowns.get(),
                "store shutdown belongs to the owning lifecycle, not the facade");
    }

    /// Two concurrent shutdown callers never both report observing a
    /// completed close prematurely: the second either joins the in-flight
    /// attempt and observes its outcome (throw included), or returns after
    /// it has finished. Neither may return while teardown is still running.
    @Test
    void concurrentShutdownJoinsTheInFlightAttempt() throws Exception {
        final StorageManager store = (StorageManager) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{StorageManager.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("shutdown")) return true;
                    if (method.getName().equals("isRunning")) return true;
                    throw new UnsupportedOperationException(method.getName());
                });
        final java.util.concurrent.CountDownLatch inClose = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        final RuntimeException boom = new IllegalStateException("close failed");
        final AtomicInteger closeCalls = new AtomicInteger();
        final NodeClose nodeClose = new NodeClose() {
            @Override
            public boolean close() {
                closeCalls.incrementAndGet();
                inClose.countDown();
                try {
                    if (!release.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new AssertionError("close was never released");
                    }
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                throw boom;
            }

            @Override
            public void checkOpen() {
            }
        };
        final peruncs.cluster.api.ClusterStorageManager<Object> manager = ClusterStorageManagers.guarding(
                store, StorageSizeValidation.notReached(), nodeClose,
                new peruncs.cluster.storage.StorageGraphCoordinator());
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        final AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        final AtomicReference<Boolean> secondResult = new AtomicReference<>();
        final Thread first = Thread.ofVirtual().start(() ->
        {
            try {
                firstFailure.set(manager.shutdown() ? null : new AssertionError("close must report performed"));
            } catch (final Throwable failure) {
                firstFailure.compareAndSet(null, failure);
            }
        });
        assertTrue(inClose.await(30, java.util.concurrent.TimeUnit.SECONDS), "first close never ran");
        /* The second shutdown must join the in-flight attempt instead of
         * answering early with a stale flag. While the first close is blocked,
         * the second MUST NOT have returned yet. */
        final Thread second = Thread.ofVirtual().start(() ->
        {
            try {
                secondResult.set(manager.shutdown());
            } catch (final Throwable failure) {
                secondFailure.set(failure);
            }
        });
        second.join(300L);
        assertTrue(second.isAlive(), "the second shutdown returned before the in-flight close finished");
        release.countDown();
        first.join(30_000L);
        second.join(30_000L);
        assertSame(boom, firstFailure.get(), "the first caller observed the close failure");
        assertSame(boom, secondFailure.get(), "the joined caller observes the same recorded failure");
        assertNull(secondResult.get(), "the joined caller must never success-answer a failed close");
        /* The failed close leaves the manager retryable: a subsequent close
         * re-enters the lifecycle (stub observably delegates the retry). */
        assertThrows(IllegalStateException.class, manager::shutdown);
        assertEquals(3, closeCalls.get(), "the lifecycle owns every retry decision");
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
        final NodeClose nodeClose = new NodeClose() {
            @Override
            public boolean close() {
                return closeCalls.incrementAndGet() == 1;
            }

            @Override
            public void checkOpen() {
            }
        };
        final ClusterStorageManager<Object> manager = ClusterStorageManagers.guarding(
                store, StorageSizeValidation.notReached(), nodeClose,
                new peruncs.cluster.storage.StorageGraphCoordinator());
        assertTrue(manager.shutdown());
        assertFalse(manager.shutdown(), "a second shutdown observes the completed close");
        assertThrows(IllegalStateException.class, manager::start,
                "start() must never resurrect a closed Store");
    }
}
