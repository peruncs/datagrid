package peruncs.datagrid.cluster.node.store;

import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the single-flight check discipline and the close/check race.
class StorageTaskExecutorTest {
    private static final class GatedConnection {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger checks = new AtomicInteger();
        final StorageConnection connection = (StorageConnection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("issueFullGarbageCollection")) {
                        this.checks.incrementAndGet();
                        this.entered.countDown();
                        try {
                            if (!this.release.await(30L, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("check was never released");
                            }
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("check interrupted", interrupted);
                        }
                        return null;
                    }
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return null;
                });
    }

    private static void await(final CountDownLatch latch, final String what) throws InterruptedException {
        assertTrue(latch.await(30L, TimeUnit.SECONDS), what);
    }

        /// A second request while a check runs is ignored instead of piling up.
    @Test
    void concurrentRunChecksRunsOnce() throws Exception {
        final GatedConnection gated = new GatedConnection();
        try (final StorageTaskExecutor executor = StorageTaskExecutor.New(gated.connection)) {
            executor.runChecks();
            await(gated.entered, "check task did not start");
            assertTrue(executor.isRunningChecks());
            executor.runChecks();
            gated.release.countDown();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
            while (executor.isRunningChecks() && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(gated.checks.get() == 1, "expected exactly one check task");
        }
    }

        /// Closing while a check runs cancels it and records the failure.
    @Test
    void closeDuringRunCancelsTheCheck() throws Exception {
        final GatedConnection gated = new GatedConnection();
        final StorageTaskExecutor executor = StorageTaskExecutor.New(gated.connection);
        executor.runChecks();
        await(gated.entered, "check task did not start");
        executor.close();
        executor.close();
        assertNotNull(executor.failure());
    }

        /// Checks after close fail with closed status, never a raw rejection.
    @Test
    void runChecksAfterCloseIsRejected() {
        final GatedConnection gated = new GatedConnection();
        final StorageTaskExecutor executor = StorageTaskExecutor.New(gated.connection);
        executor.close();
        assertThrows(IllegalStateException.class, executor::runChecks);
    }
}
