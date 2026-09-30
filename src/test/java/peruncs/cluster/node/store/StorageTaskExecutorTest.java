package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the single-flight check discipline and the close/check race.
class StorageTaskExecutorTest {
    private static final class GatedConnection {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
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
                        } finally {
                            this.finished.countDown();
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

        /// Concurrent requests share one check instead of queuing duplicates.
    @Test
    void concurrentRunChecksRunsOnce() throws Exception {
        final GatedConnection gated = new GatedConnection();
        final CountDownLatch submittersReady = new CountDownLatch(2);
        final CountDownLatch startSubmitters = new CountDownLatch(1);
        final AtomicReference<Throwable> submissionFailure = new AtomicReference<>();
        try (final StorageTaskExecutor executor = StorageTaskExecutor.create(gated.connection)) {
            final Runnable submit = () -> {
                submittersReady.countDown();
                try {
                    await(startSubmitters, "simultaneous submissions were not released");
                    executor.runChecks();
                } catch (final Throwable failure) {
                    submissionFailure.compareAndSet(null, failure);
                }
            };
            final Thread first = Thread.ofVirtual().start(submit);
            final Thread second = Thread.ofVirtual().start(submit);
            await(submittersReady, "submission threads did not start");
            startSubmitters.countDown();
            first.join(TimeUnit.SECONDS.toMillis(30L));
            second.join(TimeUnit.SECONDS.toMillis(30L));
            assertFalse(first.isAlive(), "first submission did not finish");
            assertFalse(second.isAlive(), "second submission did not finish");
            assertNull(submissionFailure.get(), "concurrent submission failed");
            await(gated.entered, "check task did not start");
            assertTrue(executor.isRunningChecks());
            gated.release.countDown();
            await(gated.finished, "check task did not finish");
            assertEquals(1, gated.checks.get(), "expected exactly one check task");
        }
    }

        /// Closing while a check runs cancels it and records the failure.
    @Test
    void closeDuringRunCancelsTheCheck() throws Exception {
        final GatedConnection gated = new GatedConnection();
        final StorageTaskExecutor executor = StorageTaskExecutor.create(gated.connection);
        executor.runChecks();
        await(gated.entered, "check task did not start");
        final CountDownLatch closersReady = new CountDownLatch(2);
        final CountDownLatch releaseClosers = new CountDownLatch(1);
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final Runnable close = () -> {
            closersReady.countDown();
            try {
                await(releaseClosers, "concurrent close was not released");
                executor.close();
            } catch (final Throwable failure) {
                closeFailure.compareAndSet(null, failure);
            }
        };
        final Thread first = Thread.ofVirtual().start(close);
        final Thread second = Thread.ofVirtual().start(close);
        await(closersReady, "close threads did not start");
        releaseClosers.countDown();
        first.join(TimeUnit.SECONDS.toMillis(30L));
        second.join(TimeUnit.SECONDS.toMillis(30L));
        assertFalse(first.isAlive(), "first close did not finish");
        assertFalse(second.isAlive(), "second close did not finish");
        assertNull(closeFailure.get(), "concurrent close failed");
        executor.close();
        assertNotNull(executor.failure());
    }

        /// Checks after close fail with closed status, never a raw rejection.
    @Test
    void runChecksAfterCloseIsRejected() {
        final GatedConnection gated = new GatedConnection();
        final StorageTaskExecutor executor = StorageTaskExecutor.create(gated.connection);
        executor.close();
        assertThrows(IllegalStateException.class, executor::runChecks);
    }
}
