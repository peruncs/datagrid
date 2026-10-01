package peruncs.cluster.node.store;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.errors.GraphDrainTimeoutException;
import peruncs.cluster.storage.StorageGraphCoordinator;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Checks that close waits for facade calls and can retry a bounded drain.
class ApplicationSectionDrainTest {
    @Test
    void drainWaitsForOuterApplicationSectionsAndCanRetry(@TempDir final Path directory) throws Exception {
        try (EmbeddedStorageManager delegate = StorageWriteGatingTest.start(directory)) {
            final ClusterStorageManager<Object> manager = TestManagers.guarding(
                    delegate, () -> false, newNodeClose(), new StorageGraphCoordinator(100L));
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            final Thread application = Thread.ofVirtual().start(() -> {
                try {
                    manager.graphBoundary().read(() -> await(release, entered));
                } catch (final Throwable failure) {
                    workerFailure.set(failure);
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertThrows(GraphDrainTimeoutException.class, () ->
                    ClusterStorageManagers.awaitApplicationSections(manager, Duration.ofMillis(100)));
            release.countDown();
            application.join(5_000L);
            assertFalse(application.isAlive());
            assertNull(workerFailure.get());
            assertDoesNotThrow(() -> ClusterStorageManagers.awaitApplicationSections(manager, Duration.ofSeconds(1)));
        }
    }

    @Test
    void sectionCountIsReleasedWhenTheCallbackThrows(@TempDir final Path directory) {
        try (EmbeddedStorageManager delegate = StorageWriteGatingTest.start(directory)) {
            final ClusterStorageManager<Object> manager = TestManagers.guarding(
                    delegate, () -> false, newNodeClose(), new StorageGraphCoordinator());
            assertThrows(IllegalArgumentException.class, () -> manager.graphBoundary().read(() -> {
                throw new IllegalArgumentException("expected");
            }));
            assertDoesNotThrow(() -> ClusterStorageManagers.awaitApplicationSections(manager, Duration.ofSeconds(1)));
        }
    }

    @Test
    void writeSectionKeepsStoreOpenAfterDrainTimeout(@TempDir final Path directory) throws Exception {
        try (EmbeddedStorageManager delegate = StorageWriteGatingTest.start(directory)) {
            final AtomicReference<ClusterStorageManager<Object>> reference = new AtomicReference<>();
            final NodeClose close = drainingNodeClose(delegate, reference, Duration.ofMillis(100), new CountDownLatch(1));
            final ClusterStorageManager<Object> manager = TestManagers.guarding(
                    delegate, () -> false, close, new StorageGraphCoordinator(100L));
            reference.set(manager);
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            final Thread application = Thread.ofVirtual().start(() -> {
                try {
                    manager.graphBoundary().write(() -> {
                        await(release, entered);
                        manager.store(new StorageWriteGatingTest.Payload("committed"));
                    });
                } catch (final Throwable failure) {
                    workerFailure.set(failure);
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertThrows(GraphDrainTimeoutException.class, manager::shutdown);
            assertTrue(delegate.isRunning(), "Store must remain open when application drain times out");
            release.countDown();
            application.join(5_000L);
            assertFalse(application.isAlive());
            assertNull(workerFailure.get());
            assertTrue(manager.shutdown());
            assertFalse(delegate.isRunning(), "a close retry shuts down the Store after the section exits");
        }
    }

    @Test
    void closeWaitsForWriterSectionBeforeStoppingStore(@TempDir final Path directory) throws Exception {
        try (EmbeddedStorageManager delegate = StorageWriteGatingTest.start(directory)) {
            final AtomicReference<ClusterStorageManager<Object>> reference = new AtomicReference<>();
            final CountDownLatch closeEntered = new CountDownLatch(1);
            final NodeClose close = drainingNodeClose(delegate, reference, Duration.ofSeconds(5), closeEntered);
            final ClusterStorageManager<Object> manager = TestManagers.guarding(
                    delegate, () -> false, close, new StorageGraphCoordinator(5_000L));
            reference.set(manager);
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            final Thread application = Thread.ofVirtual().start(() -> {
                try {
                    manager.graphBoundary().write(() -> {
                        await(release, entered);
                        manager.store(new StorageWriteGatingTest.Payload("committed while close waits"));
                    });
                } catch (final Throwable failure) {
                    workerFailure.set(failure);
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            final Thread closer = Thread.ofVirtual().start(() -> {
                try {
                    manager.shutdown();
                } catch (final Throwable failure) {
                    closeFailure.set(failure);
                }
            });
            assertTrue(closeEntered.await(5, TimeUnit.SECONDS));
            release.countDown();
            application.join(5_000L);
            closer.join(5_000L);
            assertFalse(application.isAlive());
            assertFalse(closer.isAlive());
            assertNull(workerFailure.get());
            assertNull(closeFailure.get());
            assertFalse(delegate.isRunning());
        }
    }

    @Test
    void lifecycleAdmissionRejectsNewSections(@TempDir final Path directory) {
        try (EmbeddedStorageManager delegate = StorageWriteGatingTest.start(directory)) {
            final AtomicBoolean closing = new AtomicBoolean();
            final NodeClose nodeClose = new NodeClose() {
                @Override public boolean close() { return false; }
                @Override public void awaitAppIdle(final Duration timeout) { }
                @Override
                public void checkOpen() {
                    if (closing.get()) throw new IllegalStateException("cluster node is closed");
                }
            };
            final ClusterStorageManager<Object> manager = TestManagers.guarding(
                    delegate, () -> false, nodeClose, new StorageGraphCoordinator());
            closing.set(true);
            assertThrows(IllegalStateException.class, () -> manager.graphBoundary().read(() -> {}));
        }
    }

    private static void await(final CountDownLatch release, final CountDownLatch entered) {
        entered.countDown();
        try {
            if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("application section was not released");
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    private static NodeClose newNodeClose() {
        return new NodeClose() {
            @Override public boolean close() { return false; }
            @Override public void awaitAppIdle(final Duration timeout) { }
            @Override public void checkOpen() { }
        };
    }

    private static NodeClose drainingNodeClose(
            final EmbeddedStorageManager delegate,
            final AtomicReference<ClusterStorageManager<Object>> manager,
            final Duration timeout,
            final CountDownLatch closeEntered
    ) {
        return new NodeClose() {
            private final AtomicBoolean closing = new AtomicBoolean();

            @Override
            public boolean close() {
                this.closing.set(true);
                closeEntered.countDown();
                try {
                    this.awaitAppIdle(timeout);
                    return delegate.shutdown();
                } catch (final RuntimeException | Error failure) {
                    this.closing.set(false);
                    throw failure;
                }
            }

            @Override
            public void awaitAppIdle(final Duration wait) {
                ClusterStorageManagers.awaitApplicationSections(manager.get(), wait);
            }

            @Override
            public void checkOpen() {
                if (this.closing.get()) throw new IllegalStateException("cluster node is closing");
            }

        };
    }
}
