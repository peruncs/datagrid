package peruncs.cluster.storage;

import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.binary.ObjectGraphUpdateHandler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


/// Verifies the per-Store graph coordinator: concurrent reads overlap, a
/// materialization excludes reads, and coordinators are independent.
class StorageGraphCoordinatorTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10L);

    private static void await(final CountDownLatch latch, final String what) throws InterruptedException {
        assertTrue(latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "timed out waiting for " + what);
    }

    /// Verifies concurrent reads overlap instead of serializing, reaching a peak concurrency of two.
    @Test
    void readsProceedConcurrentlyWhileAWriteExcludesThem() throws Exception {
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final CountDownLatch bothInside = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);

        final Runnable reader = () -> coordinator.read(() ->
        {
            final int now = concurrent.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            bothInside.countDown();
            try {
                assertTrue(release.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "readers were not released");
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            } finally {
                concurrent.decrementAndGet();
            }
        });
        final Thread first = Thread.ofVirtual().start(reader);
        final Thread second = Thread.ofVirtual().start(reader);

        await(bothInside, "concurrent reads to overlap");
        release.countDown();
        first.join(TIMEOUT.toMillis());
        second.join(TIMEOUT.toMillis());
        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        assertEquals(2, peak.get(), "read-side access must overlap instead of serializing");
    }

    /// Verifies a write waits while a read holds the coordinator and runs once the read releases.
    @Test
    void writeWaitsForReadsAndReadsWaitForWrite() throws Exception {
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final CountDownLatch readHeld = new CountDownLatch(1);
        final CountDownLatch readRelease = new CountDownLatch(1);
        final AtomicBoolean writeRan = new AtomicBoolean();

        final Thread reader = Thread.ofVirtual().start(() -> coordinator.read(() ->
        {
            readHeld.countDown();
            try {
                assertTrue(readRelease.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "read was not released");
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }));
        await(readHeld, "read to hold the coordinator");

        final Thread writer = Thread.ofVirtual().start(() ->
                coordinator.write(() -> writeRan.set(true)));
        /* The writer must stay out while the read side is held. */
        writer.join(500L);
        assertTrue(writer.isAlive(), "write side entered while a read was held");
        assertFalse(writeRan.get());

        readRelease.countDown();
        writer.join(TIMEOUT.toMillis());
        reader.join(TIMEOUT.toMillis());
        assertTrue(writeRan.get(), "write side never ran after the read released");
    }

    /// Verifies a write held on one coordinator does not block reads on another coordinator.
    @Test
    void coordinatorsAreIndependent() throws Exception {
        final StorageGraphCoordinator first = new StorageGraphCoordinator();
        final StorageGraphCoordinator second = new StorageGraphCoordinator();
        final CountDownLatch writeHeld = new CountDownLatch(1);
        final CountDownLatch writeRelease = new CountDownLatch(1);
        final CountDownLatch secondReadRan = new CountDownLatch(1);

        final Thread writer = Thread.ofVirtual().start(() -> first.write(() ->
        {
            writeHeld.countDown();
            try {
                assertTrue(writeRelease.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "write was not released");
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }));
        await(writeHeld, "write to hold the first coordinator");

        /* A write on one Store must not serialize reads on another Store. */
        second.read(secondReadRan::countDown);
        assertTrue(secondReadRan.await(100L, TimeUnit.MILLISECONDS),
                "a coordinator blocked on an unrelated Store");

        writeRelease.countDown();
        writer.join(TIMEOUT.toMillis());
        assertFalse(writer.isAlive());
    }

    /// A failed write section invalidates the graph before the write lock
    /// releases: later coordinated reads and writes fail closed with the
    /// latched cause, because the graph may be partially updated.
    @Test
    void failedWriteInvalidatesGraphForReadsAndWrites() {
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final IllegalStateException boom = new IllegalStateException("half-applied");
        final IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
                coordinator.write(() -> {
                    throw boom;
                }));
        assertSame(boom, thrown, "the original failure must propagate to the writer");

        assertInstanceOf(peruncs.cluster.errors.GraphInvalidatedException.class, coordinator.graphFailure());
        assertSame(boom, coordinator.graphFailure().getCause(), "the latched cause must name the failed update");
        final var readFailure = assertThrows(peruncs.cluster.errors.GraphInvalidatedException.class,
                () -> coordinator.read(() -> {
                }), "a coordinated read must fail closed on a torn graph");
        assertSame(boom, readFailure.getCause());
        assertThrows(peruncs.cluster.errors.GraphInvalidatedException.class,
                () -> coordinator.read(() -> 1), "supplier reads fail closed too");
        final var writeFailure = assertThrows(peruncs.cluster.errors.GraphInvalidatedException.class,
                () -> coordinator.write(() -> {
                }), "further writes must not build on a torn graph");
        assertSame(boom, writeFailure.getCause());
    }

    /// A supplier-form write also invalidates the graph on failure and
    /// returns results on success.
    @Test
    void supplierWriteReturnsAndInvalidatesOnFailure() {
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        assertEquals(42, coordinator.write(() -> 42));
        assertNull(coordinator.graphFailure(), "a successful write keeps the graph valid");
        assertThrows(IllegalStateException.class, () -> coordinator.write(() -> {
            throw new IllegalStateException("mid-mutation");
        }));
        assertNotNull(coordinator.graphFailure(), "a failing supplier write must invalidate the graph");
    }

    /// Reads racing a failing write must either complete before the write or
    /// fail closed after it — never observe the half-applied state.
    @Test
    void readRacingAFailedWriteNeverObservesIt() throws Exception {
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final CountDownLatch writeInside = new CountDownLatch(1);
        final CountDownLatch failNow = new CountDownLatch(1);
        final Thread writer = Thread.ofVirtual().start(() ->
        {
            try {
                coordinator.write(() ->
                {
                    writeInside.countDown();
                    try {
                        assertTrue(failNow.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    throw new IllegalStateException("torn batch");
                });
            } catch (final IllegalStateException expected) {
                /* The write failure itself is the precondition being set up. */
            }
        });
        await(writeInside, "write to hold the coordinator");
        final java.util.concurrent.atomic.AtomicReference<Throwable> readOutcome =
                new java.util.concurrent.atomic.AtomicReference<>();
        final Thread reader = Thread.ofVirtual().start(() ->
        {
            try {
                coordinator.read(() -> {
                });
            } catch (final Throwable failure) {
                readOutcome.set(failure);
            }
        });
        /* The queued read cannot enter while the write section holds the lock. */
        reader.join(200L);
        assertTrue(reader.isAlive(), "coordinated read entered during a write section");
        failNow.countDown();
        writer.join(TIMEOUT.toMillis());
        reader.join(TIMEOUT.toMillis());
        assertInstanceOf(peruncs.cluster.errors.GraphInvalidatedException.class, readOutcome.get(),
                "the read queued behind the failed write must fail closed, outcome: " + readOutcome.get());
    }

    /// Verifies per-store graph updates run on the write side and wait while an application read is held.
    @Test
    void perStoreHandlerRunsUpdatesOnTheWriteSide() throws Exception {
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final ObjectGraphUpdateHandler handler = ObjectGraphUpdateHandler.PerStore(coordinator);
        final CountDownLatch readHeld = new CountDownLatch(1);
        final CountDownLatch readRelease = new CountDownLatch(1);
        final AtomicBoolean updateRan = new AtomicBoolean();

        final Thread reader = Thread.ofVirtual().start(() -> coordinator.read(() ->
        {
            readHeld.countDown();
            try {
                assertTrue(readRelease.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "read was not released");
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }));
        await(readHeld, "read to hold the coordinator");

        final Thread materialization = Thread.ofVirtual().start(() ->
                handler.objectGraphUpdateAvailable(() -> updateRan.set(true)));
        materialization.join(500L);
        assertTrue(materialization.isAlive(), "materialization entered while an application read was held");
        assertFalse(updateRan.get());

        readRelease.countDown();
        materialization.join(TIMEOUT.toMillis());
        reader.join(TIMEOUT.toMillis());
        assertTrue(updateRan.get(), "materialization never ran after the read released");
    }

    /// Verifies creating a per-store handler with a null coordinator fails fast.
    @Test
    void perStoreHandlerRejectsANullCoordinator() {
        assertThrows(NullPointerException.class, () -> ObjectGraphUpdateHandler.PerStore(null));
    }

        /// A guarded read never observes a half-applied multi-field update.
    @Test
    void guardedReadSeesOnlyCompleteMultiFieldUpdates() throws Exception {
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final int[] pair = new int[2];
        final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicReference<Throwable> torn =
                new java.util.concurrent.atomic.AtomicReference<>();
        final Thread writer = Thread.ofVirtual().start(() -> {
            int value = 0;
            while (!stop.get()) {
                final int next = ++value;
                coordinator.write(() -> {
                    pair[0] = next;
                    pair[1] = -next;
                });
            }
        });
        final Thread reader = Thread.ofVirtual().start(() -> {
            while (!stop.get()) {
                coordinator.read(() -> {
                    if (pair[0] + pair[1] != 0) {
                        torn.compareAndSet(null, new IllegalStateException(
                                "torn read: %s + %s".formatted(pair[0], pair[1])));
                    }
                });
            }
        });
        try {
            Thread.sleep(300L);
            assertNull(torn.get(), "guarded read observed a torn invariant: " + torn.get());
        } finally {
            stop.set(true);
            writer.join(TIMEOUT.toMillis());
            reader.join(TIMEOUT.toMillis());
        }
        assertNull(torn.get());
    }
}
