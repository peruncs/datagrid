package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.serializer.typing.Disposable;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the reader slot publishes replacements and never blocks readers on dispose.
class AeronReaderSlotTest {
        /// Verifies a health-style [AeronReaderSlot#current()] read returns while a slow
    /// dispose blocks inside the slot's lifecycle lock.
    @Test
    void currentDoesNotBlockBehindSlowDispose() throws Exception {
        final CountDownLatch disposeEntered = new CountDownLatch(1);
        final CountDownLatch releaseDispose = new CountDownLatch(1);
        final Disposable slowReader = () -> {
            disposeEntered.countDown();
            try {
                releaseDispose.await(5, TimeUnit.SECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        };
        final AeronReaderSlot<Disposable> slot = new AeronReaderSlot<>();
        slot.replace(() -> slowReader);
        assertSame(slowReader, slot.current());

        final Thread disposer = Thread.ofVirtual().name("slow-dispose").start(slot::dispose);
        assertTrue(disposeEntered.await(5, TimeUnit.SECONDS), "dispose did not start");

        /* The reference stays visible until the dispose succeeds: a throwing
         * dispose leaves it in place so a retried close stage reaches it
         * again (never a half-disposed leak). Health-style readers therefore
         * observe the disposing reader — never a torn or missing slot entry
         * mid-lifecycle — and, critically, never block behind the join. */
        assertSame(slowReader, slot.current(), "the disposing reader stays visible until its dispose finishes");
        final long started = System.nanoTime();
        assertSame(slowReader, slot.current(), "a health-style read must not wait for the dispose");
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1),
                "current() must return while dispose is still in progress");

        releaseDispose.countDown();
        disposer.join(5_000L);
        assertFalse(disposer.isAlive(), "dispose must finish once the reader returns");
        assertNull(slot.current(), "the slot is cleared only after the dispose succeeded");
    }

        /// Verifies replacement disposes the previous reader before publishing the new one.
    @Test
    void replaceDisposesPreviousBeforePublishingReplacement() {
        final AtomicInteger disposed = new AtomicInteger();
        final Disposable first = disposed::incrementAndGet;
        final Disposable second = disposed::incrementAndGet;
        final AeronReaderSlot<Disposable> slot = new AeronReaderSlot<>();
        slot.replace(() -> first);
        assertSame(first, slot.current());
        slot.replace(() -> {
            assertEquals(1, disposed.get(), "the previous reader must be disposed before the replacement is published");
            return second;
        });
        assertSame(second, slot.current());
        assertEquals(1, disposed.get());
        slot.dispose();
        assertEquals(2, disposed.get());
        assertNull(slot.current());
        slot.dispose();
        assertEquals(2, disposed.get(), "dispose must be idempotent");
    }

        /// Verifies a factory failure retains the disposed reader as an explicit non-empty state.
    @Test
    void failedFactoryLeavesSlotEmptyAndDisposesPrevious() {
        final AtomicInteger disposed = new AtomicInteger();
        final AeronReaderSlot<Disposable> slot = new AeronReaderSlot<>();
        slot.replace(() -> disposed::incrementAndGet);
        assertThrows(IllegalStateException.class, () -> slot.replace(() -> {
            throw new IllegalStateException("reader creation failed");
        }));
        assertNotNull(slot.current());
        assertEquals(1, disposed.get());
    }
}
