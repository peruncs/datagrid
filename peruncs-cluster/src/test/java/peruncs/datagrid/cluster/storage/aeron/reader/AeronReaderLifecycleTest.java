package peruncs.datagrid.cluster.storage.aeron.reader;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that reader shutdown stops polling before closing its subscription.
class AeronReaderLifecycleTest {
        /// Verifies subscription cleanup when the polling thread is interrupted while waiting.
    @Test
    void closesSubscriptionWhenPollingThreadWaitIsInterrupted() {
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicBoolean closed = new AtomicBoolean();
        final CountDownLatch stopped = new CountDownLatch(1);
        final Thread pollingThread = Thread.ofVirtual().unstarted(() ->
        {
            try {
                Thread.sleep(30_000L);
            } catch (final InterruptedException ignored) {
                // Expected shutdown path.
            } finally {
                stopped.countDown();
            }
        });
        pollingThread.start();

        AeronReaderLifecycle.stopAndClose(active, pollingThread, stopped, () -> closed.set(true));

        assertFalse(active.get());
        assertFalse(pollingThread.isAlive());
        org.junit.jupiter.api.Assertions.assertTrue(closed.get());
    }

        /// Verifies subscription cleanup even when the close callback fails.
    @Test
    void closesSubscriptionEvenWhenTheCloseCallbackFails() {
        final AtomicBoolean active = new AtomicBoolean(true);
        final IllegalStateException expected = new IllegalStateException("close failed");

        final IllegalStateException actual = assertThrows(
                IllegalStateException.class,
                () -> AeronReaderLifecycle.stopAndClose(active, null, new CountDownLatch(0), () -> {
                    throw expected;
                })
        );

        assertSame(expected, actual);
        assertFalse(active.get());
    }

        /// A timeout retains ownership so a later disposal can finish cleanup safely.
    @Test
    void timeoutLeavesSubscriptionOpenForRetry() {
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicBoolean closed = new AtomicBoolean();
        final CountDownLatch stopped = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread pollingThread = Thread.ofVirtual().unstarted(() ->
        {
            try {
                while (!release.await(1L, TimeUnit.MILLISECONDS)) {
                    // Deliberately ignore interruption until the owner releases the poller.
                }
            } catch (final InterruptedException ignored) {
                try {
                    while (!release.await(1L, TimeUnit.MILLISECONDS)) {
                        // Keep the simulated callback blocked after interruption.
                    }
                } catch (final InterruptedException retryInterrupted) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                stopped.countDown();
            }
        });
        pollingThread.start();
        try {
            assertThrows(IllegalStateException.class, () -> AeronReaderLifecycle.stopAndClose(
                    active, pollingThread, stopped, () -> closed.set(true), TimeUnit.MILLISECONDS.toNanos(1L)));
            assertFalse(closed.get());
            release.countDown();
            AeronReaderLifecycle.stopAndClose(
                    active, pollingThread, stopped, () -> closed.set(true), TimeUnit.SECONDS.toNanos(1L));
            assertTrue(closed.get());
            assertFalse(pollingThread.isAlive());
        } finally {
            release.countDown();
        }
    }

        /// An interrupted disposer keeps subscription ownership for a later retry.
    @Test
    void interruptedDisposalDoesNotCloseSubscription()
            throws Exception {
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicBoolean closed = new AtomicBoolean();
        final CountDownLatch stopped = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread pollingThread = Thread.ofVirtual().unstarted(() ->
        {
            try {
                release.await();
            } catch (final InterruptedException ignored) {
                try {
                    release.await();
                } catch (final InterruptedException retry) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                stopped.countDown();
            }
        });
        pollingThread.start();
        final AtomicBoolean interrupted = new AtomicBoolean();
        final Thread disposer = Thread.ofVirtual().unstarted(() ->
        {
            try {
                AeronReaderLifecycle.stopAndClose(active, pollingThread, stopped, () -> closed.set(true));
            } catch (final IllegalStateException expected) {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        disposer.start();
        final long disposeStart = System.nanoTime();
        while (active.get() && System.nanoTime() - disposeStart < TimeUnit.SECONDS.toNanos(1L)) {
            Thread.yield();
        }
        disposer.interrupt();
        disposer.join(1_000L);
        assertTrue(interrupted.get());
        assertFalse(closed.get());
        release.countDown();
        pollingThread.join(1_000L);
        AeronReaderLifecycle.stopAndClose(active, pollingThread, stopped, () -> closed.set(true));
        assertTrue(closed.get());
    }

        /// A live polling thread must always provide the latch that owns its exit.
    @Test
    void rejectsMissingExitLatchForLivePollingThread() {
        final Thread pollingThread = Thread.ofVirtual().unstarted(() -> {
        });
        assertThrows(NullPointerException.class, () -> AeronReaderLifecycle.stopAndClose(
                new AtomicBoolean(true), pollingThread, null, () -> {
                }, 1L));
    }

        /// Verifies shared polling loop stops only after an idle poll.
    @Test
    void sharedPollingLoopStopsOnlyAfterAnIdlePoll() {
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicInteger polls = new AtomicInteger();

        AeronReaderLifecycle.runPollingLoop(
                active, () -> false, () -> polls.incrementAndGet() == 1 ? 1 : 0,
                () -> polls.get() >= 2, () -> false, () -> {
                }, AeronReaderLifecycle.defaultIdleStrategy());

        assertFalse(active.get());
        assertEquals(2, polls.get());
    }

        /// Verifies shared polling loop reports archive tail timeout.
    @Test
    void sharedPollingLoopReportsArchiveTailTimeout() {
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicBoolean timedOut = new AtomicBoolean();

        AeronReaderLifecycle.runPollingLoop(
                active, () -> false, () -> 0, () -> false, () -> true, () -> timedOut.set(true),
                AeronReaderLifecycle.defaultIdleStrategy());

        assertFalse(active.get());
        assertTrue(timedOut.get());
    }

        /// Verifies a timeout callback failure still publishes the stopped state.
    @Test
    void pollingLoopClearsActiveWhenTimeoutCallbackFails() {
        final AtomicBoolean active = new AtomicBoolean(true);
        final IllegalStateException expected = new IllegalStateException("timeout callback failed");

        final IllegalStateException actual = assertThrows(
                IllegalStateException.class,
                () -> AeronReaderLifecycle.runPollingLoop(
                        active, () -> false, () -> 0, () -> false, () -> true, () -> {
                            throw expected;
                        }, AeronReaderLifecycle.defaultIdleStrategy()
                )
        );

        assertSame(expected, actual);
        assertFalse(active.get());
    }

        /// Verifies an assembler failure stops polling before another fragment is consumed.
    @Test
    void pollingLoopStopsWhenAssemblerFails() {
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicInteger polls = new AtomicInteger();
        AeronReaderLifecycle.runPollingLoop(
                active, () -> polls.get() == 0, polls::incrementAndGet,
                () -> false, () -> false, () -> {
                }, AeronReaderLifecycle.defaultIdleStrategy());
        assertEquals(0, polls.get());
    }
}
