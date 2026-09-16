package peruncs.datagrid.cluster.storage.aeron.reader;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/// Shared polling and shutdown rules for the Aeron readers.
interface AeronReaderLifecycle {

        /// Runs the subscription duty cycle used by both readers. Keeping idle and
    /// stop-at-tail handling here prevents the test reader and the production
    /// reader from acquiring different lifecycle semantics.
    ///
    /// @param active        reader running flag
    /// @param stopPolling   terminal condition that ends the loop
    /// @param poller        returns the number of fragments read in one poll
    /// @param stopWhenIdle  whether an idle poll should stop at the live boundary
    /// @param timedOut      whether the stop budget has expired
    /// @param onTimeout     action run once on timeout
    /// @param idleStrategy  Agrona idle strategy built from the configured retry policy
    static void runPollingLoop(
            final AtomicBoolean active,
            final BooleanSupplier stopPolling,
            final IntSupplier poller,
            final BooleanSupplier stopWhenIdle,
            final BooleanSupplier timedOut,
            final Runnable onTimeout,
            final IdleStrategy idleStrategy
    ) {
        while (active.get() && !stopPolling.getAsBoolean()) {
            if (timedOut.getAsBoolean()) {
                runTimeout(onTimeout, active);
                return;
            }
            final int work = poller.getAsInt();
            /* A live image only means the subscription is attached; it does not mean
             * the current poll drained the replay.  Stop at the boundary only after an
             * actually idle poll, otherwise a prepared chunk can be left without its
             * terminal marker. */
            if (work == 0 && stopWhenIdle.getAsBoolean()) {
                active.set(false);
                return;
            }
            if (timedOut.getAsBoolean()) {
                runTimeout(onTimeout, active);
                return;
            }
            idleStrategy.idle(work);
        }
    }

    private static void runTimeout(final Runnable onTimeout, final AtomicBoolean active) {
        try {
            onTimeout.run();
        } finally {
            active.set(false);
        }
    }

        /// Stops polling with the default five-second budget.
    ///
    /// Convenience for tests and non-configurable callers; production code passes
    /// the reader's configured stop timeout to the explicit-budget overload.
    ///
    /// @param active            reader running flag
    /// @param thread            reader polling thread, or `null`
    /// @param stopped           latch released by the polling thread on exit
    /// @param closeSubscription callback that closes the reader subscription
    static void stopAndClose(
            final AtomicBoolean active,
            final Thread thread,
            final CountDownLatch stopped,
            final Runnable closeSubscription) {
        stopAndClose(active, thread, stopped, closeSubscription, TimeUnit.SECONDS.toNanos(5L));
    }

        /// Stops polling and waits up to the configured stop budget for a different
    /// polling thread, then closes the subscription only after that thread has
    /// exited. On timeout the subscription remains open so the caller can retry
    /// without a use-after-close.
    ///
    /// @param active            reader running flag
    /// @param thread            reader polling thread, or `null`
    /// @param stopped           latch released by the polling thread on exit
    /// @param closeSubscription callback that closes the reader subscription
    /// @param timeoutNanos      bounded wait budget in nanoseconds
    static void stopAndClose(
            final AtomicBoolean active,
            final Thread thread,
            final CountDownLatch stopped,
            final Runnable closeSubscription,
            final long timeoutNanos) {
        if (timeoutNanos <= 0L) throw new IllegalArgumentException("timeoutNanos must be positive");
        Objects.requireNonNull(active, "active");
        Objects.requireNonNull(closeSubscription, "closeSubscription");
        if (thread != null) Objects.requireNonNull(stopped, "stopped latch is required for a polling thread");
        active.set(false);
        RuntimeException failure = null;
        if (thread != null) {
            if (thread == Thread.currentThread()) {
                /* A delivery callback may request shutdown, but the polling thread must
                 * leave its duty cycle before its subscription is closed. Let the caller
                 * retry after this thread reaches its finally block. */
                throw new IllegalStateException("cannot dispose Aeron reader from its polling thread");
            }
            thread.interrupt();
            final long deadline = ReplicationRetry.deadlineNanos(timeoutNanos);
            try {
                final long remaining = ReplicationRetry.remainingNanos(deadline);
                if (remaining <= 0L || !stopped.await(remaining, TimeUnit.NANOSECONDS)) {
                    failure = new IllegalStateException("Aeron reader polling thread did not stop");
                } else {
                    /* The latch is released from the polling thread's finally block. Use
                     * the same deadline for the tiny interval between countDown() and
                     * Thread termination; an unbounded join defeats the shutdown budget. */
                    final long joinNanos = ReplicationRetry.remainingNanos(deadline);
                    if (joinNanos <= 0L) {
                        failure = new IllegalStateException("Aeron reader polling thread did not stop");
                    } else {
                        final long joinMillis = TimeUnit.NANOSECONDS.toMillis(joinNanos);
                        thread.join(Math.max(1L, joinMillis));
                        if (thread.isAlive()) {
                            failure = new IllegalStateException("Aeron reader polling thread did not stop");
                        }
                    }
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failure = new IllegalStateException("interrupted while stopping Aeron reader", interrupted);
            }
        }
        /* Do not close a subscription while a polling thread is still able to
         * access it. The caller retains ownership and may retry after it stops. */
        if (failure == null) {
            try {
                closeSubscription.run();
            } catch (final RuntimeException closeFailure) {
                failure = closeFailure;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /// Builds the default reader idle strategy.
    static IdleStrategy defaultIdleStrategy() {
        return new BackoffIdleStrategy();
    }
}
