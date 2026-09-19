package peruncs.datagrid.cluster.storage.aeron.reader;

import org.agrona.concurrent.IdleStrategy;
import peruncs.datagrid.cluster.storage.aeron.config.AeronRetryPolicy;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/// Shared polling and shutdown rules for the Aeron readers.
///
/// Idle pacing always comes from the configured [AeronRetryPolicy#idleStrategy()];
/// there is no hard-coded default here, so tests and production readers share
/// the same pacing contract. Shutdown is bounded and close-once: the
/// subscription is closed only after the polling thread has exited, exactly
/// once per successful close, and ownership stays with the caller after any
/// timeout, interrupt, or failed close so it can retry.
final class AeronReaderLifecycle {
    private AeronReaderLifecycle() {
    }

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

        /// Stops polling and closes the subscription exactly once.
    ///
    /// [closed] is read first, so a repeated call after a successful close is a
    /// no-op. It is set only after [closeSubscription] returns normally; a
    /// bounded-wait timeout, an interrupted wait, or a failed close leaves it
    /// `false` and the subscription open, so the caller can retry without
    /// closing a subscription the polling thread may still touch.
    ///
    /// @param active            reader running flag
    /// @param thread            reader polling thread, or `null`
    /// @param stopped           latch released by the polling thread on exit
    /// @param closed            close-once guard, set after a successful close
    /// @param closeSubscription callback that closes the reader subscription
    /// @param timeoutNanos      bounded wait budget in nanoseconds; must be positive
    /// @throws IllegalStateException when the polling thread does not stop within
    ///                               the budget, the wait is interrupted, the
    ///                               caller is the polling thread itself, or the
    ///                               close callback fails
    static void stopAndClose(
            final AtomicBoolean active,
            final Thread thread,
            final CountDownLatch stopped,
            final AtomicBoolean closed,
            final Runnable closeSubscription,
            final long timeoutNanos) {
        if (timeoutNanos <= 0L) throw new IllegalArgumentException("timeoutNanos must be positive");
        Objects.requireNonNull(active, "active");
        Objects.requireNonNull(closed, "closed");
        Objects.requireNonNull(closeSubscription, "closeSubscription");
        if (thread != null) Objects.requireNonNull(stopped, "stopped latch is required for a polling thread");
        if (closed.get()) return;
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
                closed.set(true);
            } catch (final RuntimeException closeFailure) {
                failure = closeFailure;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
