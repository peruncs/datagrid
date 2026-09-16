package peruncs.datagrid.cluster.storage.types;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/// Monotonic-clock helpers shared by bounded transport retry loops.
///
/// Replication code must use a monotonic deadline. A wall-clock deadline
/// can move backwards during clock correction and can turn a bounded retry into
/// an unbounded wait. The helpers also saturate addition so a very large,
/// explicitly configured timeout cannot wrap into an already-expired deadline.
///
/// The clock-taking overloads exist for deterministic tests: production loops
/// pass [System#nanoTime] (directly or through the single-clock methods).
public final class ReplicationRetry {
    private ReplicationRetry() {
    }

    /// Returns a deadline measured by [System#nanoTime()].
    ///
    /// @param timeoutNanos positive retry budget
    /// @return saturated monotonic deadline
    /// @throws IllegalArgumentException when the budget is not positive
    public static long deadlineNanos(final long timeoutNanos) {
        return deadlineNanos(timeoutNanos, System::nanoTime);
    }

    /// Returns a deadline measured by the supplied monotonic clock.
    ///
    /// @param timeoutNanos positive retry budget
    /// @param clock        monotonic nanosecond source
    /// @return saturated monotonic deadline
    /// @throws IllegalArgumentException when the budget is not positive
    public static long deadlineNanos(final long timeoutNanos, final LongSupplier clock) {
        if (timeoutNanos <= 0L) throw new IllegalArgumentException("timeoutNanos must be positive");
        if (timeoutNanos == Long.MAX_VALUE) return Long.MAX_VALUE;
        try {
            return Math.addExact(clock.getAsLong(), timeoutNanos);
        } catch (final ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /// Returns the non-negative time left before a deadline.
    ///
    /// @param deadlineNanos saturated deadline returned by this class
    /// @return remaining nanoseconds, or zero after expiry
    public static long remainingNanos(final long deadlineNanos) {
        return remainingNanos(deadlineNanos, System::nanoTime);
    }

    /// Returns the non-negative time left before a deadline.
    ///
    /// @param deadlineNanos saturated deadline returned by this class
    /// @param clock         monotonic nanosecond source
    /// @return remaining nanoseconds, or zero after expiry
    public static long remainingNanos(final long deadlineNanos, final LongSupplier clock) {
        if (deadlineNanos == Long.MAX_VALUE) return Long.MAX_VALUE;
        final long now = clock.getAsLong();
        try {
            final long remaining = Math.subtractExact(deadlineNanos, now);
            return Math.max(0L, remaining);
        } catch (final ArithmeticException overflow) {
            /* nanoTime has an arbitrary origin and may be negative. When subtraction
             * overflows, ordering the operands still tells us whether the deadline is
             * in the future; never expose a wrapped positive wait for an expired one. */
            return deadlineNanos > now ? Long.MAX_VALUE : 0L;
        }
    }

    /// Returns whether the supplied monotonic deadline has expired.
    ///
    /// @param deadlineNanos saturated deadline returned by this class
    /// @return `true` when no retry time remains
    public static boolean expired(final long deadlineNanos) {
        return expired(deadlineNanos, System::nanoTime);
    }

    /// Returns whether the supplied monotonic deadline has expired.
    ///
    /// @param deadlineNanos saturated deadline returned by this class
    /// @param clock         monotonic nanosecond source
    /// @return `true` when no retry time remains
    public static boolean expired(final long deadlineNanos, final LongSupplier clock) {
        return remainingNanos(deadlineNanos, clock) == 0L;
    }

    /// Returns a full-jitter exponential backoff delay for one retry attempt.
    ///
    /// The delay is uniform in `[0, min(capNanos, baseNanos * 2^(attempt-1))]`
    /// so concurrent writers do not retry in lockstep after a shared outage.
    /// The per-operation deadline from [#deadlineNanos] still bounds the total
    /// wait; this delay only spaces individual attempts.
    ///
    /// @param attempt   1-based retry attempt number
    /// @param baseNanos delay for the first attempt, must be positive
    /// @param capNanos  maximum delay, must be positive
    /// @return backoff delay in nanoseconds
    public static long fullJitterDelayNanos(final long attempt, final long baseNanos, final long capNanos) {
        if (attempt < 1L) throw new IllegalArgumentException("attempt must be positive");
        if (baseNanos <= 0L) throw new IllegalArgumentException("baseNanos must be positive");
        if (capNanos <= 0L) throw new IllegalArgumentException("capNanos must be positive");
        long exponential;
        try {
            exponential = Math.multiplyExact(baseNanos, 1L << Math.min(attempt - 1L, 62L));
        } catch (final ArithmeticException overflow) {
            exponential = Long.MAX_VALUE;
        }
        final long capped = Math.min(exponential, capNanos);
        final long bound = capped == Long.MAX_VALUE ? Long.MAX_VALUE : capped + 1L;
        return ThreadLocalRandom.current().nextLong(bound);
    }
}
