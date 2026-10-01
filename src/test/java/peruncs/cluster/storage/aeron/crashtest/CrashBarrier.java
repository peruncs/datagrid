package peruncs.cluster.storage.aeron.crashtest;

import peruncs.cluster.storage.io.FaultInjection;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/// Deterministic test barrier. In throw mode it turns a named seam into a
/// controlled failure; in gate mode it blocks until the controller releases or
/// the test times out. Production fault points are inert without a scoped hook.
/// Gate mode is reserved for forked-child tests; an in-process test must use
/// throw mode so the releasing thread cannot deadlock against a held writer
/// monitor.
///
/// The seam name is the same literal the production code passes to
/// {@code FaultInjection.invoke}, so there is exactly one spelling of every
/// boundary across the crash harness.
public final class CrashBarrier implements AutoCloseable {
    private final CrashPoint point;
    private final boolean throwOnReach;
    private final CountDownLatch release = new CountDownLatch(1);
    private final long timeoutNanos;

    /// Arms a barrier at one crash seam, throwing or gating on arrival.
    ///
    /// @param point crash seam to arm
    /// @param throwOnReach whether arrival throws instead of blocking
    /// @param timeoutNanos maximum gate wait in nanoseconds, must be positive
    public CrashBarrier(final CrashPoint point, final boolean throwOnReach, final long timeoutNanos) {
        this.point = Objects.requireNonNull(point, "point");
        if (timeoutNanos <= 0) throw new IllegalArgumentException("timeoutNanos must be positive");
        this.throwOnReach = throwOnReach;
        this.timeoutNanos = timeoutNanos;
    }

    /// Callback adapter for writer fault seams.
    ///
    /// @param name reached seam, ignored unless it is the armed point
    /// @param sequence transaction sequence at the seam, or `-1` for file operations
    /// @param path filesystem path at the seam, or `null` for transport operations
    public void reached(final FaultInjection.Point name, final long sequence, final Path path) {
        if (!this.point.name().equals(name.name())) return;
        if (this.throwOnReach) throw new SimulatedCrash(this.point, sequence);
        try {
            if (!this.release.await(this.timeoutNanos, TimeUnit.NANOSECONDS)) {
                throw new AssertionError("timed out waiting to release %s".formatted(this.point));
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted at %s".formatted(this.point), interrupted);
        }
    }

    /// Releases a gated writer so the held seam proceeds. Closing does the same.
    public void release() {
        this.release.countDown();
    }

    @Override
    public void close() {
        this.release();
    }

    /// Exception used only by in-process crash tests. The crash site stays in
    /// the message; no accessor is needed because tests only assert the type.
    public static final class SimulatedCrash extends RuntimeException {
        /// Records a simulated crash site.
        ///
        /// @param point armed crash seam
        /// @param sequence transaction sequence at the seam
        public SimulatedCrash(final CrashPoint point, final long sequence) {
            super("simulated crash at %s sequence=%s".formatted(point, sequence));
        }
    }
}
