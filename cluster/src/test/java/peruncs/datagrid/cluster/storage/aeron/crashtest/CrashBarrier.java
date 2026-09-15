package peruncs.datagrid.cluster.storage.aeron.crashtest;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/// Deterministic test barrier. In throw mode it turns a named seam into a
/// controlled failure; in gate mode it blocks until the controller releases or
/// the test times out. Production code only sees a package-private callback.
/// Gate mode is reserved for forked-child tests; an in-process test must use
/// throw mode so the releasing thread cannot deadlock against a held writer
/// monitor.
public final class CrashBarrier implements AutoCloseable {
    private final CrashPoint point;
    private final boolean throwOnReach;
    private final CountDownLatch release = new CountDownLatch(1);
    private final long timeoutNanos;

    public CrashBarrier(final CrashPoint point, final boolean throwOnReach, final long timeoutNanos) {
        this.point = Objects.requireNonNull(point, "point");
        if (timeoutNanos <= 0) throw new IllegalArgumentException("timeoutNanos must be positive");
        this.throwOnReach = throwOnReach;
        this.timeoutNanos = timeoutNanos;
    }

        /// Callback adapter for writer fault seams.
    public void reached(final String name, final long sequence) {
        if (!this.point.name().equals(name)) return;
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

    public void release() {
        this.release.countDown();
    }

    @Override
    public void close() {
        this.release();
    }

        /// Exception used only by in-process crash tests.
    public static final class SimulatedCrash extends RuntimeException {
        private final CrashPoint point;
        private final long sequence;

        public SimulatedCrash(final CrashPoint point, final long sequence) {
            super("simulated crash at %s sequence=%s".formatted(point, sequence));
            this.point = point;
            this.sequence = sequence;
        }

        public CrashPoint point() {
            return this.point;
        }

        public long sequence() {
            return this.sequence;
        }
    }
}
