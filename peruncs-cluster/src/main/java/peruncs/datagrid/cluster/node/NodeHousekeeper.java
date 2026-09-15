package peruncs.datagrid.cluster.node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.eclipse.serializer.util.X.notNull;

/// Runs periodic node maintenance on shared daemon threads.
///
/// Each node owns one housekeeper. Callers schedule every task first and
/// start the housekeeper last; close stops future runs and releases the
/// threads. Tasks run with a fixed delay, so a slow run postpones its own
/// next run instead of overlapping it. A failing task is logged and the
/// remaining tasks keep running.
public final class NodeHousekeeper implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(NodeHousekeeper.class.getName());
    private static final int THREADS = 2;
    private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;

    private final ScheduledThreadPoolExecutor scheduler;
    private final List<ScheduledTask> pending = new ArrayList<>();
    private boolean started;
    private boolean closing;
    private boolean closed;

    private NodeHousekeeper(final int threads) {
        final AtomicInteger threadCount = new AtomicInteger();
        this.scheduler = new ScheduledThreadPoolExecutor(threads, task ->
                Thread.ofVirtual()
                        .name("datagrid-housekeeper-%s".formatted(threadCount.incrementAndGet()))
                        .unstarted(task));
        this.scheduler.setRemoveOnCancelPolicy(true);
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

        /// Creates a housekeeper with two daemon threads.
    ///
    /// @return a housekeeper ready for scheduling
    public static NodeHousekeeper New() {
        return new NodeHousekeeper(THREADS);
    }

    private static void runGuarded(final ScheduledTask scheduled) {
        LOGGER.log(System.Logger.Level.INFO, "Running housekeeper task '%s'".formatted(scheduled.name()));
        try {
            scheduled.task().run();
            LOGGER.log(System.Logger.Level.DEBUG, "Finished housekeeper task '%s'".formatted(scheduled.name()));
        } catch (final RuntimeException failure) {
            LOGGER.log(System.Logger.Level.ERROR, "Housekeeper task '%s' failed".formatted(scheduled.name()), failure);
        } catch (final Error failure) {
            LOGGER.log(System.Logger.Level.ERROR, "Fatal housekeeper task '%s' failure".formatted(scheduled.name()), failure);
            throw failure;
        }
    }

        /// Registers one periodic task. Tasks must be scheduled before start.
    ///
    /// @param name     human-readable task name used in log messages
    /// @param task     the work to run
    /// @param interval delay between the end of one run and the start of the next
    public synchronized void schedule(final String name, final Runnable task, final Duration interval) {
        if (this.closed || this.closing) {
            throw new IllegalStateException("Node housekeeper is closed");
        }
        if (this.started) {
            throw new IllegalStateException("Node housekeeper is already started");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Housekeeper task name must be configured");
        }
        notNull(task);
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("Housekeeper task '%s' interval must be positive".formatted(name));
        }
        final long intervalMillis;
        try {
            intervalMillis = interval.toMillis();
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException("Housekeeper task '%s' interval is too large".formatted(name), overflow);
        }
        if (intervalMillis <= 0L) {
            throw new IllegalArgumentException(
                    "Housekeeper task '%s' interval must be at least one millisecond".formatted(name));
        }
        LOGGER.log(System.Logger.Level.INFO, "Scheduling housekeeper task '%s' every %s".formatted(name, interval));
        this.pending.add(new ScheduledTask(name, task, intervalMillis));
    }

        /// Starts firing the scheduled tasks. The first run of each task waits one interval.
    public synchronized void start() {
        if (this.closed || this.closing) {
            throw new IllegalStateException("Node housekeeper is closed");
        }
        if (this.started) {
            throw new IllegalStateException("Node housekeeper is already started");
        }
        this.started = true;
        for (final ScheduledTask scheduled : this.pending) {
            this.scheduler.scheduleWithFixedDelay(
                    () -> runGuarded(scheduled),
                    scheduled.intervalMillis(),
                    scheduled.intervalMillis(),
                    TimeUnit.MILLISECONDS
            );
        }
        LOGGER.log(System.Logger.Level.INFO, "Started node housekeeper with %s task(s)".formatted(this.pending.size()));
    }

        /// Stops future runs and releases the threads. A running task is interrupted.
    @Override
    public synchronized void close() {
        if (this.closed) {
            return;
        }
        this.closing = true;
        LOGGER.log(System.Logger.Level.INFO, "Shutting down node housekeeper");
        this.scheduler.shutdownNow();
        try {
            if (!this.scheduler.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                LOGGER.log(System.Logger.Level.WARNING, "Node housekeeper did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
                throw new IllegalStateException(
                        "Node housekeeper did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping node housekeeper", interrupted);
        }
        synchronized (this) {
            this.closed = true;
            this.closing = false;
        }
    }

    private record ScheduledTask(String name, Runnable task, long intervalMillis) {
    }
}
