package peruncs.cluster.node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.Logger.Level.*;
import static java.lang.System.Logger.Level.WARNING;
import static org.eclipse.serializer.util.X.notNull;

/// Triggers periodic maintenance on one platform thread and runs the work on virtual threads.
///
/// Each node owns one maintenance scheduler. Callers schedule every task first and
/// start the scheduler last; close stops future runs and releases the
/// threads. A slow run skips later ticks instead of overlapping itself.
/// A task that throws is logged and the
/// remaining tasks keep running; a task that fails [#FAILURE_THRESHOLD]
/// consecutive runs degrades [#failure()] so readiness reports the node
/// instead of hiding the repeated failure. Each task clears only its own
/// failure after it recovers. A fatal [Error] never clears.
final class NodeMaintenanceScheduler implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(NodeMaintenanceScheduler.class.getName());
    /// Runs a task must fail consecutively before health degrades.
    static final int FAILURE_THRESHOLD = 3;
    private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;

    private final ScheduledThreadPoolExecutor scheduler;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<Error> fatalFailure = new AtomicReference<>();
    private final ConcurrentHashMap<String, RuntimeException> degradedFailures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> consecutiveFailures = new ConcurrentHashMap<>();
    private final List<ScheduledTask> pending = new ArrayList<>();
    private boolean started;
    private volatile boolean closing;
    private volatile boolean closed;

    private NodeMaintenanceScheduler() {
        final AtomicInteger threadCount = new AtomicInteger();
        this.scheduler = new ScheduledThreadPoolExecutor(1, task ->
                Thread.ofPlatform()
                        .daemon()
                        .name("datagrid-housekeeper-%s".formatted(threadCount.incrementAndGet()))
                        .unstarted(task));
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

        /// Creates a scheduler with one daemon thread and virtual task threads.
    ///
    /// @return a scheduler ready for task submission
    static NodeMaintenanceScheduler create() {
        return new NodeMaintenanceScheduler();
    }

    private void runGuarded(final ScheduledTask scheduled) {
        LOGGER.log(DEBUG, "Running housekeeper task '%s'".formatted(scheduled.name()));
        try {
            scheduled.task().run();
            this.consecutiveFailures.remove(scheduled.name());
            this.degradedFailures.remove(scheduled.name());
            LOGGER.log(DEBUG, "Finished housekeeper task '%s'".formatted(scheduled.name()));
        } catch (final RuntimeException failure) {
            LOGGER.log(ERROR, "Housekeeper task '%s' failed".formatted(scheduled.name()), failure);
            final int failures = this.consecutiveFailures
                    .computeIfAbsent(scheduled.name(), ignored -> new AtomicInteger())
                    .incrementAndGet();
            if (failures >= FAILURE_THRESHOLD) {
                this.degradedFailures.put(scheduled.name(), failure);
            }
        } catch (final Error failure) {
            /* Never let an Error escape scheduleWithFixedDelay: ScheduledExecutorService
             * cancels that task permanently when its runnable throws. Record the fatal
             * condition so the node health boundary fails closed while later tasks and
             * diagnostics remain schedulable. */
            this.fatalFailure.compareAndSet(null, failure);
            LOGGER.log(ERROR, "Fatal housekeeper task '%s' failure".formatted(scheduled.name()), failure);
        }
    }

        /// Returns the fatal maintenance failure, or the first task failure that
        /// repeated past the degradation threshold, or `null` while healthy.
    ///
    /// @return failure requiring health degradation, or `null`
    Throwable failure() {
        final Error fatal = this.fatalFailure.get();
        return fatal != null ? fatal : this.degradedFailures.values().stream().findFirst().orElse(null);
    }

        /// Registers one periodic task. Tasks must be scheduled before start.
    ///
    /// @param name     human-readable task name used in log messages
    /// @param task     the work to run
    /// @param interval delay between the end of one run and the start of the next
    synchronized void schedule(final String name, final Runnable task, final Duration interval) {
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
        LOGGER.log(INFO, "Scheduling housekeeper task '%s' every %s".formatted(name, interval));
        this.pending.add(new ScheduledTask(name, task, intervalMillis, new AtomicBoolean()));
    }

        /// Starts firing the scheduled tasks. The first run of each task waits one interval.
    synchronized void start() {
        if (this.closed || this.closing) {
            throw new IllegalStateException("Node housekeeper is closed");
        }
        if (this.started) {
            throw new IllegalStateException("Node housekeeper is already started");
        }
        this.started = true;
        for (final ScheduledTask scheduled : this.pending) {
            this.scheduler.scheduleWithFixedDelay(
                    () -> {
                        if (this.closed || this.closing || !scheduled.running().compareAndSet(false, true)) return;
                        try {
                            this.workers.execute(() -> {
                                try {
                                    if (!this.closed && !this.closing) this.runGuarded(scheduled);
                                } finally {
                                    scheduled.running().set(false);
                                }
                            });
                        } catch (final RejectedExecutionException rejected) {
                            scheduled.running().set(false);
                            if (!this.closing) throw rejected;
                        }
                    },
                    scheduled.intervalMillis(),
                    scheduled.intervalMillis(),
                    TimeUnit.MILLISECONDS
            );
        }
        LOGGER.log(INFO, "Started node housekeeper with %s task(s)".formatted(this.pending.size()));
        this.pending.clear();
    }

        /// Stops future runs and releases the threads. A running task is interrupted.
    ///
    /// Only the flag flips hold the monitor; the bounded join runs without
    /// it so scheduling threads are never blocked behind shutdown. A timeout
    /// is logged as a warning and still marks the scheduler closed: the pool
    /// was already shut down, so retrying cannot release anything more, and a
    /// close must not fail the node for a bounded wait.
    @Override
    public void close() {
        synchronized (this) {
            if (this.closed || this.closing) {
                return;
            }
            this.closing = true;
        }
        LOGGER.log(INFO, "Shutting down node housekeeper");
        this.scheduler.shutdownNow();
        this.workers.shutdownNow();
        try {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLOSE_TIMEOUT_MILLIS);
            final boolean schedulerStopped = this.scheduler.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            final boolean workersStopped = this.workers.awaitTermination(
                    Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (!schedulerStopped || !workersStopped) {
                LOGGER.log(WARNING,
                        "Node housekeeper did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            LOGGER.log(WARNING, "Interrupted while stopping node housekeeper", interrupted);
        } finally {
            synchronized (this) {
                this.closed = true;
                this.closing = false;
            }
        }
    }

    private record ScheduledTask(String name, Runnable task, long intervalMillis, AtomicBoolean running) {
    }
}
