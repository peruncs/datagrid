package peruncs.datagrid.cluster.node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.eclipse.serializer.util.X.notNull;

/// Runs periodic node maintenance on shared daemon platform threads.
///
/// Each node owns one housekeeper. Callers schedule every task first and
/// start the housekeeper last; close stops future runs and releases the
/// threads. Tasks run with a fixed delay, so a slow run postpones its own
/// next run instead of overlapping it. A task that throws is logged and the
/// remaining tasks keep running; a task that fails [#FAILURE_THRESHOLD]
/// consecutive runs degrades [#failure()] so readiness reports the node
/// instead of hiding the repeated failure. Each task clears only its own
/// failure after it recovers. A fatal [Error] never clears.
final class NodeHousekeeper implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(NodeHousekeeper.class.getName());
    private static final int THREADS = 2;
    /// Runs a task must fail consecutively before health degrades.
    static final int FAILURE_THRESHOLD = 3;
    private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;

    private final ScheduledThreadPoolExecutor scheduler;
    private final AtomicReference<Error> fatalFailure = new AtomicReference<>();
    private final ConcurrentHashMap<String, RuntimeException> degradedFailures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> consecutiveFailures = new ConcurrentHashMap<>();
    private final List<ScheduledTask> pending = new ArrayList<>();
    private boolean started;
    private boolean closing;
    private boolean closed;

    private NodeHousekeeper() {
        final AtomicInteger threadCount = new AtomicInteger();
        this.scheduler = new ScheduledThreadPoolExecutor(THREADS, task ->
                Thread.ofPlatform()
                        .daemon()
                        .name("datagrid-housekeeper-%s".formatted(threadCount.incrementAndGet()))
                        .unstarted(task));
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

        /// Creates a housekeeper with two daemon threads.
    ///
    /// @return a housekeeper ready for scheduling
    public static NodeHousekeeper New() {
        return new NodeHousekeeper();
    }

    private void runGuarded(final ScheduledTask scheduled) {
        LOGGER.log(System.Logger.Level.DEBUG, "Running housekeeper task '%s'".formatted(scheduled.name()));
        try {
            scheduled.task().run();
            this.consecutiveFailures.remove(scheduled.name());
            this.degradedFailures.remove(scheduled.name());
            LOGGER.log(System.Logger.Level.DEBUG, "Finished housekeeper task '%s'".formatted(scheduled.name()));
        } catch (final RuntimeException failure) {
            LOGGER.log(System.Logger.Level.ERROR, "Housekeeper task '%s' failed".formatted(scheduled.name()), failure);
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
            LOGGER.log(System.Logger.Level.ERROR, "Fatal housekeeper task '%s' failure".formatted(scheduled.name()), failure);
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
        this.pending.clear();
    }

        /// Stops future runs and releases the threads. A running task is interrupted.
    ///
    /// Only the flag flips hold the monitor; the bounded join runs without
    /// it so scheduling threads are never blocked behind shutdown. A timeout
    /// is logged as a warning and still marks the housekeeper closed: the pool
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
        LOGGER.log(System.Logger.Level.INFO, "Shutting down node housekeeper");
        this.scheduler.shutdownNow();
        try {
            if (!this.scheduler.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Node housekeeper did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            LOGGER.log(System.Logger.Level.WARNING, "Interrupted while stopping node housekeeper", interrupted);
        } finally {
            synchronized (this) {
                this.closed = true;
                this.closing = false;
            }
        }
    }

    private record ScheduledTask(String name, Runnable task, long intervalMillis) {
    }
}
