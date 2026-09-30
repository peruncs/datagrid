package peruncs.cluster.node;

import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.storage.ReplicationRetry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.Logger.Level.*;
import static org.eclipse.serializer.util.X.notNull;

/// Triggers periodic maintenance on one platform thread and runs the work on virtual threads.
///
/// Each node owns one maintenance scheduler. Callers schedule every task first and
/// start the scheduler last; close stops future runs and releases the
/// threads. A slow run skips later ticks instead of overlapping itself.
/// A task that throws is logged and the
/// remaining tasks keep running; a task that fails the configured threshold
/// consecutive runs degrades [#failure()] so readiness reports the node
/// instead of hiding the repeated failure. Each task clears only its own
/// failure after it recovers. A fatal [Error] never clears.
final class NodeMaintenanceScheduler implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(NodeMaintenanceScheduler.class.getName());
    private final ScheduledThreadPoolExecutor scheduler;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<Error> fatalFailure = new AtomicReference<>();
    private final ConcurrentHashMap<String, RuntimeException> degradedFailures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> consecutiveFailures = new ConcurrentHashMap<>();
    private final List<ScheduledTask> pending = new ArrayList<>();
    private final NodeConfig.Operations operations;
    private boolean started;
    private volatile boolean closing;
    private volatile boolean closed;

    private NodeMaintenanceScheduler(final NodeConfig.Operations operations) {
        this.operations = Objects.requireNonNull(operations, "operations");
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
        return create(NodeConfig.Operations.DEFAULT);
    }

    static NodeMaintenanceScheduler create(final NodeConfig.Operations operations) {
        return new NodeMaintenanceScheduler(operations);
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
            if (failures >= this.operations.maintenanceFailureThreshold()) {
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
        if (fatal != null) return fatal;
        for (final RuntimeException failure : this.degradedFailures.values()) return failure;
        return null;
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

    /// Stops future runs and waits for every worker to terminate.
    ///
    /// A timeout leaves the scheduler closing so no more work can start, but
    /// does not report success. A later close can prove termination again.
    @Override
    public void close() {
        synchronized (this) {
            if (this.closed) {
                return;
            }
            this.closing = true;
        }
        LOGGER.log(INFO, "Shutting down node housekeeper");
        this.scheduler.shutdownNow();
        this.workers.shutdownNow();
        try {
            final long timeoutMillis = this.operations.maintenanceCloseTimeout().toMillis();
            final long deadline = ReplicationRetry.deadlineNanos(TimeUnit.MILLISECONDS.toNanos(timeoutMillis));
            final boolean schedulerStopped = this.scheduler.awaitTermination(
                    timeoutMillis, TimeUnit.MILLISECONDS);
            final boolean workersStopped = this.workers.awaitTermination(
                    ReplicationRetry.remainingNanos(deadline), TimeUnit.NANOSECONDS);
            if (!schedulerStopped || !workersStopped) {
                throw new IllegalStateException(
                        "Node housekeeper workers did not stop within %s ms"
                                .formatted(this.operations.maintenanceCloseTimeout().toMillis()));
            }
            synchronized (this) {
                this.closed = true;
                this.closing = false;
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping node housekeeper", interrupted);
        }
    }

    boolean isStopped() {
        return this.closed && this.scheduler.isTerminated() && this.workers.isTerminated();
    }

    private record ScheduledTask(String name, Runnable task, long intervalMillis, AtomicBoolean running) {
    }
}
