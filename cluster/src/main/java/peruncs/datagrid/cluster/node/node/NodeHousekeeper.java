package peruncs.datagrid.cluster.node.node;

import peruncs.datagrid.cluster.node.backup.StorageBackupTaskExecutor;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageLimitGate;

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
    private boolean closed;

    private NodeHousekeeper(final int threads) {
        final AtomicInteger threadCount = new AtomicInteger();
        this.scheduler = new ScheduledThreadPoolExecutor(threads, task ->
        {
            final Thread thread = new Thread(task, "datagrid-housekeeper-%s".formatted(threadCount.incrementAndGet()));
            thread.setDaemon(true);
            return thread;
        });
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
        } catch (final Throwable failure) {
            LOGGER.log(System.Logger.Level.ERROR, "Housekeeper task '%s' failed".formatted(scheduled.name()), failure);
        }
    }

        /// Creates the periodic full-backup task.
    ///
    /// The task uses the automatic backup slot of the shared single-flight
    /// backup executor. A run while another backup is active is skipped instead
    /// of queuing behind it.
    ///
    /// @param backupExecutor shared backup task executor
    /// @return backup task
    public static Runnable backupWork(final StorageBackupTaskExecutor backupExecutor) {
        notNull(backupExecutor);
        return () ->
        {
            LOGGER.log(System.Logger.Level.INFO, "Issuing full backup");
            try {
                backupExecutor.runBackup(false);
            } catch (final IllegalStateException busy) {
                if (backupExecutor.isRunningBackup()) {
                    LOGGER.log(System.Logger.Level.INFO, "Skipping scheduled backup because one is already running");
                    return;
                }
                throw busy;
            }
        };
    }

        /// Creates the periodic storage-limit check task.
    ///
    /// The task measures used disk space and records it in the gate, which
    /// request threads read to decide whether writes are still accepted.
    ///
    /// @param diskSpaceReader storage measurement source
    /// @param limitGate       shared limit state
    /// @return limit-check task
    public static Runnable limitCheckWork(
            final StorageDiskSpaceReader diskSpaceReader,
            final StorageLimitGate limitGate
    ) {
        notNull(diskSpaceReader);
        notNull(limitGate);
        return () ->
        {
            LOGGER.log(System.Logger.Level.TRACE, "Executing storage limit checker task");
            final long usedBytes = diskSpaceReader.readUsedDiskSpaceBytes();
            final long usedGb = usedBytes / 1_000_000_000L;
            LOGGER.log(System.Logger.Level.INFO, "Storage Size: %sgb/%sgb (%s bytes)".formatted(usedGb, limitGate.limitGb(), usedBytes));
            if (usedBytes >= limitGate.limitBytes()) {
                LOGGER.log(System.Logger.Level.WARNING, "Storage limit reached! No more data will be stored!");
            }
            limitGate.updateUsage(usedBytes);
        };
    }

        /// Registers one periodic task. Tasks must be scheduled before start.
    ///
    /// @param name     human-readable task name used in log messages
    /// @param task     the work to run
    /// @param interval delay between the end of one run and the start of the next
    public synchronized void schedule(final String name, final Runnable task, final Duration interval) {
        if (this.closed) {
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
        LOGGER.log(System.Logger.Level.INFO, "Scheduling housekeeper task '%s' every %s".formatted(name, interval));
        this.pending.add(new ScheduledTask(name, task, interval));
    }

        /// Starts firing the scheduled tasks. The first run of each task waits one interval.
    public synchronized void start() {
        if (this.closed) {
            throw new IllegalStateException("Node housekeeper is closed");
        }
        if (this.started) {
            throw new IllegalStateException("Node housekeeper is already started");
        }
        this.started = true;
        for (final ScheduledTask scheduled : this.pending) {
            final long intervalMillis = scheduled.interval().toMillis();
            this.scheduler.scheduleWithFixedDelay(
                    () -> runGuarded(scheduled),
                    intervalMillis,
                    intervalMillis,
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
        this.closed = true;
        LOGGER.log(System.Logger.Level.INFO, "Shutting down node housekeeper");
        this.scheduler.shutdownNow();
        try {
            if (!this.scheduler.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                LOGGER.log(System.Logger.Level.WARNING, "Node housekeeper did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            LOGGER.log(System.Logger.Level.WARNING, "Interrupted while stopping node housekeeper", interrupted);
        }
    }

    private record ScheduledTask(String name, Runnable task, Duration interval) {
    }
}
