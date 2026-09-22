package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.backup.StorageBackupTaskExecutor;
import peruncs.datagrid.cluster.node.store.StorageLimitGate;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies housekeeper tasks, scheduling rules and lifecycle.
class NodeHousekeeperTest {
    private static void awaitCondition(
            final BooleanSupplier condition,
            final long timeoutMillis,
            final String message
    ) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(message);
            }
            Thread.sleep(25L);
        }
    }

        /// The backup task delegates to the automatic slot when idle.
    @Test
    void backupWorkRunsBackupWhenIdle() {
        final BackupFake backups = new BackupFake();
        final Runnable task = backups.createScheduledWork();

        task.run();

        assertEquals(1, backups.backupRequests.get());
        assertFalse(backups.manualSlot.get());
    }

        /// A backup while another one runs is skipped instead of queued.
    @Test
    void backupWorkSkipsWhenBusy() {
        final BackupFake backups = new BackupFake();
        backups.busy.set(true);
        final Runnable task = backups.createScheduledWork();

        task.run();

        assertEquals(1, backups.backupRequests.get());
    }

        /// A failed start is treated as a harmless concurrent-start rejection.
    @Test
    void backupWorkSkipsWhenStartIsRejected() {
        final BackupFake backups = new BackupFake();
        backups.rejectWithoutRunning.set(true);
        final Runnable task = backups.createScheduledWork();

        assertDoesNotThrow(task::run);
        assertEquals(1, backups.backupRequests.get());
    }

        /// The limit task records measurements in the gate.
    @Test
    void limitCheckWorkUpdatesGate() {
        final AtomicLong usedBytes = new AtomicLong(10_000_000_000L);
        final StorageLimitGate gate = StorageLimitGate.create(10);
        final Runnable task = gate.createScheduledWork(usedBytes::get);

        task.run();
        assertTrue(gate.limitReached());

        usedBytes.set(9_500_000_000L);
        task.run();
        assertTrue(gate.limitReached());

        usedBytes.set(0L);
        task.run();
        assertFalse(gate.limitReached());
    }

        /// Invalid registrations are rejected before anything runs.
    @Test
    void rejectsInvalidSchedule() {
        try (final NodeHousekeeper housekeeper = NodeHousekeeper.create()) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> housekeeper.schedule(" ", () -> {
                    }, Duration.ofMillis(50))
            );
            assertThrows(
                    NullPointerException.class,
                    () -> housekeeper.schedule("task", null, Duration.ofMillis(50))
            );
            assertThrows(
                    IllegalArgumentException.class,
                    () -> housekeeper.schedule("task", () -> {
                    }, Duration.ZERO)
            );
            assertThrows(
                    IllegalArgumentException.class,
                    () -> housekeeper.schedule("task", () -> {
                    }, Duration.ofMillis(-1))
            );
            assertThrows(
                    IllegalArgumentException.class,
                    () -> housekeeper.schedule("task", () -> {
                    }, Duration.ofNanos(1))
            );
            assertThrows(
                    IllegalArgumentException.class,
                    () -> housekeeper.schedule("task", () -> {
                    }, null)
            );
        }
    }

        /// Registration ends once the housekeeper has started.
    @Test
    void rejectsScheduleAfterStart() {
        try (final NodeHousekeeper housekeeper = NodeHousekeeper.create()) {
            housekeeper.schedule("task", () -> {
            }, Duration.ofMillis(50));
            housekeeper.start();

            assertThrows(
                    IllegalStateException.class,
                    () -> housekeeper.schedule("late", () -> {
                    }, Duration.ofMillis(50))
            );
            assertThrows(IllegalStateException.class, housekeeper::start);
        }
    }

        /// Tasks fire repeatedly and stop after close.
    @Test
    void firesPeriodicallyAndStopsOnClose() throws InterruptedException {
        final AtomicInteger runs = new AtomicInteger();
        try (final NodeHousekeeper housekeeper = NodeHousekeeper.create()) {
            housekeeper.schedule("counter", runs::incrementAndGet, Duration.ofMillis(50));
            housekeeper.start();
            awaitCondition(() -> runs.get() >= 2, 5_000L, "periodic task did not run");
        }

        final int stopped = runs.get();
        Thread.sleep(200L);
        assertEquals(stopped, runs.get());
    }

        /// A failing task is logged while the remaining tasks keep running.
    @Test
    void failingTaskDoesNotStopOthers() throws InterruptedException {
        final AtomicInteger runs = new AtomicInteger();
        try (final NodeHousekeeper housekeeper = NodeHousekeeper.create()) {
            housekeeper.schedule("failing", () ->
            {
                throw new IllegalStateException("boom");
            }, Duration.ofMillis(50));
            housekeeper.schedule("counter", runs::incrementAndGet, Duration.ofMillis(50));
            housekeeper.start();

            awaitCondition(() -> runs.get() >= 2, 5_000L, "healthy task stopped after a failure");
        }
    }

        /// An Error is recorded for health and does not cancel its fixed-delay task.
    @Test
    void fatalTaskFailureDoesNotCancelMaintenanceSchedule() throws InterruptedException {
        final AtomicBoolean first = new AtomicBoolean(true);
        final AtomicInteger runs = new AtomicInteger();
        try (final NodeHousekeeper housekeeper = NodeHousekeeper.create()) {
            housekeeper.schedule("fatal", () -> {
                runs.incrementAndGet();
                if (first.getAndSet(false)) throw new AssertionError("fatal maintenance failure");
            }, Duration.ofMillis(50));
            housekeeper.start();
            awaitCondition(() -> runs.get() >= 2, 5_000L, "fatal task was cancelled after Error");
            assertNotNull(housekeeper.failure());
        }
    }

        /// A task that fails past the degradation threshold degrades health,
    /// and the next successful run clears the degradation: a transient flap
    /// must not mark the node degraded for its lifetime.
    @Test
    void degradedTaskRecoversOnNextSuccess() throws InterruptedException {
        final AtomicInteger runs = new AtomicInteger();
        /* Fails until the test has observed the degradation, then succeeds:
         * without the gate, the clearing run could race the observation and
         * the first await would miss the latched window. */
        final AtomicBoolean degradedObserved = new AtomicBoolean(false);
        try (final NodeHousekeeper housekeeper = NodeHousekeeper.create()) {
            housekeeper.schedule("flap", () -> {
                if (!degradedObserved.get()) {
                    runs.incrementAndGet();
                    throw new IllegalStateException("transient maintenance failure");
                }
            }, Duration.ofMillis(20));
            housekeeper.start();
            awaitCondition(() -> housekeeper.failure() != null, 10_000L,
                    "repeated failures must degrade health");
            assertTrue(runs.get() >= NodeHousekeeper.FAILURE_THRESHOLD,
                    "degradation must require the documented threshold, saw %s".formatted(runs.get()));
            degradedObserved.set(true);
            awaitCondition(() -> housekeeper.failure() == null, 10_000L,
                    "the first success after degradation must clear it");
        }
    }


        /// A slow run postpones its own next run instead of overlapping it.
    @Test
    void slowRunDoesNotOverlapItself() throws InterruptedException {
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();
        try (final NodeHousekeeper housekeeper = NodeHousekeeper.create()) {
            housekeeper.schedule("slow", () ->
            {
                final int active = concurrent.incrementAndGet();
                maxConcurrent.accumulateAndGet(active, Math::max);
                try {
                    Thread.sleep(150L);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    concurrent.decrementAndGet();
                }
            }, Duration.ofMillis(50));
            housekeeper.start();

            Thread.sleep(700L);
        }

        assertEquals(1, maxConcurrent.get());
    }

        /// Controllable backup executor double.
    private static final class BackupFake implements StorageBackupTaskExecutor {
        private final AtomicInteger backupRequests = new AtomicInteger();
        private final AtomicBoolean manualSlot = new AtomicBoolean(true);
        private final AtomicBoolean busy = new AtomicBoolean(false);
        private final AtomicBoolean rejectWithoutRunning = new AtomicBoolean(false);
        private final AtomicReference<Boolean> running = new AtomicReference<>(false);

        @Override
        public StorageBackupTaskExecutor.BackupStartResult runBackup(final boolean useManualSlot) {
            this.backupRequests.incrementAndGet();
            this.manualSlot.set(useManualSlot);
            if (this.rejectWithoutRunning.get()) {
                return StorageBackupTaskExecutor.BackupStartResult.BUSY;
            }
            if (this.busy.get()) {
                this.running.set(true);
                return StorageBackupTaskExecutor.BackupStartResult.BUSY;
            }
            return StorageBackupTaskExecutor.BackupStartResult.STARTED;
        }

        @Override
        public boolean isRunningBackup() {
            return this.running.get();
        }

        @Override
        public Throwable backupFailure() {
            return null;
        }

        @Override
        public void runChecks() {
        }

        @Override
        public boolean isRunningChecks() {
            return false;
        }

        @Override
        public Throwable failure() {
            return null;
        }
    }
}
