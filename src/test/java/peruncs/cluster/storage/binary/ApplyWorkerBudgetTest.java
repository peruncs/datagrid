package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationUnavailableException;

import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.eclipse.serializer.concurrency.LockedExecutor.New;
import static org.junit.jupiter.api.Assertions.*;

/// Verifies the per-phase budget accounting: materialization is bounded from
/// batch start to the materialized stamp, and the whole-store index refresh
/// is bounded separately from that stamp onward — a slow refresh must never
/// be reported as a materialization timeout.
class ApplyWorkerBudgetTest {

    private static ApplyWorker worker(final MergerLifecycle owner, final ObjectGraphUpdateHandler handler, final long budgetMs) {
        final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
        return new ApplyWorker(owner, new ApplyQueue(owner, 1L, 1L << 20, 60_000L), New(), watchdog,
                StorageBinaryDataMergerTestSupport.foundation(),
                StorageBinaryDataMergerTestSupport.connection(),
                handler, 0L, 10_000, budgetMs);
    }

    /// Minimal merger lifecycle stub recording latched failures.
    private static final class RecordingLifecycle implements MergerLifecycle {
        RuntimeException latch;
        String noted;
        @Override
        public RuntimeException failure() {
            return this.latch;
        }
        @Override
        public boolean isDisposed() {
            return false;
        }
        @Override
        public void latchFailure(final RuntimeException failure) {
            this.latch = failure;
        }
        @Override
        public void noteFailure(final String message, final Throwable cause) {
            this.noted = message;
            this.latch = new ReplicationUnavailableException(message, cause);
        }
        @Override
        public RuntimeException recordLifecycleFailure(final String message, final Throwable cause) {
            this.noted = message;
            this.latch = new ReplicationUnavailableException(message, cause);
            return this.latch;
        }
        @Override
        public void onMaterializationBudgetExpired(final long startedNanos) {
            this.noteFailure("materialization watchdog expired", null);
        }
        @Override
        public void onRefreshBudgetExpired(final long startedNanos, final long rebuildStartedNanos) {
            this.noteFailure("refresh watchdog expired", null);
        }
    }

    /// A materialization that ends within its budget followed by a slow index
    /// refresh must fail the refresh phase, never the materialization budget.
    @Test
    void slowIndexRefreshIsNotChargedToMaterialization() throws Exception {
        final RecordingLifecycle owner = new RecordingLifecycle();
        final long budgetMs = 200L;
        final ApplyWorker worker = worker(owner, Runnable::run, budgetMs);
        try {
            final long started = System.nanoTime();
            /* Materialization takes far less than its (large) budget; the
             * refresh phase takes more than the budget of its own scale but
             * must be attributed as an index-refresh overrun, not as a
             * materialization timeout. */
            final long materializedAt = started + TimeUnit.MILLISECONDS.toNanos(10L);
            final long refreshedAt = materializedAt + TimeUnit.MILLISECONDS.toNanos(10_000L);
            stamp(worker, materializedAt, refreshedAt);
            final ReplicationUnavailableException failure = assertThrows(
                    ReplicationUnavailableException.class, () -> worker.verifyBatchBudgets(started),
                    "a refresh overrun bounded by the materialization phase would be a false failure");
            assertTrue(failure.getMessage().contains("refreshing reader index views"),
                    "a slow refresh must be reported as an index-refresh overrun: " + failure.getMessage());
            assertFalse(failure.getMessage().contains("batch took"),
                    "the materialization message must not absorb the refresh: " + failure.getMessage());
        } finally {
            workerShutdown(worker);
        }
    }

    /// A materialization that overruns its budget is reported as such.
    @Test
    void materializationOverrunIsChargedToMaterialization() throws Exception {
        final RecordingLifecycle owner = new RecordingLifecycle();
        final ApplyWorker worker = worker(owner, Runnable::run, 50L);
        try {
            final long started = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(5_000L);
            stamp(worker, System.nanoTime(), System.nanoTime());
            final ReplicationUnavailableException failure = assertThrows(
                    ReplicationUnavailableException.class, () -> worker.verifyBatchBudgets(started));
            assertTrue(failure.getMessage().contains("batch took"),
                    "a materialization overrun must say timed out while applying: " + failure.getMessage());
        } finally {
            workerShutdown(worker);
        }
    }

    /// A batch comfortably inside both budgets records no failure.
    @Test
    void bothPhasesWithinBudgetDoNotFail() throws Exception {
        final RecordingLifecycle owner = new RecordingLifecycle();
        final ApplyWorker worker = worker(owner, Runnable::run, 5_000L);
        try {
            final long started = System.nanoTime();
            stamp(worker, started + 1_000_000L, started + 2_000_000L);
            assertDoesNotThrow(() -> worker.verifyBatchBudgets(started));
            assertNull(owner.failure());
        } finally {
            workerShutdown(worker);
        }
    }

    /// A watchdog that fires after its phase stamped must not latch anything:
    /// the phase boundary and the cancel run inside the same critical section
    /// the expiry check reads.
    @Test
    void phaseWatchdogLosesTheRaceAgainstCompletion() throws Exception {
        final RecordingLifecycle owner = new RecordingLifecycle();
        final ApplyWorker worker = worker(owner, Runnable::run, 5_000L);
        try {
            final long started = System.nanoTime();
            stamp(worker, System.nanoTime(), System.nanoTime());
            worker.materializationBudgetExpired(started);
            /* The materialization watchdog already saw its stamp; simulate a
             * still-armed refresh watchdog firing after the refresh stamped. */
            worker.refreshBudgetExpired(started, System.nanoTime() - 1);
            assertNull(owner.failure(), "an expiry after the phase stamp must be ignored");
        } finally {
            workerShutdown(worker);
        }
    }

    private static void stamp(final ApplyWorker worker, final long materializedAt, final long refreshedAt)
            throws Exception {
        final Field lockField = ApplyWorker.class.getDeclaredField("budgetLock");
        lockField.setAccessible(true);
        final Object lock = lockField.get(worker);
        final Field materializedField = ApplyWorker.class.getDeclaredField("materializedAtNanos");
        materializedField.setAccessible(true);
        final Field refreshedField = ApplyWorker.class.getDeclaredField("indexRefreshedAtNanos");
        refreshedField.setAccessible(true);
        synchronized (lock) {
            materializedField.setLong(worker, materializedAt);
            refreshedField.setLong(worker, refreshedAt);
        }
    }

    private static void workerShutdown(final ApplyWorker worker) throws Exception {
        final Field watchdogField = ApplyWorker.class.getDeclaredField("watchdog");
        watchdogField.setAccessible(true);
        ((ScheduledExecutorService) watchdogField.get(worker)).shutdownNow();
    }
}
