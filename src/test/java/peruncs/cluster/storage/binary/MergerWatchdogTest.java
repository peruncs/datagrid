package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Verifies per-batch watchdog tasks do not accumulate once they are cancelled.
class MergerWatchdogTest {
    @Test
    void cancelledWatchdogTasksLeaveTheDelayQueueImmediately() {
        final ScheduledThreadPoolExecutor watchdog = StorageBinaryDataMerger.newWatchdog();
        try {
            for (int batch = 0; batch < 10_000; batch++) {
                final ScheduledFuture<?> materialization = watchdog.schedule(() -> {}, 180, TimeUnit.SECONDS);
                final ScheduledFuture<?> refresh = watchdog.schedule(() -> {}, 1_800, TimeUnit.SECONDS);
                materialization.cancel(false);
                refresh.cancel(false);
            }
            assertEquals(0, watchdog.getQueue().size(), "cancelled tasks must not wait for their deadline");
            assertTrue(watchdog.getRemoveOnCancelPolicy());
        } finally {
            watchdog.shutdownNow();
        }
    }
}
