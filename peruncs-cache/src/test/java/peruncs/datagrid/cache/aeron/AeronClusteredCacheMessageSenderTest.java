package peruncs.datagrid.cache.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import peruncs.datagrid.cache.test.ClusteredCacheTestSupport;

import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the shared sequence lock is bounded and still admits a sender that
/// obtains it within the publish budget.
@Timeout(60)
class AeronClusteredCacheMessageSenderTest {
    private static final long PUBLISH_TIMEOUT_NANOS = 200_000_000L;

    @Test
    void boundedSequenceLockWaitFailsWithinPublishTimeout() throws Exception {
        try (final AeronClusteredCacheResources resources =
                     new AeronClusteredCacheResources(null, "aeron:ipc", 2001, 10_000L, false)) {
            final ReentrantLock lock = new ReentrantLock();
            final AeronClusteredCacheMessageSender sender = AeronClusteredCacheMessageSender.New(
                    resources,
                    new byte[16],
                    AeronClusteredCacheSenderSequence.SequenceLease.local(),
                    lock,
                    () -> {
                    },
                    PUBLISH_TIMEOUT_NANOS,
                    TimeUnit.MINUTES.toNanos(1L),
                    1024,
                    null);

            final CountDownLatch lockHeld = new CountDownLatch(1);
            final CountDownLatch releaseLock = new CountDownLatch(1);
            final Thread holder = Thread.ofVirtual().start(() ->
            {
                lock.lock();
                try {
                    lockHeld.countDown();
                    releaseLock.await(30, TimeUnit.SECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                }
            });
            try {
                assertTrue(lockHeld.await(10, TimeUnit.SECONDS), "the competing holder never took the lock");

                final long start = System.nanoTime();
                final CacheEntryListenerException failure = assertThrows(
                        CacheEntryListenerException.class,
                        () -> ClusteredCacheTestSupport.publish(sender, EventType.CREATED, "cache", "table", 1L));
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                assertTrue(elapsedMillis < 2_000L,
                        "the sequence-lock wait was not bounded by the %s ms budget: %s ms"
                                .formatted(PUBLISH_TIMEOUT_NANOS / 1_000_000L, elapsedMillis));
                assertTrue(failure.getMessage().contains("shared sequence"),
                        "unexpected failure message: " + failure.getMessage());
                assertEquals(0L, sender.published(),
                        "a failed lock acquisition must not consume a sequence or reach the publication");
            } finally {
                releaseLock.countDown();
                holder.join(TimeUnit.SECONDS.toMillis(10));
                sender.dispose();
            }
        }
    }

    @Test
    void sequenceLockContentionWithinBudgetStillPublishes() throws Exception {
        try (final AeronClusteredCacheResources resources =
                     new AeronClusteredCacheResources(null, "aeron:ipc", 2001, 10_000L, true)) {
            /* An IPC publication reports NOT_CONNECTED until a subscriber
             * exists, so create the matching subscription before publishing. */
            resources.subscription();
            final ReentrantLock lock = new ReentrantLock();
            final AeronClusteredCacheMessageSender sender = AeronClusteredCacheMessageSender.New(
                    resources,
                    new byte[16],
                    AeronClusteredCacheSenderSequence.SequenceLease.local(),
                    lock,
                    () -> {
                    },
                    TimeUnit.SECONDS.toNanos(10L),
                    TimeUnit.MINUTES.toNanos(1L),
                    1024,
                    null);

            final CountDownLatch lockHeld = new CountDownLatch(1);
            final CountDownLatch releaseLock = new CountDownLatch(1);
            final Thread holder = Thread.ofVirtual().start(() ->
            {
                lock.lock();
                try {
                    lockHeld.countDown();
                    releaseLock.await(30, TimeUnit.SECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                }
            });
            final Thread releasing = Thread.ofVirtual().start(() ->
            {
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                releaseLock.countDown();
            });
            try {
                assertTrue(lockHeld.await(10, TimeUnit.SECONDS), "the competing holder never took the lock");

                ClusteredCacheTestSupport.publish(sender, EventType.CREATED, "cache", "table", 1L);

                assertEquals(1L, sender.published(),
                        "a sender that obtains the lock within the budget must publish normally");
            } finally {
                releaseLock.countDown();
                holder.join(TimeUnit.SECONDS.toMillis(10));
                releasing.join(TimeUnit.SECONDS.toMillis(10));
                sender.dispose();
                resources.closeSubscription();
            }
        }
    }
}
