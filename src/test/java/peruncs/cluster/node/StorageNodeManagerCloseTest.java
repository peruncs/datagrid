package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.StorageNodeHealthCheck;
import peruncs.cluster.node.store.StorageTaskExecutor;
import peruncs.cluster.node.store.StorageUsageGauge;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.ReplicationPublisher;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies close-once disposal for both fixed roles: every resource is
/// released exactly once, and a failed close is never retried.
class StorageNodeManagerCloseTest {
    private static final class CountingHandler implements InvocationHandler {
        final AtomicInteger disposeCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        volatile Throwable disposeFailure;
        volatile RuntimeException latestFailure;

        @Override
        public Object invoke(final Object proxy, final java.lang.reflect.Method method, final Object[] args)
                throws Throwable {
            switch (method.getName()) {
                case "dispose" -> {
                    this.disposeCalls.incrementAndGet();
                    if (this.disposeFailure != null) throw this.disposeFailure;
                    return null;
                }
                case "latest" -> {
                    if (this.latestFailure != null) throw this.latestFailure;
                    return new ReplicationCursor("test", UUID.randomUUID(), 5L, "");
                }
                case "close" -> {
                    this.closeCalls.incrementAndGet();
                    return null;
                }
                case "cursor" -> {
                    return new ReplicationCursor("test", UUID.randomUUID(), 5L, "");
                }
                case "stopResult" -> {
                    return new ReplicationApplier.StopResult(
                            ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY, 5L, -1L);
                }
                default -> {
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return null;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T tracked(final Class<T> type, final CountingHandler handler) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static StorageNodeManager manager(
            final StorageNodeManager.Role role,
            final CountingHandler distributor, final CountingHandler client,
            final CountingHandler health, final CountingHandler position) {
        return StorageNodeManager.create(new StorageNodeManager.Configuration(
                tracked(ReplicationPublisher.class, distributor),
                tracked(StorageTaskExecutor.class, new CountingHandler()),
                tracked(ReplicationApplier.class, client),
                tracked(StorageNodeHealthCheck.class, health),
                tracked(StorageUsageGauge.class, new CountingHandler()),
                tracked(ReplicationPositionProvider.class, position),
                "aeron",
                role, new peruncs.cluster.storage.StorageGraphCoordinator()));
    }

    private static StorageNodeManager reader(
            final CountingHandler distributor, final CountingHandler client,
            final CountingHandler health, final CountingHandler position) {
        return manager(StorageNodeManager.Role.READER, distributor, client, health, position);
    }

        /// A close that fails mid-way stays retryable: the completed
        /// disposals are never repeated and a later retry finishes exactly
        /// the disposal that failed.
    @Test
    void failedCloseIsRetryableAndCompletesTheRemainder() {
        final CountingHandler distributor = new CountingHandler();
        final CountingHandler client = new CountingHandler();
        final CountingHandler health = new CountingHandler();
        final CountingHandler position = new CountingHandler();
        distributor.disposeFailure = new IllegalStateException("distributor close failed");
        final StorageNodeManager manager = reader(distributor, client, health, position);

        assertThrows(NodeException.class, manager::close);
        assertEquals(1, client.disposeCalls.get(), "data client disposed on the first attempt");
        assertEquals(1, health.closeCalls.get(), "health check closed on the first attempt");
        assertEquals(1, position.closeCalls.get(), "position provider closed on the first attempt");

        /* A transient failure clears: the retry must finish the distributor
         * without re-disposing anything that already completed. */
        distributor.disposeFailure = null;
        manager.close();

        assertEquals(2, distributor.disposeCalls.get(), "the failed disposal must be retried");
        assertEquals(1, client.disposeCalls.get(), "data client re-disposed on retry");
        assertEquals(1, health.closeCalls.get(), "health check re-closed on retry");
        assertEquals(1, position.closeCalls.get(), "position provider re-closed on retry");

        /* A fully completed close is idempotent. */
        manager.close();
        assertEquals(2, distributor.disposeCalls.get());
    }

        /// Any provider fault — unavailable boundary or transport failure —
        /// reads as unknown (`-1`) so a metrics scrape never fails.
    @Test
    void latestSequenceFaultsReadAsUnknown() {
        final CountingHandler position = new CountingHandler();
        position.latestFailure = new NodeException("writer boundary not readable");
        final StorageNodeManager transportFailure = reader(
                new CountingHandler(), new CountingHandler(), new CountingHandler(), position);
        assertEquals(-1L, transportFailure.latestSequence());

        final CountingHandler unavailable = new CountingHandler();
        unavailable.latestFailure = new ReplicationPositionUnavailableException(
                "no writer boundary for this role");
        final StorageNodeManager boundaryMissing = reader(
                new CountingHandler(), new CountingHandler(), new CountingHandler(), unavailable);
        assertEquals(-1L, boundaryMissing.latestSequence());
    }

        /// An Error during close is rethrown even when a RuntimeException came first.
    @Test
    void closeRethrowsErrorAheadOfRuntimeException() {
        final CountingHandler distributor = new CountingHandler();
        final CountingHandler client = new CountingHandler();
        distributor.disposeFailure = new IllegalStateException("distributor close failed");
        client.disposeFailure = new AssertionError("fatal client failure");
        final StorageNodeManager manager = reader(distributor, client, new CountingHandler(), new CountingHandler());

        final AssertionError fatal = assertThrows(AssertionError.class, manager::close);
        assertEquals(1, fatal.getSuppressed().length);
        assertInstanceOf(IllegalStateException.class, fatal.getSuppressed()[0]);
    }

        /// A fixed writer closes every resource exactly once.
    @Test
    void writerCloseDisposesEverythingOnce() {
        final CountingHandler distributor = new CountingHandler();
        final CountingHandler client = new CountingHandler();
        final CountingHandler health = new CountingHandler();
        final CountingHandler position = new CountingHandler();
        final StorageNodeManager manager =
                manager(StorageNodeManager.Role.WRITER, distributor, client, health, position);

        assertTrue(manager.isWriter());
        manager.close();
        manager.close();

        assertEquals(1, distributor.disposeCalls.get(), "distributor not disposed exactly once");
        assertEquals(1, client.disposeCalls.get(), "data client not disposed exactly once");
        assertEquals(1, health.closeCalls.get(), "health check not closed exactly once");
        assertEquals(1, position.closeCalls.get(), "position provider not closed exactly once");
    }
}
