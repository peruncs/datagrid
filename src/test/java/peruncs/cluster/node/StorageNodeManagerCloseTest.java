package peruncs.cluster.node;

import org.eclipse.store.storage.types.StorageController;
import org.junit.jupiter.api.Test;
import peruncs.cluster.api.NodeRole;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;
import peruncs.cluster.node.replication.ReplicationHealth;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.StorageNodeHealthCheck;
import peruncs.cluster.node.store.StorageTaskExecutor;
import peruncs.cluster.storage.ReplicationPosition;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.ReplicationApplier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies close-once disposal for both fixed roles: completed releases are
/// not repeated after a close failure and are skipped after success.
class StorageNodeManagerCloseTest {
    private static final class CountingHandler implements InvocationHandler {
        final AtomicInteger disposeCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        volatile Throwable disposeFailure;
        volatile RuntimeException closeFailure;
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
                    final UUID id = UUID.randomUUID();
                    return new ReplicationPosition(id, id, 1L, 1L, 5L, 1L, 1L, id);
                }
                case "close" -> {
                    this.closeCalls.incrementAndGet();
                    if (this.closeFailure != null) throw this.closeFailure;
                    return null;
                }
                case "isRunning" -> {
                    return true;
                }
                case "replicationState" -> {
                    return ReplicationState.LIVE;
                }
                case "position" -> {
                    final UUID id = UUID.randomUUID();
                    return new ReplicationPosition(id, id, 1L, 1L, 5L, 1L, 1L, id);
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
            final NodeRole role,
            final CountingHandler client,
            final CountingHandler health, final CountingHandler position) {
        return StorageNodeManager.create(new StorageNodeManager.Configuration(
                tracked(StorageTaskExecutor.class, new CountingHandler()),
                tracked(ReplicationApplier.class, client),
                healthCheck(health),
                () -> -1L,
                tracked(ReplicationPositionProvider.class, position),
                true,
                role, new StorageGraphCoordinator()));
    }

    private static StorageNodeHealthCheck healthCheck(final CountingHandler handler) {
        return StorageNodeHealthCheck.create(
                tracked(StorageController.class, new CountingHandler()),
                new ReplicationHealth() {
                    @Override public boolean isReady() { return true; }
                    @Override public boolean isHealthy() { return true; }
                    @Override public void close() { handler.closeCalls.incrementAndGet(); }
                },
                () -> true);
    }

    private static StorageNodeManager reader(
            final CountingHandler client, final CountingHandler health, final CountingHandler position) {
        return manager(NodeRole.READER, client, health, position);
    }

    /// A close that fails mid-way stays retryable: the completed
    /// disposals are never repeated and a later retry finishes exactly
    /// the disposal that failed.
    @Test
    void failedCloseIsRetryableAndCompletesTheRemainder() {
        final CountingHandler client = new CountingHandler();
        final CountingHandler health = new CountingHandler();
        final CountingHandler position = new CountingHandler();
        client.disposeFailure = new IllegalStateException("client close failed");
        final StorageNodeManager manager = reader(client, health, position);

        assertThrows(NodeException.class, manager::close);
        assertEquals(1, client.disposeCalls.get(), "data client disposal attempted on the first attempt");
        assertEquals(1, health.closeCalls.get(), "health check closed on the first attempt");
        assertEquals(1, position.closeCalls.get(), "position provider closed on the first attempt");

        /* A transient failure clears: the retry must finish the client
         * without re-closing anything that already completed. */
        client.disposeFailure = null;
        manager.close();

        assertEquals(2, client.disposeCalls.get(), "the failed disposal must be retried");
        assertEquals(1, health.closeCalls.get(), "health check re-closed on retry");
        assertEquals(1, position.closeCalls.get(), "position provider re-closed on retry");

        /* A fully completed close is idempotent. */
        manager.close();
        assertEquals(2, client.disposeCalls.get());
    }

    /// Any provider fault — unavailable boundary or transport failure —
    /// reads as an unknown sequence so a metrics scrape never fails.
    @Test
    void latestSequenceFaultsReadAsUnknown() {
        final CountingHandler position = new CountingHandler();
        position.latestFailure = new NodeException("writer boundary not readable");
        final StorageNodeManager transportFailure = reader(
                new CountingHandler(), new CountingHandler(), position);
        assertEquals(-1L, transportFailure.replicationStatus().latestSequence());

        final CountingHandler unavailable = new CountingHandler();
        unavailable.latestFailure = new ReplicationPositionUnavailableException(
                "no writer boundary for this role");
        final StorageNodeManager boundaryMissing = reader(
                new CountingHandler(), new CountingHandler(), unavailable);
        assertEquals(-1L, boundaryMissing.replicationStatus().latestSequence());
    }

    /// An Error during close is rethrown even when a RuntimeException came first.
    @Test
    void closeRethrowsErrorAheadOfRuntimeException() {
        final CountingHandler client = new CountingHandler();
        final CountingHandler position = new CountingHandler();
        position.closeFailure = new IllegalStateException("position provider close failed");
        client.disposeFailure = new AssertionError("fatal client failure");
        final StorageNodeManager manager = reader(client, new CountingHandler(), position);

        final AssertionError fatal = assertThrows(AssertionError.class, manager::close);
        assertEquals(1, fatal.getSuppressed().length);
        assertInstanceOf(IllegalStateException.class, fatal.getSuppressed()[0]);
    }

    /// A fixed writer closes every resource exactly once.
    @Test
    void writerCloseDisposesEverythingOnce() {
        final CountingHandler client = new CountingHandler();
        final CountingHandler health = new CountingHandler();
        final CountingHandler position = new CountingHandler();
        final StorageNodeManager manager =
                manager(NodeRole.WRITER, client, health, position);

        assertTrue(manager.isWriter());
        manager.close();
        manager.close();

        assertEquals(1, client.disposeCalls.get(), "data client not disposed exactly once");
        assertEquals(1, health.closeCalls.get(), "health check not closed exactly once");
        assertEquals(1, position.closeCalls.get(), "position provider not closed exactly once");
    }
}
