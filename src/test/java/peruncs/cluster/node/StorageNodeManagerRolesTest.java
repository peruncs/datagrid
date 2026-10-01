package peruncs.cluster.node;

import org.eclipse.store.storage.types.StorageController;
import org.junit.jupiter.api.Test;
import peruncs.cluster.api.NodeRole;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.api.ReplicationStatus;
import peruncs.cluster.node.replication.ReplicationHealth;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.StorageNodeHealthCheck;
import peruncs.cluster.node.store.StorageTaskExecutor;
import peruncs.cluster.storage.ReplicationPosition;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.ReplicationApplier;

import java.lang.reflect.Proxy;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the fixed reader/writer role split: the role is chosen at
/// creation and never changes, so an unsupported transition is
/// unrepresentable.
class StorageNodeManagerRolesTest {
    /// A fixed-role reader never distributes.
    @Test
    void readerNeverDistributes() {
        final StorageNodeManager manager = manager(NodeRole.READER, true);

        assertFalse(manager.isWriter());
        /* The transport is no longer a monitoring surface: metrics compose
         * from the state, sequence, and readiness components only. */
        assertFalse(manager.isRunningStorageChecks());
    }

    /// A fixed writer reports itself as the writer and derives its
    /// health from the distributor instead of a reader health check.
    @Test
    void writerManagerIsWriter() {
        final StorageNodeManager manager = manager(NodeRole.WRITER, true);

        assertTrue(manager.isWriter());
        assertTrue(manager.isReady(), "a writer must not depend on a reader health check");
        assertTrue(manager.isHealthy(), "a writer must not depend on a reader health check");
    }

    /// The reader reports its current sequence from the replication client.
    @Test
    void readerReportsClientSequence() {
        final ReplicationApplier client = stub(ReplicationApplier.class);
        final StorageNodeManager manager = StorageNodeManager.create(new StorageNodeManager.Configuration(
                stub(StorageTaskExecutor.class),
                client,
                healthCheck(),
                () -> -1L,
                positionProvider(),
                true,
                NodeRole.READER, new StorageGraphCoordinator()));

        assertFalse(manager.isWriter());
        final ReplicationStatus status = manager.replicationStatus();
        assertEquals(ReplicationState.LIVE, status.state());
        assertEquals(0L, status.currentSequence());
        assertEquals(0L, status.latestSequence());
        assertEquals(0L, status.appliedSequence());
    }

    /// A latched graph invalidity surfaces through status: the node is
    /// neither healthy nor ready until it reloads or reseeds.
    @Test
    void graphInvalidationMakesTheNodeUnhealthy() {
        final StorageGraphCoordinator coordinator =
                new StorageGraphCoordinator();
        final StorageNodeManager manager = StorageNodeManager.create(new StorageNodeManager.Configuration(
                stub(StorageTaskExecutor.class),
                stub(ReplicationApplier.class),
                healthCheck(),
                () -> -1L,
                stub(ReplicationPositionProvider.class),
                true,
                NodeRole.WRITER,
                coordinator));
        assertTrue(manager.isHealthy(), "healthy before any failed update");
        assertTrue(manager.isReady(), "ready before any failed update");
        assertThrows(IllegalStateException.class, () ->
                coordinator.write(() -> {
                    throw new IllegalStateException("mid-batch failure");
                }));
        assertFalse(manager.isHealthy(), "an invalidated graph must fail health");
        assertFalse(manager.isReady(), "an invalidated graph must fail readiness");
    }

    /// The writer flag reflects the fixed role.
    @Test
    void writerFlagReflectsFixedRole() {
        final StorageNodeManager reader = manager(NodeRole.READER, true);
        final StorageNodeManager writer = manager(NodeRole.WRITER, true);

        final StorageNodeManager standalone = manager(NodeRole.STANDALONE, false);

        assertFalse(reader.isWriter());
        assertTrue(writer.isWriter());
        assertTrue(standalone.isWriter());
    }

    @Test
    void backupReaderUsesItsBackupManager() {
        assertThrows(IllegalArgumentException.class, () -> manager(NodeRole.BACKUP_READER, true));
    }

    private static StorageNodeManager manager(final NodeRole role, final boolean replicationEnabled) {
        return StorageNodeManager.create(new StorageNodeManager.Configuration(
                stub(StorageTaskExecutor.class),
                stub(ReplicationApplier.class),
                healthCheck(),
                () -> -1L,
                stub(ReplicationPositionProvider.class),
                replicationEnabled,
                role, new StorageGraphCoordinator()));
    }

    private static StorageNodeHealthCheck healthCheck() {
        return StorageNodeHealthCheck.create(
                (StorageController) Proxy.newProxyInstance(StorageController.class.getClassLoader(),
                        new Class<?>[]{StorageController.class},
                        (proxy, method, args) -> method.getName().equals("isRunning")),
                new ReplicationHealth() {
                    @Override public boolean isReady() { return true; }
                    @Override public boolean isHealthy() { return true; }
                    @Override public long appliedSequence() { return 0L; }
                    @Override public void close() { }
                },
                () -> true);
    }

    private static ReplicationPositionProvider positionProvider() {
        final UUID id = UUID.randomUUID();
        return new ReplicationPositionProvider() {
            @Override public void init() { }
            @Override public ReplicationPosition latest() {
                return new ReplicationPosition(id, id, 0L, 0L, 0L, 0L, 1L, id);
            }
            @Override public void close() { }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> T stub(final Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> {
                    if (method.getReturnType() == ReplicationPosition.class) {
                        final UUID id = UUID.randomUUID();
                        return new ReplicationPosition(id, id, 0L, 0L, 0L, 0L, 1L, id);
                    }
                    if (method.getReturnType() == ReplicationState.class) return ReplicationState.LIVE;
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return null;
                });
    }
}
