package peruncs.cluster.node.store;

import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/// Verifies public Store writes persist their replication mark in the same transaction.
class ReplicationMarkPersistenceTest {
    private static final UUID CLUSTER_ID = UUID.randomUUID();
    private static final UUID GENERATION = UUID.randomUUID();

    @Test
    void managerPersistenceManagerAndStorerWritesAllCarryTheMark(@TempDir final Path storePath) {
        final ReplicationMark mark = new ReplicationMark(CLUSTER_ID, GENERATION, 1L, 17L);
        final EmbeddedStorageManager storage = foundation(storePath, mark).start(new Root());
        final ClusterStorageManager<Root> manager = TestManagers.guarding(
                storage, () -> false, close(storage), new StorageGraphCoordinator(),
                mark, current -> current.reserve(current.recordingId(), current.fencingToken(),
                        current.sequence() + 1L, current.prepareStartPosition()));
        try {
            manager.store(new Entity());
            manager.storeAll(List.of(new Entity(), new Entity()));
            manager.storeRoot();
            manager.persistenceManager().store(new Entity());
            final var storer = manager.createStorer();
            storer.store(new Entity());
            storer.commit();
            manager.createStorer().commit();
            assertEquals(5L, mark.sequence());
            final boolean[] visible = {false};
            manager.viewRoots().iterateEntries((identifier, ignored) ->
                    visible[0] |= ReplicationMark.ROOT_ID.equals(identifier));
            assertFalse(visible[0], "application root views must hide the transport mark");
        } finally {
            manager.shutdown();
        }

        final ReplicationMark loaded = new ReplicationMark(CLUSTER_ID, GENERATION, 1L, 17L);
        try (EmbeddedStorageManager reopened = foundation(storePath, loaded).start()) {
            assertEquals(5L, loaded.sequence());
        }
    }

    @Test
    void readOnlyFacadeHidesTheReservedReplicationRoot(@TempDir final Path storePath) {
        final ReplicationMark mark = new ReplicationMark(CLUSTER_ID, GENERATION, 1L, 17L);
        final EmbeddedStorageManager storage = foundation(storePath, mark).start(new Root());
        final ClusterStorageManager<Root> manager = TestManagers.readOnly(
                storage, close(storage), new StorageGraphCoordinator(), mark);
        try {
            final boolean[] visible = {false};
            manager.viewRoots().iterateEntries((identifier, ignored) ->
                    visible[0] |= ReplicationMark.ROOT_ID.equals(identifier));
            assertFalse(visible[0], "reader root views must hide the transport mark");
        } finally {
            manager.shutdown();
        }
    }

    private static EmbeddedStorageFoundation<?> foundation(final Path path, final ReplicationMark mark) {
        final EmbeddedStorageFoundation<?> foundation = EmbeddedStorage.Foundation(
                StorageConfiguration.Builder().setStorageFileProvider(Storage.FileProvider(path))
                        .createConfiguration());
        foundation.getConnectionFoundation().getRootResolverProvider()
                .registerRoot(ReplicationMark.ROOT_ID, mark);
        return foundation;
    }

    private static NodeClose close(final EmbeddedStorageManager storage) {
        return new NodeClose() {
            @Override
            public boolean close() {
                storage.shutdown();
                return true;
            }

            @Override
            public void awaitAppIdle(final Duration timeout) {
            }

            @Override
            public void checkOpen() {
            }
        };
    }

    public static final class Root {
    }

    public static final class Entity {
        public int value;
    }
}
