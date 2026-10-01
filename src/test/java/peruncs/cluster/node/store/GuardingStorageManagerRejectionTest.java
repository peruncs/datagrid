package peruncs.cluster.node.store;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.errors.*;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.TypeDictionaryOutbox;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Checks the fail-closed cause-chain rule for writer rejections.
class GuardingStorageManagerRejectionTest {
    @Test
    void facadeKeepsRecordedRejectionRetryableAndLatchesUncertainFailure(@TempDir final Path directory) {
        final AtomicReference<RuntimeException> targetFailure = new AtomicReference<>();
        final EmbeddedStorageFoundation<?> foundation = foundation(directory, targetFailure);
        final StorageGraphCoordinator graph = new StorageGraphCoordinator();
        try (EmbeddedStorageManager delegate = foundation.start()) {
            final ClusterStorageManager<Object> manager = TestManagers.guarding(
                    delegate, () -> false, openNode(), graph);

            targetFailure.set(new WriteRejectedException("capacity"));
            assertThrows(RuntimeException.class, () -> manager.store(new StorageWriteGatingTest.Payload("retry")));
            assertDoesNotThrow(() -> manager.store(new StorageWriteGatingTest.Payload("accepted after retry")));
            targetFailure.set(new WriteRejectedException("capacity"));
            assertThrows(RuntimeException.class, () -> manager.storeAll(
                    List.of(new StorageWriteGatingTest.Payload("void persistence path"))));
            assertDoesNotThrow(() -> manager.storeAll(
                    List.of(new StorageWriteGatingTest.Payload("void persistence retry"))));
            assertThrows(IllegalArgumentException.class, () -> manager.graphBoundary().write(() -> {
                throw new IllegalArgumentException("application owns invalidation");
            }));
            assertNull(graph.graphFailure(), "application write failures remain caller-managed");
            assertDoesNotThrow(() -> manager.graphBoundary().read(() -> manager.root()));

            targetFailure.set(new WriteRejectedException("corrupt", new CorruptReplicationDataException("bad frame")));
            assertThrows(RuntimeException.class, () -> manager.store(new StorageWriteGatingTest.Payload("uncertain")));
            assertThrows(GraphInvalidatedException.class, () -> manager.graphBoundary().read(() -> { }));
        }
    }

    @Test
    void recognizesWrappedCleanRejections() {
        assertTrue(GuardingStorageManager.isCleanRejection(
                new IllegalStateException("Store wrapper", new WriteRejectedException("capacity"))));
    }

    @Test
    void pendingLocalCommitKeepsTheGraphValid(@TempDir final Path directory) {
        final AtomicReference<RuntimeException> targetFailure = new AtomicReference<>();
        final StorageGraphCoordinator graph = new StorageGraphCoordinator();
        try (EmbeddedStorageManager delegate = foundation(directory, targetFailure).start()) {
            final ClusterStorageManager<Object> manager = TestManagers.guarding(
                    delegate, () -> false, openNode(), graph);
            targetFailure.set(new IllegalStateException("Store wrapper", new ReplicationPendingException(1L,
                    new ReplicationUnavailableException("commit offer timed out"))));

            assertThrows(IllegalStateException.class,
                    () -> manager.store(new StorageWriteGatingTest.Payload("locally accepted")));
            assertNull(graph.graphFailure());
            assertDoesNotThrow(() -> manager.graphBoundary().read(() -> manager.root()));
            assertDoesNotThrow(() -> manager.store(new StorageWriteGatingTest.Payload("after recovery")));
        }
    }

    @Test
    void uncertainReplicationFailuresAndErrorsWinOverRejection() {
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException(
                "wrapped corruption", new CorruptReplicationDataException("bad frame"))));
        assertFalse(GuardingStorageManager.isCleanRejection(
                new WriteRejectedException("fatal", new OutOfMemoryError("oom"))));
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException(
                "transport unavailable", new ReplicationUnavailableException("offline"))));
    }

    @Test
    void recognizesWrappedPendingCommitsButRejectsUncertainChains() {
        final ReplicationPendingException pending = new ReplicationPendingException(1L,
                new ReplicationUnavailableException("commit offer timed out"));
        assertTrue(GuardingStorageManager.isPendingCommit(
                new IllegalStateException("Store wrapper", pending)));
        assertFalse(GuardingStorageManager.isPendingCommit(
                new ReplicationUnavailableException("unrelated", pending)));
    }

    @Test
    void boundsTheCauseWalkAndRejectsCycles() {
        final Throwable first = new Throwable();
        final Throwable second = new Throwable();
        first.initCause(second);
        second.initCause(first);
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException("cycle", first)));

        final Throwable pendingCause = new Throwable();
        final ReplicationPendingException pending = new ReplicationPendingException(1L, pendingCause);
        pendingCause.initCause(pending);
        assertFalse(GuardingStorageManager.isPendingCommit(pending));

        Throwable deep = new OutOfMemoryError("too deep");
        for (int index = 0; index < 17; index++) deep = new IllegalStateException("wrapper", deep);
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException("deep", deep)));
        assertFalse(GuardingStorageManager.isPendingCommit(new ReplicationPendingException(2L, deep)));
    }

    @Test
    void recordedAbortIsCleanButRetainsItsReplicationCause() {
        final var rejection = WriteRejectedException.afterRecordedAbort(
                "abort recorded", new ReplicationUnavailableException("publication unavailable"), 17L);

        assertTrue(GuardingStorageManager.isCleanRejection(rejection));
        assertEquals(17L, rejection.recordedAbortPosition());
        assertInstanceOf(ReplicationUnavailableException.class, rejection.getCause());
    }

    private static EmbeddedStorageFoundation<?> foundation(
            final Path directory, final AtomicReference<RuntimeException> targetFailure) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(directory))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        final EmbeddedStorageFoundation<?> foundation = EmbeddedStorage.Foundation(configuration);
        DistributedStorage.configureWriting(foundation, new TypeDictionaryOutbox(), delegate -> new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                final RuntimeException failure = targetFailure.getAndSet(null);
                if (GuardingStorageManager.isPendingCommit(failure)) {
                    delegate.write(data);
                    throw failure;
                }
                if (failure != null) throw failure;
                delegate.write(data);
            }

            @Override
            public boolean isWritable() {
                return delegate.isWritable();
            }

            @Override
            public void prepareTarget() {
                delegate.prepareTarget();
            }

            @Override
            public void closeTarget() {
                delegate.closeTarget();
            }
        });
        return foundation;
    }

    private static NodeClose openNode() {
        return new NodeClose() {
            @Override public boolean close() { return false; }
            @Override public void awaitAppIdle(final Duration timeout) { }
            @Override public void checkOpen() { }
        };
    }
}
