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
import peruncs.cluster.errors.CorruptReplicationDataException;
import peruncs.cluster.errors.GraphInvalidatedException;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.ReplicationPublisher;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Checks the fail-closed cause-chain rule for writer rejections.
class GuardingStorageManagerRejectionTest {
    @Test
    void facadeKeepsRecordedRejectionRetryableAndLatchesUncertainFailure(@TempDir final Path directory) {
        final AtomicReference<RuntimeException> targetFailure = new AtomicReference<>();
        final EmbeddedStorageFoundation<?> foundation = foundation(directory, targetFailure);
        final StorageGraphCoordinator graph = new StorageGraphCoordinator();
        try (EmbeddedStorageManager delegate = foundation.start()) {
            final ClusterStorageManager<Object> manager = ClusterStorageManagers.guarding(
                    delegate, StorageSizeValidation.notReached(), openNode(), graph);

            targetFailure.set(new WriteRejectedException("capacity"));
            assertThrows(RuntimeException.class, () -> manager.store(new StorageWriteGatingTest.Payload("retry")));
            assertDoesNotThrow(() -> manager.store(new StorageWriteGatingTest.Payload("accepted after retry")));
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
    void uncertainReplicationFailuresAndErrorsWinOverRejection() {
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException(
                "wrapped corruption", new CorruptReplicationDataException("bad frame"))));
        assertFalse(GuardingStorageManager.isCleanRejection(
                new WriteRejectedException("fatal", new OutOfMemoryError("oom"))));
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException(
                "transport unavailable", new ReplicationUnavailableException("offline"))));
    }

    @Test
    void boundsTheCauseWalkAndRejectsCycles() {
        final Throwable first = new Throwable();
        final Throwable second = new Throwable();
        first.initCause(second);
        second.initCause(first);
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException("cycle", first)));

        Throwable deep = new OutOfMemoryError("too deep");
        for (int index = 0; index < 17; index++) deep = new IllegalStateException("wrapper", deep);
        assertFalse(GuardingStorageManager.isCleanRejection(new WriteRejectedException("deep", deep)));
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
        DistributedStorage.configureWriting(foundation, ReplicationPublisher.noOp(), delegate -> new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                final RuntimeException failure = targetFailure.getAndSet(null);
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
