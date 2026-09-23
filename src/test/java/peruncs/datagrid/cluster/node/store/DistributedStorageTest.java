package peruncs.datagrid.cluster.node.store;

import org.eclipse.serializer.functional.InstanceDispatcherLogic;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.storage.binary.ReplicationPublisher;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

/// Pins the writing-setup delegation: an explicit target factory wraps the
/// local target so committed binaries reach the distributor, and the factory
/// returns the same foundation for fluent setup.
class DistributedStorageTest {
    @TempDir
    Path storagePath;

    /// Verifies configuring writing with an explicit target factory returns the same foundation for fluent setup.
    @Test
    void explicitFactoryReturnsTheSameFoundation() {
        final CapturingDistributor distributor = new CapturingDistributor();
        final EmbeddedStorageFoundation<?> configured = foundation(this.storagePath.resolve("three"));
        assertSame(configured, DistributedStorage.configureWriting(
                configured, distributor, new ReplicatingTargetFactory(distributor)));
    }

    /// Verifies configureWriting replaces a dispatcher already installed on the
    /// connection foundation; the upstream facade exposes no usable accessor
    /// for the installed logic, so replacement is the documented contract.
    @Test
    void configureWritingReplacesPreviouslyInstalledDispatcher() {
        final CapturingDistributor distributor = new CapturingDistributor();
        final EmbeddedStorageFoundation<?> configured = foundation(this.storagePath.resolve("replaced"));
        final AtomicInteger previousCalls = new AtomicInteger();
        configured.getConnectionFoundation().setInstanceDispatcher(new InstanceDispatcherLogic() {
            @Override
            public <T> T apply(final T subject) {
                previousCalls.incrementAndGet();
                return subject;
            }
        });

        DistributedStorage.configureWriting(configured, distributor, new ReplicatingTargetFactory(distributor));

        final Root root = new Root();
        try (EmbeddedStorageManager storage = configured.start(root)) {
            root.values.add("replacement-probe");
            storage.store(root.values);
        }
        assertEquals(0, previousCalls.get(),
                "configureWriting replaces the previously installed dispatcher instead of chaining it");
        assertTrue(distributor.deliveries.get() > 0,
                "the installed configurator must distribute the committed transaction");
    }

        /// Pins the upstream bug that makes chaining impossible.
    ///
    /// `PersistenceFoundation.Default.getInstanceDispatcherLogic()` invokes
    /// itself (`aload_0; invokevirtual` on the same method), so reading the
    /// installed logic is a guaranteed `StackOverflowError`. Until upstream
        /// fixes the accessor, replacement is the only implementable contract;
    /// when this assertion starts failing, upstream has fixed it — restore
        /// chaining in `DistributedStorage.configureWriting` by passing the
        /// installed logic as the previous link.
    @Test
    void upstreamDispatcherLogicAccessorRemainsUnusable() {
        final EmbeddedStorageFoundation<?> configured = foundation(this.storagePath.resolve("upstream-accessor"));
        final StackOverflowError selfRecursion = assertThrows(StackOverflowError.class,
                () -> configured.getConnectionFoundation().getInstanceDispatcherLogic());
        /* The failure must be the self-recursive accessor, not a wrapper. */
        for (StackTraceElement element : selfRecursion.getStackTrace()) {
            if (element.getMethodName().equals("getInstanceDispatcherLogic")) {
                return;
            }
        }
        fail("getInstanceDispatcherLogic failed without self-recursion; upstream may have fixed it — restore chaining");
    }

    /// Verifies committing through the wrapped target delivers the transaction binaries to the distributor.
    @Test
    void committingThroughTheWrappedTargetDistributes() {
        final CapturingDistributor distributor = new CapturingDistributor();
        final EmbeddedStorageFoundation<?> configured = DistributedStorage.configureWriting(
                foundation(this.storagePath.resolve("deliveries")),
                distributor,
                new ReplicatingTargetFactory(distributor));
        final Root root = new Root();
        try (EmbeddedStorageManager storage = configured.start(root)) {
            root.values.add("delegation-probe");
            storage.store(root.values);
        }
        assertTrue(distributor.deliveries.get() > 0,
                "a committed transaction must reach the distributor");
    }

    /// The production pattern: wrap the local target and distribute committed
    /// binaries, mirroring what the Aeron transport's factory installs.
    private record ReplicatingTargetFactory(CapturingDistributor distributor)
            implements UnaryOperator<PersistenceTarget<Binary>> {

        @Override
        public PersistenceTarget<Binary> apply(final PersistenceTarget<Binary> delegate) {
            return new PersistenceTarget<>() {
                @Override
                public void write(final Binary data) {
                    data.iterateChannelChunks(Binary::mark);
                    try {
                        delegate.write(data);
                    } finally {
                        data.iterateChannelChunks(Binary::reset);
                    }
                    ReplicatingTargetFactory.this.distributor.distributeData(data);
                }

                @Override
                public boolean isWritable() {
                    return delegate.isWritable();
                }
            };
        }
    }

    private static EmbeddedStorageFoundation<?> foundation(final Path path) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        return EmbeddedStorage.Foundation(configuration);
    }

    public static final class Root {
        public final List<String> values = new ArrayList<>();
    }

    private static final class CapturingDistributor implements ReplicationPublisher {
        final AtomicInteger deliveries = new AtomicInteger();

        @Override
        public void distributeData(final Binary data) {
            this.deliveries.incrementAndGet();
        }

        @Override
        public void distributeTypeDictionary(final String ignored) {
        }

        @Override
        public void dispose() {
        }
    }
}
