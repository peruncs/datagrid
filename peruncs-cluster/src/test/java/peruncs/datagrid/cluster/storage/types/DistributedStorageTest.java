package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            implements java.util.function.UnaryOperator<org.eclipse.serializer.persistence.types.PersistenceTarget<Binary>> {

        @Override
        public org.eclipse.serializer.persistence.types.PersistenceTarget<Binary> apply(
                final org.eclipse.serializer.persistence.types.PersistenceTarget<Binary> delegate) {
            return new org.eclipse.serializer.persistence.types.PersistenceTarget<>() {
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

    private static final class CapturingDistributor implements StorageBinaryDataDistributor {
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
