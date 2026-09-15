package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.storage.types.DistributedStorage;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

/// Forked real-Store writer used to prove channel routing across process restart.
public final class AeronStoreProcessChildMain {
    private AeronStoreProcessChildMain() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 1) throw new IllegalArgumentException("mode is required");
        final String mode = arguments[0];
        if (!mode.equals("initial") && !mode.equals("restart") && !mode.equals("dictionary"))
            throw new IllegalArgumentException("unknown Store process mode: %s".formatted(mode));
        final Path root = Path.of(System.getProperty("dg.aeron.store.root"));
        final UUID clusterId = UUID.fromString(System.getProperty("dg.aeron.store.cluster"));
        final UUID nodeId = UUID.fromString(System.getProperty("dg.aeron.store.node"));
        final UUID generation = UUID.fromString(System.getProperty("dg.aeron.store.generation"));
        final Path storePath = root.resolve("store");
        Files.createDirectories(root.resolve("control"));
        final AtomicBoolean rejectNext = new AtomicBoolean(mode.equals("dictionary"));
        final AtomicBoolean sawFourChannels = new AtomicBoolean();
        try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties(root, clusterId, nodeId, generation))) {
            final AtomicInteger dictionaryChunks = new AtomicInteger();
            transport.positionProvider("store").init();
            /* Count dictionary chunks at the publication seam, rather than counting
             * exporter notifications.  A rejected ARCHIVE_FIRST transaction must
             * publish its retained dictionary again even though the Store exporter
             * quite correctly reports that type only once. */
            AeronCrashHooks.callWithHook((name, ignored) ->
            {
                if ("AFTER_DICTIONARY_CHUNKS".equals(name)) dictionaryChunks.incrementAndGet();
            }, () -> {
                final StorageBinaryDataDistributor distributor = transport.distributor("store", false);
                final UnaryOperator<PersistenceTarget<Binary>> targetFactory = delegate ->
                        transport.persistenceTargetFactory("store", distributor).apply(new PersistenceTarget<>() {
                            @Override
                            public void write(final Binary data) {
                                final int[] channels = {0};
                                data.iterateChannelChunks(ignored -> channels[0]++);
                                if (channels[0] >= 4) sawFourChannels.set(true);
                                if (rejectNext.compareAndSet(true, false))
                                    throw new IllegalStateException("injected real-Store rejection");
                                delegate.write(data);
                            }

                            @Override
                            public boolean isWritable() {
                                return delegate.isWritable();
                            }
                        });
                final EmbeddedStorageFoundation<?> foundation = foundation(storePath);
                DistributedStorage.configureWriting(foundation, distributor, targetFactory);
                final EmbeddedStorageManager manager;
                if (mode.equals("initial")) {
                    final Root value = new Root();
                    for (int i = 0; i < 2_048; i++) value.objects.add(new StoreType("object-%s".formatted(i)));
                    manager = foundation.start(value);
                    try {
                        manager.storeRoot();
                        for (final StoreType object : value.objects) object.value += "-updated";
                        manager.storeAll(value.objects);
                    } finally {
                        manager.shutdown();
                    }
                } else {
                    manager = foundation.start();
                    try {
                        final Root value = manager.root();
                        if (mode.equals("restart")) {
                            value.objects.getFirst().value += "-restart";
                            manager.store(value.objects.getFirst());
                        } else {
                            /* Retry a genuinely new entity type.  Reusing StoreType would only
                             * exercise data retry; it would not prove that a dictionary emitted by
                             * the rejected transaction is retained for the next commit. */
                            value.retryObjects.add(new RetryType());
                            try {
                                manager.store(value.retryObjects);
                            } catch (final RuntimeException expected) {
                                manager.store(value.retryObjects);
                            }
                        }
                    } finally {
                        manager.shutdown();
                    }
                }
                final long sequence = transport.positionProvider("store").latestSequence();
                Files.writeString(root.resolve("control").resolve(mode),
                        "channels=%s;dictionaries=%s;sequence=%s".formatted(sawFourChannels.get(), dictionaryChunks.get(), sequence));
                return null;
            });
        }
    }

    private static EmbeddedStorageFoundation<?> foundation(final Path path) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path))
                .setChannelCountProvider(Storage.ChannelCountProvider(4))
                .createConfiguration();
        return EmbeddedStorage.Foundation(configuration);
    }

    private static NodeLibraryPropertiesProvider properties(
            final Path root, final UUID clusterId, final UUID nodeId, final UUID generation) {
        return new NodeLibraryPropertiesProvider.Env() {
            @Override
            public String replicationRole() {
                return "writer";
            }

            @Override
            public boolean replicationRoleConfigured() {
                return true;
            }

            @Override
            public String replicationProperty(final String name) {
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> clusterId.toString();
                    case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> nodeId.toString();
                    case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
                    case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
                    case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> root.resolve("checkpoint/writer.checkpoint").toString();
                    default -> null;
                };
            }
        };
    }

    public static final class Root {
        public final List<StoreType> objects = new ArrayList<>();
        public final List<RetryType> retryObjects = new ArrayList<>();
    }

    public static final class StoreType {
        public String value;

        public StoreType() {
        }

        StoreType(final String value) {
            this.value = value;
        }
    }

        /// Entity introduced only by the rejection/retry phase of the process fixture.
    public static final class RetryType {
        public final String value;

        public RetryType() {
            this.value = "dictionary-retry";
        }
    }

}
