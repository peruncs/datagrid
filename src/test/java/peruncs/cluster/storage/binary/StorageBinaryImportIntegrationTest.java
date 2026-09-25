package peruncs.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.util.X;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import peruncs.cluster.node.store.DistributedStorage;
import peruncs.cluster.storage.StorageGraphCoordinator;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/// Proves the Store-level replacement contract used by at-least-once replay.
class StorageBinaryImportIntegrationTest {
    private static EmbeddedStorageManager startExisting(final Path path, final CapturingDistributor capture) {
        final var foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, capture, new ReplicatingTargetFactory(capture));
        return foundation.start();
    }

    private static EmbeddedStorageManager start(
            final Path path,
            final Root initialRoot,
            final CapturingDistributor capture
    ) {
        final var foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, capture, new ReplicatingTargetFactory(capture));
        return foundation.start(initialRoot);
    }

    private static EmbeddedStorageFoundation<?> foundation(final Path path) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path))
                .setChannelCountProvider(Storage.ChannelCountProvider(4))
                .createConfiguration();
        return EmbeddedStorage.Foundation(configuration);
    }

    private static List<ByteBuffer> copy(final List<ByteBuffer> source) {
        final List<ByteBuffer> copy = new ArrayList<>(source.size());
        for (final ByteBuffer value : source) {
            final ByteBuffer duplicate = value.duplicate();
            final ByteBuffer direct = ByteBuffer.allocateDirect(duplicate.remaining());
            direct.put(duplicate).flip();
            copy.add(direct);
        }
        return copy;
    }

    private static void delete(final Path path) throws Exception {
        if (Files.exists(path)) {
            try (var paths = Files.walk(path)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(value ->
                {
                    try {
                        Files.deleteIfExists(value);
                    } catch (final Exception ignored) {
                    }
                });
            }
        }
    }

    private static void copyDirectory(final Path source, final Path target) throws Exception {
        try (var paths = Files.walk(source)) {
            for (final Path path : paths.toList()) {
                final Path destination = target.resolve(source.relativize(path));
                if (Files.isDirectory(path)) Files.createDirectories(destination);
                else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination);
                }
            }
        }
    }

        /// Verifies imports the same binary transactions twice and survives restart.
    @Test
    void importsTheSameBinaryTransactionsTwiceAndSurvivesRestart() throws Exception {
        final Path root = Files.createTempDirectory("datagrid-store-import-");
        final Path sourcePath = root.resolve("source");
        final Path readerPath = root.resolve("reader");
        final CapturingDistributor capture = new CapturingDistributor();
        try {
            final Root initialRoot = new Root();
            initialRoot.values.add("initial");
            initialRoot.entries.add(new Entry("seed", "created"));
            final EmbeddedStorageManager writer = start(sourcePath, initialRoot, capture);
            writer.storeRoot();
            writer.shutdown();
            copyDirectory(sourcePath, readerPath);
            capture.transactions.clear();

            final EmbeddedStorageManager resumedWriter = startExisting(sourcePath, capture);
            final Root resumedRoot = resumedWriter.root();
            final Entry entry = resumedRoot.entries.getFirst();
            resumedWriter.store(resumedRoot.entries);
            entry.value = "updated";
            resumedWriter.store(entry);
            resumedRoot.entries.remove(entry);
            resumedWriter.store(resumedRoot.entries);
            resumedRoot.values.add("one");
            resumedWriter.store(resumedRoot.values);
            resumedRoot.values.add("two");
            resumedWriter.store(resumedRoot.values);
            for (int i = 0; i < 512; i++) {
                resumedRoot.values.add("channel-value-%s".formatted(i));
            }
            resumedWriter.store(resumedRoot.values);
            resumedWriter.shutdown();
            assertTrue(capture.transactions.size() >= 3, "writer must produce replayable Store transactions");
            assertTrue(capture.maximumChannelCount >= 4,
                    "the real Store fixture must capture data from all four channels");

            final EmbeddedStorageManager reader = foundation(readerPath).start();
            final StorageConnection connection = reader.createConnection();
            for (final List<ByteBuffer> transaction : capture.transactions) {
                connection.importData(X.Enum(copy(transaction)));
                connection.importData(X.Enum(copy(transaction)));
            }
            reader.shutdown();

            final EmbeddedStorageManager restarted = foundation(readerPath).start();
            final Root importedRoot = restarted.root();
            assertTrue(importedRoot.values.containsAll(List.of("one", "two")));
            assertTrue(importedRoot.entries.isEmpty(), "deleted Store objects must remain deleted after import");
            restarted.shutdown();
        } finally {
            delete(root);
        }
    }

        /// Regression guard for preserved Store transaction boundaries inside
    /// one coalesced graph/index flush.
    @Test
    void coalescedReplayImportsQueuedTransactionsAsOneBatch() throws Exception {
        final Path root = Files.createTempDirectory("datagrid-store-batch-import-");
        final Path sourcePath = root.resolve("source");
        final Path readerPath = root.resolve("reader");
        final CapturingDistributor capture = new CapturingDistributor();
        try {
            final Root initialRoot = new Root();
            initialRoot.values.add("initial");
            final EmbeddedStorageManager writer = start(sourcePath, initialRoot, capture);
            writer.storeRoot();
            writer.shutdown();
            copyDirectory(sourcePath, readerPath);
            capture.transactions.clear();

            final EmbeddedStorageManager resumedWriter = startExisting(sourcePath, capture);
            final Root resumedRoot = resumedWriter.root();
            for (final String value : java.util.List.of("one", "two", "three")) {
                resumedRoot.values.add(value);
                resumedWriter.store(resumedRoot.values);
            }
            resumedWriter.shutdown();
            assertEquals(3, capture.transactions.size(), "fixture must capture three transactions");

            final EmbeddedStorageFoundation<?> readerFoundation = foundation(readerPath);
            final EmbeddedStorageManager reader = readerFoundation.start();
            final StorageConnection delegate = reader.createConnection();
            final long valuesObjectId = delegate.persistenceManager()
                    .lookupObjectId(((Root) reader.root()).values);
            assertTrue(valuesObjectId > 0L, "loaded values collection must have a persistent object id");
            assertTrue(delegate.persistenceManager().objectRegistry().containsLiveObject(valuesObjectId),
                    "loaded values collection must be live in the object registry");
            final java.util.Set<Long> importedObjectIds = new java.util.HashSet<>();
            final BinaryEntityRawDataIterator rawIterator = BinaryEntityRawDataIterator.New();
            for (final List<ByteBuffer> transaction : capture.transactions) {
                for (final ByteBuffer buffer : transaction) {
                    if (buffer.remaining() == 0) continue;
                    final long address = org.eclipse.serializer.memory.XMemory
                            .getDirectByteBufferAddress(buffer);
                    rawIterator.iterateEntityRawData(address, address + buffer.remaining(),
                            (entity, _) -> {
                                importedObjectIds.add(Binary.getEntityObjectIdRawValue(entity));
                                return true;
                            });
                }
            }
            assertTrue(importedObjectIds.contains(valuesObjectId),
                    () -> "fixture must carry the loaded values object id " + valuesObjectId
                            + "; imported=" + importedObjectIds);
            final java.util.concurrent.atomic.AtomicInteger importCalls = new java.util.concurrent.atomic.AtomicInteger();
            final StorageConnection counting = (StorageConnection) java.lang.reflect.Proxy.newProxyInstance(
                    StorageConnection.class.getClassLoader(), new Class<?>[]{StorageConnection.class},
                    (proxy, method, args) ->
                    {
                        if (method.getName().equals("importData")) importCalls.incrementAndGet();
                        return method.invoke(delegate, args);
                    });
            final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(
                    StorageBinaryDataMergerTestSupport.configuration(
                            readerFoundation.getConnectionFoundation(), counting,
                            ObjectGraphUpdateHandler.PerStore(coordinator),
                            0L, 1L << 30, 60_000L, coordinator));
            try {
                for (final List<ByteBuffer> transaction : capture.transactions) {
                    final ByteBuffer[] owned = copy(transaction).toArray(ByteBuffer[]::new);
                    for (final ByteBuffer buffer : owned) buffer.position(buffer.limit());
                    merger.receiveDataOwned(org.eclipse.serializer.persistence.binary.types.ChunksWrapper.New(owned));
                }
                merger.awaitApplied();
                assertEquals(3, importCalls.get(),
                        "each Store transaction must retain its import boundary");
                assertTrue(((Root) reader.root()).values.containsAll(java.util.List.of("one", "two", "three")),
                        () -> "live graph must reflect imported transactions; actual=" + ((Root) reader.root()).values);
            } finally {
                merger.dispose();
                reader.shutdown();
            }

            final EmbeddedStorageManager restarted = foundation(readerPath).start();
            try {
                final Root importedRoot = restarted.root();
                assertTrue(importedRoot.values.containsAll(java.util.List.of("one", "two", "three")),
                        () -> "the batched import must apply every transaction; actual=" + importedRoot.values);
                assertEquals(1, importedRoot.values.stream().filter("one"::equals).count(),
                        "each transaction must be applied exactly once");
            } finally {
                restarted.shutdown();
            }
        } finally {
            delete(root);
        }
    }

    private static final class CapturingDistributor implements ReplicationPublisher {
        private final List<List<ByteBuffer>> transactions = new ArrayList<>();
        private int maximumChannelCount;
        private int dictionariesSinceTransaction;

        @Override
        public synchronized void distributeData(final Binary data) {
            if (this.dictionariesSinceTransaction > 1) {
                throw new AssertionError("Serializer must coalesce type dictionary export per Store commit");
            }
            this.dictionariesSinceTransaction = 0;
            final List<ByteBuffer> copy = new ArrayList<>();
            final int[] channelCount = {0};
            data.iterateChannelChunks(chunk ->
            {
                channelCount[0]++;
                for (final ByteBuffer buffer : chunk.buffers()) {
                    copy.add(copy(List.of(buffer)).getFirst());
                }
            });
            this.maximumChannelCount = Math.max(this.maximumChannelCount, channelCount[0]);
            this.transactions.add(copy);
        }

        @Override
        public synchronized void distributeTypeDictionary(final String ignored) {
            this.dictionariesSinceTransaction++;
        }

        @Override
        public void dispose() {
        }
    }

    public static final class Root {
        public final List<String> values = new ArrayList<>();
        public final List<Entry> entries = new ArrayList<>();
    }

    public static final class Entry {
        public String id;
        public String value;

        public Entry() {
        }

        Entry(final String id, final String value) {
            this.id = id;
            this.value = value;
        }
    }

}
