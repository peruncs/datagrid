package peruncs.datagrid.cluster.storage.aeron.writer;

import org.apache.lucene.document.Document;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceRootsView;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.VectorSimilarityFunction;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.index.ClusterStoreIndexes;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/// Proves writer-side index enforcement fails before publication.
///
/// A Lucene or vector index registered directly — bypassing the cluster
/// registration paths — must fail the writer target's startup check and its
/// commit path before the diverging transaction reaches the local target or
/// the Aeron publication. The file lives with the storage-types tests because
/// it covers the writer hook owned by those contracts.
class WriterIndexValidationTest {
    @Test
    void directExternalLuceneRegistrationFailsBeforePublication(@TempDir final Path directory) {
        final Root root = new Root();
        root.articles = GigaMap.New();
        root.articles.index().register(LuceneIndex.Category(LuceneContext.New(
                directory.resolve("external-lucene"), new ArticlePopulator())));

        try (Probe probe = probe(rootsViewConnection(root))) {
            assertThrows(IllegalArgumentException.class, probe.target::validateWriterState,
                    "the startup check must reject a direct external registration");
            assertTrue(probe.hookRan.get(), "the startup check must run the validation hook");

            probe.hookRan.set(false);
            assertThrows(IllegalArgumentException.class,
                    () -> probe.target.write(binary()),
                    "the commit path must reject a direct external registration");
            assertTrue(probe.hookRan.get(), "the commit path must run the validation hook");
            assertTrue(probe.localWrites.isEmpty(),
                    "a rejected transaction must not reach the local target: " + probe.localWrites);
            assertTrue(probe.publications.isEmpty(),
                    "a rejected transaction must not reach publication: " + probe.publications);
            assertEquals(-1L, probe.committed.get(),
                    "a rejected transaction must not report a committed sequence");
        }
    }

    @Test
    void directExternalVectorRegistrationFailsBeforePublication(@TempDir final Path directory) {
        final Root root = new Root();
        root.articles = GigaMap.New();
        final VectorIndices<Article> indices = root.articles.index().register(VectorIndices.Category());
        indices.add("external-vectors", VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .onDisk(true)
                .indexDirectory(directory.resolve("external-vectors"))
                .build(), new ArticleVectorizer());

        try (Probe probe = probe(rootsViewConnection(root))) {
            assertThrows(IllegalArgumentException.class, probe.target::validateWriterState,
                    "the startup check must reject a direct external registration");
            assertThrows(IllegalArgumentException.class, () -> probe.target.write(binary()),
                    "the commit path must reject a direct external registration");
            assertTrue(probe.localWrites.isEmpty(), "a rejected transaction must not reach the local target");
            assertTrue(probe.publications.isEmpty(), "a rejected transaction must not reach publication");
        }
    }

    @Test
    void embeddedIndexesPassWriterValidation(@TempDir final Path directory) {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());

        try (Probe probe = probe(rootsViewConnection(root))) {
            assertDoesNotThrow(probe.target::validateWriterState,
                    "embedded indexes must pass the startup check");
            assertDoesNotThrow(() -> probe.target.write(binary()),
                    "embedded indexes must pass the commit path");
            assertTrue(probe.hookRan.get(), "the commit path must run the validation hook");
            assertEquals(List.of("local"), probe.localWrites,
                    "the accepted transaction must reach the local target");
            assertEquals(0L, probe.committed.get(),
                    "the accepted transaction must report its committed sequence");
        }
    }

    @Test
    void unwiredTargetSkipsWriterValidation() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                .build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> {
        });
        try {
            final List<String> localWrites = new ArrayList<>();
            final AeronStorageBinaryReplicationTarget target = new AeronStorageBinaryReplicationTarget(
                    recordingTarget(localWrites), coordinator, null, ignored -> {
            }, () -> true);
            assertDoesNotThrow(target::validateWriterState,
                    "a target without a hook cannot validate: it sees only the Binary, never the Store");
            assertDoesNotThrow(() -> target.write(binary()));
            assertEquals(List.of("local"), localWrites);
        } finally {
            coordinator.dispose();
        }
    }

    private static Binary binary() {
        return ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{7}));
    }

    private static PersistenceTarget<Binary> recordingTarget(final List<String> localWrites) {
        return new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                localWrites.add("local");
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
    }

    private static final class Probe implements AutoCloseable {
        final List<String> localWrites = new ArrayList<>();
        final List<String> publications = new ArrayList<>();
        final AtomicLong committed = new AtomicLong(-1L);
        final AtomicBoolean hookRan = new AtomicBoolean();
        final AeronReplicationWriteCoordinator coordinator;
        final AeronStorageBinaryReplicationTarget target;

        Probe(final StorageConnection connection) {
            final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                    .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                    .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                    .build();
            final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                    (buffer, offset, length) ->
                    {
                        this.publications.add("archive");
                        return length;
                    }, configuration.maxMessageLength(), configuration,
                    UUID.randomUUID(), 1, 0);
            this.coordinator = new AeronReplicationWriteCoordinator(
                    publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> {
            });
            this.target = new AeronStorageBinaryReplicationTarget(
                    recordingTarget(this.localWrites),
                    this.coordinator,
                    null,
                    this.committed::set,
                    () -> true,
                    () ->
                    {
                        this.hookRan.set(true);
                        ClusterStoreIndexes.validateForPublication(connection);
                    });
        }

        @Override
        public void close() {
            this.coordinator.dispose();
        }
    }

    private static Probe probe(final StorageConnection connection) {
        return new Probe(connection);
    }

    private static StorageConnection rootsViewConnection(final Object root) {
        final PersistenceRootsView view = (PersistenceRootsView) Proxy.newProxyInstance(
                PersistenceRootsView.class.getClassLoader(),
                new Class<?>[]{PersistenceRootsView.class},
                (proxy, method, args) ->
                {
                    if (method.getName().equals("iterateEntries")) {
                        @SuppressWarnings("unchecked")
                        final BiConsumer<String, Object> entries = (BiConsumer<String, Object>) args[0];
                        entries.accept("root", root);
                        return entries;
                    }
                    return defaultValue(method.getReturnType());
                });
        final PersistenceManager<?> managers = (PersistenceManager<?>) Proxy.newProxyInstance(
                PersistenceManager.class.getClassLoader(),
                new Class<?>[]{PersistenceManager.class},
                (proxy, method, args) -> method.getName().equals("viewRoots") ? view
                        : defaultValue(method.getReturnType()));
        return (StorageConnection) Proxy.newProxyInstance(
                StorageConnection.class.getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) -> method.getName().equals("persistenceManager") ? managers
                        : defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(final Class<?> type) {
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0d;
        if (type == float.class) return 0f;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        if (type == char.class) return (char) 0;
        return null;
    }

    private static final class Article {
        final String title;
        final String body;
        final float[] vector;

        Article(final String title, final String body, final float[] vector) {
            this.title = title;
            this.body = body;
            this.vector = vector;
        }
    }

    private static final class Root {
        GigaMap<Article> articles;
    }

    private static final class ArticlePopulator extends DocumentPopulator<Article> {
        @Override
        public void populate(final Document document, final Article article) {
            document.add(createTextField("title", article.title));
            document.add(createTextField("body", article.body));
        }
    }

    private static final class ArticleVectorizer extends Vectorizer<Article> {
        @Override
        public float[] vectorize(final Article article) {
            return article.vector;
        }
    }
}
