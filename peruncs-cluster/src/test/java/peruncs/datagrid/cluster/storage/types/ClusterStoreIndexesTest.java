package peruncs.datagrid.cluster.storage.types;

import org.apache.lucene.document.Document;
import org.eclipse.store.gigamap.jvector.*;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.gigamap.types.IndexCategory;
import org.eclipse.store.gigamap.types.IndexGroup;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Checks the cluster index boundary and the Store lifecycle it protects.
///
/// The test deliberately exercises mutations, Store reload, text search,
/// vector search, and vectorizer reuse. A directory configuration is not a
/// supported fallback, so both index families must fail before registration
/// can create one.
@SuppressWarnings("unchecked")
class ClusterStoreIndexesTest {
    private static final AtomicInteger VECTORIZE_CALLS = new AtomicInteger();
    @TempDir
    Path storagePath;

    private static VectorIndexConfiguration vectorConfiguration() {
        return VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .build();
    }

    @Test
    void externalDirectoriesAreRejected() {
        final LuceneContext<Article> externalLucene = LuceneContext.New(
                this.storagePath.resolve("lucene"),
                new ArticlePopulator()
        );
        assertThrows(IllegalArgumentException.class,
                () -> ClusterStoreIndexes.validateLuceneContext(externalLucene));

        final VectorIndexConfiguration externalVector = VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .onDisk(true)
                .indexDirectory(this.storagePath.resolve("vectors"))
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> ClusterStoreIndexes.validateVectorConfiguration(externalVector));
    }

    @Test
    void eventualIndexingIsRejectedForReplicatedVectors() {
        final VectorIndexConfiguration eventual = VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .eventualIndexing(true)
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> ClusterStoreIndexes.validateVectorConfiguration(eventual));
        final Root root = new Root();
        root.articles = GigaMap.New();
        assertThrows(IllegalArgumentException.class,
                () -> ClusterStoreIndexes.registerVector(
                        root.articles, "article-vectors", eventual, new ArticleVectorizer()));
        /* A mode smuggled past registration through the raw upstream add,
         * then persisted and reloaded, must still fail root validation: the
         * import refresh cannot retire a graph an in-flight background
         * worker may be using. */
        root.articles.index().register(VectorIndices.Category())
                .add("article-vectors", eventual, new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            storage.storeRoot();
        }
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(this.storagePath)) {
            final Root reloaded = storage.root();
            assertThrows(IllegalArgumentException.class,
                    () -> ClusterStoreIndexes.validateVectorIndexes(reloaded.articles));
        }
    }

    @Test
    void backgroundOptimizationIsRejectedForReplicatedVectors() {
        final VectorIndexConfiguration optimized = VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .optimizationIntervalMs(1_000L)
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> ClusterStoreIndexes.validateVectorConfiguration(optimized));
        final Root root = new Root();
        root.articles = GigaMap.New();
        assertThrows(IllegalArgumentException.class,
                () -> ClusterStoreIndexes.registerVector(
                        root.articles, "article-vectors", optimized, new ArticleVectorizer()));
        /* Same smuggled-persisted-reloaded path as eventual indexing: root
         * validation reads the stored configuration, so no background mode
         * can reach a reader unnoticed. */
        root.articles.index().register(VectorIndices.Category())
                .add("article-vectors", optimized, new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            storage.storeRoot();
        }
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(this.storagePath)) {
            final Root reloaded = storage.root();
            assertThrows(IllegalArgumentException.class,
                    () -> ClusterStoreIndexes.validateVectorIndexes(reloaded.articles));
        }
    }

    @Test
    void embeddedIndexesFollowAddUpdateDeleteAndReload() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        final LuceneIndex<Article> text = ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
        final VectorIndices<Article> vectors = root.articles.index().register(VectorIndices.Category());
        final VectorIndex<Article> vector = ClusterStoreIndexes.addVector(
                vectors, "article-vectors", vectorConfiguration(), new ArticleVectorizer()
        );

        final long removedId;
        final long retainedId;
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            retainedId = root.articles.add(new Article("Eclipse", "distributed storage", new float[]{1, 0, 0}));
            removedId = root.articles.add(new Article("Obsolete", "message transport", new float[]{0, 1, 0}));
            root.articles.update(retainedId, article ->
            {
                article.title = "Aeron";
                article.body = "fast transport";
                article.vector = new float[]{0.9f, 0.1f, 0};
            });
            root.articles.removeById(removedId);
            root.articles.store();

            assertEquals(1, text.query("body:fast").size());
            assertEquals(0, text.query("body:message").size());
            final VectorSearchResult<Article> result = vector.search(new float[]{1, 0, 0}, 10);
            assertEquals(1, result.size());
            assertEquals("Aeron", result.toList().getFirst().entity().title);
        }

        VECTORIZE_CALLS.set(0);
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(this.storagePath)) {
            final Root reloaded = storage.root();
            assertNotNull(reloaded);
            assertFalse(reloaded.articles.isEmpty());
            ClusterStoreIndexes.validateVectorIndexes(reloaded.articles);

            final LuceneIndex<Article> reloadedText = reloaded.articles.index().get(LuceneIndex.class);
            final VectorIndices<Article> reloadedVectors = reloaded.articles.index().get(VectorIndices.Category());
            assertEquals(1, reloadedText.query("title:Aeron").size());
            assertEquals(0, reloadedText.query("title:Obsolete").size());
            final VectorSearchResult<Article> result = reloadedVectors.get("article-vectors")
                    .search(new float[]{1, 0, 0}, 10);
            assertEquals(1, result.size());
            assertEquals("Aeron", result.toList().getFirst().entity().title);
            assertEquals(0, VECTORIZE_CALLS.get(), "computed vectors must be loaded from Store state");
        }
    }

    @Test
    void refreshResetsGraphsWithoutEagerVectorization() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
        ClusterStoreIndexes.registerVector(root.articles, "article-vectors", vectorConfiguration(), new ArticleVectorizer());
        final int entities = 1_000;
        final List<Long> ids = new ArrayList<>(entities);
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            for (int index = 0; index < entities; index++) {
                ids.add(root.articles.add(new Article(
                        "doc" + index, "body-" + index, new float[]{index + 1.0f, 1.0f, 0.0f})));
            }
            storage.storeRoot();
            final StorageConnection connection = storage.createConnection();
            /* One imported batch touching three entities out of a thousand:
             * an update, an add, and a remove. The refresh resets the vector
             * graphs to their just-loaded state instead of touching entities:
             * no vectorize call happens at refresh time, and the next search
             * rebuilds the graph from the already-current vector store — the
             * same lazy rebuild every restart performs. */
            root.articles.update(ids.get(7), article ->
            {
                article.body = "updated-body-7";
                article.vector = new float[]{-1.0f, 0.0f, 0.0f};
            });
            root.articles.add(new Article("docnew", "body-new", new float[]{0.0f, -1.0f, 0.0f}));
            root.articles.removeById(ids.get(13));
            /* Cross the commit boundary first: uncommitted writer-buffered
             * documents are near-real-time-visible only, and a reader-side
             * refresh must close the writer rather than roll it back —
             * rollback deletes the replicated commit point — exactly as the
             * replicated Store arrives committed. */
            root.articles.store();
            VECTORIZE_CALLS.set(0);
            ClusterStoreIndexes.refreshImportedIndexes(connection);
            assertEquals(0, VECTORIZE_CALLS.get(), "refresh must reset graphs without vectorizing entities");
            final VectorIndices<Article> vectors = root.articles.index().get(VectorIndices.Category());
            assertEquals("doc7", vectors.get("article-vectors")
                    .search(new float[]{-1.0f, 0.0f, 0.0f}, 1).toList().getFirst().entity().title);
            assertEquals("docnew", vectors.get("article-vectors")
                    .search(new float[]{0.0f, -1.0f, 0.0f}, 1).toList().getFirst().entity().title);
            final VectorSearchResult<Article> nearRemoved = vectors.get("article-vectors")
                    .search(new float[]{14.0f, 1.0f, 0.0f}, 5);
            assertTrue(nearRemoved.toList().stream().noneMatch(hit -> hit.entity().title.equals("doc13")),
                    "removed entity still indexed");
            final LuceneIndex<Article> text = root.articles.index().get(LuceneIndex.class);
            assertEquals(1, text.query("title:doc7").size());
            assertEquals(0, text.query("title:doc13").size());
        }
    }

    @Test
    void repeatedRefreshRetiresAndReopensLuceneViews() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
        ClusterStoreIndexes.registerVector(root.articles, "article-vectors", vectorConfiguration(), new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            final long kept = root.articles.add(new Article("kept", "steady body", new float[]{1, 0, 0}));
            storage.storeRoot();
            final StorageConnection connection = storage.createConnection();
            final LuceneIndex<Article> text = root.articles.index().get(LuceneIndex.class);
            /* Twenty-five import cycles, each opening the Lucene view with a
             * query and retiring it with a refresh — plus a second refresh
             * over already-retired (nulled) handles, which must succeed
             * without double-close failures. Every cycle reopens over the
             * current files with identical results: retired handles release
             * their resources instead of accumulating, and no refresh ever
             * commits against the replicated files. */
            for (int cycle = 0; cycle < 25; cycle++) {
                final int round = cycle;
                assertEquals(1, text.query("title:kept").size(), "cycle " + cycle);
                root.articles.update(kept, article -> article.body = "steady body " + round);
                root.articles.store();
                ClusterStoreIndexes.refreshImportedIndexes(connection);
                ClusterStoreIndexes.refreshImportedIndexes(connection);
                assertEquals(1, text.query("title:kept").size(), "reopen after cycle " + cycle);
                assertEquals(1, text.query("body:steady").size(), "reopen after cycle " + cycle);
            }
            final VectorIndices<Article> vectors = root.articles.index().get(VectorIndices.Category());
            assertEquals(1, vectors.get("article-vectors").search(new float[]{1, 0, 0}, 1).size());
        }
    }

    @Test
    void directExternalLuceneRegistrationIsRejectedByGraphValidation() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        root.articles.index().register(LuceneIndex.Category(LuceneContext.New(
                this.storagePath.resolve("external-lucene"), new ArticlePopulator())));

        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(root));
    }

    @Test
    void directExternalVectorRegistrationIsRejectedByGraphValidation() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        final VectorIndices<Article> indices = root.articles.index().register(VectorIndices.Category());
        indices.add("external-vectors", VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .onDisk(true)
                .indexDirectory(this.storagePath.resolve("external-vectors"))
                .build(), new ArticleVectorizer());

        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(root));
    }

    @Test
    void unregisteredExternalConfigurationsAreRejectedByGraphValidation() {
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(
                LuceneContext.New(this.storagePath.resolve("stray-lucene"), new ArticlePopulator())));
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(
                VectorIndexConfiguration.builder()
                        .dimension(3)
                        .similarityFunction(VectorSimilarityFunction.COSINE)
                        .onDisk(true)
                        .indexDirectory(this.storagePath.resolve("stray-vectors"))
                        .build()));
    }

    @Test
    void externalConfigurationBehindAtomicReferenceIsRejected() {
        final AtomicReference<Object> reference = new AtomicReference<>(
                LuceneContext.New(this.storagePath.resolve("wrapped-lucene"), new ArticlePopulator()));
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(reference));
    }

    @Test
    void opaqueJdkHolderIsRejectedInsteadOfBeingPruned() {
        final AtomicMarkableReference<Object> holder = new AtomicMarkableReference<>(
                LuceneContext.New(this.storagePath.resolve("opaque-jdk-lucene"), new ArticlePopulator()), false);
        assertThrows(IllegalStateException.class, () -> ClusterStoreIndexes.validateGraph(holder));
    }

    @Test
    void externalConfigurationBehindReferenceIsInspected() {
        final java.lang.ref.WeakReference<Object> reference = new java.lang.ref.WeakReference<>(
                LuceneContext.New(this.storagePath.resolve("ref-lucene"), new ArticlePopulator()));
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(reference),
                "a Reference wrapper must not hide an external index from validation");
    }

    @Test
    void externalConfigurationBehindSoftReferenceIsInspected() {
        final java.lang.ref.SoftReference<Object> reference = new java.lang.ref.SoftReference<>(
                VectorIndexConfiguration.builder()
                        .dimension(3)
                        .similarityFunction(VectorSimilarityFunction.COSINE)
                        .onDisk(true)
                        .indexDirectory(this.storagePath.resolve("ref-vectors"))
                        .build());
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(reference),
                "a SoftReference wrapper must not hide an external index from validation");
    }

    @Test
    void indexGroupsBehindReferenceAreInspectedThroughIndexGroup() {
        final GigaMap<Article> map = GigaMap.New();
        final LuceneIndex<Article> text = map.index().register(LuceneIndex.Category(LuceneContext.New(
                this.storagePath.resolve("ref-group-lucene"), new ArticlePopulator())));
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(
                new java.lang.ref.WeakReference<>(text)),
                "an IndexGroup referent must stay visible after simplifying the Reference check");
        final VectorIndices<Article> vectors = map.index().register(VectorIndices.Category());
        vectors.add("ref-group-vectors", VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .onDisk(true)
                .indexDirectory(this.storagePath.resolve("ref-group-vectors"))
                .build(), new ArticleVectorizer());
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(
                new java.lang.ref.WeakReference<>(vectors)),
                "an IndexGroup referent must stay visible after simplifying the Reference check");
    }


    @Test
    void embeddedInGraphIndexesPassGraphValidation() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
        ClusterStoreIndexes.registerVector(root.articles, "article-vectors", vectorConfiguration(), new ArticleVectorizer());

        assertDoesNotThrow(() -> ClusterStoreIndexes.validateGraph(root));
        assertDoesNotThrow(() -> ClusterStoreIndexes.validateMap(root.articles));
    }

    @Test
    void materializedStorageRootsPassGraphValidation() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
        ClusterStoreIndexes.registerVector(root.articles, "article-vectors", vectorConfiguration(), new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            root.articles.add(new Article("Eclipse", "distributed storage", new float[]{1, 0, 0}));
            storage.storeRoot();
            assertDoesNotThrow(() -> ClusterStoreIndexes.validateStorageRoots(storage.createConnection()));
        }
    }

    @Test
    void duplicateLuceneRegistrationFailsExplicitly() {
        final GigaMap<Article> map = GigaMap.New();
        ClusterStoreIndexes.registerLucene(map, new ArticlePopulator());
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> ClusterStoreIndexes.registerLucene(map, new ArticlePopulator()));
        assertTrue(failure.getMessage().contains("already has a Lucene index"),
                "a duplicate must be named as a duplicate: " + failure.getMessage());
    }

    @Test
    void concurrentDuplicateLuceneRegistrationLeavesASingleIndex() throws Exception {
        final GigaMap<Article> map = GigaMap.New();
        final int attempts = 8;
        final AtomicInteger successes = new AtomicInteger();
        final AtomicInteger duplicates = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < attempts; i++) {
                futures.add(executor.submit(() ->
                {
                    try {
                        ClusterStoreIndexes.registerLucene(map, new ArticlePopulator());
                        successes.incrementAndGet();
                    } catch (final IllegalStateException expected) {
                        duplicates.incrementAndGet();
                    }
                }));
            }
            for (final Future<?> future : futures) future.get(1, TimeUnit.MINUTES);
        }

        assertEquals(1, successes.get(), "exactly one duplicate registration must win");
        assertEquals(attempts - 1, duplicates.get());
        assertNotNull(map.index().get(LuceneIndex.class));
    }

    @Test
    void bitmapOnlyMapPassesMapValidation() {
        /* The core bitmap group is in-graph by construction: enumerating it
         * must accept the map instead of rejecting an "unknown" category. */
        assertDoesNotThrow(() -> ClusterStoreIndexes.validateMap(GigaMap.New()));
    }

    @Test
    @SuppressWarnings("unchecked") // The proxy stands in for an unsupported third-party index group.
    void unknownIndexCategoryIsRejectedByMapValidation() {
        final GigaMap<Article> map = GigaMap.New();
        map.index().register(new IndexCategory<>() {
            @Override
            public Class<? extends IndexGroup<Article>> indexType() {
                return (Class<? extends IndexGroup<Article>>) (Class<?>) CustomGroup.class;
            }

            @Override
            public IndexGroup.Internal<Article> createIndexGroup(final GigaMap<Article> gigaMap) {
                return (IndexGroup.Internal<Article>) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[]{CustomGroup.class},
                        (proxy, method, args) ->
                        {
                            if (method.getDeclaringClass() == Object.class) {
                                return switch (method.getName()) {
                                    case "toString" -> "CustomGroup(?)";
                                    case "hashCode" -> System.identityHashCode(proxy);
                                    default -> proxy == args[0];
                                };
                            }
                            final Class<?> type = method.getReturnType();
                            if (type == boolean.class) return false;
                            if (type == int.class) return 0;
                            if (type == long.class) return 0L;
                            return null;
                        });
            }
        });

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusterStoreIndexes.validateMap(map));
        assertTrue(failure.getMessage().contains("unsupported index group"),
                "an unknown category must fail closed: " + failure.getMessage());
    }

    @Test
    void writerEntryRejectsDirectExternalRegistrations() {
        final Root luceneRoot = new Root();
        luceneRoot.articles = GigaMap.New();
        luceneRoot.articles.index().register(LuceneIndex.Category(LuceneContext.New(
                this.storagePath.resolve("writer-external-lucene"), new ArticlePopulator())));
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(luceneRoot, this.storagePath.resolve("lucene"))) {
            assertThrows(IllegalArgumentException.class,
                    () -> ClusterStoreIndexes.validateForPublication(storage.createConnection()));
        }

        final Root vectorRoot = new Root();
        vectorRoot.articles = GigaMap.New();
        final VectorIndices<Article> indices = vectorRoot.articles.index().register(VectorIndices.Category());
        indices.add("writer-external-vectors", VectorIndexConfiguration.builder()
                .dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .onDisk(true)
                .indexDirectory(this.storagePath.resolve("writer-external-vectors"))
                .build(), new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(vectorRoot, this.storagePath.resolve("vectors"))) {
            assertThrows(IllegalArgumentException.class,
                    () -> ClusterStoreIndexes.validateForPublication(storage.createConnection()));
        }
    }

    @Test
    void largeOrdinaryGraphPassesValidation() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        root.catalog = new ArrayList<>();
        for (int index = 0; index < 5_000; index++) {
            root.catalog.add(new CatalogEntry("title-" + index, index));
        }

        /* 5,000 realistic entities far exceed the object bound, but none can
         * reach index metadata: `java.time` fields are provably index-free,
         * so the prune must skip the entries and their date values without
         * failing — roughly 10,000 objects under the old rule. */
        assertDoesNotThrow(() -> ClusterStoreIndexes.validateGraph(root));
        assertDoesNotThrow(() -> ClusterStoreIndexes.validateMap(root.articles));
    }

    @Test
    void externalIndexHiddenInLargeGraphIsStillRejected() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        root.articles.index().register(LuceneIndex.Category(LuceneContext.New(
                this.storagePath.resolve("hidden-external-lucene"), new ArticlePopulator())));
        root.catalog = new ArrayList<>();
        for (int index = 0; index < 5_000; index++) {
            root.catalog.add(new CatalogEntry("title-" + index, index));
        }

        /* Pruning ordinary entities must not hide the directly registered
         * external index among them. */
        assertThrows(IllegalArgumentException.class, () -> ClusterStoreIndexes.validateGraph(root));
    }

    @Test
    void writerEntryScopesValidationToTheStoreBeingWritten() {
        /* Store A holds a directly registered external Lucene index. */
        final Root violating = new Root();
        violating.articles = GigaMap.New();
        violating.articles.index().register(LuceneIndex.Category(LuceneContext.New(
                this.storagePath.resolve("other-store-external-lucene"), new ArticlePopulator())));
        try (EmbeddedStorageManager other = EmbeddedStorage.start(violating, this.storagePath.resolve("other"))) {
            other.storeRoot();
            /* Store B is clean: validation of B must pass even though the
             * violating map of Store A is still live in this JVM. */
            final Root clean = new Root();
            clean.articles = GigaMap.New();
            ClusterStoreIndexes.registerLucene(clean.articles, new ArticlePopulator());
            try (EmbeddedStorageManager mine = EmbeddedStorage.start(clean, this.storagePath.resolve("mine"))) {
                mine.storeRoot();
                assertDoesNotThrow(() -> ClusterStoreIndexes.validateForPublication(mine.createConnection()),
                        "another Store's violating map must not block this writer");
                assertThrows(IllegalArgumentException.class,
                        () -> ClusterStoreIndexes.validateForPublication(other.createConnection()),
                        "the violating Store must still fail its own validation");
            }
        }
    }

    @Test
    void writerEntryAcceptsLargeOrdinaryGraph() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
        root.catalog = new ArrayList<>();
        for (int index = 0; index < 5_000; index++) {
            root.catalog.add(new CatalogEntry("title-" + index, index));
        }
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            storage.storeRoot();
            assertDoesNotThrow(() -> ClusterStoreIndexes.validateForPublication(storage.createConnection()),
                    "the writer entry must not walk the application's data set per transaction");
        }
    }

    @Test
    void writerEntryAcceptsEmbeddedIndexes() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
        ClusterStoreIndexes.registerVector(root.articles, "article-vectors", vectorConfiguration(), new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            root.articles.add(new Article("Eclipse", "distributed storage", new float[]{1, 0, 0}));
            storage.storeRoot();
            assertDoesNotThrow(() -> ClusterStoreIndexes.validateForPublication(storage.createConnection()));
        }
    }

    private interface CustomGroup<E> extends IndexGroup.Internal<E> {
    }

    @Test
    void concurrentVectorRegistrationsAllSurvive() throws Exception {
        final GigaMap<Article> map = GigaMap.New();
        final int registrations = 8;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<VectorIndex<Article>>> futures = new ArrayList<>();
            for (int i = 0; i < registrations; i++) {
                final String name = "vectors-" + i;
                futures.add(executor.submit(() -> ClusterStoreIndexes.registerVector(
                        map, name, vectorConfiguration(), new ArticleVectorizer())));
            }
            for (final Future<VectorIndex<Article>> future : futures) {
                assertNotNull(future.get(1, TimeUnit.MINUTES));
            }
        }

        final VectorIndices<Article> indices = map.index().get(VectorIndices.Category());
        assertNotNull(indices, "the index group must exist after registration");
        for (int i = 0; i < registrations; i++) {
            assertNotNull(indices.get("vectors-" + i), "concurrent registration lost vectors-" + i);
        }
    }

    @Test
    void concurrentDuplicateVectorRegistrationLeavesASingleIndex() throws Exception {
        final GigaMap<Article> map = GigaMap.New();
        final int attempts = 8;
        final AtomicInteger successes = new AtomicInteger();
        final AtomicInteger duplicates = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < attempts; i++) {
                futures.add(executor.submit(() ->
                {
                    try {
                        ClusterStoreIndexes.registerVector(
                                map, "vectors", vectorConfiguration(), new ArticleVectorizer());
                        successes.incrementAndGet();
                    } catch (final IllegalStateException expected) {
                        duplicates.incrementAndGet();
                    }
                }));
            }
            for (final Future<?> future : futures) future.get(1, TimeUnit.MINUTES);
        }

        assertEquals(1, successes.get(), "exactly one duplicate registration must win");
        assertEquals(attempts - 1, duplicates.get());
        assertNotNull(map.index().get(VectorIndices.Category()).get("vectors"));
    }

    private static final class Article {
        String title;
        String body;
        float[] vector;

        Article(final String title, final String body, final float[] vector) {
            this.title = title;
            this.body = body;
            this.vector = vector;
        }
    }

    private static final class Root {
        GigaMap<Article> articles;
        List<CatalogEntry> catalog;
    }

    private static final class CatalogEntry {
        final String title;
        final long ordinal;
        final LocalDate created;

        CatalogEntry(final String title, final long ordinal) {
            this.title = title;
            this.ordinal = ordinal;
            this.created = LocalDate.of(2026, 1, 1).plusDays(ordinal);
        }
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
            VECTORIZE_CALLS.incrementAndGet();
            return article.vector;
        }
    }
}
