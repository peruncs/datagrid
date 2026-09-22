package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.VectorSearchResult;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexRoot;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexedArticle;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.ReaderNode;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// Deterministic regression for live-reader index freshness.
///
/// A reader that applies a sustained stream of live transactions must keep its
/// Lucene and JVector indexes queryable without any restart: the graph, both
/// indexes, and a reopened store must agree. Volume is the trigger — a single
/// imported transaction stays fresh, so this test publishes a deterministic
/// stream of small transactions and checks every boundary without restarting.
class ReaderLiveIndexFreshnessTest {
    private static final int SEED_ARTICLES = 40;
    private static final int STREAM_TRANSACTIONS = 200;

    /// Verifies writer graph, Lucene, and JVector state stay queryable across a Store reopen.
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void writerIndexesSurviveStorageAndRestart() throws Exception {
        final Path root = Files.createTempDirectory("dg-index-boundary-");
        try {
            final IndexRoot initial = new IndexRoot();
            initial.articles = GigaMap.New();
            AeronStoreIntegrationIT.configureIndexes(initial.articles);
            final List<String> titles = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final String title = "boundary" + i;
                initial.articles.add(new IndexedArticle(title, "body" + i, new float[]{i + 1.0f, 1.0f, 0.0f}));
                titles.add(title);
            }
            try (EmbeddedStorageManager writer = AeronStoreIntegrationIT.foundation(root.resolve("writer")).start()) {
                writer.setRoot(initial);
                writer.storeRoot();
                assertGraphHas(initial, titles);
                assertLuceneHas(initial, titles);
                assertJVectorHas(initial);
            }
            try (EmbeddedStorageManager reopened = AeronStoreIntegrationIT.foundation(root.resolve("writer")).start()) {
                final IndexRoot imported = reopened.root();
                assertGraphHas(imported, titles);
                assertLuceneHas(imported, titles);
                assertJVectorHas(imported);
            }
        } finally {
            AeronStoreIntegrationIT.delete(root);
        }
    }

    /// Verifies live readers keep Lucene and JVector queryable across a sustained transaction stream without restart.
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void liveReaderIndexesStayFreshWithoutRestart() throws Exception {
        final Path root = Files.createTempDirectory("dg-index-freshness-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = AeronStoreIntegrationIT.freePort();
        final int livePort = AeronStoreIntegrationIT.freePort();
        final int watermarkPort = AeronStoreIntegrationIT.freePort();
        final Path writerStore = root.resolve("writer-store");
        final Path[] readerStores = {
                root.resolve("reader-1-store"), root.resolve("reader-2-store"), root.resolve("reader-3-store")};
        final Path[] readerNodes = {
                root.resolve("reader-1"), root.resolve("reader-2"), root.resolve("reader-3")};
        final Random random = new Random(7L);
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            writerTransport.positionProvider("store").init();
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final IndexRoot initial = new IndexRoot();
            initial.articles = GigaMap.New();
            AeronStoreIntegrationIT.configureIndexes(initial.articles);
            final List<Long> liveIds = new ArrayList<>();
            final List<String> liveTitles = new ArrayList<>();
            final Map<String, float[]> liveVectors = new HashMap<>();
            for (int i = 0; i < SEED_ARTICLES; i++) {
                final String title = "fresh" + i;
                final float[] vector = {random.nextFloat() + 0.5f, random.nextFloat() + 0.5f, random.nextFloat() + 0.5f};
                liveIds.add(initial.articles.add(new IndexedArticle(title, "seedbody" + i, vector)));
                liveTitles.add(title);
                liveVectors.put(title, vector);
            }
            final EmbeddedStorageManager seeded = AeronStoreIntegrationIT.startIndex(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            for (final Path readerStore : readerStores) AeronStoreIntegrationIT.copyDirectory(writerStore, readerStore);

            final EmbeddedStorageManager writer = AeronStoreIntegrationIT.startExistingIndex(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final IndexRoot writerRoot = writer.root();
            try {
                final ReplicationCursor baseline = AeronStoreIntegrationIT.latest(writerTransport);
                final ReaderNode[] readers = new ReaderNode[readerStores.length];
                for (int i = 0; i < readers.length; i++) {
                    readers[i] = ReaderNode.open(readerNodes[i], readerStores[i], "reader", UUID.randomUUID(),
                            clusterId, generation, baseline, controlPort, livePort, watermarkPort);
                    readers[i].start();
                    readers[i].awaitLive();
                }
                try {
                    /* Query continuously while the stream lands: without live
                     * queries the first search would open over the final state
                     * and could never catch a view frozen mid-stream. Queries
                     * join the coordinator read side like production query
                     * endpoints, so every completed read must observe a whole
                     * batch boundary: the merger holds one write section
                     * across materialization, validation, and index refresh.
                     * Any torn read — an exception or a graph/index mismatch
                     * — fails the test. */
                    final java.util.concurrent.atomic.AtomicBoolean streaming =
                            new java.util.concurrent.atomic.AtomicBoolean(true);
                    final java.util.concurrent.atomic.AtomicLong tornQueries =
                            new java.util.concurrent.atomic.AtomicLong();
                    final java.util.concurrent.atomic.AtomicReference<RuntimeException> firstTornQuery =
                            new java.util.concurrent.atomic.AtomicReference<>();
                    final List<Thread> queryWorkers = new ArrayList<>();
                    for (final ReaderNode reader : readers) {
                        queryWorkers.add(Thread.ofVirtual().name("freshness-query").unstarted(() -> {
                            final Random queryRandom = new Random(99L);
                            while (streaming.get()) {
                                try {
                                    reader.graphCoordinator().read(() -> queryReader(reader, queryRandom));
                                } catch (final RuntimeException torn) {
                                    tornQueries.incrementAndGet();
                                    firstTornQuery.compareAndSet(null, torn);
                                }
                                Thread.yield();
                            }
                        }));
                    }
                    for (final Thread queryWorker : queryWorkers) queryWorker.start();
                    long sequence = 0L;
                    for (int tx = 0; tx < STREAM_TRANSACTIONS; tx++) {
                        final int op = random.nextInt(100);
                        if (op < 60 || liveIds.isEmpty()) {
                            final String title = "stream" + sequence++;
                            final float[] vector = {random.nextFloat() + 0.5f, random.nextFloat() + 0.5f,
                                    random.nextFloat() + 0.5f};
                            liveIds.add(writerRoot.articles.add(new IndexedArticle(
                                    title, "word" + (sequence % 20), vector)));
                            liveTitles.add(title);
                            liveVectors.put(title, vector);
                        } else if (op < 85) {
                            final int slot = random.nextInt(liveIds.size());
                            final String body = "word" + random.nextInt(20) + "u" + sequence++;
                            final float[] vector = {random.nextFloat() + 0.5f, random.nextFloat() + 0.5f,
                                    random.nextFloat() + 0.5f};
                            writerRoot.articles.update(liveIds.get(slot), article -> {
                                article.body = body;
                                article.vector = vector;
                            });
                            liveVectors.put(liveTitles.get(slot), vector);
                        } else {
                            final int slot = random.nextInt(liveIds.size());
                            writerRoot.articles.removeById(liveIds.remove(slot));
                            liveVectors.remove(liveTitles.remove(slot));
                        }
                        writerRoot.articles.store();
                        final ReplicationCursor target = AeronStoreIntegrationIT.latest(writerTransport);
                        for (final ReaderNode reader : readers) reader.await(target);
                    }
                    for (final ReaderNode reader : readers) reader.assertHealthy();
                    streaming.set(false);
                    for (final Thread queryWorker : queryWorkers) queryWorker.join();
                    assertEquals(0, tornQueries.get(),
                            () -> "joined reads observed torn batch boundaries during import; first="
                                    + firstTornQuery.get());
                    for (final ReaderNode reader : readers) {
                        reader.await(AeronStoreIntegrationIT.latest(writerTransport));
                        reader.assertHealthy();
                    }
                    assertGraphHas(writerRoot, liveTitles);
                    assertLuceneHas(writerRoot, liveTitles);
                    assertJVectorHas(writerRoot, liveVectors);
                    for (final ReaderNode reader : readers) {
                        final IndexRoot readerRoot = (IndexRoot) reader.rootObject();
                        assertGraphHas(readerRoot, liveTitles);
                        assertLuceneHas(readerRoot, liveTitles);
                        assertJVectorHas(readerRoot, liveVectors);
                    }
                } finally {
                    for (final ReaderNode reader : readers) reader.close();
                }
            } finally {
                writer.shutdown();
            }
        } finally {
            AeronStoreIntegrationIT.delete(root);
        }
    }

    private static void queryReader(final ReaderNode reader, final Random random) {
        /* Samples one article and requires both indexes to agree with the
         * graph on it. The caller holds the coordinator read side, and the
         * merger holds one write section per batch, so any mismatch is a
         * torn boundary, not a race with the next transaction. */
        final IndexRoot root = (IndexRoot) reader.rootObject();
        final List<IndexedArticle> snapshot = new ArrayList<>();
        root.articles.iterate(snapshot::add);
        if (snapshot.isEmpty()) return;
        final IndexedArticle sample = snapshot.get(random.nextInt(snapshot.size()));
        final List<IndexedArticle> luceneHits = luceneIndex(root.articles).query("title:" + sample.title);
        if (luceneHits.size() != 1 || !sample.title.equals(luceneHits.getFirst().title)) {
            throw new IllegalStateException("torn Lucene read for " + sample.title);
        }
        final VectorIndices<IndexedArticle> vectors = root.articles.index().get(VectorIndices.Category());
        /* Top-5 inclusion, not top-1 identity: JVector is approximate, and a
         * rebuilt graph can rank a near-duplicate vector above the sampled
         * article itself (observed with cosine neighbors 1 apart in 100).
         * Inclusion still catches every real staleness mode — a missing,
         * removed, or re-vectored entity never appears in its own top 5. */
        final VectorSearchResult<IndexedArticle> nearest = vectors.get("articles").search(sample.vector, 5);
        if (nearest.toList().stream().noneMatch(hit -> sample.title.equals(hit.entity().title))) {
            throw new IllegalStateException("torn JVector read for " + sample.title);
        }
    }

    private static void assertGraphHas(final IndexRoot root, final List<String> titles) {
        final Set<String> seen = new HashSet<>();
        root.articles.iterate(article -> seen.add(article.title));
        for (final String title : titles) {
            assertTrue(seen.contains(title), "graph missed " + title);
        }
    }

    private static void assertLuceneHas(final IndexRoot root, final List<String> titles) {
        final LuceneIndex<IndexedArticle> text = luceneIndex(root.articles);
        for (final String title : titles) {
            assertEquals(1, text.query("title:" + title).size(), "Lucene missed " + title);
        }
    }

    private static void assertJVectorHas(final IndexRoot root) {
        final Map<String, float[]> liveVectors = new HashMap<>();
        root.articles.iterate(article -> liveVectors.put(article.title, article.vector));
        assertJVectorHas(root, liveVectors);
    }

    private static void assertJVectorHas(final IndexRoot root, final Map<String, float[]> liveVectors) {
        final VectorIndices<IndexedArticle> vectors = root.articles.index().get(VectorIndices.Category());
        final Set<String> titles = new HashSet<>();
        root.articles.iterate(article -> titles.add(article.title));
        for (final Map.Entry<String, float[]> entry : liveVectors.entrySet()) {
            if (!titles.contains(entry.getKey())) continue;
            /* Top-5 inclusion for the same approximation reason as the live
             * query path: a rebuilt graph can rank a near-duplicate above
             * the entity itself without anything being stale. */
            final VectorSearchResult<IndexedArticle> nearest =
                    vectors.get("articles").search(entry.getValue(), 5);
            assertTrue(nearest.toList().stream().anyMatch(hit -> entry.getKey().equals(hit.entity().title)),
                    "JVector missed " + entry.getKey());
        }
        assertFalse(titles.isEmpty(), "expected indexed articles");
    }

    @SuppressWarnings("unchecked") // Lucene's class token cannot retain its entity type.
    private static LuceneIndex<IndexedArticle> luceneIndex(final GigaMap<IndexedArticle> articles) {
        return articles.index().get(LuceneIndex.class);
    }
}
