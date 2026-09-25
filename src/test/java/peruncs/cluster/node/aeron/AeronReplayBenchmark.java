package peruncs.cluster.node.aeron;

import org.apache.lucene.document.Document;
import org.eclipse.serializer.typing.Disposable;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.VectorSimilarityFunction;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.replication.CommitAppliedListener;
import peruncs.cluster.node.replication.DurableCursorFile;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.*;
import peruncs.cluster.storage.index.ClusterStoreIndexes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/// Test-only reader replay benchmark: a writer builds a transaction backlog in
/// its Archive while the reader is offline, then the reader starts at the
/// baseline cursor and replays the backlog. It measures end-to-end replay
/// throughput — Archive replay, envelope assembly, Store import,
/// materialization, index maintenance, and cursor persistence — as it would
/// hit an abrupt reader restart. Run it under
/// `-XX:StartFlightRecording=filename=...` to profile where replay time goes;
/// it deliberately adds no production instrumentation or benchmark dependency.
public final class AeronReplayBenchmark {
    private AeronReplayBenchmark() {
    }

    static void main(final String[] arguments) throws Exception {
        int transactions = 500;
        int seedArticles = 200;
        int warmup = 0;
        boolean lucene = true;
        boolean vector = true;
        boolean cursor = true;
        boolean noopReceiver = false;
        for (final String argument : arguments) {
            if (argument.startsWith("--transactions=")) transactions = Integer.parseInt(argument.substring(15));
            else if (argument.startsWith("--seed-articles=")) seedArticles = Integer.parseInt(argument.substring(16));
            else if (argument.startsWith("--warmup=")) warmup = Integer.parseInt(argument.substring(9));
            else if (argument.startsWith("--lucene=")) lucene = Boolean.parseBoolean(argument.substring(9));
            else if (argument.startsWith("--vector=")) vector = Boolean.parseBoolean(argument.substring(9));
            /* Ablation only: skipping cursor persistence measures the fsync
             * share; it is never a supported production mode. */
            else if (argument.startsWith("--cursor=")) cursor = Boolean.parseBoolean(argument.substring(9));
            /* Ablation only: a swallow-everything receiver isolates transport
             * and assembly pacing from Store import/materialization. */
            else if (argument.startsWith("--receiver=noop")) noopReceiver = true;
            else throw new IllegalArgumentException("unknown argument: " + argument);
        }
        final boolean useLucene = lucene;
        final boolean useVector = vector;
        final boolean useCursor = cursor;
        final boolean swallow = noopReceiver;
        for (int round = 0; round < warmup; round++) measure(transactions, seedArticles, useLucene, useVector, useCursor, swallow);
        final Result result = measure(transactions, seedArticles, useLucene, useVector, useCursor, swallow);
        System.out.printf(
                "transactions=%d seedArticles=%d lucene=%s vector=%s cursor=%s replayMs=%.1f msPerTx=%.2f txPerSecond=%.1f storeMs=%d%n",
                result.transactions(), result.seedArticles(), useLucene, useVector, useCursor,
                result.replayNanos() / 1_000_000.0,
                result.millisPerTransaction(), result.transactionsPerSecond(),
                TimeUnit.NANOSECONDS.toMillis(result.publishNanos()));
    }

    /// Measures one backlog replay: build `transactions` committed transactions
    /// while the reader is offline, then time how long a fresh reader takes to
    /// apply every one of them from the recorded baseline cursor.
    static Result measure(final int transactions, final int seedArticles,
                          final boolean lucene, final boolean vector, final boolean persistCursor,
                          final boolean noopReceiver) throws Exception {
        if (transactions <= 0 || seedArticles < 0) {
            throw new IllegalArgumentException("invalid replay benchmark parameters");
        }
        final Path root = Files.createTempDirectory("dg-aeron-replay-benchmark-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = AeronStoreIntegrationIT.freePort();
        final int livePort = AeronStoreIntegrationIT.freePort();
        final int watermarkPort = AeronStoreIntegrationIT.freePort();
        final Path writerPath = root.resolve("writer-store");
        final Path readerPath = root.resolve("reader-store");
        try (ClusterReplicationTransport writerTransport = new AeronTransport(
                AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation,
                        "writer", -1L, controlPort, livePort, watermarkPort))) {
            writerTransport.positionProvider("store").init();
            final ReplicationPublisher distributor = writerTransport.distributor("store");
            final AeronStoreIntegrationIT.IndexRoot initial = new AeronStoreIntegrationIT.IndexRoot();
            initial.articles = GigaMap.New();
            configureSelectedIndexes(initial.articles, lucene, vector);
            for (int i = 0; i < seedArticles; i++) {
                initial.articles.add(new AeronStoreIntegrationIT.IndexedArticle(
                        "seed-%d".formatted(i), "seed body %d".formatted(i),
                        new float[]{1.0f, i % 7, (i % 3) * 0.5f}));
            }
            final EmbeddedStorageManager seed = AeronStoreIntegrationIT.startIndex(
                    writerPath, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seed.storeRoot();
            seed.shutdown();
            final ReplicationCursor baseline = AeronStoreIntegrationIT.latest(writerTransport);
            AeronStoreIntegrationIT.copyDirectory(writerPath, readerPath);

            final EmbeddedStorageManager writer = AeronStoreIntegrationIT.startExistingIndex(
                    writerPath, distributor, writerTransport.persistenceTargetFactory("store", distributor));
            final AeronStoreIntegrationIT.IndexRoot writerRoot = writer.root();
            /* Build the backlog while the reader is offline: each map store is
             * one replicated transaction, matching the soak's article op. */
            final long publishStarted = System.nanoTime();
            for (int i = 0; i < transactions; i++) {
                writerRoot.articles.add(new AeronStoreIntegrationIT.IndexedArticle(
                        "replay-%d".formatted(i), "replay body %d".formatted(i),
                        new float[]{0.25f, i % 5, 1.0f}));
                writerRoot.articles.store();
            }
            final long publishNanos = System.nanoTime() - publishStarted;
            final ReplicationCursor settledTarget = awaitSettled(writerTransport);
            final long targetSequence = settledTarget.logicalSequence();

            final Path readerRoot = root.resolve("reader");
            Files.createDirectories(readerRoot);
            try (ClusterReplicationTransport readerTransport = new AeronTransport(
                    AeronStoreIntegrationIT.properties(readerRoot, clusterId, UUID.randomUUID(), generation, "reader",
                            -1L, controlPort, livePort, watermarkPort));
                 DurableCursorFile cursorManager = DurableCursorFile.of(
                         readerRoot.resolve("cursor"))) {
                final EmbeddedStorageFoundation<?> readerFoundation = AeronStoreIntegrationIT.foundation(readerPath);
                final EmbeddedStorageManager reader = readerFoundation.start();
                final StorageGraphCoordinator graphCoordinator = new StorageGraphCoordinator();
                /* Production-equivalent merger tuning (the same defaults a node
                 * gets from Configuration.New): coalescing timeout, byte-based
                 * backpressure limit, and validation bound. */
                final StorageBinaryDataReceiver receiver = noopReceiver
                        ? new StorageBinaryDataReceiver() {
                            @Override
                            public void receiveTypeDictionary(final String value) {
                            }

                            @Override
                            public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
                            }

                            @Override
                            public boolean receiveDataOwned(final org.eclipse.serializer.persistence.binary.types.Binary value) {
                                return true;
                            }
                        }
                        : StorageBinaryDataMerger.create(
                        StorageBinaryDataMerger.Configuration.create(
                                readerFoundation.getConnectionFoundation(), reader.createConnection(),
                                ObjectGraphUpdateHandler.PerStore(graphCoordinator), graphCoordinator));
                final long[] appliedAt = new long[transactions + 64];
                final ReplicationApplier client = readerTransport.client(receiver, "store",
                        new CommitAppliedListener() {
                            private int appliedCount;
                            private long firstNanos;

                            @Override
                            public void onApplied(final ReplicationCursor cursor) {
                                final long now = System.nanoTime();
                                if (this.appliedCount == 0) this.firstNanos = now;
                                if (this.appliedCount < appliedAt.length) appliedAt[this.appliedCount] = now - this.firstNanos;
                                this.appliedCount++;
                                if (persistCursor) cursorManager.set(cursor);
                            }

                            @Override
                            public void close() {
                            }
                        }, baseline);
                try {
                    final long replayStarted = System.nanoTime();
                    client.start();
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(600);
                    while (client.cursor().logicalSequence() < targetSequence && client.failure() == null
                           && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(200_000L);
                    }
                    final long replayNanos = System.nanoTime() - replayStarted;
                    reportGaps(appliedAt);
                    if (client.failure() != null) throw client.failure();
                    if (client.cursor().logicalSequence() < targetSequence) {
                        throw new IllegalStateException(
                                "reader replay stalled at %s of %s"
                                        .formatted(client.cursor().logicalSequence(), targetSequence));
                    }
                    client.stopAtLatestMessage();
                    final long stopDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (client.isRunning() && System.nanoTime() < stopDeadline) LockSupport.parkNanos(100_000L);
                    receiver.awaitApplied();
                    return new Result(transactions, seedArticles, replayNanos, publishNanos);
                } finally {
                    client.dispose();
                    if (receiver instanceof Disposable disposable) disposable.dispose();
                    if (!noopReceiver) reader.shutdown();
                    writer.shutdown();
                }
            }
        } finally {
            AeronStoreIntegrationIT.delete(root);
        }
    }

    /// Registers only the index kinds this run ablates in: with both enabled
    /// this is exactly the soak fixture, and each `false` removes that index
    /// kind's per-batch maintenance from the replay path.
    private static void configureSelectedIndexes(
            final GigaMap<AeronStoreIntegrationIT.IndexedArticle> articles,
            final boolean lucene, final boolean vector) {
        if (lucene) {
            ClusterStoreIndexes.registerLucene(articles, new DocumentPopulator<>() {
                @Override
                public void populate(final Document document, final AeronStoreIntegrationIT.IndexedArticle article) {
                    document.add(DocumentPopulator.createTextField("title", article.title));
                    document.add(DocumentPopulator.createTextField("body", article.body));
                }
            });
        }
        if (vector) {
            final VectorIndices<AeronStoreIntegrationIT.IndexedArticle> vectors =
                    articles.index().register(VectorIndices.Category());
            ClusterStoreIndexes.addVector(vectors, "articles", VectorIndexConfiguration.builder()
                            .dimension(3).similarityFunction(VectorSimilarityFunction.COSINE).build(),
                    new Vectorizer<>() {
                        @Override
                        public float[] vectorize(final AeronStoreIntegrationIT.IndexedArticle article) {
                            return article.vector;
                        }
                    });
        }
    }

    /// Reads the writer boundary once it is unchanged across a settle window,
    /// so the replay target covers every published transaction (including the
    /// trailing checkpoint-confirmed record).
    private static ReplicationCursor awaitSettled(final ClusterReplicationTransport transport) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        final long settleNanos = TimeUnit.MILLISECONDS.toNanos(500);
        ReplicationCursor settled = null;
        long settledAtNanos = 0L;
        while (System.nanoTime() < deadline) {
            final ReplicationCursor cursor = AeronStoreIntegrationIT.latest(transport);
            if (settled == null || cursor.logicalSequence() != settled.logicalSequence()) {
                settled = cursor;
                settledAtNanos = System.nanoTime();
            } else if (System.nanoTime() - settledAtNanos >= settleNanos) {
                return settled;
            }
            LockSupport.parkNanos(50_000_000L);
        }
        throw new IllegalStateException("writer boundary did not settle within 60s");
    }

    /// Prints the applied-transaction inter-arrival distribution: median, p90,
    /// p99, the ten largest gaps, and the counts in coarse millisecond bands.
    private static void reportGaps(final long[] appliedAt) {
        int count = 0;
        while (count < appliedAt.length && (count > 0 ? appliedAt[count] > 0 : appliedAt[0] >= 0)) count++;
        if (count < 2) return;
        final long[] gaps = new long[count - 1];
        for (int i = 1; i < count; i++) gaps[i - 1] = appliedAt[i] - appliedAt[i - 1];
        final long[] sorted = gaps.clone();
        java.util.Arrays.sort(sorted);
        final int[] bands = new int[7]; // <1, 1-5, 5-20, 20-50, 50-100, 100-500, >500 ms
        for (final long gap : gaps) {
            final double ms = gap / 1e6;
            bands[ms < 1 ? 0 : ms < 5 ? 1 : ms < 20 ? 2 : ms < 50 ? 3 : ms < 100 ? 4 : ms < 500 ? 5 : 6]++;
        }
        System.out.printf(
                "appliedGaps n=%d p50=%.2fms p90=%.2fms p99=%.2fms max=%.2fms bands(<1|1-5|5-20|20-50|50-100|100-500|>500)=%s%n",
                gaps.length, sorted[gaps.length / 2] / 1e6, sorted[(int) (gaps.length * 0.9)] / 1e6,
                sorted[Math.min(gaps.length - 1, (int) (gaps.length * 0.99))] / 1e6, sorted[gaps.length - 1] / 1e6,
                java.util.Arrays.toString(bands));
    }

    record Result(int transactions, int seedArticles, long replayNanos, long publishNanos) {
        double transactionsPerSecond() {
            return this.transactions / (this.replayNanos / 1_000_000_000.0);
        }

        double millisPerTransaction() {
            return this.replayNanos / 1_000_000.0 / this.transactions;
        }
    }
}
