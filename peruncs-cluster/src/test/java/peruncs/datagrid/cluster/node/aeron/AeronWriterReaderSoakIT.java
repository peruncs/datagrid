package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexRoot;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexedArticle;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.ReaderNode;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.*;

/// Soak test: one writer, three readers, threaded load with jitter, index
/// queries served while replication lands, and random reader restarts.
///
/// This is deliberately heavy and lives outside the default gate: it only
/// runs under `-Psoak` (see the `soak` profile in peruncs-cluster/pom.xml),
/// tuned with `-Dsoak.seconds=`, `-Dsoak.seed=`, `-Dsoak.writer.threads=`,
/// `-Dsoak.query.threads=` and `-Dsoak.restarts=`. The seed is printed on
/// every run, so any failure reproduces with the same seed.
///
/// The test emits `SOAK` audit lines with elapsed times throughout the run,
/// so a watcher can follow setup, live throughput, restarts, convergence and
/// verification without attaching a profiler.
class AeronWriterReaderSoakIT {
    private static final String[] WORDS = {
            "alpha", "bravo", "cargo", "delta", "ember", "frost", "granite", "harbor", "ivory", "jungle",
            "karma", "lumen", "magnet", "noble", "onyx", "prism", "quartz", "ridge", "solar", "tundra"};
    private static final int SEED_ARTICLES = 60;
    private static final int STRICT_PRESENT_SAMPLE = 30;
    private static final int STRICT_ABSENT_SAMPLE = 10;

    private final Object writeLock = new Object();
    private final AtomicLong titleSequence = new AtomicLong();
    private final AtomicLong publishedTransactions = new AtomicLong();
    private final AtomicLong servedQueries = new AtomicLong();
    private final AtomicLong tornReads = new AtomicLong();
    private final AtomicLong completedRestarts = new AtomicLong();
    private final AtomicLong liveFlips = new AtomicLong();
    private final AtomicLong reseedsDemanded = new AtomicLong();
    private final Map<Long, ArticleState> live = new ConcurrentHashMap<>();
    private final List<ArticleState> seedStates = new ArrayList<>();
    private final Set<String> knownTitles = ConcurrentHashMap.newKeySet();
    private final Set<String> removedTitles = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> tornSamples = new ConcurrentLinkedQueue<>();
    private final AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
    private final ConcurrentHashMap<String, AtomicLong> beats = new ConcurrentHashMap<>();
    private volatile boolean testDone;

    @Test
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    void oneWriterThreeReadersSurviveThreadedLoadJitterAndRestarts() throws Exception {
        final long startNanos = System.nanoTime();
        final int soakSeconds = Integer.getInteger("soak.seconds", 30);
        final long seed = Long.getLong("soak.seed", 1L);
        final int writerThreads = Integer.getInteger("soak.writer.threads", 3);
        final int queryThreads = Integer.getInteger("soak.query.threads", 2);
        final int restarts = Integer.getInteger("soak.restarts", 6);
        audit(startNanos, "start seed=%d seconds=%d writerThreads=%d queryThreadsPerReader=%d restarts=%d"
                .formatted(seed, soakSeconds, writerThreads, queryThreads, restarts));

        final Path root = java.nio.file.Files.createTempDirectory("dg-aeron-soak-");
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
        final UUID[] readerIds = {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            writerTransport.positionProvider("store").init();
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final Random seedRandom = new Random(seed);
            final IndexRoot initial = new IndexRoot();
            initial.articles = GigaMap.New();
            AeronStoreIntegrationIT.configureIndexes(initial.articles);
            for (int i = 0; i < SEED_ARTICLES; i++) seedArticle(initial, seedRandom);
            final EmbeddedStorageManager seeded = AeronStoreIntegrationIT.startIndex(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            audit(startNanos, "seeded articles=%d".formatted(SEED_ARTICLES));

            final ReplicationCursor baseline = AeronStoreIntegrationIT.latest(writerTransport);
            for (final Path readerStore : readerStores) AeronStoreIntegrationIT.copyDirectory(writerStore, readerStore);
            final EmbeddedStorageManager writer = AeronStoreIntegrationIT.startExistingIndex(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final IndexRoot writerRoot = writer.root();
            try {
                final ReaderNode[] holders = new ReaderNode[readerStores.length];
                final ReadWriteLock[] gates = new ReadWriteLock[readerStores.length];
                for (int i = 0; i < holders.length; i++) {
                    holders[i] = ReaderNode.open(readerNodes[i], readerStores[i], "reader", readerIds[i],
                            clusterId, generation, baseline, controlPort, livePort, watermarkPort);
                    holders[i].start();
                    gates[i] = new ReentrantReadWriteLock();
                }
                for (final ReaderNode reader : holders) reader.awaitLive();
                audit(startNanos, "setup-ok readers=%d".formatted(holders.length));

                final long soakEndNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(soakSeconds);
                final List<Thread> workers = new ArrayList<>();
                for (int i = 0; i < writerThreads; i++) {
                    final int workerIndex = i;
                    workers.add(launch("soak-writer-%d".formatted(workerIndex),
                            () -> writeLoop(writerRoot, soakEndNanos, new Random(seed ^ 0x9E3779B9L ^ workerIndex))));
                }
                for (int r = 0; r < holders.length; r++) {
                    for (int q = 0; q < queryThreads; q++) {
                        final int readerIndex = r;
                        final int queryIndex = q;
                        workers.add(launch("soak-query-%d-%d".formatted(r, queryIndex), () -> queryLoop(
                                holders, gates, readerIndex, soakEndNanos,
                                new Random(seed ^ 0x51ED734BL ^ readerIndex ^ queryIndex))));
                    }
                }
                workers.add(launch("soak-chaos", () -> chaosLoop(startNanos, holders, gates, baseline,
                        soakEndNanos, restarts, new Random(seed ^ 0xC0FFEE11L))));
                workers.add(launch("soak-audit", () -> auditLoop(startNanos, writerTransport, holders, gates,
                        soakEndNanos)));
                /* The watchdog is deliberately not joined: it watches the join
                 * itself, so joining it would deadlock the test it guards. */
                launch("soak-watchdog", () -> watchdogLoop(startNanos, workers));
                audit(startNanos, "soak-start threads=%d".formatted(workers.size()));
                for (final Thread worker : workers) worker.join();
                assertNoWorkerFailures();
                audit(startNanos, "soak-end tx=%d queries=%d torn=%d restarts=%d liveFlips=%d"
                        .formatted(this.publishedTransactions.get(), this.servedQueries.get(),
                                this.tornReads.get(), this.completedRestarts.get(), this.liveFlips.get()));
                if (!this.tornSamples.isEmpty()) {
                    System.out.println("SOAK torn-read samples (queries retried while replication landed):");
                    this.tornSamples.stream().limit(5).forEach(sample -> System.out.println("SOAK   " + sample));
                }

                final ReplicationCursor target;
                synchronized (this.writeLock) {
                    target = AeronStoreIntegrationIT.latest(writerTransport);
                }
                audit(startNanos, "converge-start target=%d".formatted(target.logicalSequence()));
                int converged = 0;
                for (int r = 0; r < holders.length; r++) {
                    markActivity();
                    if (holders[r] == null) {
                        audit(startNanos, "converge-skipped reader=%d (awaiting reseed)".formatted(r));
                        continue;
                    }
                    audit(startNanos, "converge-await reader=%d target=%d".formatted(r, target.logicalSequence()));
                    gates[r].readLock().lock();
                    try {
                        holders[r].await(target);
                        /* A reader that rode out a live-image loss rejoins via
                         * replay; Aeron heals this by itself, but the rejoin
                         * lags the cursor, so wait for it boundedly instead of
                         * asserting instant liveness. A reader that never
                         * rejoins still fails here. */
                        holders[r].awaitLive();
                        holders[r].assertHealthy();
                    } finally {
                        gates[r].readLock().unlock();
                    }
                    audit(startNanos, "converged reader=%d".formatted(r));
                    converged++;
                }
                assertTrue(converged > 0,
                        "no reader survived the soak to converge; reseeds demanded=%d".formatted(this.reseedsDemanded.get()));

                final List<ArticleState> present = samplePresent(seed);
                final List<String> absent = sampleAbsent(seed);
                final long visibilityDeadlineNanos =
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                final List<String> strictProblems = new ArrayList<>();
                for (int r = 0; r < holders.length; r++) {
                    markActivity();
                    if (holders[r] == null) continue;
                    gates[r].readLock().lock();
                    try {
                        assertConverged(startNanos, holders, gates, r, present, absent, visibilityDeadlineNanos);
                    } catch (final AssertionError strict) {
                        strictProblems.add("reader=%d: %s".formatted(r, strict.getMessage()));
                    } finally {
                        gates[r].readLock().unlock();
                    }
                    audit(startNanos, "verified reader=%d present=%d absent=%d"
                            .formatted(r, present.size(), absent.size()));
                }

                for (int r = 0; r < holders.length; r++) {
                    if (holders[r] == null) continue;
                    gates[r].writeLock().lock();
                    try {
                        holders[r].stopAtLatest();
                        /* Keep this Store open while checking the quiescent
                         * graph and both indexes. Same-JVM close/reopen is a
                         * known upstream materializer race; process-restart
                         * durability is covered by the smoke test. */
                        assertQuiescent(startNanos, holders[r], r, present);
                        holders[r].close();
                    } finally {
                        gates[r].writeLock().unlock();
                    }
                    audit(startNanos, "quiescent reader=%d".formatted(r));
                }
                if (!strictProblems.isEmpty()) {
                    throw new AssertionError("soak strict verification failed: " + strictProblems);
                }
                /* Live reads run joined on the coordinator's read side, so a
                 * torn index view is a product defect, not load noise. */
                assertEquals(0, this.tornReads.get(),
                        "joined live reads observed torn index state");
            } finally {
                writer.shutdown();
            }
        } finally {
            AeronStoreIntegrationIT.delete(root);
        }
        this.testDone = true;
        audit(startNanos, "soak-ok tx=%d queries=%d torn=%d restarts=%d liveFlips=%d reseeds=%d"
                .formatted(this.publishedTransactions.get(), this.servedQueries.get(),
                        this.tornReads.get(), this.completedRestarts.get(), this.liveFlips.get(),
                        this.reseedsDemanded.get()));
    }

    private void seedArticle(final IndexRoot initial, final Random random) {
        final String title = "soakt" + this.titleSequence.incrementAndGet();
        final String body = WORDS[random.nextInt(WORDS.length)] + "s" + this.titleSequence.get();
        final float[] vector = randomVector(random);
        final long id = initial.articles.add(new IndexedArticle(title, body, vector));
        final ArticleState state = new ArticleState(title, body, vector, this.publishedTransactions.get());
        this.live.put(id, state);
        this.seedStates.add(state);
        this.knownTitles.add(title);
    }

    private void writeLoop(final IndexRoot writerRoot, final long soakEndNanos, final Random random) throws Exception {
        final String worker = Thread.currentThread().getName();
        while (System.nanoTime() < soakEndNanos) {
            beat(worker, "store");
            final int op = random.nextInt(100);
            synchronized (this.writeLock) {
                if (op < 55) addBatch(writerRoot, random, 1 + random.nextInt(4));
                else if (op < 80) updateOne(writerRoot, random);
                else removeOne(writerRoot, random);
                writerRoot.articles.store();
                this.publishedTransactions.incrementAndGet();
            }
            sleepJitter(random, 5);
        }
    }

    private void addBatch(final IndexRoot writerRoot, final Random random, final int count) {
        for (int i = 0; i < count; i++) {
            final String title = "soakt" + this.titleSequence.incrementAndGet();
            final String body = WORDS[random.nextInt(WORDS.length)] + "s" + this.titleSequence.get();
            final float[] vector = randomVector(random);
            final long id = writerRoot.articles.add(new IndexedArticle(title, body, vector));
            this.live.put(id, new ArticleState(title, body, vector, this.publishedTransactions.get()));
            this.knownTitles.add(title);
        }
    }

    private void updateOne(final IndexRoot writerRoot, final Random random) {
        final Long[] ids = this.live.keySet().toArray(new Long[0]);
        if (ids.length == 0) return;
        final long id = ids[random.nextInt(ids.length)];
        final ArticleState current = this.live.get(id);
        if (current == null) return;
        final String body = WORDS[random.nextInt(WORDS.length)] + "s" + this.titleSequence.incrementAndGet();
        final float[] vector = randomVector(random);
        writerRoot.articles.update(id, article -> {
            article.body = body;
            article.vector = vector;
        });
        this.live.put(id, new ArticleState(current.title(), body, vector, this.publishedTransactions.get()));
    }

    private void removeOne(final IndexRoot writerRoot, final Random random) {
        final Long[] ids = this.live.keySet().toArray(new Long[0]);
        if (ids.length == 0) return;
        final long id = ids[random.nextInt(ids.length)];
        final ArticleState removed = this.live.remove(id);
        if (removed == null) return;
        writerRoot.articles.removeById(id);
        this.removedTitles.add(removed.title());
    }

    private void queryLoop(final ReaderNode[] holders, final ReadWriteLock[] gates, final int readerIndex,
                           final long soakEndNanos, final Random random) throws Exception {
        final String worker = Thread.currentThread().getName();
        while (System.nanoTime() < soakEndNanos) {
            beat(worker, "lock");
            gates[readerIndex].readLock().lock();
            try {
                beat(worker, "query");
                final ReaderNode node = holders[readerIndex];
                if (node == null) {
                    Thread.sleep(50L);
                    continue;
                }
                /* Joined read: the product contract requires reads to hold the
                 * coordinator's read side. An unjoined query can trigger a
                 * lazy vector-graph rebuild that races the next batch's bulk
                 * materialization — wedging the reader and reading torn
                 * entities. */
                node.graphCoordinator().read(() -> queryOnce(node, random));
                this.servedQueries.incrementAndGet();
            } catch (final RuntimeException torn) {
                /* Replication may land mid-query and tear the read view; the
                 * strict assertions run at quiescence, so here we only count
                 * the retry and keep serving. */
                this.tornReads.incrementAndGet();
                if (this.tornSamples.size() < 16) {
                    this.tornSamples.add("%s: %s".formatted(
                            torn.getClass().getSimpleName(), String.valueOf(torn.getMessage())));
                }
            } finally {
                gates[readerIndex].readLock().unlock();
            }
            sleepJitter(random, 3);
        }
    }

    private void queryOnce(final ReaderNode node, final Random random) {
        final IndexRoot root = (IndexRoot) node.rootObject();
        if (random.nextBoolean()) {
            final String[] known = this.knownTitles.toArray(new String[0]);
            if (known.length == 0) return;
            luceneIndex(root.articles).query("title:" + known[random.nextInt(known.length)]);
        } else {
            final VectorIndices<IndexedArticle> vectors = root.articles.index().get(VectorIndices.Category());
            vectors.get("articles").search(randomVector(random), 3);
        }
    }

    private void chaosLoop(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                           final ReplicationCursor baseline, final long soakEndNanos,
                           final int restarts, final Random random) throws Exception {
        for (int i = 0; i < restarts && System.nanoTime() < soakEndNanos; i++) {
            beat(Thread.currentThread().getName(), "sleep");
            Thread.sleep(2_000L + random.nextInt(6_000));
            markActivity();
            if (System.nanoTime() >= soakEndNanos) return;
            final int readerIndex = random.nextInt(holders.length);
            final boolean abrupt = random.nextBoolean();
            if (holders[readerIndex] == null) {
                audit(startNanos, "reader-restart-skipped reader=%d (awaiting reseed)".formatted(readerIndex));
                continue;
            }
            gates[readerIndex].writeLock().lock();
            try {
                final ReaderNode node = holders[readerIndex];
                ReplicationCursor resume = node.persistedCursor() != null
                        ? node.persistedCursor() : baseline;
                audit(startNanos, "reader-restart-start reader=%d abrupt=%s resume=%d"
                        .formatted(readerIndex, abrupt, resume.logicalSequence()));
                if (!abrupt) {
                    audit(startNanos, "reader-stopping reader=%d".formatted(readerIndex));
                    node.stopAtLatest();
                    audit(startNanos, "reader-stopped reader=%d".formatted(readerIndex));
                    /* Use the boundary reached by the graceful stop. Replaying
                     * from the older cursor against the same live Store would
                     * apply already materialised transactions a second time. */
                    final ReplicationCursor stopped = node.persistedCursor();
                    if (stopped != null) resume = stopped;
                }
                audit(startNanos, "reader-transport-restarting reader=%d".formatted(readerIndex));
                /* Keep the Store and its materializer alive. Only the Aeron
                 * reader and merger are replaced, which models a transport
                 * restart without reopening the Store in this JVM. A graceful
                 * stop must always resume; an abrupt interruption may demand a
                 * reseed and park the reader by design. */
                try {
                    node.restartTransport(resume);
                    audit(startNanos, "reader-awaiting-live reader=%d".formatted(readerIndex));
                    node.awaitLive();
                    node.assertHealthy();
                    this.completedRestarts.incrementAndGet();
                } catch (final ReseedRequiredException reseed) {
                    /* Close the retained Store only when this reader is parked;
                     * no subsequent same-JVM reopen is attempted. */
                    node.close();
                    holders[readerIndex] = null;
                    this.reseedsDemanded.incrementAndGet();
                    audit(startNanos, "reader-reseed-demanded reader=%d resume=%d (%s)"
                            .formatted(readerIndex, resume.logicalSequence(), reseed.getMessage()));
                    if (!abrupt) throw reseed;
                }
            } finally {
                gates[readerIndex].writeLock().unlock();
            }
            audit(startNanos, "reader-restart reader=%d abrupt=%s".formatted(readerIndex, abrupt));
        }
    }

    private void auditLoop(final long startNanos, final ClusterReplicationTransport writerTransport,
                           final ReaderNode[] holders, final ReadWriteLock[] gates,
                           final long soakEndNanos) throws Exception {
        final boolean[] wasLive = {true, true, true};
        int iteration = 0;
        while (System.nanoTime() < soakEndNanos) {
            Thread.sleep(5_000L);
            beat(Thread.currentThread().getName(), "sample");
            markActivity();
            iteration++;
            /* Seeds never change identity: their Lucene visibility per reader
             * shows when (and on whom) index maintenance first diverges. */
            if (iteration % 2 == 0) seedProbe(startNanos, holders, gates);
            /* AeronStoreIntegrationIT.latest() is the synchronous offered position; latestSequence() trails
             * it because it reports the durable Archive position instead. */
            final long writerSequence;
            synchronized (this.writeLock) {
                writerSequence = AeronStoreIntegrationIT.latest(writerTransport).logicalSequence();
            }
            final StringBuilder state = new StringBuilder();
            for (int r = 0; r < holders.length; r++) {
                gates[r].readLock().lock();
                try {
                    if (holders[r] == null) {
                        state.append(" reader%d=reseed-pending".formatted(r));
                        continue;
                    }
                    final ReplicationCursor cursor = holders[r].persistedCursor();
                    final boolean live = holders[r].isLive();
                    final boolean running = holders[r].isRunning();
                    if (live != wasLive[r]) {
                        wasLive[r] = live;
                        this.liveFlips.incrementAndGet();
                        audit(startNanos, "live-flip reader=%d live=%s running=%s".formatted(r, live, running));
                    }
                    state.append(" lag%d=%d live%d=%s run%d=%s".formatted(r, cursor == null
                            ? -1L : writerSequence - cursor.logicalSequence(), r, live, r, running));
                } finally {
                    gates[r].readLock().unlock();
                }
            }
            audit(startNanos, "progress tx=%d queries=%d torn=%d restarts=%d writerSeq=%d%s jvm=%s"
                    .formatted(this.publishedTransactions.get(), this.servedQueries.get(),
                            this.tornReads.get(), this.completedRestarts.get(), writerSequence, state,
                            jvmTelemetry()));
        }
    }

    private static String jvmTelemetry() {
        final long heapUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() / (1024 * 1024);
        final long heapMax = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() / (1024 * 1024);
        final int threads = ManagementFactory.getThreadMXBean().getThreadCount();
        final java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        final String cpu = os instanceof com.sun.management.OperatingSystemMXBean extended
                ? "%.0f%%".formatted(extended.getProcessCpuLoad() * 100.0) : "n/a";
        return "heap=%dM/%dM threads=%d cpu=%s".formatted(heapUsed, heapMax, threads, cpu);
    }

    private void assertNoWorkerFailures() {
        if (!this.failures.isEmpty()) {
            final AssertionError error = new AssertionError(
                    "soak worker failed: " + this.failures.peek());
            for (final Throwable failure : this.failures) error.addSuppressed(failure);
            throw error;
        }
    }

    private List<ArticleState> samplePresent(final long seed) {
        final List<ArticleState> states = new ArrayList<>(this.live.values());
        Collections.shuffle(states, new Random(seed ^ 0x5A41E1L));
        return states.subList(0, Math.min(STRICT_PRESENT_SAMPLE, states.size()));
    }

    private List<String> sampleAbsent(final long seed) {
        final List<String> titles = new ArrayList<>(this.removedTitles);
        Collections.shuffle(titles, new Random(seed ^ 0xAB5E7L));
        return titles.subList(0, Math.min(STRICT_ABSENT_SAMPLE, titles.size()));
    }

    private void assertConverged(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                                 final int readerIndex, final List<ArticleState> present,
                                 final List<String> absent, final long visibilityDeadlineNanos) {
        /* Joined like every other read: the product contract requires the
         * coordinator's read side even this late — quiescence today means no
         * batch can race these queries, but the phase argument is fragile and
         * the 20 s visibility polls hold no coordinator protection at all. */
        holders[readerIndex].graphCoordinator().read(() -> assertConvergedJoined(
                startNanos, holders, gates, readerIndex, present, absent, visibilityDeadlineNanos));
    }

    private void assertConvergedJoined(final long startNanos, final ReaderNode[] holders,
                                       final ReadWriteLock[] gates, final int readerIndex,
                                       final List<ArticleState> present, final List<String> absent,
                                       final long visibilityDeadlineNanos) {
        final IndexRoot root = (IndexRoot) holders[readerIndex].rootObject();
        final Set<String> titles = new HashSet<>();
        final Map<String, String> bodies = new HashMap<>();
        root.articles.iterate(article -> {
            titles.add(article.title);
            bodies.put(article.title, article.body);
        });
        /* Graph state rides the applied cursor, so it must match immediately;
         * a miss here is divergence, not lag. */
        for (final ArticleState state : present) {
            assertTrue(titles.contains(state.title()), "reader graph missed " + state.title());
            assertEquals(state.body(), bodies.get(state.title()),
                    "reader graph has a stale body for " + state.title());
        }
        for (final String title : absent) {
            assertFalse(titles.contains(title), "reader graph retained deleted " + title);
        }
        /* Embedded index visibility may trail the applied cursor (searcher
         * refresh batches index maintenance), so poll boundedly: recovery
         * inside the deadline is measured lag, expiry is divergence. */
        final VectorIndices<IndexedArticle> vectors = root.articles.index().get(VectorIndices.Category());
        for (final ArticleState state : present) {
            assertIndexVisible(startNanos, holders, gates, readerIndex, visibilityDeadlineNanos,
                    "Lucene missed " + state.title(),
                    () -> luceneIndex(root.articles).query("title:" + state.title()).size() == 1);
            assertIndexVisible(startNanos, holders, gates, readerIndex, visibilityDeadlineNanos,
                    "JVector missed " + state.title(), () -> jvectorHits(vectors, state));
        }
        for (final String title : absent) {
            assertIndexVisible(startNanos, holders, gates, readerIndex, visibilityDeadlineNanos,
                    "Lucene retained deleted " + title,
                    () -> luceneIndex(root.articles).query("title:" + title).isEmpty());
        }
    }

    private void assertIndexVisible(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                                        final int readerIndex,
                                        final long visibilityDeadlineNanos, final String what,
                                        final java.util.function.BooleanSupplier visible) {
        final long firstMissNanos = System.nanoTime();
        boolean logged = false;
        while (!visible.getAsBoolean()) {
            if (System.nanoTime() >= visibilityDeadlineNanos) {
                indexDivergenceCensus(startNanos, holders, gates, readerIndex, what);
                fail("reader %d index never converged (%s)".formatted(readerIndex, what));
            }
            if (!logged) {
                audit(startNanos, "index-wait reader=%d (%s)".formatted(readerIndex, what));
                logged = true;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(100_000_000L);
        }
        if (logged) {
            audit(startNanos, "index-visible reader=%d (%s) lag=%.1fs".formatted(readerIndex, what,
                    (System.nanoTime() - firstMissNanos) / 1_000_000_000.0));
        }
    }

    private void seedProbe(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates) {
        for (int r = 0; r < holders.length; r++) {
            final ReaderNode node = holders[r];
            if (node == null) continue;
            final int reader = r;
            gates[reader].readLock().lock();
            try {
                node.graphCoordinator().read(() -> seedProbeJoined(startNanos, node, reader));
            } finally {
                gates[r].readLock().unlock();
            }
        }
    }

    private void seedProbeJoined(final long startNanos, final ReaderNode node, final int r) {
        final IndexRoot root = (IndexRoot) node.rootObject();
        final Set<String> graphTitles = new HashSet<>();
        root.articles.iterate(article -> graphTitles.add(article.title));
        int graphSeeds = 0;
        int luceneSeeds = 0;
        for (final ArticleState seed : this.seedStates) {
            if (!graphTitles.contains(seed.title())) continue;
            graphSeeds++;
            if (!luceneIndex(root.articles).query("title:" + seed.title()).isEmpty()) luceneSeeds++;
        }
        audit(startNanos, "seed-probe reader=%d graphSeeds=%d luceneSeeds=%d"
                .formatted(r, graphSeeds, luceneSeeds));
    }

    /// Compares the whole live graph against the reader Lucene index when a
    /// strict check expires: a handful of missing fresh docs points at commit
    /// lag, while scattered or old misses point at lost index maintenance.
    private void indexDivergenceCensus(final long startNanos, final ReaderNode[] holders,
                                   final ReadWriteLock[] gates, final int failedReader, final String trigger) {
        for (int r = 0; r < holders.length; r++) {
            final ReaderNode node = holders[r];
            if (node == null) {
                audit(startNanos, "index-census reader=%d reseed-pending".formatted(r));
                continue;
            }
            final int reader = r;
            gates[reader].readLock().lock();
            try {
                node.graphCoordinator().read(() ->
                        indexDivergenceCensusJoined(startNanos, node, reader, failedReader, trigger));
            } finally {
                gates[r].readLock().unlock();
            }
        }
    }

    private void indexDivergenceCensusJoined(final long startNanos, final ReaderNode node, final int r,
                                            final int failedReader, final String trigger) {
        final IndexRoot root = (IndexRoot) node.rootObject();
        final Set<String> graphTitles = new HashSet<>();
        root.articles.iterate(article -> graphTitles.add(article.title));
        final VectorIndices<IndexedArticle> vectors =
                root.articles.index().get(VectorIndices.Category());
        int checked = 0;
        int missing = 0;
        int jvectorMissing = 0;
        long newestMissingTx = -1L;
        long newestCheckedTx = -1L;
        final List<String> missingSample = new ArrayList<>();
        for (final ArticleState state : this.live.values()) {
            if (checked >= 600) break;
            checked++;
            newestCheckedTx = Math.max(newestCheckedTx, state.modifiedTx());
            if (!graphTitles.contains(state.title())) continue;
            if (luceneIndex(root.articles).query("title:" + state.title()).isEmpty()) {
                missing++;
                newestMissingTx = Math.max(newestMissingTx, state.modifiedTx());
                if (r == failedReader && missingSample.size() < 8) {
                    missingSample.add("%s@tx%d".formatted(state.title(), state.modifiedTx()));
                }
            }
            try {
                if (!jvectorHits(vectors, state)) jvectorMissing++;
            } catch (final RuntimeException jvector) {
                jvectorMissing++;
            }
        }
        audit(startNanos, "index-census reader=%d graph=%d checked=%d missing=%d jvectorMissing=%d newestMissingTx=%d newestCheckedTx=%d%s"
                .formatted(r, graphTitles.size(), checked, missing, jvectorMissing, newestMissingTx,
                        newestCheckedTx,
                        r == failedReader ? " trigger=(%s) sample=%s".formatted(trigger, missingSample) : ""));
    }

    /// Verifies a quiesced reader end to end: the graph must hold recent
    /// transactions, and both indexes must answer for every live article.
    private void assertQuiescent(final long startNanos, final ReaderNode node, final int readerIndex,
                                final List<ArticleState> present) {
        /* Joined like every other read: the reader is stopped here so no
         * batch can race this census, but the join keeps the invariant
         * unconditional instead of phase-dependent. */
        node.graphCoordinator().read(() ->
                assertQuiescentJoined(startNanos, node, readerIndex, present));
    }

    private void assertQuiescentJoined(final long startNanos, final ReaderNode node, final int readerIndex,
                                       final List<ArticleState> present) {
        final IndexRoot imported = (IndexRoot) node.rootObject();
        final Set<String> titles = new HashSet<>();
        final Map<String, String> bodies = new HashMap<>();
        imported.articles.iterate(article -> {
            titles.add(article.title);
            bodies.put(article.title, article.body);
        });
        for (final ArticleState state : present.subList(0, Math.min(8, present.size()))) {
            assertTrue(titles.contains(state.title()),
                    "quiescent reader %d graph lost %s".formatted(readerIndex, state.title()));
            assertEquals(state.body(), bodies.get(state.title()),
                    "quiescent reader %d graph has a stale body for %s".formatted(readerIndex, state.title()));
        }
        final VectorIndices<IndexedArticle> vectors = imported.articles.index().get(VectorIndices.Category());
        int checked = 0;
        int graphMissing = 0;
        int luceneMissing = 0;
        int jvectorMissing = 0;
        for (final ArticleState state : this.live.values()) {
            if (checked >= 600) break;
            checked++;
            if (!titles.contains(state.title())) {
                graphMissing++;
                continue;
            }
            if (luceneIndex(imported.articles).query("title:" + state.title()).isEmpty()) luceneMissing++;
            try {
                if (!jvectorHits(vectors, state)) jvectorMissing++;
            } catch (final RuntimeException jvector) {
                jvectorMissing++;
            }
        }
        audit(startNanos, "quiescent-census reader=%d graph=%d checked=%d graphMissing=%d luceneMissing=%d jvectorMissing=%d"
                .formatted(readerIndex, titles.size(), checked, graphMissing, luceneMissing, jvectorMissing));
        assertEquals(0, graphMissing,
                "quiescent reader %d Store lost live transactions".formatted(readerIndex));
        assertEquals(0, luceneMissing,
                "quiescent reader %d Lucene index is not durable".formatted(readerIndex));
        assertEquals(0, jvectorMissing,
                "quiescent reader %d JVector index is not durable".formatted(readerIndex));
    }

    @SuppressWarnings("unchecked") // Lucene's class token cannot retain its entity type.
    private static LuceneIndex<IndexedArticle> luceneIndex(final GigaMap<IndexedArticle> articles) {
        return articles.index().get(LuceneIndex.class);
    }

    private static boolean jvectorHits(final VectorIndices<IndexedArticle> vectors, final ArticleState state) {
        /* Top-5, not top-1: dense random vectors in a small cube make near-duplicates
         * crowd the entity itself out of rank 1, and HNSW search is approximate, so a
         * top-1 title comparison flakes seed-dependently on a healthy index. A truly
         * unindexed entity still scores zero title hits in the top 5. */
        return vectors.get("articles").search(state.vector(), 5).toList().stream()
                .anyMatch(hit -> state.title().equals(hit.entity().title));
    }

    private static float[] randomVector(final Random random) {
        return new float[]{0.5f + random.nextFloat(), 0.5f + random.nextFloat(), 0.5f + random.nextFloat()};
    }

    private static void sleepJitter(final Random random, final int boundMillis) throws InterruptedException {
        final int delay = random.nextInt(boundMillis + 1);
        if (delay > 0) Thread.sleep(delay);
    }

    private Thread launch(final String name, final ThrowingRunnable task) {
        final Thread thread = Thread.ofVirtual().name(name).unstarted(() -> {
            try {
                task.run();
            } catch (final Throwable failure) {
                this.failures.add(failure);
            }
        });
        thread.start();
        return thread;
    }

    private static void audit(final long startNanos, final String message) {
        System.out.printf(Locale.ROOT, "SOAK t=+%.1fs %s%n",
                (System.nanoTime() - startNanos) / 1_000_000_000.0, message);
    }

    private void markActivity() {
        this.lastActivityNanos.set(System.nanoTime());
    }

    private void beat(final String worker, final String phase) {
        this.beats.computeIfAbsent(worker + "/" + phase, key -> new AtomicLong()).set(System.nanoTime());
    }

    /// Watches for silent workers: if no audit, restart, or converge step
    /// lands for a while, prints thread dumps with lock info so a hang can be
    /// diagnosed from the fork log without attaching a profiler.
    private void watchdogLoop(final long startNanos, final List<Thread> workers) throws Exception {
        int dumps = 0;
        while (!this.testDone && dumps < 2) {
            Thread.sleep(10_000L);
            if (this.testDone) return;
            final long idleSeconds =
                    (System.nanoTime() - this.lastActivityNanos.get()) / 1_000_000_000L;
            if (idleSeconds < 75L) continue;
            dumps++;
            audit(startNanos, "watchdog dump=%d idle=%ds".formatted(dumps, idleSeconds));
            final StringBuilder alive = new StringBuilder();
            for (final Thread worker : workers) {
                if (worker.isAlive()) alive.append(worker.getName()).append(' ');
            }
            System.out.println("SOAK alive workers: " + alive);
            /* Parked virtual threads appear in neither getAllStackTraces nor
             * jstack/jcmd output, so dump the watched workers directly: the
             * test holds their references and getStackTrace works while parked. */
            for (final Thread worker : workers) {
                if (!worker.isAlive()) continue;
                System.out.printf("SOAK worker-stack \"%s\" state=%s%n", worker.getName(), worker.getState());
                final StackTraceElement[] stack = worker.getStackTrace();
                for (int i = 0; i < Math.min(24, stack.length); i++) {
                    System.out.println("SOAK    at " + stack[i]);
                }
            }
            final long nowNanos = System.nanoTime();
            final List<String> beatNames = new ArrayList<>(this.beats.keySet());
            Collections.sort(beatNames);
            for (final String beat : beatNames) {
                System.out.printf("SOAK beat %s %ds ago%n", beat,
                        (nowNanos - this.beats.get(beat).get()) / 1_000_000_000L);
            }
            dumpThreads();
        }
    }

    private static void dumpThreads() {
        /* getAllStackTraces omits parked virtual threads (verified on JDK 26),
         * so alive soak workers are dumped separately in the watchdog via
         * getStackTrace; the MXBean dump here is only for deadlock detection. */
        final java.lang.management.ThreadMXBean beans = ManagementFactory.getThreadMXBean();
        try {
            final long[] deadlocked = beans.findDeadlockedThreads();
            if (deadlocked != null && deadlocked.length > 0) {
                System.out.println("SOAK DEADLOCKED threads=" + deadlocked.length);
            }
        } catch (final RuntimeException unsupported) {
            System.out.println("SOAK deadlock detection unavailable: " + unsupported);
        }
        final List<Map.Entry<Thread, StackTraceElement[]>> traces =
                new ArrayList<>(Thread.getAllStackTraces().entrySet());
        traces.sort(Comparator.comparing(e -> e.getKey().getName()));
        System.out.println("SOAK thread count=" + traces.size());
        for (final Map.Entry<Thread, StackTraceElement[]> entry : traces) {
            final Thread thread = entry.getKey();
            if (thread.getName().startsWith("ForkJoinPool") && entry.getValue().length == 0) continue;
            System.out.printf("SOAK  \"%s\" state=%s daemon=%s%n", thread.getName(), thread.getState(), thread.isDaemon());
            final StackTraceElement[] stack = entry.getValue();
            for (int i = 0; i < Math.min(16, stack.length); i++) {
                System.out.println("SOAK    at " + stack[i]);
            }
        }
    }

    private record ArticleState(String title, String body, float[] vector, long modifiedTx) {
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
