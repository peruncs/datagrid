package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import peruncs.datagrid.cluster.errors.ReseedRequiredException;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexRoot;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexedArticle;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.ReaderNode;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationLogRetention;
import peruncs.datagrid.cluster.node.replication.StoredReplicationCursorManager;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.datagrid.cluster.storage.aeron.crashtest.ArchiveArtifactMutator;
import peruncs.datagrid.cluster.storage.types.FileStoreCrashHooks;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/// Soak test: one writer, `-Dsoak.readers` readers (default three), threaded
/// load with jitter, index queries served while replication lands, and seeded
/// multi-op chaos.
///
/// This is deliberately heavy and lives outside the default gate: it only
/// runs under `-Psoak` (see the `soak` profile in peruncs-cluster/pom.xml),
/// tuned with `-Dsoak.seconds=`, `-Dsoak.seed=`, `-Dsoak.readers=`,
/// `-Dsoak.writer.threads=`, `-Dsoak.query.threads=`, `-Dsoak.payloadBytes=`,
/// `-Dsoak.restarts=`, `-Dsoak.lagSlots=`, `-Dsoak.miniCensus=`,
/// `-Dsoak.pollDelayMs=`, `-Dsoak.pollStallMs=`,
/// `-Dsoak.fsyncDelayMs=`, `-Dsoak.maxTornReads=`, `-Dsoak.corrupt=`,
/// `-Dsoak.gc=`, `-Dsoak.writerRestart=`, `-Dsoak.reseed=`,
/// `-Dsoak.retention=`, `-Dsoak.events=`, and
/// `-Dsoak.jfr.fail=`. The seed drives the
/// workload model and the chaos-op distribution, so reruns explore the same
/// space — but the exact op schedule is not bit-reproducible: skip paths
/// (parked victims, lagging readers) and reseed-vs-resume outcomes depend on
/// product timing and shift downstream draws. The event log below is the
/// authoritative per-run schedule. Coverage scales with independent seeds,
/// not duration: prefer `N x 15 s` sweeps across `N` seeds over one long run.
///
/// Every reader must reach an explicitly planned outcome (converged, reseed
/// parked, or corruption parked); a green run with silent reader loss fails.
/// The chaos loop must demonstrably fire (bounded restarts including at
/// least one abrupt), so a green soak can never be a no-chaos soak.
///
/// The test emits `SOAK` audit lines with elapsed times throughout the run,
/// so a watcher can follow setup, live throughput, restarts, convergence and
/// verification without attaching a profiler. Key transitions are also
/// appended as JSON lines to `soak.events` (default `target/soak-events.jsonl`)
/// for post-run replay.
///
/// The scenario has four phases:
///
/// 1. Seed one writer Store with ordinary indexed articles plus three stable
///    sentinel vectors. Copy that Store to every reader directory and record
///    the writer's baseline replication sequence.
/// 2. Run concurrent writer, reader-query, audit, and chaos workers. Writers
///    add, update, and remove articles under the one-writer lock. Query workers
///    verify ghost-title absence, cursor-gated graph state, Lucene visibility,
///    JVector visibility, and title/body checksums. The audit worker samples
///    lag, performs mini-censuses, and enforces the bounded-lag SLO.
/// 3. Dispatch a seeded, fixed rotation of single restart, dual restart,
///    slow-reader, CPU/GC, forced-GC burst, cursor-corruption,
///    rollback-cursor, live Archive-tail-corruption, writer-restart,
///    reseed-and-rejoin, and watermark-retention operations. There is no
///    privileged first operation: heavy ops (including writer-restart) are
///    schedulable from the first draw, and transport interruption stays
///    guaranteed because the first restart-capable op dealt is forced abrupt.
/// Coverage is gated per op type, not per full rotation: every ENABLED op
///    must close the run with at least one effective execution or one
///    park-caused skip (parked-victim or quorum-guard, where the parks
///    themselves are the evidence). An op disabled via its knob is absent
///    from the required set. Any other skip reason parked in the ledger is a
///    coverage gap and fails the run. When a deep op (an abrupt dual restart
///    replaying a long backlog) eats most of the workload window, the rotation
///    continues past it: in-window ops exercise load, tail ops exercise the
///    protocol, and coverage — not the clock — decides when chaos stops.
/// 4. Stop writers, converge or explicitly park every reader, then run strict
///    sampled checks and an uncapped graph/Lucene/JVector census. A run is green
///    only when all workers completed, the event log stayed writable, the
///    configured torn-read ceiling was respected, and every reader has a
///    planned fate.
///
/// The transaction model is the oracle, not the live writer Store. Reader
/// assertions are cursor-gated so lag is not mistaken for loss; positive index
/// checks allow a short refresh window. A reseed-required or corruption-parked
/// reader is a valid outcome only when its exact fate is logged. Silent reader
/// disappearance, unexpected exceptions, failed logging, phantom hits, stale
/// checksums, or convergence from a torn boundary fail the test.
///
/// A seed reproduces workload values and the chaos distribution, not necessarily
/// the exact schedule: parked victims, timing-dependent reseed/resume choices,
/// and reader lag can change later draws. Use the run/sequence fields in the
/// JSONL event log as the authoritative schedule. The test deliberately does
/// not claim to simulate UDP loss/duplication, disk-full ENOSPC, SIGSTOP, or
/// epoch regression. Reseed-and-rejoin is executed as the documented manual
/// procedure (copy the writer Store plus a valid cursor into the parked
/// reader's home, restart) driven by the chaos thread — the production library
/// has no automatic reseed, and the soak does not pretend otherwise.
///
/// The deterministic parked-reader-behind-a-purged-segment case (restart must
/// demand RESEED_REQUIRED, never torn data) lives as a first-class
/// integration test on [AeronStoreIntegrationIT]; the retention op here keeps
/// the chaos-driven variant: it proves a deletion boundary ahead of a live
/// reader is refused, then purges through the quorum minimum.
class AeronWriterReaderSoakIT {
    private static final String[] WORDS = {
            "alpha", "bravo", "cargo", "delta", "ember", "frost", "granite", "harbor", "ivory", "jungle",
            "karma", "lumen", "magnet", "noble", "onyx", "prism", "quartz", "ridge", "solar", "tundra"};
    private static final int SEED_ARTICLES = 60;
    private static final int STRICT_PRESENT_SAMPLE = 30;
    private static final int STRICT_ABSENT_SAMPLE = 10;
    /* No hyphens: the analyzer splits them into tokens, so a title query
     * for one sentinel would match every sentinel. Plain alphanumeric
     * titles stay single tokens with exact title: lookups. */
    private static final String SENTINEL_PREFIX = "soaksentinel";
    private static final float[][] SENTINELS = {{1.0f, 0.0f, 0.0f}, {0.0f, 1.0f, 0.0f}, {0.0f, 0.0f, 1.0f}};
    private static final long MID_SOAK_INDEX_WAIT_NANOS = TimeUnit.SECONDS.toNanos(2L);

    private final Object writeLock = new Object();
    private final Object eventLock = new Object();
    private final AtomicLong titleSequence = new AtomicLong();
    private final AtomicLong publishedTransactions = new AtomicLong();
    private final AtomicLong servedQueries = new AtomicLong();
    private final AtomicLong verifiedQueries = new AtomicLong();
    private final AtomicLong tornReads = new AtomicLong();
    /// Completed per-reader observability cross-checks (live vs durable cursor).
    private long metricsCrossChecks;
    private final AtomicLong completedRestarts = new AtomicLong();
    private final AtomicLong abruptRestarts = new AtomicLong();
    private final AtomicLong chaosOps = new AtomicLong();
    private final AtomicLong slowBursts = new AtomicLong();
    private final AtomicLong fsyncStalls = new AtomicLong();
    private final AtomicLong corruptionFired = new AtomicLong();
    private final AtomicLong corruptionTimeouts = new AtomicLong();
    private final AtomicLong liveFlips = new AtomicLong();
    private final AtomicLong reseedsDemanded = new AtomicLong();
    /// Manual reseeds actually executed by the reseed chaos op (parked reader
    /// revived, or live reader proactively reseeded). Distinct fate from
    /// `reseedsDemanded`: a demanded reseed parks; an executed reseed rejoins.
    private final AtomicLong reseedsExecuted = new AtomicLong();
    private final AtomicLong writerRestarts = new AtomicLong();
    private final AtomicLong gcBursts = new AtomicLong();
    private volatile byte gcBurstSink;
    private final AtomicLong retentionPurges = new AtomicLong();
    private final AtomicLong lagViolations = new AtomicLong();
    private final Map<Long, ArticleState> live = new ConcurrentHashMap<>();
    private final Map<String, ArticleState> liveByTitle = new ConcurrentHashMap<>();
    private final List<ArticleState> seedStates = new ArrayList<>();
    private final Set<String> removedTitles = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> tornSamples = new ConcurrentLinkedQueue<>();
    private final AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
    private final ConcurrentHashMap<String, AtomicLong> beats = new ConcurrentHashMap<>();
    private volatile boolean testDone;
    private volatile String soakOutcome = "cancelled";
    private String runId = "?";
    private final AtomicLong eventSeq = new AtomicLong();
    private final AtomicLong fateSeq = new AtomicLong();
    private final AtomicLong eventLogFailures = new AtomicLong();
    private final ConcurrentHashMap<Integer, String> readerFates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> chaosDealt = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> chaosEffective = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> chaosSkipped = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> chaosSkippedParkCaused = new ConcurrentHashMap<>();
    /// Per-op skip ledger keyed `op/reason`: the end-of-run gate and the
    /// failure diagnostics both read skip causes from here, never from logs.
    private final ConcurrentHashMap<String, AtomicLong> chaosSkippedByReason = new ConcurrentHashMap<>();
    /// The enabled op list captured by the chaos thread before its first
    /// dispatch; the coverage gate iterates exactly this set, so an op
    /// disabled via its knob is never required.
    private volatile List<String> enabledChaosOps = List.of();
    /// Set once the first restart-capable op claimed the guaranteed abrupt
    /// restart for this run. Replaces the old forced-first-op shape.
    private final AtomicBoolean guaranteedAbruptUsed = new AtomicBoolean();
    private final AtomicLong chaosIterations = new AtomicLong();
    private final AtomicLong chaosBusyNanos = new AtomicLong();
    private volatile long lastWriterSeq = -1L;
    private volatile int chaosRotationSize;

    private int lagSlots = 100;
    private int miniCensus = 20;
    private long baselineSeq;
    private long pollDelayMs = 2L;
    private long pollStallMs = 800L;
    private long fsyncDelayMs = 3L;
    private long maxTornReads = 64L;
    private boolean corruptEnabled = true;
    private boolean gcEnabled = true;
    private boolean writerRestartEnabled = true;
    private boolean reseedEnabled = true;
    private boolean retentionEnabled = true;
    private int payloadBytes = 0;
    private volatile long eventStartNanos;

    /// Exercises one writer against `-Dsoak.readers` readers under seeded
    /// threaded load with query traffic, slow-reader/fsync/CPU chaos, transport
    /// restarts, and cursor/Archive corruption, then requires every reader to
    /// converge or park fail-closed with a fully verified Store and index state.
    @Test
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    void oneWriterManyReadersSurviveThreadedLoadJitterAndRestarts() throws Exception {
        final long startNanos = System.nanoTime();
        final int soakSeconds = Integer.getInteger("soak.seconds", 30);
        final long seed = Long.getLong("soak.seed", 1L);
        final int readerCount = Integer.getInteger("soak.readers", 3);
        final int writerThreads = Integer.getInteger("soak.writer.threads", 3);
        final int queryThreads = Integer.getInteger("soak.query.threads", 2);
        final int restarts = Integer.getInteger("soak.restarts", 10);
        this.lagSlots = Integer.getInteger("soak.lagSlots", 100);
        this.miniCensus = Integer.getInteger("soak.miniCensus", 20);
        this.pollDelayMs = Long.getLong("soak.pollDelayMs", 2L);
        this.pollStallMs = Long.getLong("soak.pollStallMs", 800L);
        this.fsyncDelayMs = Long.getLong("soak.fsyncDelayMs", 3L);
        this.maxTornReads = Long.getLong("soak.maxTornReads", 64L);
        this.payloadBytes = Integer.getInteger("soak.payloadBytes", 0);
        if (readerCount < 1) throw new IllegalArgumentException("soak.readers must be at least 1");
        if (this.payloadBytes < 0) throw new IllegalArgumentException("soak.payloadBytes must be non-negative");
        if (this.maxTornReads < 0L) throw new IllegalArgumentException("soak.maxTornReads must be non-negative");
        this.corruptEnabled = Boolean.parseBoolean(System.getProperty("soak.corrupt", "true"));
        this.gcEnabled = Boolean.parseBoolean(System.getProperty("soak.gc", "true"));
        this.writerRestartEnabled = Boolean.parseBoolean(System.getProperty("soak.writerRestart", "true"));
        this.reseedEnabled = Boolean.parseBoolean(System.getProperty("soak.reseed", "true"));
        this.retentionEnabled = Boolean.parseBoolean(System.getProperty("soak.retention", "true"));
        this.runId = UUID.randomUUID().toString().substring(0, 8);
        this.eventStartNanos = startNanos;
        audit(startNanos, "start seed=%d seconds=%d readers=%d writerThreads=%d queryThreadsPerReader=%d payloadBytes=%d restarts=%d lagSlots=%d miniCensus=%d corrupt=%s"
                .formatted(seed, soakSeconds, readerCount, writerThreads, queryThreads, this.payloadBytes, restarts,
                        this.lagSlots, this.miniCensus, this.corruptEnabled));
        event(startNanos, "start", "seed=%d seconds=%d readers=%d writerThreads=%d queryThreadsPerReader=%d payloadBytes=%d restarts=%d"
                .formatted(seed, soakSeconds, readerCount, writerThreads, queryThreads, this.payloadBytes, restarts));

        final Path root = java.nio.file.Files.createTempDirectory("dg-aeron-soak-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = AeronStoreIntegrationIT.freePort();
        final int livePort = AeronStoreIntegrationIT.freePort();
        final int watermarkPort = AeronStoreIntegrationIT.freePort();
        final Path writerStore = root.resolve("writer-store");
        final Path[] readerStores = new Path[readerCount];
        final Path[] readerNodes = new Path[readerCount];
        final UUID[] readerIds = new UUID[readerCount];
        for (int i = 0; i < readerCount; i++) {
            readerStores[i] = root.resolve("reader-%d-store".formatted(i + 1));
            readerNodes[i] = root.resolve("reader-%d".formatted(i + 1));
            readerIds[i] = UUID.randomUUID();
        }
        final UUID writerNodeId = UUID.randomUUID();
        /* Watermark quorum for the retention chaos op: the writer authenticates
         * every soak reader, so deleteThrough is gated on all of them. */
        final Set<UUID> retentionReaders = this.retentionEnabled ? Set.of(readerIds) : Set.of();
        try (WriterHandle writerHandle = new WriterHandle(writerStore, root.resolve("writer"),
                clusterId, writerNodeId, generation, controlPort, livePort, watermarkPort,
                new AeronClusterReplicationTransportProvider().create(
                        AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, writerNodeId, generation, "writer", -1L,
                                controlPort, livePort, watermarkPort, retentionReaders)), retentionReaders)) {
            final ClusterReplicationTransport writerTransport = writerHandle.transport();
            writerTransport.positionProvider("store").init();
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final Random seedRandom = new Random(seed);
            final IndexRoot initial = new IndexRoot();
            initial.articles = GigaMap.New();
            AeronStoreIntegrationIT.configureIndexes(initial.articles);
            for (int i = 0; i < SEED_ARTICLES; i++) seedArticle(initial, seedRandom);
            seedSentinels(initial);
            final EmbeddedStorageManager seeded = AeronStoreIntegrationIT.startIndex(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            audit(startNanos, "seeded articles=%d".formatted(SEED_ARTICLES + SENTINELS.length));

            final ReplicationCursor baseline = writerHandle.latest();
            this.baselineSeq = baseline.logicalSequence();
            this.lastWriterSeq = this.baselineSeq;
            for (final Path readerStore : readerStores) AeronStoreIntegrationIT.copyDirectory(writerStore, readerStore);
            final EmbeddedStorageManager writer = AeronStoreIntegrationIT.startExistingIndex(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            writerHandle.install(writer, writer.root());
            final IndexRoot writerRoot = writerHandle.root();
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

                final AtomicLong[] readerRestarts = new AtomicLong[holders.length];
                final AtomicLong[] readerReseeds = new AtomicLong[holders.length];
                final AtomicLong[] readerCorruptionParks = new AtomicLong[holders.length];
                final AtomicLong[] readerLagStrikes = new AtomicLong[holders.length];
                for (int i = 0; i < holders.length; i++) {
                    readerRestarts[i] = new AtomicLong();
                    readerReseeds[i] = new AtomicLong();
                    readerCorruptionParks[i] = new AtomicLong();
                    readerLagStrikes[i] = new AtomicLong();
                }

                final long soakEndNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(soakSeconds);
                final List<Thread> workers = new ArrayList<>();
                for (int i = 0; i < writerThreads; i++) {
                    final int workerIndex = i;
                    workers.add(launch("soak-writer-%d".formatted(workerIndex),
                            () -> writeLoop(writerHandle, soakEndNanos,
                                    new Random(seed ^ 0x9E3779B9L ^ workerIndex))));
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
                final Topology topology = new Topology(clusterId, generation,
                        controlPort, livePort, watermarkPort);
                final ReaderSpec[] readerSpecs = new ReaderSpec[readerNodes.length];
                for (int i = 0; i < readerSpecs.length; i++) {
                    readerSpecs[i] = new ReaderSpec(readerNodes[i], readerStores[i], readerIds[i]);
                }
                workers.add(launch("soak-chaos", () -> chaosLoop(startNanos, root, topology, writerHandle,
                        holders, gates, readerSpecs, baseline, soakEndNanos, restarts,
                        readerRestarts, readerReseeds, readerCorruptionParks,
                        new Random(seed ^ 0xC0FFEE11L))));
                workers.add(launch("soak-audit", () -> auditLoop(startNanos, holders, gates,
                        soakEndNanos, readerLagStrikes)));
                /* The watchdog is deliberately not joined: it watches the join
                 * itself, so joining it would deadlock the test it guards. */
                launch("soak-watchdog", () -> watchdogLoop(startNanos, workers));
                audit(startNanos, "soak-start threads=%d".formatted(workers.size()));
                for (final Thread worker : workers) worker.join();
                assertNoWorkerFailures();
                audit(startNanos, "soak-end tx=%d queries=%d verified=%d torn=%d restarts=%d abrupt=%d chaosOps=%d slow=%d fsyncStalls=%d corrupt=%d liveFlips=%d"
                        .formatted(this.publishedTransactions.get(), this.servedQueries.get(),
                                this.verifiedQueries.get(), this.tornReads.get(), this.completedRestarts.get(),
                                this.abruptRestarts.get(), this.chaosOps.get(), this.slowBursts.get(),
                                this.fsyncStalls.get(), this.corruptionFired.get(), this.liveFlips.get()));
                if (!this.tornSamples.isEmpty()) {
                    System.out.println("SOAK torn-read samples (queries retried while replication landed):");
                    this.tornSamples.stream().limit(5).forEach(sample -> System.out.println("SOAK   " + sample));
                }
                assertTrue(this.tornReads.get() <= this.maxTornReads,
                        "benign torn-read retries exceeded the configured ceiling: %d > %d".formatted(
                                this.tornReads.get(), this.maxTornReads));
                 /* A green soak must have exercised chaos. Op durations vary
                 * from a second (restarts) to tens of seconds (history-replay
                 * injections), so the firing bar meters effort, not count: the
                 * chaos thread either finished its whole program or stayed
                 * busy for a substantial share of wall time. A dead or
                 * starved chaos thread satisfies neither. Transport
                 * interruption is guaranteed separately: the first
                 * restart-capable op dealt is forced abrupt. */
                if (restarts > 0) {
                    final long elapsedNanos = Math.max(1L, System.nanoTime() - startNanos);
                    final double busyFraction = (double) this.chaosBusyNanos.get() / elapsedNanos;
                    final boolean programComplete = this.chaosOps.get() >= restarts;
                    assertTrue(programComplete || busyFraction >= 0.30,
                            "chaos thread neither finished its program nor stayed busy: ops=%d restarts=%d iterations=%d/%d busy=%.0f%%".formatted(
                                    this.chaosOps.get(), restarts, this.chaosIterations.get(), this.chaosRotationSize,
                                    busyFraction * 100.0));
                    assertTrue(this.abruptRestarts.get() >= 1, "no abrupt restart executed; soak ran without transport interruption");
                    /* Coverage gate, per op type and independent of run length:
                     * the coverage loop stopped either at the time bound or
                     * after every ENABLED op resolved at least once, so the
                     * gate below fires on short runs exactly as on long ones.
                     * Two invariants per op: dealt == effective + skipped
                     * (a missing account means a code path forgot its mark),
                     * and effective >= 1 or park-caused skipped >= 1 (an op
                     * that only ever skipped for environmental reasons never
                     * landed, and the soak must not launder that into green).
                     * Ops disabled via their knobs are absent from
                     * enabledChaosOps and are never required. */
                    final List<String> gaps = new ArrayList<>();
                    for (final String op : this.enabledChaosOps) {
                        final long dealtCount = this.chaosDealt.getOrDefault(op, new AtomicLong()).get();
                        final long effectiveCount =
                                this.chaosEffective.getOrDefault(op, new AtomicLong()).get();
                        final long skippedCount =
                                this.chaosSkipped.getOrDefault(op, new AtomicLong()).get();
                        if (effectiveCount + skippedCount != dealtCount) {
                            gaps.add("%s accounting violated: dealt=%d effective=%d skipped=%d".formatted(
                                    op, dealtCount, effectiveCount, skippedCount));
                        }
                        if (effectiveCount >= 1) continue;
                        if (this.chaosSkippedParkCaused.getOrDefault(op, new AtomicLong()).get() >= 1) {
                            audit(startNanos, "chaos-coverage-park-waived op=%s dealt=%d skipped=%d"
                                    .formatted(op, dealtCount, skippedCount));
                            continue;
                        }
                        gaps.add("%s never landed: dealt=%d effective=0 skipped=%d".formatted(
                                op, dealtCount, skippedCount));
                    }
                    if (!gaps.isEmpty()) {
                        throw new AssertionError("chaos coverage incomplete: %s [%s]"
                                .formatted(gaps, chaosBreakdown()));
                    }
                    audit(startNanos, "chaos-coverage iterations=%d [%s]"
                            .formatted(this.chaosIterations.get(), chaosBreakdown()));
                }

                /* Resolve the writer pair through the handle, never through
                 * the setup-time local: the writer-restart chaos op swaps the
                 * transport underfoot. */
                final ReplicationCursor target;
                synchronized (this.writeLock) {
                    target = writerHandle.latest();
                }
                audit(startNanos, "converge-start target=%d".formatted(target.logicalSequence()));
                int converged = 0;
                int parked = 0;
                for (int r = 0; r < holders.length; r++) {
                    markActivity();
                    if (holders[r] == null) {
                        final long reseeds = readerReseeds[r].get() + readerCorruptionParks[r].get();
                        assertTrue(reseeds > 0, "reader %d was lost without a recorded reseed/corruption outcome".formatted(r));
                        /* Counters alone are not enough: every parked reader
                         * must carry the attributable fate recorded by the op
                         * that parked it, so the run cannot pass on an
                         * unplanned loss that merely bumped a counter. */
                        final String fate = this.readerFates.get(r);
                        assertNotNull(fate, "reader %d parked without an attributable chaos fate".formatted(r));
                        parked++;
                        audit(startNanos, "converge-parked reader=%d fate=%s reseeds=%d corruptionParks=%d"
                                .formatted(r, fate, readerReseeds[r].get(), readerCorruptionParks[r].get()));
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
                    audit(startNanos, "converged reader=%d restarts=%d".formatted(r, readerRestarts[r].get()));
                    converged++;
                }
                /* Every reader reaches a planned outcome: converged healthy, or
                 * parked after an intentionally demanded reseed/corruption
                 * failure. One surviving reader is no longer enough. */
                assertTrue(converged >= 1, "no reader survived the soak to converge; reseeds demanded=%d".formatted(this.reseedsDemanded.get()));
                assertEquals(holders.length, converged + parked, "every reader must reach a planned outcome (converged=%d parked=%d)".formatted(converged, parked));
                /* Convergence ledger: per-reader fates live on their own
                 * events; this row is the machine-checkable total. */
                event(startNanos, "converge-summary", "readers=%d converged=%d parked=%d"
                        .formatted(holders.length, converged, parked));

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
                if (this.corruptionTimeouts.get() > 0) {
                    audit(startNanos, "corruption-timeouts=%d (parked, see events)".formatted(this.corruptionTimeouts.get()));
                }
                /* A blind run is a failed run: every logging failure so far
                 * lands in the worker-failure queue, so the outer finally
                 * reports result=failed instead of a contradicting ok. */
                if (this.eventLogFailures.get() > 0) {
                    final AssertionError blind =
                            new AssertionError("soak event log lost entries; the run is blind");
                    this.failures.add(blind);
                    throw blind;
                }
                /* Write the success marker before the outer finally emits the
                 * terminal outcome. A marker-write failure is therefore
                 * included in the outcome's failure-first verdict. */
                this.testDone = true;
                audit(startNanos, "soak-ok tx=%d queries=%d verified=%d torn=%d restarts=%d abrupt=%d liveFlips=%d reseeds=%d rejoined=%d writerRestarts=%d gcBursts=%d retentionPurges=%d"
                        .formatted(this.publishedTransactions.get(), this.servedQueries.get(),
                                this.verifiedQueries.get(), this.tornReads.get(), this.completedRestarts.get(),
                                this.abruptRestarts.get(), this.liveFlips.get(),
                                this.reseedsDemanded.get(), this.reseedsExecuted.get(),
                                this.writerRestarts.get(), this.gcBursts.get(), this.retentionPurges.get()));
                event(startNanos, "soak-ok", "tx=%d queries=%d verified=%d restarts=%d".formatted(
                        this.publishedTransactions.get(), this.servedQueries.get(),
                        this.verifiedQueries.get(), this.completedRestarts.get()));
                assertNoWorkerFailures();
                this.soakOutcome = "ok";
            } finally {
                /* The locally captured manager may belong to a pre-restart
                 * writer incarnation (the writer-restart op swaps it); the
                 * handle owns shutdown of whichever pair is current. */
                writerHandle.close();
            }
        } finally {
            /* The outcome record fires on every termination — green, failed,
             * or cancelled — so a dead run can never masquerade as a run that
             * simply never happened. */
            final String result = !this.failures.isEmpty() ? "failed"
                    : ("ok".equals(this.soakOutcome) ? "ok" : "cancelled");
            event(startNanos, "outcome", "result=%s readers=%d tx=%d queries=%d verified=%d torn=%d restarts=%d abrupt=%d corruption=%d reseeds=%d rejoined=%d eventLogFailures=%d".formatted(
                    result, readerCount, this.publishedTransactions.get(), this.servedQueries.get(),
                    this.verifiedQueries.get(), this.tornReads.get(), this.completedRestarts.get(), this.abruptRestarts.get(),
                    this.corruptionFired.get(), this.reseedsDemanded.get(), this.reseedsExecuted.get(), this.eventLogFailures.get()));
            /* Cleanup must never mask the result: on a failed run, readers can
             * still be mid-cleanup elsewhere and a racy directory delete here
             * would otherwise replace the real failure with a filesystem one. */
            try {
                AeronStoreIntegrationIT.delete(root);
            } catch (final Exception cleanup) {
                System.out.printf(Locale.ROOT, "SOAK cleanup failed (temp dir retained): %s%n", cleanup);
            }
        }
    }

    private void seedArticle(final IndexRoot initial, final Random random) {
        final String title = "soakt" + this.titleSequence.incrementAndGet();
        final String body = WORDS[random.nextInt(WORDS.length)] + "s" + this.titleSequence.get();
        final float[] vector = randomVector(random);
        final long id = initial.articles.add(new IndexedArticle(title, body, vector));
        /* Seeds predate every soak transaction: modifiedTx -1 sorts them
         * below any applied depth, so they are always assertable. */
        final ArticleState state = new ArticleState(title, body, vector, -1L, checksum(title, body));
        this.live.put(id, state);
        this.liveByTitle.put(title, state);
        this.seedStates.add(state);
    }

    /// Deterministic orthogonal sentinels used as stability fixtures. They are
    /// never updated or removed, so their graph and Lucene checks are exact
    /// with no model-lag caveat — they ride every strict present-sample as
    /// always-assertable entries alongside their transaction-model records.
    /// They assert nothing about vector ranks: axis vectors are HNSW outliers
    /// whose approximate recall is nondeterministic run to run, so any rank
    /// assertion would flake on the algorithm.
    private void seedSentinels(final IndexRoot initial) {
        for (int i = 0; i < SENTINELS.length; i++) {
            final String title = SENTINEL_PREFIX + i;
            final String body = "sentinelbody" + i;
            initial.articles.add(new IndexedArticle(title, body, SENTINELS[i].clone()));
            final ArticleState state = new ArticleState(title, body, SENTINELS[i].clone(),
                    -1L, checksum(title, body));
            this.liveByTitle.put(title, state);
            this.seedStates.add(state);
        }
    }

    private static boolean isSentinel(final String title) {
        return title.startsWith(SENTINEL_PREFIX);
    }

    private static int checksum(final String title, final String body) {
        final CRC32C crc = new CRC32C();
        final byte[] titleBytes = title.getBytes(StandardCharsets.UTF_8);
        crc.update(titleBytes, 0, titleBytes.length);
        final byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        crc.update(bodyBytes, 0, bodyBytes.length);
        return (int) crc.getValue();
    }

    /// Applied depth of one reader: how many soak transactions its durable
    /// cursor has absorbed past the baseline. Entities modified below this
    /// depth are materialized in its Store; graph state rides the cursor
    /// exactly, so they are assertable with no refresh-lag caveat. Returns
    /// -1 when the boundary is unknown (reader parked or unreadable).
    private long readerDepth(final ReaderNode node) {
        try {
            final ReplicationCursor cursor = node.persistedCursor();
            if (cursor == null) return -1L;
            return Math.max(0L, cursor.logicalSequence() - this.baselineSeq);
        } catch (final RuntimeException unreadable) {
            return -1L;
        }
    }

    private void writeLoop(final WriterHandle writer,
                           final long soakEndNanos, final Random random) throws Exception {
        final String worker = Thread.currentThread().getName();
        while (System.nanoTime() < soakEndNanos) {
            beat(worker, "store");
            final int op = random.nextInt(100);
            synchronized (this.writeLock) {
                /* Resolve the current writer pair under the mutation lock: a
                 * writer-restart chaos op swaps both under this same lock, so
                 * one iteration can never publish through a disposed pair. */
                final IndexRoot writerRoot = writer.root();
                final ClusterReplicationTransport writerTransport = writer.transport();
                if (op < 55) addBatch(writerRoot, random, 1 + random.nextInt(4));
                else if (op < 80) updateOne(writerRoot, random);
                else removeOne(writerRoot, random);
                /* Fsync-delay chaos rides the real AtomicFileWriter hook when
                 * the checkpoint path fires synchronously on this thread;
                 * when it does not fire the occasional writer stall below
                 * still widens the backlog window deterministically. Kept
                 * mild: the lag SLO must observe backlog, not drown in it. */
                final long stallMs = random.nextInt(100) < 2 ? 20L + random.nextInt(40) : 0L;
                if (stallMs > 0) this.fsyncStalls.incrementAndGet();
                FileStoreCrashHooks.runWithHook(
                        (phase, path) -> {
                            if (this.fsyncDelayMs > 0) sleepUnchecked(this.fsyncDelayMs);
                            if (stallMs > 0) sleepUnchecked(stallMs);
                        },
                        () -> writerRoot.articles.store());
                this.publishedTransactions.incrementAndGet();
            }
            /* Publish the writer boundary at each transaction. Keep the
             * position read outside the mutation lock: the boundary is a
             * writer-side snapshot, and the guard only needs a current upper
             * bound rather than lock-coupled sequencing. */
            this.lastWriterSeq = writer.latest().logicalSequence();
            /* Occasional writer-side stall widens the backlog even when the
             * fsync hook has nothing to delay on this thread. */
            sleepJitter(random, 5);
        }
    }

    private static void sleepUnchecked(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("soak delay interrupted", interrupted);
        }
    }

    /// Builds one transaction body sized toward `soak.payloadBytes` (0 keeps
    /// the compact default). The pad is deterministic word noise: the title
    /// stays the exact Lucene key, and the checksum covers title+body either
    /// way, so strict verification is unchanged at any payload size.
    private String transactionBody(final Random random, final long sequence) {
        final String base = WORDS[random.nextInt(WORDS.length)] + "s" + sequence;
        if (this.payloadBytes <= 0) return base;
        final StringBuilder padded = new StringBuilder(this.payloadBytes);
        padded.append(base).append(' ');
        while (padded.length() < this.payloadBytes) {
            padded.append(WORDS[random.nextInt(WORDS.length)]).append(' ');
        }
        return padded.toString();
    }

    private void addBatch(final IndexRoot writerRoot, final Random random, final int count) {
        for (int i = 0; i < count; i++) {
            final String title = "soakt" + this.titleSequence.incrementAndGet();
            final String body = transactionBody(random, this.titleSequence.get());
            final float[] vector = randomVector(random);
            final long id = writerRoot.articles.add(new IndexedArticle(title, body, vector));
            final ArticleState state = new ArticleState(title, body, vector,
                    this.publishedTransactions.get(), checksum(title, body));
            this.live.put(id, state);
            this.liveByTitle.put(title, state);
        }
    }

    private void updateOne(final IndexRoot writerRoot, final Random random) {
        final Long[] ids = this.live.keySet().toArray(new Long[0]);
        if (ids.length == 0) return;
        final long id = ids[random.nextInt(ids.length)];
        final ArticleState current = this.live.get(id);
        if (current == null || isSentinel(current.title())) return;
        final String body = transactionBody(random, this.titleSequence.incrementAndGet());
        final float[] vector = randomVector(random);
        writerRoot.articles.update(id, article -> {
            article.body = body;
            article.vector = vector;
        });
        final ArticleState next = new ArticleState(current.title(), body, vector,
                this.publishedTransactions.get(), checksum(current.title(), body));
        this.live.put(id, next);
        this.liveByTitle.put(current.title(), next);
    }

    private void removeOne(final IndexRoot writerRoot, final Random random) {
        final List<Long> candidates = new ArrayList<>();
        for (final Map.Entry<Long, ArticleState> entry : this.live.entrySet()) {
            if (!isSentinel(entry.getValue().title())) candidates.add(entry.getKey());
        }
        if (candidates.isEmpty()) return;
        final Long id = candidates.get(random.nextInt(candidates.size()));
        final ArticleState removed = this.live.remove(id);
        if (removed == null) return;
        writerRoot.articles.removeById(id);
        this.liveByTitle.remove(removed.title(), removed);
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
                /* Re-read under the gate: chaos may have parked (closed and
                 * nulled) this reader between iterations; a stale reference
                 * would query a closed Store. */
                final ReaderNode node = holders[readerIndex];
                if (node == null) {
                    Thread.sleep(50L);
                    continue;
                }
                /* Joined read: the product contract requires reads to hold the
                 * coordinator's read side. An unjoined query can trigger a
                 * lazy vector-graph rebuild that races the next batch's bulk
                 * materialization — wedging the reader and reading torn
                 * entities. Verification is gated on the reader's own applied
                 * depth: only materialized entities are asserted, so lag can
                 * never masquerade as divergence (lag is covered by the SLO
                 * instead). Known-title Lucene and dense-vector lookups are
                 * asserted only after the reader cursor proves materialization;
                 * arbitrary vector probes remain load rather than an oracle. */
                final long depth = readerDepth(node);
                node.graphCoordinator().read(() -> queryOnce(node, random, depth, readerIndex));
                this.servedQueries.incrementAndGet();
            } catch (final RuntimeException torn) {
                /* Only enumerated benign races are retried here. Anything else
                 * is a product defect: record it as a worker failure so the
                 * soak fails instead of laundering it as load noise. */
                if (isBenignTornRead(torn)) {
                    this.tornReads.incrementAndGet();
                    if (this.tornSamples.size() < 16) {
                        this.tornSamples.add("%s: %s".formatted(
                                torn.getClass().getSimpleName(), String.valueOf(torn.getMessage())));
                    }
                } else {
                    this.failures.add(torn);
                    throw torn;
                }
            } finally {
                gates[readerIndex].readLock().unlock();
            }
            sleepJitter(random, 3);
        }
    }

    /// Narrow classifier for races that replication may legitimately cause
    /// mid-query. Everything outside this list fails the soak: catching every
    /// RuntimeException as "torn" would hide real index defects.
    private static boolean isBenignTornRead(final RuntimeException failure) {
        if (failure instanceof ConcurrentModificationException
                || failure instanceof NoSuchElementException
                || failure instanceof ArrayIndexOutOfBoundsException) {
            return true;
        }
        if (failure instanceof IllegalStateException illegal) {
            final String message = String.valueOf(illegal.getMessage()).toLowerCase(Locale.ROOT);
            return message.contains("concurrent") || message.contains("torn")
                    || message.contains("modified") || message.contains("closed");
        }
        return false;
    }

    /// True when the reader is too far behind for a graceful stop. Stopping at
    /// the latest boundary under a live writer chases a moving target, so chaos
    /// skips graceful stops on buried readers instead of manufacturing a hang.
    /// Abrupt restarts need no stop and are unaffected; the final converge
    /// stops run after writers join, when latest is fixed. Unknown boundaries
    /// fail open toward attempting the stop.
    private boolean isBuried(final ReaderNode node) {
        final long writerSeq = this.lastWriterSeq;
        if (writerSeq < 0) return false;
        try {
            final ReplicationCursor cursor = node.persistedCursor();
            return cursor != null && writerSeq - cursor.logicalSequence() > 2L * this.lagSlots;
        } catch (final RuntimeException unreadable) {
            /* An unreadable cursor cannot prove that the boundary is safe.
             * Skip the graceful stop rather than reintroducing the moving-tail
             * hang that this guard exists to prevent. */
            return true;
        }
    }

    /// Mid-soak verification checks ghost titles immediately and checks a
    /// cursor-gated entity against the graph and both indexes. Index refresh
    /// can trail materialization, so positive index checks use a short bounded
    /// retry; a cursor-gated miss that remains after that window is a failure.
    /// The bounded window is only a meaningful divergence signal for near-live
    /// readers: a reader replaying a deep backlog bundles index maintenance
    /// into batches far larger than the window, so positive checks there are
    /// load (they would flake, not catch divergence) — buried readers get
    /// their index evidence from mini-censuses and the final 20 s
    /// post-convergence verification. Graph-content and ghost-title checks
    /// stay unconditional: both are exact at the cursor, lag or not.
    ///
    /// @param depth reader applied depth from [#readerDepth]; negative means unknown
    private void queryOnce(final ReaderNode node, final Random random, final long depth, final int readerIndex) {
        final IndexRoot root = (IndexRoot) node.rootObject();
        final long cursorSeq = depth < 0L ? -1L : this.baselineSeq + depth;
        final boolean nearLive = cursorSeq >= 0L
                && Math.max(0L, this.lastWriterSeq - cursorSeq) <= this.lagSlots;
        final int pick = random.nextInt(100);
        if (pick < 30) {
            /* Ghost titles can never exist: a hit is phantom index state, and
             * no refresh lag can excuse it. */
            final String ghost = "soakghost" + Math.abs(random.nextLong());
            if (!luceneIndex(root.articles).query("title:" + ghost).isEmpty()) {
                throw new AssertionError("soak query served phantom title " + ghost);
            }
            this.verifiedQueries.incrementAndGet();
        } else if (pick < 60) {
            if (!nearLive) return;
            final ArticleState expected = randomAppliedState(depth, random);
            if (expected == null) return;
            assertMidSoakIndexVisible("Lucene missed materialized title %s on reader %d"
                            .formatted(expected.title(), readerIndex),
                    () -> luceneIndex(root.articles).query("title:" + expected.title()).size() == 1);
            this.verifiedQueries.incrementAndGet();
        } else {
            final VectorIndices<IndexedArticle> vectors = root.articles.index().get(VectorIndices.Category());
            if (random.nextBoolean()) {
                if (!nearLive) return;
                final ArticleState expected = randomAppliedState(depth, random);
                if (expected == null) return;
                if (!isSentinel(expected.title())) {
                    assertMidSoakIndexVisible("JVector missed materialized title %s on reader %d"
                                    .formatted(expected.title(), readerIndex),
                            () -> jvectorHits(vectors, expected));
                }
                if (!isSentinel(expected.title())) this.verifiedQueries.incrementAndGet();
            } else {
                vectors.get("articles").search(randomVector(random), 3);
            }
        }
        /* One in ten queries carries an exact graph spot-check: a random
         * materialized entity must resolve with its modeled body. */
        if (depth >= 0 && random.nextInt(10) == 0) verifyGraphSpot(root, random, depth);
    }

    private ArticleState randomAppliedState(final long depth, final Random random) {
        if (depth < 0L) return null;
        int count = 0;
        for (final ArticleState state : this.liveByTitle.values()) {
            if (state.modifiedTx() < depth) count++;
        }
        if (count == 0) return null;
        final int target = random.nextInt(count);
        int index = 0;
        for (final ArticleState state : this.liveByTitle.values()) {
            if (state.modifiedTx() < depth && index++ == target) return state;
        }
        throw new AssertionError("applied entity disappeared while selecting a soak query");
    }

    /// Asserts one random materialized entity against the reader graph: title
    /// presence, body, and the title/body checksum from the model.
    private void verifyGraphSpot(final IndexRoot root, final Random random, final long depth) {
        final List<ArticleState> applied = new ArrayList<>();
        for (final ArticleState state : this.liveByTitle.values()) {
            if (state.modifiedTx() < depth) applied.add(state);
        }
        if (applied.isEmpty()) return;
        final ArticleState expected = applied.get(random.nextInt(applied.size()));
        final String[] body = {null};
        final boolean[] found = {false};
        root.articles.iterate(article -> {
            if (expected.title().equals(article.title)) {
                found[0] = true;
                body[0] = article.body;
            }
        });
        if (!found[0]) {
            throw new AssertionError("soak graph lost materialized title " + expected.title());
        }
        if (!expected.body().equals(body[0])) {
            throw new AssertionError("soak graph holds stale body for " + expected.title());
        }
        if (checksum(expected.title(), body[0]) != expected.checksum()) {
            throw new AssertionError("soak graph checksum mismatch for " + expected.title());
        }
        this.verifiedQueries.incrementAndGet();
    }

    /// Fixed chaos rotation with a seeded start offset. A pure random draw
    /// starves rare injections in short runs; rotation deals every enabled
    /// type regularly while victims, timings, and payloads stay seeded-random.
    /// Per-type dealt/effective/skipped counters prove what actually landed.
    private static final List<String> CHAOS_ROTATION =
            List.of("single", "dual", "slow", "cpu", "gc", "cursor", "rollback", "segment",
                    "writer-restart", "reseed", "retention");

    private void chaosLoop(final long startNanos, final Path root, final Topology topology,
                           final WriterHandle writer,
                           final ReaderNode[] holders, final ReadWriteLock[] gates,
                           final ReaderSpec[] readers, final ReplicationCursor baseline,
                           final long soakEndNanos, final int restarts,
                           final AtomicLong[] readerRestarts, final AtomicLong[] readerReseeds,
                           final AtomicLong[] readerCorruptionParks, final Random random) throws Exception {
        final List<String> rotation = new ArrayList<>(CHAOS_ROTATION);
        if (!this.corruptEnabled) {
            rotation.removeIf(op -> op.equals("cursor") || op.equals("rollback") || op.equals("segment"));
            rotation.add("single");
        }
        if (!this.gcEnabled) rotation.remove("gc");
        if (!this.writerRestartEnabled) rotation.remove("writer-restart");
        if (!this.reseedEnabled) rotation.remove("reseed");
        if (!this.retentionEnabled) rotation.remove("retention");
        this.chaosRotationSize = rotation.size();
        this.enabledChaosOps = List.copyOf(rotation);
        int rotationPos = random.nextInt(rotation.size());
        /* Complete one full rotated schedule independently of weighted work:
         * dual/corruption injections may count two units of progress, but they
         * must not make the remaining chaos types disappear from a run. While
         * coverage is still incomplete the inter-op pause stays sub-second:
         * the end-of-run gate requires every enabled op dealt and resolved
         * exactly once as effective-or-park-skipped, even on short runs, so
         * the heavy ops (writer-restart included) must be schedulable EARLY
         * rather than waiting out a full rotation. Once every enabled op has
         * landed, the pause stretches for load spread. */
        boolean tailAnnounced = false;
        while (restarts > 0 && (this.chaosOps.get() < restarts
                || !coverageComplete())) {
            beat(Thread.currentThread().getName(), "sleep");
            Thread.sleep(coverageComplete() ? 500L + random.nextInt(2_500)
                    : 100L + random.nextInt(400));
            markActivity();
            /* Coverage is per-op, not per-window: when one deep op eats most
             * of the workload window (a replaying abrupt dual-restart can take
             * tens of seconds), the remaining enabled ops still must land, so
             * the rotation continues into the post-workload tail where ops
             * are fast without load. In-window ops exercise load; tail ops
             * exercise protocol. A run that can never finish coverage fails at
             * the test timeout, not here. */
            if (System.nanoTime() >= soakEndNanos && !coverageComplete()) {
                if (!tailAnnounced) {
                    tailAnnounced = true;
                    audit(startNanos, "chaos-coverage-tail (workload over; finishing op coverage unloaded)");
                    event(startNanos, "coverage-tail", "ops=%d".formatted(this.chaosOps.get()));
                }
            } else if (System.nanoTime() >= soakEndNanos) {
                return;
            }
            final String selected = rotation.get(rotationPos % rotation.size());
            rotationPos++;
            this.chaosIterations.incrementAndGet();
            dealt(selected);
            event(startNanos, "op-selected", "op=%s pos=%d".formatted(selected, rotationPos));
            final ThrowingSupplier<OpOutcome> run = switch (selected) {
                case "single" -> () -> restartSingle(startNanos, holders, gates, baseline,
                        readerRestarts, readerReseeds, random);
                case "dual" -> () -> restartDual(startNanos, holders, gates, baseline,
                        readerRestarts, readerReseeds, random);
                case "slow" -> () -> slowReaderBurst(startNanos, holders, random);
                case "cpu" -> () -> cpuGarbageBurst(startNanos, random);
                case "gc" -> () -> gcBurst(startNanos, random);
                case "cursor" -> () -> corruptCursorFile(startNanos, holders, gates,
                        readerCorruptionParks, random);
                case "rollback" -> () -> rollbackCursor(startNanos, holders, gates, baseline,
                        readerReseeds, readerRestarts, random);
                case "segment" -> () -> corruptLiveSegment(startNanos, root, writer.root(), writer.transport(),
                        holders, gates, readerCorruptionParks, random);
                case "writer-restart" -> () -> writerRestart(startNanos, root, writer, holders, gates, random);
                case "reseed" -> () -> reseedAndRejoin(startNanos, writer, holders, gates, readers, topology, random);
                case "retention" -> () -> retentionPurge(startNanos, writer, holders, gates, readers, random);
                default -> throw new IllegalStateException("unknown chaos op " + selected);
            };
            accountOp(startNanos, selected, runOp(run));
        }
    }

    /// Runs one chaos op while metering busy time. The firing gate proves the
    /// chaos thread worked (busy fraction of wall time) or finished its whole
    /// program — an op-count bar would punish slow history-replay injections
    /// for taking tens of seconds each.
    private <T> T runOp(final ThrowingSupplier<T> op) throws Exception {
        final long opStart = System.nanoTime();
        try {
            return op.get();
        } finally {
            this.chaosBusyNanos.addAndGet(System.nanoTime() - opStart);
        }
    }

    /// Accounts one dispatched op exactly once: effective outcomes advance loop
    /// progress by their weight, skips are logged with cause. Throws propagate
    /// without accounting — the run already fails.
    private void accountOp(final long startNanos, final String op, final OpOutcome outcome) {
        if (outcome.isEffective()) {
            effective(op);
            this.chaosOps.addAndGet(outcome.weight());
        } else {
            skipped(startNanos, op, outcome.skipReason());
        }
    }

    /// Chaos coverage accounting: dealt counts every dispatched op, effective
    /// counts ops that reached a terminal assertion (completed, parked, or
    /// tolerated — never silently skipped), skipped counts early exits with a
    /// logged reason. The end-of-soak gate requires effective coverage per
    /// enabled type once a full rotation cycle completes.
    private void dealt(final String op) {
        this.chaosDealt.computeIfAbsent(op, ignored -> new AtomicLong()).incrementAndGet();
    }

    private void effective(final String op) {
        this.chaosEffective.computeIfAbsent(op, ignored -> new AtomicLong()).incrementAndGet();
    }

    private void skipped(final long startNanos, final String op, final String reason) {
        this.chaosSkipped.computeIfAbsent(op, ignored -> new AtomicLong()).incrementAndGet();
        /* Only quorum-guard and parked-victim skips are park-caused: they prove
         * no eligible victim existed because readers were already parked. Every
         * other reason is an injection that never landed. */
        this.chaosSkippedByReason.computeIfAbsent(op + "/" + reason, ignored -> new AtomicLong()).incrementAndGet();
        if (reason.equals("quorum-guard") || reason.equals("parked-victim")) {
            this.chaosSkippedParkCaused.computeIfAbsent(op, ignored -> new AtomicLong()).incrementAndGet();
        }
        event(startNanos, "op-skipped", "op=%s reason=%s".formatted(op, reason));
    }

    /// One-line per-op ledger: `dealt/effective/skipped`, disabled ops marked
    /// `off`, and — when a type skipped at all — the reason counts behind it,
    /// so a coverage failure diagnosably names the blocking cause.
    private String chaosBreakdown() {
        final StringBuilder breakdown = new StringBuilder();
        for (final String op : CHAOS_ROTATION) {
            if (!breakdown.isEmpty()) breakdown.append(' ');
            if (!this.enabledChaosOps.contains(op)) {
                breakdown.append(op).append("=off");
                continue;
            }
            breakdown.append("%s=%d/%d/%d".formatted(op,
                    this.chaosDealt.getOrDefault(op, new AtomicLong()).get(),
                    this.chaosEffective.getOrDefault(op, new AtomicLong()).get(),
                    this.chaosSkipped.getOrDefault(op, new AtomicLong()).get()));
            final List<String> reasons = new ArrayList<>();
            for (final Map.Entry<String, AtomicLong> reason : this.chaosSkippedByReason.entrySet()) {
                if (reason.getKey().startsWith(op + "/")) {
                    reasons.add("%s:%d".formatted(reason.getKey().substring(op.length() + 1), reason.getValue().get()));
                }
            }
            if (!reasons.isEmpty()) breakdown.append(reasons.stream().sorted().toList());
        }
        return breakdown.toString();
    }

    /// Abruptness draw shared by every restart-capable op: the FIRST such op
    /// dealt in a run is forced abrupt, so transport interruption is covered
    /// by construction no matter where the rotation starts; later ops coin-flip.
    private boolean drawAbrupt(final Random random) {
        if (this.guaranteedAbruptUsed.compareAndSet(false, true)) return true;
        return random.nextBoolean();
    }

    /// True when every enabled op has been dealt and resolved at least once
    /// — effective, or skipped for a park-caused reason. This is the tempo
    /// signal for the chaos loop; the hard gate with diagnostics runs once
    /// workers have joined.
    private boolean coverageComplete() {
        for (final String op : this.enabledChaosOps) {
            if (this.chaosEffective.getOrDefault(op, new AtomicLong()).get() >= 1) continue;
            if (this.chaosSkippedParkCaused.getOrDefault(op, new AtomicLong()).get() >= 1) continue;
            return false;
        }
        return true;
    }

    private OpOutcome restartSingle(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                                        final ReplicationCursor baseline, final AtomicLong[] readerRestarts,
                                        final AtomicLong[] readerReseeds, final Random random) {
        final int readerIndex = random.nextInt(holders.length);
        final boolean abrupt = drawAbrupt(random);
        if (abrupt) this.abruptRestarts.incrementAndGet();
        if (!abrupt) {
            final ReaderNode peeked = holders[readerIndex];
            if (peeked != null && isBuried(peeked)) {
                audit(startNanos, "reader-restart-skipped reader=%d (buried reader)".formatted(readerIndex));
                return OpOutcome.skipped("buried-reader");
            }
        }
        return restartSingleAt(startNanos, holders, gates, baseline, readerIndex, abrupt,
                readerRestarts, readerReseeds);
    }

    private OpOutcome restartSingleAt(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                                          final ReplicationCursor baseline, final int readerIndex, final boolean abrupt,
                                          final AtomicLong[] readerRestarts, final AtomicLong[] readerReseeds) {
        if (holders[readerIndex] == null) {
            audit(startNanos, "reader-restart-skipped reader=%d (awaiting reseed)".formatted(readerIndex));
            return OpOutcome.skipped("parked-victim");
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
                readerRestarts[readerIndex].incrementAndGet();
                event(startNanos, "reader-restart", "reader=%d abrupt=%s resume=%d".formatted(
                        readerIndex, abrupt, resume.logicalSequence()));
            } catch (final ReseedRequiredException reseed) {
                /* Close the retained Store only when this reader is parked;
                 * no subsequent same-JVM reopen is attempted. */
                node.close();
                holders[readerIndex] = null;
                this.reseedsDemanded.incrementAndGet();
                readerReseeds[readerIndex].incrementAndGet();
                audit(startNanos, "reader-reseed-demanded reader=%d resume=%d (%s)"
                        .formatted(readerIndex, resume.logicalSequence(), reseed.getMessage()));
                event(startNanos, "reader-reseed", "reader=%d abrupt=%s".formatted(readerIndex, abrupt));
                recordFate(readerIndex, "restart:reseed-demanded abrupt=%s".formatted(abrupt));
                if (!abrupt) throw reseed;
            }
        } finally {
            gates[readerIndex].writeLock().unlock();
        }
        audit(startNanos, "reader-restart reader=%d abrupt=%s".formatted(readerIndex, abrupt));
        return OpOutcome.effective();
    }

    /// Overlapping restart storm: two readers restart at once so a reseed and
    /// a live tail overlap. Locks are always taken in index order.
    private OpOutcome restartDual(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                                      final ReplicationCursor baseline, final AtomicLong[] readerRestarts,
                                      final AtomicLong[] readerReseeds, final Random random) {
        int first = random.nextInt(holders.length);
        int second = random.nextInt(holders.length);
        if (first == second) second = (second + 1) % holders.length;
        final int low = Math.min(first, second);
        final int high = Math.max(first, second);
        if (holders[low] == null && holders[high] == null) {
            audit(startNanos, "dual-restart-skipped readers=%d,%d (both awaiting reseed)".formatted(low, high));
            return OpOutcome.skipped("parked-victim");
        }
        final boolean abrupt = drawAbrupt(random);
        if (abrupt) this.abruptRestarts.incrementAndGet();
        if (!abrupt) {
            final ReaderNode peekLow = holders[low];
            final ReaderNode peekHigh = holders[high];
            if ((peekLow != null && isBuried(peekLow)) || (peekHigh != null && isBuried(peekHigh))) {
                audit(startNanos, "dual-restart-skipped readers=%d,%d (buried reader)".formatted(low, high));
                return OpOutcome.skipped("buried-reader");
            }
        }
        gates[low].writeLock().lock();
        gates[high].writeLock().lock();
        try {
            audit(startNanos, "dual-restart-start readers=%d,%d abrupt=%s".formatted(low, high, abrupt));
            for (final int readerIndex : new int[]{low, high}) {
                if (holders[readerIndex] == null) continue;
                final ReaderNode node = holders[readerIndex];
                ReplicationCursor resume = node.persistedCursor() != null
                        ? node.persistedCursor() : baseline;
                if (!abrupt) {
                    node.stopAtLatest();
                    final ReplicationCursor stopped = node.persistedCursor();
                    if (stopped != null) resume = stopped;
                }
                try {
                    node.restartTransport(resume);
                    node.awaitLive();
                    node.assertHealthy();
                    this.completedRestarts.incrementAndGet();
                    readerRestarts[readerIndex].incrementAndGet();
                } catch (final ReseedRequiredException reseed) {
                    node.close();
                    holders[readerIndex] = null;
                    this.reseedsDemanded.incrementAndGet();
                    readerReseeds[readerIndex].incrementAndGet();
                    audit(startNanos, "dual-reseed-demanded reader=%d (%s)".formatted(readerIndex, reseed.getMessage()));
                    recordFate(readerIndex, "dual-restart:reseed-demanded abrupt=%s".formatted(abrupt));
                    if (!abrupt) throw reseed;
                }
            }
            event(startNanos, "dual-restart", "readers=%d,%d abrupt=%s".formatted(low, high, abrupt));
        } finally {
            gates[high].writeLock().unlock();
            gates[low].writeLock().unlock();
        }
        audit(startNanos, "dual-restart readers=%d,%d abrupt=%s".formatted(low, high, abrupt));
        /* Weight two: the storm restarted two transports toward loop progress. */
        return OpOutcome.effectiveHeavy();
    }

    /// Slow-reader chaos: the victim's applied path stalls, so its durable
    /// cursor lags while the writer advances. This is the original flake
    /// shape — a slow-but-advancing reader under load — induced on purpose so
    /// the lag SLO and the sliding stop deadline are exercised, not waited for.
    private OpOutcome slowReaderBurst(final long startNanos, final ReaderNode[] holders, final Random random)
            throws InterruptedException {
        final int victim = random.nextInt(holders.length);
        if (holders[victim] == null) {
            audit(startNanos, "slow-burst-skipped (victim parked)");
            return OpOutcome.skipped("parked-victim");
        }
        final long delayMs = Math.max(1L, this.pollDelayMs * 8L);
        holders[victim].setApplyDelayMs(delayMs);
        this.slowBursts.incrementAndGet();
        audit(startNanos, "slow-burst-start reader=%d delayMs=%d".formatted(victim, delayMs));
        event(startNanos, "slow-burst", "reader=%d delayMs=%d".formatted(victim, delayMs));
        Thread.sleep(2_000L + random.nextInt(2_000));
        if (holders[victim] != null) holders[victim].setApplyDelayMs(0L);
        /* An occasional sub-timeout stall widens the backlog and exercises lag
         * accounting under a stalled applied path. It only probes the sliding
         * stop deadline itself when soak.pollStallMs is set near the
         * configured reader stop timeout; at the small default it is purely a
         * backlog widener. */
        if (random.nextInt(100) < 25) {
            holders[victim].setApplyDelayMs(this.pollStallMs);
            Thread.sleep(Math.min(this.pollStallMs, 1_500L));
            if (holders[victim] != null) holders[victim].setApplyDelayMs(0L);
        }
        audit(startNanos, "slow-burst-end reader=%d".formatted(victim));
        markActivity();
        return OpOutcome.effective();
    }

    /// Bounded CPU starvation plus heap-garbage churn to force GC pauses
    /// mid-replay. Seeded, short, and counted — never an unbounded spin. The
    /// block count (not wall-clock time) bounds the work so the op consumes a
    /// fixed number of RNG draws: a time-bounded fill loop would poison the
    /// seeded draw stream timing-dependently and break schedule replay.
    private OpOutcome cpuGarbageBurst(final long startNanos, final Random random) {
        final int blocks = 32 + random.nextInt(96);
        long garbage = 0L;
        for (int b = 0; b < blocks; b++) {
            final byte[] block = new byte[64 * 1024];
            random.nextBytes(block);
            garbage += block.length;
            Thread.onSpinWait();
        }
        audit(startNanos, "cpu-burst garbageBytes=%d".formatted(garbage));
        event(startNanos, "cpu-burst", "garbageBytes=%d".formatted(garbage));
        markActivity();
        return OpOutcome.effective();
    }

    /// Forced-GC chaos: churns a seeded 32-256 MB through the young generation
    /// in 1 MB blocks, then issues System.gc(), so checkpoints, watermark
    /// publication, and reader apply paths all ride through a genuine GC pause.
    /// No data assertions here — the pause evidence lands in the JFR recording,
    /// while correctness is covered by the surrounding convergence, lag-SLO,
    /// and index gates. Weight one; the block count bounds the RNG draw so the
    /// seeded schedule stays stable.
    private OpOutcome gcBurst(final long startNanos, final Random random) {
        final int megabytes = 32 + random.nextInt(225);
        for (int i = 0; i < megabytes; i++) {
            final byte[] block = new byte[1 << 20];
            block[0] = (byte) i;
            if ((i & 7) == 0) Thread.onSpinWait();
            this.gcBurstSink = block[0];
        }
        System.gc();
        this.gcBursts.incrementAndGet();
        final byte marker = this.gcBurstSink;
        audit(startNanos, "gc-burst churnMB=%d marker=%d".formatted(megabytes, marker));
        event(startNanos, "gc-burst", "churnMB=%d marker=%d".formatted(megabytes, marker));
        markActivity();
        return OpOutcome.effective();
    }

    /// Writer-restart chaos: stops the writer transport and Store — abruptly on
    /// a seeded coin flip, killing the transport before the Store quiesces —
    /// then restarts both with the same cluster, node, and generation
    /// identities. The restarted writer must mint a strictly greater fencing
    /// token, and one post-restart transaction must reach every live reader
    /// from its unchanged durable cursor.
    ///
    /// Writer-mutating chaos can never overlap this op: every op is dispatched
    /// by the single chaos thread, so a "writer-busy" skip is unreachable by
    /// construction. The write lock below only contends with workload writer
    /// threads, which hold it for one transaction.
    private OpOutcome writerRestart(final long startNanos, final Path root, final WriterHandle writer,
                                    final ReaderNode[] holders, final ReadWriteLock[] gates,
                                    final Random random) {
        final Path checkpointFile = root.resolve("writer/checkpoint/writer.checkpoint");
        final ReplicationCursor target;
        synchronized (this.writeLock) {
            final long tokenBefore = currentFencingToken(checkpointFile);
            final boolean abrupt = drawAbrupt(random);
            audit(startNanos, "writer-restart-start abrupt=%s token=%d".formatted(abrupt, tokenBefore));
            try {
                writer.restart(abrupt);
                /* Land one transaction on the restarted writer: durable proof
                 * of the new fencing token and the boundary readers must
                 * reach from their unchanged cursors. */
                addBatch(writer.root(), random, 1);
                writer.root().articles.store();
                this.publishedTransactions.incrementAndGet();
                recalibrateTransactions(writer);
            } catch (final Exception failure) {
                this.failures.add(failure);
                throw new AssertionError("writer restart failed", failure);
            }
            target = writer.latest();
            this.lastWriterSeq = target.logicalSequence();
            final long tokenAfter = currentFencingToken(checkpointFile);
            assertTrue(tokenAfter > 0, "restarted writer published no fencing token");
            assertTrue(tokenBefore < 0 || tokenAfter > tokenBefore,
                    "writer fencing token did not advance across restart: %d -> %d"
                            .formatted(tokenBefore, tokenAfter));
            this.writerRestarts.incrementAndGet();
            this.completedRestarts.incrementAndGet();
            if (abrupt) this.abruptRestarts.incrementAndGet();
            event(startNanos, "writer-restart", "abrupt=%s token=%d->%d target=%d".formatted(
                    abrupt, tokenBefore, tokenAfter, target.logicalSequence()));
            audit(startNanos, "writer-restarted abrupt=%s token=%d->%d target=%d"
                    .formatted(abrupt, tokenBefore, tokenAfter, target.logicalSequence()));
        }
        /* The convergence wait runs OUTSIDE the write lock: holding it freezes
         * the writers, which starves the catching-up readers it is waiting on.
         * Reattach after a writer restart rides a full replay+live handover;
         * under load that takes tens of seconds, so the wait gets its own
         * generous bound rather than sharing the corruption-ops budget. */
        if (!awaitCursorsBeyond(holders, gates, -1, target.logicalSequence(),
                TimeUnit.SECONDS.toNanos(60L))) {
            throw new AssertionError("post-restart transaction %d did not reach all live readers"
                    .formatted(target.logicalSequence()));
        }
        audit(startNanos, "writer-restart-readers-caught-up target=%d".formatted(target.logicalSequence()));
        return OpOutcome.effective();
    }

    /// Reseed-and-rejoin chaos: executes the documented manual reseed — stop
    /// the victim, wipe its Store and any torn cursor, copy the writer's Store
    /// at a frozen boundary, persist that cursor, and restart. The rejoined
    /// reader is re-inserted into the holder slot, so it re-enters the chaos
    /// victim pool and the final convergence census like any live reader. With
    /// a parked victim this is the recovery path collectors demanded earlier;
    /// without one it proactively reseeds a live reader (guarded: the last
    /// live reader is never touched).
    private OpOutcome reseedAndRejoin(final long startNanos, final WriterHandle writer,
                                      final ReaderNode[] holders, final ReadWriteLock[] gates,
                                      final ReaderSpec[] readers, final Topology topology,
                                      final Random random) {
        final List<Integer> parked = new ArrayList<>();
        for (int r = 0; r < holders.length; r++) if (holders[r] == null) parked.add(r);
        final boolean proactive = parked.isEmpty();
        final int victim;
        if (proactive) {
            if (liveHolders(holders) < 2) {
                audit(startNanos, "reseed-skipped (quorum guard: last live reader)");
                return OpOutcome.skipped("quorum-guard");
            }
            victim = random.nextInt(holders.length);
        } else {
            victim = parked.get(random.nextInt(parked.size()));
        }
        gates[victim].writeLock().lock();
        try {
            audit(startNanos, "reseed-start reader=%d proactive=%s".formatted(victim, proactive));
            final ReaderNode existing = holders[victim];
            if (existing != null) existing.close();
            holders[victim] = null;
            final Path storeDir = readers[victim].storeDir();
            /* Quiesce the writer Store for the whole snapshot, exactly like
             * the documented stopped-writer seeding procedure — but only the
             * Store: stopping the transport would close the Archive under the
             * other readers' replays, which a reseed of ONE reader must never
             * do. Anything weaker than a closed Store is a torn seed: channel
             * file writes trail the committed transaction, so a running
             * writer's files lag its cursor and those deltas would never be
             * replayed by the rejoined reader. */
            final ReplicationCursor seedTarget;
            synchronized (this.writeLock) {
                writer.root().articles.store();
                seedTarget = writer.snapshotCopy(storeDir);
                recalibrateTransactions(writer);
            }
            /* The node home is wiped wholesale, not just its cursor file: a
             * parked victim can carry stale recovery evidence (a torn cursor
             * or an uncertain-import reader-inflight checkpoint), and a reseed
             * must never trust any of it. The documented procedure is a fresh
             * node home plus a seeded Store and a valid cursor. */
            AeronStoreIntegrationIT.delete(readers[victim].nodeDir());
            audit(startNanos, "reseed-copied reader=%d seedTarget=%d".formatted(victim, seedTarget.logicalSequence()));
            ReaderNode rejoined = null;
            try {
                rejoined = ReaderNode.open(readers[victim].nodeDir(), storeDir, "reader",
                        readers[victim].nodeId(), topology.clusterId(), topology.generation(),
                        seedTarget, topology.controlPort(), topology.livePort(), topology.watermarkPort());
                rejoined.overwritePersistedCursor(seedTarget);
                rejoined.start();
                audit(startNanos, "reseed-await-live reader=%d".formatted(victim));
                rejoined.awaitLive();
                rejoined.assertHealthy();
            } catch (final Exception | AssertionError failure) {
                /* AssertionErrors (stall/timeout/not-live) are real reseed
                 * defects: log them here, then fail the soak loudly. */
                if (rejoined != null) {
                    try {
                        rejoined.close();
                    } catch (final RuntimeException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                audit(startNanos, "reseed-failed reader=%d proactive=%s (%s)"
                        .formatted(victim, proactive, failure.getMessage()));
                throw failure;
            }
            holders[victim] = rejoined;
            this.reseedsExecuted.incrementAndGet();
            recordFate(victim, "reseed:rejoined %s seedTarget=%d".formatted(
                    proactive ? "proactive" : "parked", seedTarget.logicalSequence()));
            audit(startNanos, "reseed-rejoined reader=%d proactive=%s seedTarget=%d"
                    .formatted(victim, proactive, seedTarget.logicalSequence()));
            event(startNanos, "reseed-rejoined", "reader=%d proactive=%s seedTarget=%d"
                    .formatted(victim, proactive, seedTarget.logicalSequence()));
            markActivity();
            return OpOutcome.effectiveHeavy();
        } catch (final AssertionError failure) {
            this.failures.add(failure);
            throw failure;
        } catch (final Exception failure) {
            this.failures.add(failure);
            throw new AssertionError("reseed chaos failed unexpectedly", failure);
        } finally {
            gates[victim].writeLock().unlock();
        }
    }

    /// Retention chaos on the real watermark quorum: when a quorum is
    /// assembled, assert deletion never touches history a lagging live reader
    /// still needs, then purge complete segments up to the minimum durable
    /// reader cursor. Parked readers count toward the minimum through whatever
    /// durable cursor they left — their last published watermark bounds safe
    /// deletion exactly.
    ///
    /// Watermark validation accepts progress past the terminal commit
    /// checkpoint when the Archive has durably recorded its bytes (see
    /// AeronArchiveRetention.requireWithinDurableBoundary), so the quorum
    /// assembles on this continuously-appending writer. The op still waits
    /// only boundedly and WITHOUT holding the writer lock (holding it starves
    /// the appends that feed the readers), and skips with
    /// "quorum-not-supported" when the quorum stays unassembled — that now
    /// means genuinely missing readers, not checkpoint-cadence rejections.
    /// Retention IS coverage-gated like every enabled op: a purge that never
    /// lands and never had a park-caused skip fails the run. When the quorum
    /// assembles, deleteThrough asserts fail-closed: the deferral probe uses a
    /// non-head boundary strictly ahead of a lagging reader, and refusal —
    /// whether a status or the provider's IllegalStateException vocabulary —
    /// is the pass, EXCEPT a refusal complaining about the durable writer
    /// boundary, which is the fixed soak bug surfacing again. The
    /// parked-behind-the-deleted-boundary restart case (a reader resumed from
    /// a cursor below deleteThrough must be answered RESEED_REQUIRED, never
    /// torn data) is covered deterministically by AeronStoreIntegrationIT's
    /// `readerRestartBehindAPurgedSegmentDemandsAReseed`; here it only
    /// surfaces as a side effect of the generic restart ops.
    private OpOutcome retentionPurge(final long startNanos, final WriterHandle writer,
                                     final ReaderNode[] holders, final ReadWriteLock[] gates,
                                     final ReaderSpec[] readers, final Random random) {
        audit(startNanos, "retention-start");
        final ReplicationLogRetention retention;
        synchronized (this.writeLock) {
            retention = writer.transport().retention();
        }
        final long quorumDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        while (!retention.isSupported() && System.nanoTime() < quorumDeadline) {
            markActivity();
            LockSupport.parkNanos(100_000_000L);
        }
        if (!retention.isSupported()) {
            audit(startNanos, "retention-skipped (watermark quorum never assembled)");
            return OpOutcome.skipped("quorum-not-supported");
        }
        /* Writer-quiesced section: deleteThrough drives the writer-paused
         * fence, and pausing a continuously appending writer waits forever,
         * so the writer must be quiesced at the application level first —
         * briefly, never for the quorum wait above. */
        synchronized (this.writeLock) {
            long minSequence = Long.MAX_VALUE;
            ReplicationCursor minCursor = null;
            long maxLiveSequence = Long.MIN_VALUE;
            ReplicationCursor maxLiveCursor = null;
            for (int r = 0; r < holders.length; r++) {
                gates[r].readLock().lock();
                try {
                    final ReaderNode node = holders[r];
                    ReplicationCursor cursor = node != null ? node.persistedCursor() : null;
                    if (cursor == null && node == null) {
                        final Path cursorFile = readers[r].nodeDir().resolve("cursor");
                        if (Files.isRegularFile(cursorFile)) {
                            try (StoredReplicationCursorManager manager =
                                         StoredReplicationCursorManager.NewAtomic(cursorFile)) {
                                cursor = manager.get();
                            } catch (final RuntimeException torn) {
                                audit(startNanos, "retention-cursor-unreadable reader=%d (%s)"
                                        .formatted(r, torn.getMessage()));
                            }
                        }
                    }
                    if (cursor == null) continue;
                    if (cursor.logicalSequence() < minSequence) {
                        minSequence = cursor.logicalSequence();
                        minCursor = cursor;
                    }
                    if (node != null && cursor.logicalSequence() > maxLiveSequence) {
                        maxLiveSequence = cursor.logicalSequence();
                        maxLiveCursor = cursor;
                    }
                } finally {
                    gates[r].readLock().unlock();
                }
            }
            boolean deferredChecked = false;
            /* Deferral probe: a boundary strictly ahead of the slowest reader
             * must never report DELETED — the lagging reader is still
             * replaying that history. Refusal arrives either as a non-DELETED
             * status or as the provider's IllegalStateException ("reader
             * quorum has not reached the requested sequence"); both are the
             * fail-closed pass, a DELETED result is the failure. */
            if (maxLiveCursor != null && maxLiveSequence > minSequence) {
                deferredChecked = true;
                try {
                    final ReplicationLogRetention.MaintenanceResult deferred =
                            retention.deleteThrough(maxLiveCursor);
                    assertNotEquals(ReplicationLogRetention.MaintenanceResult.Status.DELETED,
                            deferred.status(),
                            "retention deleted history a lagging live reader still needed: %s"
                                    .formatted(deferred));
                } catch (final IllegalStateException refused) {
                    final String message = String.valueOf(refused.getMessage());
                    if (message.contains("ahead of the durable writer boundary")) {
                        throw new AssertionError(
                                "legitimate watermark refused against the durable writer boundary: %s"
                                        .formatted(message), refused);
                    }
                    audit(startNanos, "retention-defer-refused (%s)".formatted(message));
                }
                audit(startNanos, "retention-defer-ok boundary=%d".formatted(maxLiveSequence));
            }
            if (minCursor == null || minSequence <= this.baselineSeq) {
                audit(startNanos, "retention-skipped (no reader progress past baseline)");
                return OpOutcome.skipped("no-reader-progress");
            }
            /* Purge boundary: the minimum durable cursor across every reader,
             * live or parked. A parked reader's file may be torn (cursor
             * corruption); when unreadable it is not waited on, and the purge
             * retries absorb a watermark the writer has not yet observed. */
            audit(startNanos, "retention-purge-attempt minCursor=%d".formatted(minSequence));
            ReplicationLogRetention.MaintenanceResult purged = null;
            for (int attempt = 0; attempt < 20; attempt++) {
                try {
                    purged = retention.deleteThrough(minCursor);
                } catch (final IllegalStateException refused) {
                    if (String.valueOf(refused.getMessage()).contains("ahead of the durable writer boundary")) {
                        throw new AssertionError(
                                "legitimate purge boundary refused against the durable writer boundary: %s"
                                        .formatted(refused.getMessage()), refused);
                    }
                    /* Quorum watermark below our local minimum cursor: parked
                     * readers can lag the file view. The retries absorb it. */
                    LockSupport.parkNanos(250_000_000L);
                    continue;
                }
                if (purged.status() == ReplicationLogRetention.MaintenanceResult.Status.DELETED) break;
                LockSupport.parkNanos(250_000_000L);
            }
            if (purged == null) {
                audit(startNanos, "retention-skipped (quorum watermark behind local cursor minimum)");
                return OpOutcome.skipped("quorum-watermark-behind");
            }
            if (purged.status() == ReplicationLogRetention.MaintenanceResult.Status.DELETED) {
                this.retentionPurges.incrementAndGet();
            }
            audit(startNanos, "retention-purge status=%s minCursor=%d position=%d".formatted(
                    purged.status(), minSequence, purged.position()));
            event(startNanos, "retention-purge", "status=%s minCursor=%d deferChecked=%s".formatted(
                    purged.status(), minSequence, deferredChecked));
            markActivity();
            return OpOutcome.effective();
        }
    }

    /// Aligns the transaction model with the actual writer sequence after a
    /// chaos op changed it off-band (reseed snapshots and writer restarts can
    /// mint boundary records the model did not count). Readers gate assertions
    /// on cursor-depth minus baseline, so the carry must count every sequence
    /// exactly once or near-the-boundary checks fire early on entities still
    /// in flight. Only ever moves forward, only called under the write lock.
    private void recalibrateTransactions(final WriterHandle writer) {
        final long applied = writer.latest().logicalSequence() - this.baselineSeq;
        if (applied > this.publishedTransactions.get()) {
            this.publishedTransactions.set(applied);
        }
    }

    /// The writer's latest durable fencing token from its checkpoint file, or
    /// -1 when no checkpoint exists yet; -1 weakens the restart monotonicity
    /// assertion to positivity rather than failing a run without history.
    private static long currentFencingToken(final Path checkpointFile) {
        if (!Files.exists(checkpointFile)) return -1L;
        try {
            return AeronReplicationCheckpointStore.read(checkpointFile).fencingToken();
        } catch (final Exception unreadable) {
            return -1L;
        }
    }

    /// Corrupts the victim's durable cursor file while the reader is stopped,
    /// then requires the fresh read to fail closed. A reader that converges
    /// past this injection has silently replayed from a torn boundary.
    private OpOutcome corruptCursorFile(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                                            final AtomicLong[] readerCorruptionParks,
                                            final Random random) {
        final int victim = random.nextInt(holders.length);
        if (holders[victim] == null) {
            audit(startNanos, "cursor-corrupt-skipped (victim parked)");
            return OpOutcome.skipped("parked-victim");
        }
        if (liveHolders(holders) < 2) {
            audit(startNanos, "cursor-corrupt-skipped reader=%d (quorum guard: last live reader)".formatted(victim));
            return OpOutcome.skipped("quorum-guard");
        }
        gates[victim].writeLock().lock();
        try {
            final ReaderNode node = holders[victim];
            if (isBuried(node)) {
                audit(startNanos, "cursor-corrupt-skipped reader=%d (buried reader)".formatted(victim));
                return OpOutcome.skipped("buried-reader");
            }
            node.stopAtLatest();
            final ReplicationCursor stopped = node.persistedCursor();
            final Path cursorFile = node.cursorPath();
            if (stopped == null || !Files.exists(cursorFile) || Files.isDirectory(cursorFile)) {
                audit(startNanos, "cursor-corrupt-skipped reader=%d (no durable cursor file)".formatted(victim));
                node.restartTransport(stopped != null ? stopped : node.liveCursor());
                node.awaitLive();
                return OpOutcome.skipped("no-cursor-file");
            }
            final byte[] bytes = Files.readAllBytes(cursorFile);
            if (bytes.length == 0) {
                audit(startNanos, "cursor-corrupt-skipped reader=%d (empty cursor file)".formatted(victim));
                node.restartTransport(stopped);
                node.awaitLive();
                return OpOutcome.skipped("empty-cursor-file");
            }
            bytes[bytes.length / 2] ^= 0x01;
            Files.write(cursorFile, bytes);
            this.corruptionFired.incrementAndGet();
            audit(startNanos, "cursor-corrupt-fired reader=%d stopped=%d".formatted(victim, stopped.logicalSequence()));
            event(startNanos, "cursor-corrupt", "reader=%d stopped=%d".formatted(victim, stopped.logicalSequence()));
            /* The fresh read runs in its own narrow catch: cursor decoding
             * fails with NodeLibraryException on every torn input, and only
             * that type counts as the expected fail-closed path. Any Error
             * (OOM, internal assertion) or unexpected RuntimeException must
             * propagate and fail the soak, never park as success. */
            final ReplicationCursor reread;
            try {
                reread = node.readPersistedCursorFresh();
            } catch (final NodeLibraryException expected) {
                audit(startNanos, "cursor-corrupt-failclosed reader=%d (%s)".formatted(victim, expected.getMessage()));
                event(startNanos, "cursor-failclosed", "reader=%d".formatted(victim));
                node.close();
                holders[victim] = null;
                readerCorruptionParks[victim].incrementAndGet();
                recordFate(victim, "cursor-corrupt:fail-closed");
                return OpOutcome.effective();
            }
            if (reread.logicalSequence() == stopped.logicalSequence()) {
                /* The flip did not land observably; repair and resume so
                 * the soak continues, but record that the injection was
                 * a no-op rather than a passed assertion. */
                node.overwritePersistedCursor(stopped);
                node.restartTransport(stopped);
                node.awaitLive();
                audit(startNanos, "cursor-corrupt-noop reader=%d".formatted(victim));
                return OpOutcome.skipped("injection-noop");
            }
            /* A torn cursor parsed to a different boundary: restarting
             * from it must still fail closed, never converge. This call sits
             * outside every Error-catching scope on purpose: its
             * AssertionError on silent convergence must fail the soak, never
             * be laundered into a parked-reader success. The gate records the
             * exact fate itself (reseed/fail-closed/parked+outcome); no outer
             * fate is recorded here so it can never overwrite the precise one. */
            assertCorruptionFailsClosed(startNanos, holders, victim, reread,
                    stopped.logicalSequence(), readerCorruptionParks);
            return OpOutcome.effective();
        } catch (final Exception failure) {
            this.failures.add(failure);
            throw new AssertionError("cursor corruption chaos failed unexpectedly", failure);
        } finally {
            gates[victim].writeLock().unlock();
        }
    }

    /// Counts live (unparked) readers without locking: only the single chaos
    /// thread ever parks, so the count cannot race a concurrent park.
    private static int liveHolders(final ReaderNode[] holders) {
        int live = 0;
        for (final ReaderNode holder : holders) if (holder != null) live++;
        return live;
    }

    /// Resumes the victim from a deliberately rolled-back older cursor. The
    /// duplicate-detection path must demand a reseed; silent convergence
    /// means already-materialised transactions applied twice unnoticed.
    private OpOutcome rollbackCursor(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                                         final ReplicationCursor baseline, final AtomicLong[] readerReseeds,
                                         final AtomicLong[] readerRestarts, final Random random) {
        final int victim = random.nextInt(holders.length);
        if (holders[victim] == null) {
            audit(startNanos, "rollback-skipped (victim parked)");
            return OpOutcome.skipped("parked-victim");
        }
        if (liveHolders(holders) < 2) {
            audit(startNanos, "rollback-skipped reader=%d (quorum guard: last live reader)".formatted(victim));
            return OpOutcome.skipped("quorum-guard");
        }
        gates[victim].writeLock().lock();
        try {
            final ReaderNode node = holders[victim];
            if (isBuried(node)) {
                audit(startNanos, "rollback-skipped reader=%d (buried reader)".formatted(victim));
                return OpOutcome.skipped("buried-reader");
            }
            node.stopAtLatest();
            final ReplicationCursor stopped = node.persistedCursor();
            if (stopped == null || stopped.logicalSequence() <= baseline.logicalSequence()) {
                audit(startNanos, "rollback-skipped reader=%d (no newer boundary)".formatted(victim));
                node.restartTransport(stopped != null ? stopped : baseline);
                node.awaitLive();
                return OpOutcome.skipped("no-newer-boundary");
            }
            node.overwritePersistedCursor(baseline);
            this.corruptionFired.incrementAndGet();
            audit(startNanos, "rollback-fired reader=%d stopped=%d baseline=%d"
                    .formatted(victim, stopped.logicalSequence(), baseline.logicalSequence()));
            event(startNanos, "rollback", "reader=%d stopped=%d baseline=%d".formatted(
                    victim, stopped.logicalSequence(), baseline.logicalSequence()));
            return assertRollbackOutcome(startNanos, holders, victim, baseline, stopped.logicalSequence(),
                    readerReseeds, readerRestarts);
        } catch (final Exception failure) {
            this.failures.add(failure);
            throw new AssertionError("rollback chaos failed unexpectedly", failure);
        } finally {
            gates[victim].writeLock().unlock();
        }
    }

    /// Rollback gate: the victim replays valid history from an older boundary.
    /// Reseed or bounded failure parks it (duplicate application refused).
    /// Convergence is allowed only when the replayed Store still matches the
    /// transaction model exactly — genuinely idempotent replay — otherwise a
    /// converged double-apply is silent corruption and fails the soak.
    private OpOutcome assertRollbackOutcome(final long startNanos, final ReaderNode[] holders, final int victim,
                                                final ReplicationCursor baseline, final long targetSequence,
                                                final AtomicLong[] readerReseeds, final AtomicLong[] readerRestarts) {
        final ReaderNode node = holders[victim];
        try {
            node.restartTransport(baseline);
        } catch (final ReseedRequiredException reseed) {
            audit(startNanos, "rollback-reseed reader=%d (%s)".formatted(victim, reseed.getMessage()));
            event(startNanos, "rollback-reseed", "reader=%d".formatted(victim));
            node.close();
            holders[victim] = null;
            readerReseeds[victim].incrementAndGet();
            this.reseedsDemanded.incrementAndGet();
            recordFate(victim, "rollback:reseed");
            return OpOutcome.effectiveHeavy();
        } catch (final RuntimeException restartFailure) {
/* Fail-closed allowlist, nothing broader: reseed demands are caught
             * above, and NodeLibraryException is the provider's fail-closed
             * vocabulary for unreadable durable state. AssertionError is never
             * a fail-closed signal and Error (OOM and friends) must fail
             * loudly, so neither is caught here — both propagate by design. */
            if (!(restartFailure instanceof NodeLibraryException)) throw restartFailure;
            audit(startNanos, "rollback-failclosed reader=%d (%s)".formatted(victim, restartFailure));
            event(startNanos, "rollback-failclosed", "reader=%d".formatted(victim));
            node.close();
            holders[victim] = null;
            readerReseeds[victim].incrementAndGet();
            this.reseedsDemanded.incrementAndGet();
            recordFate(victim, "rollback:fail-closed");
            return OpOutcome.effectiveHeavy();
        }
        final AwaitResult outcome = awaitTargetBounded(node, targetSequence);
        if (outcome != AwaitResult.LIVE) {
            if (outcome == AwaitResult.TIMEOUT) this.corruptionTimeouts.incrementAndGet();
            audit(startNanos, "rollback-parked reader=%d outcome=%s".formatted(victim, outcome));
            event(startNanos, "rollback-parked", "reader=%d outcome=%s".formatted(victim, outcome));
            node.close();
            holders[victim] = null;
            readerReseeds[victim].incrementAndGet();
            this.reseedsDemanded.incrementAndGet();
            recordFate(victim, "rollback:parked outcome=%s".formatted(outcome));
            return OpOutcome.effectiveHeavy();
        }
        final List<String> problems = graphSubsetProblems(node, targetSequence - this.baselineSeq);
        if (!problems.isEmpty()) {
            throw new AssertionError("rollback reader %d converged with a corrupted Store: %s".formatted(victim, problems));
        }
        audit(startNanos, "rollback-tolerated reader=%d (idempotent replay)".formatted(victim));
        event(startNanos, "rollback-tolerated", "reader=%d".formatted(victim));
        this.completedRestarts.incrementAndGet();
        readerRestarts[victim].incrementAndGet();
        return OpOutcome.effectiveHeavy();
    }

    /// Exact graph check of every entity materialized below `depth`
    /// (see [#readerDepth]): titles, bodies, and checksums against the model,
    /// capped at eight reported problems. Joined on the reader's coordinator side.
    private List<String> graphSubsetProblems(final ReaderNode node, final long depth) {
        return node.graphCoordinator().read(() -> {
            final IndexRoot root = (IndexRoot) node.rootObject();
            final Map<String, String> bodies = new HashMap<>();
            root.articles.iterate(article -> bodies.put(article.title, article.body));
            final List<String> problems = new ArrayList<>();
            for (final ArticleState state : this.liveByTitle.values()) {
                if (state.modifiedTx() >= depth) continue;
                final String body = bodies.get(state.title());
                if (body == null) problems.add("lost " + state.title());
                else if (!state.body().equals(body)) problems.add("stale " + state.title());
                else if (checksum(state.title(), body) != state.checksum()) {
                    problems.add("checksum " + state.title());
                }
                if (problems.size() >= 8) break;
            }
            return problems;
        });
    }

    /// Shared fail-closed gate for torn-cursor injections: the victim restarts
    /// from the injected boundary and must reseed or report a bounded
    /// failure. Reaching the pre-injection boundary is silent acceptance of
    /// torn history and fails the soak.
    private void assertCorruptionFailsClosed(final long startNanos, final ReaderNode[] holders,
                                             final int victim,
                                             final ReplicationCursor injected, final long targetSequence,
                                             final AtomicLong[] parks) {
        final ReaderNode node = holders[victim];
        try {
            node.restartTransport(injected);
        } catch (final ReseedRequiredException reseed) {
            audit(startNanos, "%s-reseed reader=%d (%s)".formatted("cursor-torn", victim, reseed.getMessage()));
            event(startNanos, "cursor-torn-reseed", "reader=%d".formatted(victim));
            node.close();
            holders[victim] = null;
            parks[victim].incrementAndGet();
            recordFate(victim, "cursor-torn:reseed");
            return;
        } catch (final RuntimeException restartFailure) {
/* Fail-closed allowlist, nothing broader: reseed demands are caught
             * above, and NodeLibraryException is the provider's fail-closed
             * vocabulary for unreadable durable state. AssertionError is never
             * a fail-closed signal and Error (OOM and friends) must fail
             * loudly, so neither is caught here — both propagate by design. */
            if (!(restartFailure instanceof NodeLibraryException)) throw restartFailure;
            audit(startNanos, "%s-failclosed reader=%d (%s)".formatted("cursor-torn", victim, restartFailure));
            event(startNanos, "cursor-torn-failclosed", "reader=%d".formatted(victim));
            node.close();
            holders[victim] = null;
            parks[victim].incrementAndGet();
            recordFate(victim, "cursor-torn:fail-closed");
            return;
        }
        final AwaitResult outcome = awaitTargetBounded(node, targetSequence);
        if (outcome == AwaitResult.LIVE) {
            throw new AssertionError(
                    "cursor-torn reader %d silently converged after corruption injection".formatted(victim));
        }
        if (outcome == AwaitResult.TIMEOUT) this.corruptionTimeouts.incrementAndGet();
        audit(startNanos, "%s-parked reader=%d outcome=%s".formatted("cursor-torn", victim, outcome));
        event(startNanos, "cursor-torn-parked", "reader=%d outcome=%s".formatted(victim, outcome));
        node.close();
        holders[victim] = null;
        parks[victim].incrementAndGet();
        recordFate(victim, "cursor-torn:parked outcome=%s".formatted(outcome));
    }

    /// Corrupts the not-yet-replayed tail of the shared Archive while the
    /// victim is stopped: fresh transactions are published, the live readers
    /// consume the good bytes first, then the tail frame is truncated so only
    /// the victim's replay observes torn data. It must demand a reseed, never
    /// apply torn data and converge.
    private OpOutcome corruptLiveSegment(final long startNanos, final Path root, final IndexRoot writerRoot,
                                    final ClusterReplicationTransport writerTransport,
                                    final ReaderNode[] holders, final ReadWriteLock[] gates,
                                    final AtomicLong[] readerCorruptionParks, final Random random) {
        int victim = random.nextInt(holders.length);
        if (holders[victim] == null) {
            audit(startNanos, "segment-corrupt-skipped (victim parked)");
            return OpOutcome.skipped("parked-victim");
        }
        if (liveHolders(holders) < 2) {
            audit(startNanos, "segment-corrupt-skipped reader=%d (quorum guard: last live reader)".formatted(victim));
            return OpOutcome.skipped("quorum-guard");
        }
        gates[victim].writeLock().lock();
        try {
            final ReaderNode node = holders[victim];
            if (isBuried(node)) {
                audit(startNanos, "segment-corrupt-skipped reader=%d (buried reader)".formatted(victim));
                return OpOutcome.skipped("buried-reader");
            }
            node.stopAtLatest();
            final ReplicationCursor stopped = node.persistedCursor();
            if (stopped == null) {
                node.restartTransport(node.liveCursor());
                node.awaitLive();
                return OpOutcome.skipped("no-cursor");
            }
            /* The writer stays frozen for the whole injection: fresh
             * transactions are published, live readers consume the good bytes,
             * the tail is truncated, and the victim replays — with no
             * concurrent append moving the truncation out of the victim's
             * awaited window. (A previous shape truncated a post-target frame
             * the victim never had to replay, then cried wolf on convergence.)
             * Lock order stays gates-before-writeLock everywhere; the waits
             * below only take brief read gates, so nothing cycles. */
            synchronized (this.writeLock) {
            /* Publish fresh transactions the victim has not seen. Live
             * readers consume the good bytes before the corruption lands. */
            final ReplicationCursor target;
            {
                addBatch(writerRoot, random, 2);
                writerRoot.articles.store();
                this.publishedTransactions.incrementAndGet();
                target = AeronStoreIntegrationIT.latest(writerTransport);
            }
            if (!awaitCursorsBeyond(holders, gates, victim, target.logicalSequence())) {
                audit(startNanos, "segment-corrupt-skipped reader=%d (live readers behind target=%d)"
                        .formatted(victim, target.logicalSequence()));
                node.restartTransport(stopped);
                node.awaitLive();
                return OpOutcome.skipped("live-readers-behind");
            }
            final Path archiveDir = root.resolve("writer/archive");
            final Path checkpointFile = root.resolve("writer/checkpoint/writer.checkpoint");
            final AeronReplicationCheckpoint checkpoint;
            try {
                checkpoint = AeronReplicationCheckpointStore.read(checkpointFile);
            } catch (final Exception unreadable) {
                audit(startNanos, "segment-corrupt-skipped reader=%d (no checkpoint: %s)".formatted(victim, unreadable));
                node.restartTransport(stopped);
                node.awaitLive();
                return OpOutcome.skipped("no-checkpoint");
            }
            final List<Path> segments;
            try {
                segments = ArchiveArtifactMutator.segments(archiveDir, checkpoint.recordingId());
            } catch (final Exception missing) {
                audit(startNanos, "segment-corrupt-skipped reader=%d (no segments: %s)".formatted(victim, missing));
                node.restartTransport(stopped);
                node.awaitLive();
                return OpOutcome.skipped("no-segments");
            }
            final Path tail = segments.getLast();
            final long base = segmentBase(tail);
            try {
                ArchiveArtifactMutator.truncateFinalFrame(tail, base, checkpoint.recordingPosition());
            } catch (final Exception unusable) {
                audit(startNanos, "segment-corrupt-skipped reader=%d (cannot truncate: %s)".formatted(victim, unusable));
                node.restartTransport(stopped);
                node.awaitLive();
                return OpOutcome.skipped("cannot-truncate");
            }
            this.corruptionFired.incrementAndGet();
            audit(startNanos, "segment-corrupt-fired reader=%d stopped=%d target=%d segment=%s"
                    .formatted(victim, stopped.logicalSequence(), target.logicalSequence(), tail.getFileName()));
            event(startNanos, "segment-corrupt", "reader=%d stopped=%d target=%d".formatted(
                    victim, stopped.logicalSequence(), target.logicalSequence()));
            /* The truncated tail fails closed on replay: reseed or bounded
             * failure parks the victim. Convergence past torn data fails. */
            try {
                node.restartTransport(stopped);
            } catch (final ReseedRequiredException reseed) {
                audit(startNanos, "segment-reseed reader=%d (%s)".formatted(victim, reseed.getMessage()));
                event(startNanos, "segment-reseed", "reader=%d".formatted(victim));
                node.close();
                holders[victim] = null;
                readerCorruptionParks[victim].incrementAndGet();
                recordFate(victim, "segment-corrupt:reseed");
                return OpOutcome.effectiveHeavy();
            } catch (final RuntimeException restartFailure) {
/* Fail-closed allowlist, nothing broader: reseed demands are caught
             * above, and NodeLibraryException is the provider's fail-closed
             * vocabulary for unreadable durable state. AssertionError is never
             * a fail-closed signal and Error (OOM and friends) must fail
             * loudly, so neither is caught here — both propagate by design. */
                if (!(restartFailure instanceof NodeLibraryException)) throw restartFailure;
                audit(startNanos, "segment-failclosed reader=%d (%s)".formatted(victim, restartFailure));
                event(startNanos, "segment-failclosed", "reader=%d".formatted(victim));
                node.close();
                holders[victim] = null;
                readerCorruptionParks[victim].incrementAndGet();
                recordFate(victim, "segment-corrupt:fail-closed");
                return OpOutcome.effectiveHeavy();
            }
            final AwaitResult outcome = awaitTargetBounded(node, target.logicalSequence());
            if (outcome == AwaitResult.LIVE) {
                throw new AssertionError(
                        "segment-corrupt reader %d silently converged past torn data".formatted(victim));
            }
            if (outcome == AwaitResult.TIMEOUT) this.corruptionTimeouts.incrementAndGet();
            audit(startNanos, "segment-parked reader=%d outcome=%s".formatted(victim, outcome));
            event(startNanos, "segment-parked", "reader=%d outcome=%s".formatted(victim, outcome));
            node.close();
            holders[victim] = null;
            readerCorruptionParks[victim].incrementAndGet();
            recordFate(victim, "segment-corrupt:parked outcome=%s".formatted(outcome));
            return OpOutcome.effectiveHeavy();
            } // frozen writer window
        } catch (final Exception failure) {
            this.failures.add(failure);
            throw new AssertionError("segment corruption chaos failed unexpectedly", failure);
        } finally {
            gates[victim].writeLock().unlock();
        }
    }

    private static long segmentBase(final Path segment) {
        final String name = segment.getFileName().toString();
        final int dash = name.indexOf('-');
        final int dot = name.lastIndexOf('.');
        return Long.parseLong(name.substring(dash + 1, dot));
    }

    private boolean awaitCursorsBeyond(final ReaderNode[] holders, final ReadWriteLock[] gates,
                                       final int skip, final long sequence) {
        return awaitCursorsBeyond(holders, gates, skip, sequence, TimeUnit.MILLISECONDS.toNanos(15_000L));
    }

    private boolean awaitCursorsBeyond(final ReaderNode[] holders, final ReadWriteLock[] gates,
                                       final int skip, final long sequence, final long budgetNanos) {
        final long deadline = System.nanoTime() + budgetNanos;
        while (System.nanoTime() < deadline) {
            this.markActivity();
            boolean ready = true;
            for (int r = 0; r < holders.length; r++) {
                if (r == skip) continue;
                gates[r].readLock().lock();
                try {
                    /* Read under the gate: a parked reader stays parked. */
                    if (holders[r] == null) continue;
                    final ReplicationCursor cursor = holders[r].persistedCursor();
                    if (cursor == null || cursor.logicalSequence() < sequence) ready = false;
                } finally {
                    gates[r].readLock().unlock();
                }
            }
            if (ready) return true;
            LockSupport.parkNanos(10_000_000L);
        }
        return false;
    }

    private enum AwaitResult { LIVE, FAILED, TIMEOUT }

    private AwaitResult awaitTargetBounded(final ReaderNode node, final long sequence) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(30_000L);
        while (System.nanoTime() < deadline) {
            this.markActivity();
            if (node.clientFailure() != null) return AwaitResult.FAILED;
            if (node.liveCursor().logicalSequence() >= sequence) return AwaitResult.LIVE;
            LockSupport.parkNanos(1_000_000L);
        }
        return node.clientFailure() != null ? AwaitResult.FAILED : AwaitResult.TIMEOUT;
    }

    private void auditLoop(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                           final long soakEndNanos, final AtomicLong[] readerLagStrikes) throws Exception {
        final boolean[] wasLive = new boolean[holders.length];
        Arrays.fill(wasLive, true);
        int iteration = 0;
        while (System.nanoTime() < soakEndNanos) {
            Thread.sleep(5_000L);
            beat(Thread.currentThread().getName(), "sample");
            markActivity();
            iteration++;
            /* Seeds never change identity: their Lucene visibility per reader
             * shows when (and on whom) index maintenance first diverges. */
            if (iteration % 2 == 0) seedProbe(startNanos, holders, gates);
            if (iteration % 2 == 1) miniCensus(startNanos, holders, gates, iteration);
            /* The writer publishes this boundary at every transaction. A
             * five-second refresh here would make the lag SLO observe stale
             * ground truth. */
            final long writerSequence = this.lastWriterSeq;
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
                    /* Observability cross-check: the live (applied) boundary the
                     * client reports must never sit behind the durable cursor
                     * ground truth, and the two lags must agree within one
                     * transaction — a wider gap means the operator-facing
                     * sequence is lying about durable state. */
                    if (cursor != null && live && running) {
                        final long applied = holders[r].liveCursor().logicalSequence();
                        assertTrue(applied >= cursor.logicalSequence(),
                                "reader %d reported applied sequence %s behind its durable cursor %s"
                                        .formatted(r, applied, cursor.logicalSequence()));
                        assertTrue(applied - cursor.logicalSequence() <= 1L,
                                "reader %d live sequence drifted %s transactions past its durable cursor"
                                        .formatted(r, applied - cursor.logicalSequence()));
                        this.metricsCrossChecks++;
                    }
                    if (live != wasLive[r]) {
                        wasLive[r] = live;
                        this.liveFlips.incrementAndGet();
                        audit(startNanos, "live-flip reader=%d live=%s running=%s".formatted(r, live, running));
                    }
                    final long lag = cursor == null ? -1L : writerSequence - cursor.logicalSequence();
                    /* Bounded-lag SLO: a live, running reader must stay within
                     * lagSlots transactions. Three consecutive violations fail
                     * fast instead of hanging the final converge. Reseed
                     * bursts and live flaps reset the strike count. */
                    if (cursor != null && live && running && lag > this.lagSlots) {
                        final long strikes = readerLagStrikes[r].incrementAndGet();
                        this.lagViolations.incrementAndGet();
                        audit(startNanos, "lag-slo reader=%d lag=%d strikes=%d".formatted(r, lag, strikes));
                        if (strikes >= 3) {
                            throw new AssertionError(
                                    "reader %d exceeded bounded-lag SLO (lag=%d > %d for %d audits)".formatted(
                                            r, lag, this.lagSlots, strikes));
                        }
                    } else {
                        readerLagStrikes[r].set(0L);
                    }
                    state.append(" lag%d=%d live%d=%s run%d=%s".formatted(r, lag, r, live, r, running));
                } finally {
                    gates[r].readLock().unlock();
                }
            }
            audit(startNanos, "progress tx=%d queries=%d verified=%d torn=%d restarts=%d abrupt=%d writerSeq=%d metricsChecks=%d%s jvm=%s"
                    .formatted(this.publishedTransactions.get(), this.servedQueries.get(),
                            this.verifiedQueries.get(), this.tornReads.get(), this.completedRestarts.get(),
                            this.abruptRestarts.get(), writerSequence, this.metricsCrossChecks, state,
                            jvmTelemetry()));
        }
    }

    /// Cheap mid-soak census over materialized entities: every checked title
    /// must resolve on the reader whose own cursor proves it materialized.
    /// Per-reader depth gating makes lag unobservable here by construction —
    /// a reader only ever checks entities it has provably applied — so a miss
    /// is divergence, caught here instead of only at final verification.
    private void miniCensus(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates,
                            final int iteration) {
        for (int r = 0; r < holders.length; r++) {
            gates[r].readLock().lock();
            try {
                /* Read under the gate: chaos parks readers concurrently. */
                final ReaderNode node = holders[r];
                if (node == null) continue;
                final long depth = readerDepth(node);
                if (depth < 0) continue;
                final List<ArticleState> applied = new ArrayList<>();
                for (final ArticleState state : this.liveByTitle.values()) {
                    if (state.modifiedTx() < depth) applied.add(state);
                }
                if (applied.isEmpty()) continue;
                Collections.shuffle(applied, new Random(iteration * 0x9E3779B9L + r));
                final List<ArticleState> sample = applied.subList(0, Math.min(this.miniCensus, applied.size()));
                final List<String> problems = node.graphCoordinator().read(() -> {
                    final IndexRoot root = (IndexRoot) node.rootObject();
                    final Map<String, String> bodies = new HashMap<>();
                    root.articles.iterate(article -> bodies.put(article.title, article.body));
                    final List<String> bad = new ArrayList<>(8);
                    for (final ArticleState state : sample) {
                        final String body = bodies.get(state.title());
                        if (bad.size() < 8 && (!state.body().equals(body)
                                || checksum(state.title(), body) != state.checksum())) {
                            bad.add("%s@tx%d model=%s graph=%s".formatted(state.title(), state.modifiedTx(),
                                    state.body(), body));
                        }
                    }
                    return bad;
                });
                if (!problems.isEmpty()) {
                    throw new AssertionError(
                            "mini-census reader %d diverged on %d/%d materialized titles: %s (depth=%d)"
                                    .formatted(r, problems.size(), sample.size(), problems, depth));
                }
                audit(startNanos, "mini-census reader=%d checked=%d depth=%d".formatted(r, sample.size(), depth));
            } catch (final AssertionError census) {
                this.failures.add(census);
                throw census;
            } finally {
                gates[r].readLock().unlock();
            }
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
        /* Sentinels always ride along: they are never updated or removed, so
         * their graph and Lucene checks are exact with no model-lag caveat.
         * (JVector exactness is deliberately not asserted anywhere: HNSW
         * search is approximate and flakes top-k ranks on healthy indexes.) */
        final List<ArticleState> sample = new ArrayList<>(this.liveByTitle.values().stream()
                .filter(state -> isSentinel(state.title())).toList());
        final List<ArticleState> states = new ArrayList<>(this.live.values());
        Collections.shuffle(states, new Random(seed ^ 0x5A41E1L));
        for (final ArticleState state : states) {
            if (sample.size() >= STRICT_PRESENT_SAMPLE) break;
            sample.add(state);
        }
        return sample;
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
            /* No vector check for the axis sentinels: they are HNSW outliers
             * whose approximate recall is nondeterministic run to run (exact
             * top-1 one run, outside top-5 the next, identical data), so any
             * rank assertion flakes on the algorithm, not the product. Dense
             * entities keep the top-5 check, matching the freshness contract. */
            if (isSentinel(state.title())) continue;
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

    private static void assertMidSoakIndexVisible(final String what,
                                                   final java.util.function.BooleanSupplier visible) {
        final long deadline = System.nanoTime() + MID_SOAK_INDEX_WAIT_NANOS;
        while (!visible.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(100_000_000L);
        }
        assertTrue(visible.getAsBoolean(), "mid-soak index visibility timeout: " + what);
    }

    private void seedProbe(final long startNanos, final ReaderNode[] holders, final ReadWriteLock[] gates) {
        for (int r = 0; r < holders.length; r++) {
            final int reader = r;
            gates[reader].readLock().lock();
            try {
                /* Read under the gate: chaos parks readers concurrently. */
                final ReaderNode node = holders[reader];
                if (node == null) continue;
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
            final int reader = r;
            gates[reader].readLock().lock();
            try {
                /* Read under the gate: chaos parks readers concurrently. */
                final ReaderNode node = holders[reader];
                if (node == null) {
                    audit(startNanos, "index-census reader=%d reseed-pending".formatted(r));
                    continue;
                }
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
        /* Uncapped by design: a longer soak must still check every live entity. */
        for (final ArticleState state : this.live.values()) {
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
        /* Exact model check: the quiesced graph must hold every live entity
         * with its expected body — update-after-delete, repeated updates and
         * deletes are all covered because the model tracks final state. */
        int modelMissing = 0;
        int modelStale = 0;
        for (final ArticleState state : this.liveByTitle.values()) {
            if (!titles.contains(state.title())) modelMissing++;
            else if (!state.body().equals(bodies.get(state.title()))) modelStale++;
        }
        for (final String title : this.removedTitles) {
            if (titles.contains(title) && !this.liveByTitle.containsKey(title)) modelMissing++;
        }
        assertEquals(0, modelMissing, "quiescent reader %d diverged from the transaction model".formatted(readerIndex));
        assertEquals(0, modelStale, "quiescent reader %d holds stale bodies".formatted(readerIndex));
        assertEquals(this.liveByTitle.size(), titles.size(),
                "quiescent reader %d entity count diverged (model=%d graph=%d)".formatted(
                        readerIndex, this.liveByTitle.size(), titles.size()));
        final VectorIndices<IndexedArticle> vectors = imported.articles.index().get(VectorIndices.Category());
        int checked = 0;
        int graphMissing = 0;
        int luceneMissing = 0;
        int jvectorMissing = 0;
        /* Uncapped by design: a longer soak must still check every live entity. */
        for (final ArticleState state : this.live.values()) {
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

    /// Appends one JSON line to the soak event log for post-run replay. Every
    /// line carries the run id and a per-run sequence number so interleaved or
    /// repeated runs stay separable and the chaos schedule is reconstructable
    /// from the ordered op events and their parameters. A logging failure is
    /// counted and fails the test at the end: a blind soak proves nothing.
    private void event(final long startNanos, final String type, final String detail) {
        synchronized (this.eventLock) {
            final long seq = this.eventSeq.incrementAndGet();
            final double elapsed = (System.nanoTime() - startNanos) / 1_000_000_000.0;
            final String line = "{\"run\":\"%s\",\"seq\":%d,\"t\":%.1f,\"type\":\"%s\",\"detail\":\"%s\"}%n".formatted(
                    this.runId, seq, elapsed, jsonEscape(type), jsonEscape(detail));
            try {
                final Path events = Path.of(System.getProperty("soak.events", "target/soak-events.jsonl"));
                final Path parent = events.toAbsolutePath().getParent();
                if (parent != null) Files.createDirectories(parent);
                Files.writeString(events, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (final Exception logging) {
                this.eventLogFailures.incrementAndGet();
                /* Enqueued immediately, not just counted: the outcome record below
                 * consults this queue first, so a blind run can never be labeled
                 * ok no matter when the write failed. */
                this.failures.add(new AssertionError("soak event log unavailable; the run is blind", logging));
                System.out.printf(Locale.ROOT, "SOAK event-log unavailable: %s%n", logging);
            }
        }
    }

    /// Records the attributable fate of one parked reader on its own sequence
    /// plus the chaos cause, so two parks can never collide even without an
    /// intervening event. The converge phase requires a fate for every parked
    /// reader, so a loss can never pass on counters alone.
    private void recordFate(final int reader, final String cause) {
        final String fate = "f%d:%s".formatted(this.fateSeq.incrementAndGet(), cause);
        this.readerFates.put(reader, fate);
        event(this.eventStartNanos, "reader-fate", "reader=%d fate=%s".formatted(reader, fate));
    }

    private static String jsonEscape(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
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

    private record ArticleState(String title, String body, float[] vector, long modifiedTx, int checksum) {
    }

    /// Fixed cluster facts every node restart or reseed must reproduce exactly:
    /// identity drift here manufactures a divergent member instead of a rejoin.
    private record Topology(UUID clusterId, UUID generation,
                            int controlPort, int livePort, int watermarkPort) {
    }

    /// Everything needed to rebuild one reader in place: its node home, its
    /// Store directory, and its stable node identity.
    private record ReaderSpec(Path nodeDir, Path storeDir, UUID nodeId) {
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /// Terminal outcome of one chaos op. Unexpected failures throw instead of
    /// returning, so the dispatcher accounts every dispatch exactly once as
    /// effective (weighted loop progress) or skipped (with cause).
    private record OpOutcome(int weight, String skipReason) {
        boolean isEffective() {
            return this.weight > 0;
        }

        static OpOutcome effective() {
            return new OpOutcome(1, null);
        }

        static OpOutcome effectiveHeavy() {
            return new OpOutcome(2, null);
        }

        static OpOutcome skipped(final String reason) {
            return new OpOutcome(0, reason);
        }
    }

        /// The soak writer's restartable state: transport, Store manager, and
    /// the live root, swappable as one unit.
    ///
    /// Writer threads resolve the pair under the soak's mutation lock before
    /// every transaction; [ #restart(boolean)] swaps both under the same lock, so no
    /// iteration can publish through a disposed transport. The restart keeps
    /// the cluster, Store-generation, and node identities, so the transport
    /// extends the same Archive recording and the fencing lease mints the
    /// next token for the same node — the production writer-restart cycle.
    private static final class WriterHandle implements AutoCloseable {
        private final Path storePath;
        private final Path nodeRoot;
        private final UUID clusterId;
        private final UUID nodeId;
        private final UUID generation;
        private final int controlPort;
        private final int livePort;
        private final int watermarkPort;
        private final Set<UUID> retentionReaders;
        private ClusterReplicationTransport transport;
        private EmbeddedStorageManager manager;
        private IndexRoot root;

        WriterHandle(
                final Path storePath, final Path nodeRoot,
                final UUID clusterId, final UUID nodeId, final UUID generation,
                final int controlPort, final int livePort, final int watermarkPort,
                final ClusterReplicationTransport transport,
                final Set<UUID> retentionReaders
        ) {
            this.storePath = storePath;
            this.nodeRoot = nodeRoot;
            this.clusterId = clusterId;
            this.nodeId = nodeId;
            this.generation = generation;
            this.controlPort = controlPort;
            this.livePort = livePort;
            this.watermarkPort = watermarkPort;
            this.transport = transport;
            this.retentionReaders = retentionReaders;
        }

        synchronized ClusterReplicationTransport transport() {
            return this.transport;
        }

        synchronized IndexRoot root() {
            return this.root;
        }

        synchronized ReplicationCursor latest() {
            return AeronStoreIntegrationIT.latest(this.transport);
        }

        synchronized Path storePath() {
            return this.storePath;
        }

        synchronized void install(final EmbeddedStorageManager manager, final IndexRoot root) {
            this.manager = manager;
            this.root = root;
        }

            /// Stops the Store and releases the transport, then brings both back
        /// with the same identities: the new transport extends the recording
        /// and the reloaded Store graph keeps every previously stored entity.
        /// An abrupt restart kills the transport before the Store quiesces (a
        /// crash shape); a clean one quiesces the Store first.
        ///
        /// @param abrupt transport-first teardown order
        synchronized void restart(final boolean abrupt)  {
            if (abrupt) {
                this.transport.close();
                if (this.manager != null) this.manager.shutdown();
            } else {
                if (this.manager != null) this.manager.shutdown();
                this.transport.close();
            }
            this.transport = new AeronClusterReplicationTransportProvider().create(
                    AeronStoreIntegrationIT.properties(this.nodeRoot, this.clusterId, this.nodeId,
                            this.generation, "writer", -1L,
                            this.controlPort, this.livePort, this.watermarkPort, this.retentionReaders));
            this.transport.positionProvider("store").init();
            final StorageBinaryDataDistributor distributor = this.transport.distributor("store", false);
            final EmbeddedStorageManager restarted = AeronStoreIntegrationIT.startExistingIndex(
                    this.storePath, distributor,
                    this.transport.persistenceTargetFactory("store", distributor));
            this.manager = restarted;
            this.root = restarted.root();
        }

        @Override
        public synchronized void close() {
            if (this.manager != null) {
                try {
                    this.manager.shutdown();
                } catch (final RuntimeException shutdownFailure) {
                    /* The transport close below is the stronger guarantee;
                     * a Store shutdown failure must not skip it. */
                }
            }
            if (this.transport != null) {
                this.transport.close();
                this.transport = null;
            }
            this.manager = null;
            this.root = null;
        }

        /// Snapshots the writer Store for a reseed: shuts ONLY the Store down,
        /// copies the directory while its channel files are fully quiescent,
        /// then restarts the manager on the SAME transport. The Archive keeps
        /// serving reader replays across the snapshot — a reseed of one reader
        /// must not disturb the others. The copy is consistent because the
        /// Store is closed (channel writes cannot trail the cursor); stopping
        /// the manager is the documented "writer stopped" from the seeding
        /// procedure applied to this node only.
        ///
        /// @param snapshotDir destination directory for the copy
        /// @return replication cursor matching the snapshot content
        /// @throws Exception when the stop, copy, or restart cannot complete
        synchronized ReplicationCursor snapshotCopy(final Path snapshotDir) throws Exception {
            if (this.manager != null) {
                this.manager.shutdown();
                this.manager = null;
                this.root = null;
            }
            final ReplicationCursor snapshot = this.transport.positionProvider("store").latest();
            AeronStoreIntegrationIT.delete(snapshotDir);
            AeronStoreIntegrationIT.copyDirectory(this.storePath, snapshotDir);
            final StorageBinaryDataDistributor distributor = this.transport.distributor("store", false);
            final EmbeddedStorageManager restarted = AeronStoreIntegrationIT.startExistingIndex(
                    this.storePath, distributor,
                    this.transport.persistenceTargetFactory("store", distributor));
            this.manager = restarted;
            this.root = restarted.root();
            return snapshot;
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
