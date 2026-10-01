package peruncs.cluster.node.aeron;

import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.storage.StorageGraphCoordinator;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/// Measures four concurrent Store callers against three real Aeron readers.
public final class AeronFullPathBenchmark {
    private static final int STAMP_CAPACITY = 1 << 18;
    private static final int STAMP_MASK = STAMP_CAPACITY - 1;
    private static volatile long runStarted;

    private AeronFullPathBenchmark() { }

    /// Runs the A1.10 five-window workload and writes its JSON result and JFR summary.
    static void main(final String[] arguments) throws Exception {
        int warmupSeconds = 60, windowSeconds = 60, windows = 5, writers = 4, readers = 3;
        final List<Integer> payloads = new ArrayList<>(List.of(1024, 64 * 1024));
        String commit = gitCommit();
        Path output = null;
        for (final String argument : arguments) {
            if (argument.startsWith("--payloads=")) {
                payloads.clear();
                for (final String value : argument.substring(11).split(",")) payloads.add(Integer.parseInt(value));
            } else if (argument.startsWith("--warmup-seconds=")) warmupSeconds = Integer.parseInt(argument.substring(17));
            else if (argument.startsWith("--window-seconds=")) windowSeconds = Integer.parseInt(argument.substring(17));
            else if (argument.startsWith("--windows=")) windows = Integer.parseInt(argument.substring(10));
            else if (argument.startsWith("--writers=")) writers = Integer.parseInt(argument.substring(10));
            else if (argument.startsWith("--readers=")) readers = Integer.parseInt(argument.substring(10));
            else if (argument.startsWith("--commit=")) commit = argument.substring(9);
            else if (argument.startsWith("--output=")) output = Path.of(argument.substring(9));
            else throw new IllegalArgumentException("unknown argument: " + argument);
        }
        if (payloads.isEmpty()) throw new IllegalArgumentException("at least one payload size is required");
        if (output == null) output = Path.of("bench/results", commit + ".json");

        final List<RunResult> results = new ArrayList<>(payloads.size());
        for (final int payload : payloads) {
            final Path jfr = Path.of("target/bench", "%s-%d.jfr".formatted(commit, payload));
            Files.createDirectories(jfr.getParent());
            try (Recording recording = new Recording(Configuration.getConfiguration("default"))) {
                recording.setName("A1.10-%d".formatted(payload));
                recording.setToDisk(true);
                recording.start();
                final Measurement measurement = measure(payload, warmupSeconds, windowSeconds, windows, writers, readers);
                recording.stop();
                recording.dump(jfr);
                final SoakJfrReport.Signals signals = SoakJfrReport.analyze(jfr);
                results.add(new RunResult(payload, measurement, jfr.toString(), signals.totalEvents(),
                        signals.maxGcPauseMs(), signals.gcPausesOver100Ms()));
            }
            final Measurement m = results.getLast().measurement();
            System.out.printf(Locale.ROOT,
                    "payload=%d writer-p99-ms=%.3f writer-tx/s=%.2f reader-apply-p99-ms=%.3f e2e-p99-ms=%.3f read-p99-idle/loaded-ms=%.4f/%.4f%n",
                    payload, m.writerP99Nanos() / 1e6, m.writerCommitsPerSecond(), m.readerApplyP99Nanos() / 1e6,
                    m.endToEndP99Nanos() / 1e6, m.idleReadP99Nanos() / 1e6, m.loadedReadP99Nanos() / 1e6);
        }
        writeJson(output, commit, results, warmupSeconds, windowSeconds, windows, writers, readers);
        System.out.println("result=" + output.toAbsolutePath());
    }

    /// Runs a configurable workload; short durations are intended for runner checks only.
    static Measurement measure(final int payloadBytes, final int warmupSeconds, final int windowSeconds,
                               final int windowCount, final int writerCount, final int readerCount) throws Exception {
        if (payloadBytes <= 0 || warmupSeconds < 0 || windowSeconds <= 0 || windowCount <= 0 ||
                writerCount <= 0 || readerCount <= 0) throw new IllegalArgumentException("invalid benchmark parameters");
        final Path root = Files.createTempDirectory("peruncs-aeron-bench-");
        final UUID clusterId = UUID.randomUUID(), generation = UUID.randomUUID();
        final int[] ports = AeronStoreIntegrationIT.freePorts(3);
        final Path writerAeron = root.resolve("writer-aeron"), writerStore = root.resolve("writer-store");
        final Path[] readerAerons = new Path[readerCount], readerStores = new Path[readerCount];
        final UUID[] readerIds = new UUID[readerCount];
        for (int i = 0; i < readerCount; i++) {
            readerAerons[i] = root.resolve("reader-%d-aeron".formatted(i));
            readerStores[i] = root.resolve("reader-%d-store".formatted(i));
            readerIds[i] = UUID.randomUUID();
        }
        try (ClusterReplicationTransport transport = new AeronTransport(
                AeronStoreIntegrationIT.properties(writerAeron, clusterId, UUID.randomUUID(), generation,
                        "writer", -1L, ports[0], ports[1], ports[2]))) {
            transport.positionProvider().init();
            final var distributor = transport.distributor();
            final AeronStoreIntegrationIT.Root seedRoot = new AeronStoreIntegrationIT.Root();
            seedRoot.payload = new byte[payloadBytes];
            final EmbeddedStorageManager seed = AeronStoreIntegrationIT.start(writerStore, seedRoot, distributor, transport);
            seed.shutdown();
            for (final Path readerStore : readerStores) AeronStoreIntegrationIT.copyDirectory(writerStore, readerStore);

            final EmbeddedStorageManager writer = AeronStoreIntegrationIT.startExisting(writerStore, distributor, transport);
            final AeronStoreIntegrationIT.Root writerRoot = writer.root();
            final AeronStoreIntegrationIT.ReaderNode[] readerNodes = new AeronStoreIntegrationIT.ReaderNode[readerCount];
            final StampRing stamps = new StampRing(readerCount);
            final long initialSequence = AeronStoreIntegrationIT.latestSequence(transport);
            try {
                for (int i = 0; i < readerCount; i++) {
                    final int readerIndex = i;
                    final AtomicLong nextImportSequence = new AtomicLong(initialSequence);
                    readerNodes[i] = AeronStoreIntegrationIT.ReaderNode.openForBenchmark(
                            readerAerons[i], readerStores[i], readerIds[i], clusterId, generation,
                            ports[0], ports[1], ports[2],
                            ignored -> stamps.importStarted(readerIndex, nextImportSequence.incrementAndGet(), System.nanoTime()));
                    readerNodes[i].start();
                }
                for (final AeronStoreIntegrationIT.ReaderNode reader : readerNodes) reader.awaitLive();
                awaitReaders(readerNodes, initialSequence);
                final StorageGraphCoordinator graph = new StorageGraphCoordinator();
                final long idleReadP99 = idleReadP99(graph);
                final Measurement loaded = runLoad(warmupSeconds, windowSeconds, windowCount, writerCount,
                        readerNodes, writer, writerRoot, transport, graph, initialSequence, stamps);
                return new Measurement(loaded.writerCommits(), loaded.elapsedNanos(), loaded.writerP50Nanos(),
                        loaded.writerP99Nanos(), loaded.writerCommitsPerSecond(), loaded.readerApplyP99Nanos(),
                        loaded.endToEndP99Nanos(), idleReadP99, loaded.loadedReadP99Nanos(),
                        loaded.windows(), loaded.readerMetrics());
            } finally {
                for (final AeronStoreIntegrationIT.ReaderNode reader : readerNodes) if (reader != null) reader.close();
                writer.shutdown();
            }
        } finally {
            AeronStoreIntegrationIT.delete(root);
        }
    }

    private static Measurement runLoad(
            final int warmupSeconds, final int windowSeconds, final int windowCount, final int writerCount,
            final AeronStoreIntegrationIT.ReaderNode[] readers, final EmbeddedStorageManager writer,
            final AeronStoreIntegrationIT.Root root, final ClusterReplicationTransport transport,
            final StorageGraphCoordinator graph, final long initialSequence, final StampRing stamps
    ) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(writerCount + readers.length + 1,
                Thread.ofPlatform().name("a1-benchmark-", 0).factory());
        final CountDownLatch ready = new CountDownLatch(writerCount + readers.length + 1), start = new CountDownLatch(1);
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final WriterSamples[] writerSamples = new WriterSamples[writerCount];
        final ReaderSamples[] readerSamples = new ReaderSamples[readers.length];
        final List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            writerSamples[i] = new WriterSamples(windowCount);
            final int writerIndex = i;
            futures.add(executor.submit(() -> {
                ready.countDown();
                awaitStart(start, failure);
                long value = writerIndex;
                final long[] sequence = new long[1];
                try {
                    for (;;) {
                        if (failure.get() != null) return;
                        final long txStart = System.nanoTime();
                        final int window = windowIndex(txStart, warmupSeconds, windowSeconds, windowCount);
                        if (window == DONE) return;
                        final byte payloadValue = (byte) value++;
                        graph.write(() -> {
                            Arrays.fill(root.payload, payloadValue);
                            sequence[0] = AeronStoreIntegrationIT.store(transport, writer, root.payload);
                        });
                        final long txDone = System.nanoTime();
                        stamps.publish(sequence[0], txStart, txDone);
                        if (window >= 0) writerSamples[writerIndex].commits[window].add(txDone - txStart);
                    }
                } catch (final Throwable problem) {
                    failure.compareAndSet(null, problem);
                }
            }));
        }
        for (int i = 0; i < readers.length; i++) {
            final int index = i;
            readerSamples[i] = new ReaderSamples(windowCount);
            futures.add(executor.submit(() -> {
                ready.countDown();
                awaitStart(start, failure);
                trackReader(readers[index], readerSamples[index], stamps, stop, failure,
                        index, initialSequence, warmupSeconds, windowSeconds, windowCount);
            }));
        }
        final ReadSamples readSamples = new ReadSamples(windowCount);
        futures.add(executor.submit(() -> {
            ready.countDown();
            awaitStart(start, failure);
            while (!stop.get()) {
                final long began = System.nanoTime();
                final int window = windowIndex(began, warmupSeconds, windowSeconds, windowCount);
                if (window == DONE) return;
                graph.read(() -> { });
                if (window >= 0) readSamples.latencies[window].add(System.nanoTime() - began);
                LockSupport.parkNanos(100_000L);
            }
        }));
        try {
            if (!ready.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("benchmark workers did not start");
            runStarted = System.nanoTime();
            start.countDown();
            final long end = runStarted + TimeUnit.SECONDS.toNanos((long) warmupSeconds + (long) windowSeconds * windowCount);
            while (System.nanoTime() < end && failure.get() == null) LockSupport.parkNanos(1_000_000L);
            for (int i = 0; i < writerCount; i++) futures.get(i).get(30, TimeUnit.SECONDS);
            stop.set(true);
            for (int i = writerCount; i < futures.size(); i++) futures.get(i).get(60, TimeUnit.SECONDS);
            if (failure.get() != null) throw new IllegalStateException("benchmark worker failed", failure.get());
        } finally {
            stop.set(true);
            executor.shutdownNow();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS))
                throw new IllegalStateException("benchmark workers did not stop");
        }

        final long elapsed = TimeUnit.SECONDS.toNanos((long) windowSeconds * windowCount);
        final List<WindowResult> windows = new ArrayList<>(windowCount);
        final Samples allWrites = new Samples();
        final List<ReaderMetric> aggregateReaders = new ArrayList<>(readers.length);
        long totalWrites = 0;
        for (int window = 0; window < windowCount; window++) {
            final Samples writes = new Samples();
            for (final WriterSamples sample : writerSamples) writes.addAll(sample.commits[window]);
            allWrites.addAll(writes);
            totalWrites += writes.size;
            final List<ReaderMetric> perReader = new ArrayList<>(readers.length);
            for (int reader = 0; reader < readers.length; reader++) {
                final Samples apply = readerSamples[reader].apply[window], e2e = readerSamples[reader].endToEnd[window];
                perReader.add(new ReaderMetric(apply.percentile(.99), e2e.percentile(.99), apply.size));
            }
            windows.add(new WindowResult(writes.size, writes.percentile(.50), writes.percentile(.99),
                    writes.size / (double) windowSeconds, readSamples.latencies[window].percentile(.99),
                    List.copyOf(perReader)));
        }
        for (int reader = 0; reader < readers.length; reader++) {
            final Samples apply = new Samples(), e2e = new Samples();
            for (int window = 0; window < windowCount; window++) {
                apply.addAll(readerSamples[reader].apply[window]);
                e2e.addAll(readerSamples[reader].endToEnd[window]);
            }
            aggregateReaders.add(new ReaderMetric(apply.percentile(.99), e2e.percentile(.99), apply.size));
        }
        for (int reader = 0; reader < aggregateReaders.size(); reader++) {
            if (aggregateReaders.get(reader).samples() != totalWrites) {
                throw new IllegalStateException("reader %d measured %d of %d writes"
                        .formatted(reader, aggregateReaders.get(reader).samples(), totalWrites));
            }
        }
        final Samples allReads = new Samples();
        for (final Samples windowReads : readSamples.latencies) allReads.addAll(windowReads);
        final long readP99 = allReads.percentile(.99);
        final long readerApplyP99 = aggregateReaders.stream().mapToLong(ReaderMetric::applyP99Nanos).max().orElse(0L);
        final long endToEndP99 = aggregateReaders.stream().mapToLong(ReaderMetric::endToEndP99Nanos).max().orElse(0L);
        return new Measurement(totalWrites, elapsed, allWrites.percentile(.50), allWrites.percentile(.99),
                totalWrites / (elapsed / 1e9), readerApplyP99, endToEndP99,
                0L, readP99, List.copyOf(windows), List.copyOf(aggregateReaders));
    }

    private static void trackReader(
            final AeronStoreIntegrationIT.ReaderNode reader, final ReaderSamples samples, final StampRing stamps,
            final AtomicBoolean stop, final AtomicReference<Throwable> failure, final int readerIndex,
            final long initialSequence,
            final int warmupSeconds, final int windowSeconds, final int windowCount
    ) {
        long last = initialSequence, pending = -1L, observed = 0L, missingSince = 0L;
        try {
            while (!stop.get() || last < stamps.latestSequence()) {
                final long applied = reader.appliedSequence();
                if (applied > last) {
                    if (pending < 0L) { pending = last + 1L; observed = System.nanoTime(); }
                    while (pending <= applied) {
                        final Stamp stamp = stamps.read(readerIndex, pending);
                        if (stamp == null) {
                            if (missingSince == 0L) missingSince = System.nanoTime();
                            if (System.nanoTime() - missingSince > TimeUnit.SECONDS.toNanos(15))
                                throw new IllegalStateException("writer timestamp missing for sequence " + pending);
                            break;
                        }
                        missingSince = 0L;
                        final int window = windowIndex(stamp.started(), warmupSeconds, windowSeconds, windowCount);
                        if (window >= 0) {
                            samples.apply[window].add(observed - stamp.applyStarted());
                            samples.endToEnd[window].add(Math.max(0L, observed - stamp.committed()));
                        }
                        last = pending++;
                        // One poll observed the whole applied frontier; retain its timestamp for
                        // every sequence at or below that frontier instead of undercounting later
                        // entries while walking the already-applied range.
                    }
                } else {
                    pending = -1L;
                    LockSupport.parkNanos(100_000L);
                }
                if (stop.get() && last >= stamps.latestSequence()) return;
            }
        } catch (final Throwable problem) {
            failure.compareAndSet(null, problem);
        }
    }

    private static long idleReadP99(final StorageGraphCoordinator graph) {
        final Samples samples = new Samples();
        for (int i = 0; i < 10_000; i++) {
            final long started = System.nanoTime();
            graph.read(() -> { });
            samples.add(System.nanoTime() - started);
        }
        return samples.percentile(.99);
    }

    private static void awaitReaders(final AeronStoreIntegrationIT.ReaderNode[] readers, final long target) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            boolean ready = true;
            for (final AeronStoreIntegrationIT.ReaderNode reader : readers) ready &= reader.appliedSequence() >= target;
            if (ready) return;
            LockSupport.parkNanos(100_000L);
        }
        throw new IllegalStateException("benchmark readers did not reach seed sequence " + target);
    }

    private static void awaitStart(final CountDownLatch start, final AtomicReference<Throwable> failure) {
        try {
            if (!start.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("benchmark start timed out");
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            failure.compareAndSet(null, interrupted);
        }
    }

    private static final int DONE = -2;
    private static int windowIndex(final long time, final int warmup, final int windowSeconds, final int count) {
        final long offset = time - runStarted - TimeUnit.SECONDS.toNanos(warmup);
        if (offset < 0L) return -1;
        final long window = offset / TimeUnit.SECONDS.toNanos(windowSeconds);
        return window >= count ? DONE : (int) window;
    }

    private record Stamp(long started, long committed, long applyStarted) { }
    private static final class StampRing {
        final AtomicLongArray sequences = new AtomicLongArray(STAMP_CAPACITY);
        final AtomicLongArray started = new AtomicLongArray(STAMP_CAPACITY);
        final AtomicLongArray committed = new AtomicLongArray(STAMP_CAPACITY);
        final AtomicLong latest = new AtomicLong();
        final AtomicLongArray[] applySequences;
        final AtomicLongArray[] applyStarted;
        StampRing(final int readers) {
            this.applySequences = new AtomicLongArray[readers];
            this.applyStarted = new AtomicLongArray[readers];
            for (int i = 0; i < readers; i++) {
                this.applySequences[i] = new AtomicLongArray(STAMP_CAPACITY);
                this.applyStarted[i] = new AtomicLongArray(STAMP_CAPACITY);
            }
        }
        void publish(final long sequence, final long begin, final long end) {
            final int slot = (int) sequence & STAMP_MASK;
            this.started.set(slot, begin);
            this.committed.set(slot, end);
            this.sequences.set(slot, sequence);
            this.latest.accumulateAndGet(sequence, Math::max);
        }
        void importStarted(final int reader, final long sequence, final long at) {
            final int slot = (int) sequence & STAMP_MASK;
            if (this.applySequences[reader].get(slot) != sequence) {
                this.applyStarted[reader].set(slot, at);
                this.applySequences[reader].set(slot, sequence);
            }
        }
        Stamp read(final int reader, final long sequence) {
            final int slot = (int) sequence & STAMP_MASK;
            return this.sequences.get(slot) == sequence && this.applySequences[reader].get(slot) == sequence
                    ? new Stamp(this.started.get(slot), this.committed.get(slot), this.applyStarted[reader].get(slot))
                    : null;
        }
        long latestSequence() { return this.latest.get(); }
    }

    private static final class WriterSamples {
        final Samples[] commits;
        WriterSamples(final int n) { this.commits = samples(n); }
    }
    private static final class ReaderSamples {
        final Samples[] apply, endToEnd;
        ReaderSamples(final int n) { this.apply = samples(n); this.endToEnd = samples(n); }
    }
    private static final class ReadSamples {
        final Samples[] latencies;
        ReadSamples(final int n) { this.latencies = samples(n); }
    }
    private static Samples[] samples(final int n) {
        final Samples[] result = new Samples[n];
        Arrays.setAll(result, ignored -> new Samples());
        return result;
    }
    private static final class Samples {
        long[] values = new long[4096];
        int size;
        void add(final long value) {
            if (this.size == this.values.length) this.values = Arrays.copyOf(this.values, this.size << 1);
            this.values[this.size++] = value;
        }
        void addAll(final Samples other) { for (int i = 0; i < other.size; i++) this.add(other.values[i]); }
        long percentile(final double p) {
            return this.size == 0 ? 0L : AeronFullPathBenchmark.percentile(Arrays.copyOf(this.values, this.size), p);
        }
    }
    private static long percentile(final long[] values, final double p) {
        if (values.length == 0) return 0L;
        Arrays.sort(values);
        return values[Math.min(values.length - 1, Math.max(0, (int) Math.ceil(values.length * p) - 1))];
    }

    record ReaderMetric(long applyP99Nanos, long endToEndP99Nanos, int samples) { }
    record WindowResult(long commits, long writerP50Nanos, long writerP99Nanos,
                        double commitsPerSecond, long readP99Nanos, List<ReaderMetric> readers) { }
    record Measurement(long writerCommits, long elapsedNanos, long writerP50Nanos, long writerP99Nanos,
                       double writerCommitsPerSecond, long readerApplyP99Nanos, long endToEndP99Nanos,
                       long idleReadP99Nanos, long loadedReadP99Nanos,
                       List<WindowResult> windows, List<ReaderMetric> readerMetrics) { }
    private record RunResult(int payloadBytes, Measurement measurement, String jfrFile, long jfrEvents,
                             long maxGcPauseMs, long gcPausesOver100Ms) { }

    private static void writeJson(final Path output, final String commit, final List<RunResult> results,
                                  final int warmup, final int windowSeconds, final int windows,
                                  final int writers, final int readers) throws IOException {
        Files.createDirectories(output.toAbsolutePath().getParent());
        final StringBuilder json = new StringBuilder(16_384);
        json.append("{\n  \"schemaVersion\": 1,\n  \"commit\": \"").append(commit)
                .append("\",\n  \"environment\": {\"os\": \"").append(escape(System.getProperty("os.name")))
                .append("\", \"arch\": \"").append(escape(System.getProperty("os.arch")))
                .append("\", \"java\": \"").append(escape(System.getProperty("java.runtime.version")))
                .append("\", \"processors\": ").append(ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors())
                .append("},\n  \"workload\": {\"writers\": ").append(writers).append(", \"readers\": ").append(readers)
                .append(", \"warmupSeconds\": ").append(warmup).append(", \"windowSeconds\": ").append(windowSeconds)
                .append(", \"windows\": ").append(windows).append("},\n  \"payloads\": [\n");
        for (int i = 0; i < results.size(); i++) {
            final RunResult run = results.get(i);
            final Measurement m = run.measurement();
            if (i != 0) json.append(",\n");
            json.append("    {\"payloadBytes\": ").append(run.payloadBytes())
                    .append(", \"writerCommits\": ").append(m.writerCommits())
                    .append(", \"writerP50Nanos\": ").append(m.writerP50Nanos())
                    .append(", \"writerP99Nanos\": ").append(m.writerP99Nanos())
                    .append(", \"writerCommitsPerSecond\": ").append(m.writerCommitsPerSecond())
                    .append(", \"readerApplyP99Nanos\": ").append(m.readerApplyP99Nanos())
                    .append(", \"liveEndToEndP99Nanos\": ").append(m.endToEndP99Nanos())
                    .append(", \"readBoundaryIdleP99Nanos\": ").append(m.idleReadP99Nanos())
                    .append(", \"readBoundaryUnderWriteP99Nanos\": ").append(m.loadedReadP99Nanos())
                    .append(", \"readers\": [");
            for (int r = 0; r < m.readerMetrics().size(); r++) {
                if (r != 0) json.append(',');
                final ReaderMetric metric = m.readerMetrics().get(r);
                json.append("{\"applyP99Nanos\": ").append(metric.applyP99Nanos())
                        .append(", \"endToEndP99Nanos\": ").append(metric.endToEndP99Nanos())
                        .append(", \"samples\": ").append(metric.samples()).append('}');
            }
            json.append("], \"windows\": [");
            for (int w = 0; w < m.windows().size(); w++) {
                if (w != 0) json.append(',');
                final WindowResult item = m.windows().get(w);
                json.append("{\"commits\": ").append(item.commits())
                        .append(", \"writerP50Nanos\": ").append(item.writerP50Nanos())
                        .append(", \"writerP99Nanos\": ").append(item.writerP99Nanos())
                        .append(", \"commitsPerSecond\": ").append(item.commitsPerSecond())
                        .append(", \"readBoundaryP99Nanos\": ").append(item.readP99Nanos())
                        .append(", \"readers\": [");
                for (int r = 0; r < item.readers().size(); r++) {
                    if (r != 0) json.append(',');
                    final ReaderMetric metric = item.readers().get(r);
                    json.append("{\"applyP99Nanos\": ").append(metric.applyP99Nanos())
                            .append(", \"endToEndP99Nanos\": ").append(metric.endToEndP99Nanos())
                            .append(", \"samples\": ").append(metric.samples()).append('}');
                }
                json.append("]}");
            }
            json.append("], \"jfr\": {\"file\": \"").append(escape(run.jfrFile()))
                    .append("\", \"events\": ").append(run.jfrEvents())
                    .append(", \"maxGcPauseMs\": ").append(run.maxGcPauseMs())
                    .append(", \"gcPausesOver100Ms\": ").append(run.gcPausesOver100Ms()).append("}}");
        }
        json.append("\n  ]\n}\n");
        Files.writeString(output, json);
    }
    private static String escape(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
    private static String gitCommit() {
        try {
            final Process process = new ProcessBuilder("git", "rev-parse", "--short", "HEAD").redirectErrorStream(true).start();
            final String result = new String(process.getInputStream().readAllBytes()).trim();
            return process.waitFor() == 0 ? result : "unknown";
        } catch (final IOException | InterruptedException failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            return "unknown";
        }
    }
}
