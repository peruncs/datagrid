package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import peruncs.datagrid.cluster.node.replication.AfterDataMessageConsumedListener;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.StoredReplicationCursorManager;
import peruncs.datagrid.cluster.storage.types.*;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/// Test-only full-path benchmark: real four-channel Store writer, embedded
/// Archive, replay/live reader, Store import/materialization, and atomic cursor.
/// It deliberately has no production instrumentation or benchmark dependency.
public final class AeronFullPathBenchmark {
    private AeronFullPathBenchmark() {
    }

    public static void main(final String[] arguments) throws Exception {
        int payload = 64 * 1024;
        int warmup = 20;
        int iterations = 100;
        for (final String argument : arguments) {
            if (argument.startsWith("--payload=")) payload = Integer.parseInt(argument.substring(10));
            else if (argument.startsWith("--warmup=")) warmup = Integer.parseInt(argument.substring(9));
            else if (argument.startsWith("--iterations=")) iterations = Integer.parseInt(argument.substring(13));
        }
        final Result result = measure(payload, warmup, iterations);
        System.out.printf("payload=%d iterations=%d tx/s=%.1f MiB/s=%.1f p50-us=%.1f p99-us=%.1f heap-bytes/tx=%s direct-delta=%d mapped-delta=%d%n", result.payloadBytes(), result.iterations(),
                result.transactionsPerSecond(), result.mebibytesPerSecond(), result.p50Nanos() / 1_000.0,
                result.p99Nanos() / 1_000.0, result.heapBytesPerTransaction() < 0 ? "unavailable" :
                        Long.toString(result.heapBytesPerTransaction()), result.directMemoryDelta(), result.mappedMemoryDelta());
    }

    static Result measure(final int payloadBytes, final int warmup, final int iterations) throws Exception {
        if (payloadBytes <= 0 || warmup < 0 || iterations <= 0) {
            throw new IllegalArgumentException("invalid full-path benchmark parameters");
        }
        final Path root = Files.createTempDirectory("dg-aeron-full-benchmark-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = AeronStoreIntegrationIT.freePort();
        final int livePort = AeronStoreIntegrationIT.freePort();
        final int watermarkPort = AeronStoreIntegrationIT.freePort();
        final Path writerPath = root.resolve("writer-store");
        final Path readerPath = root.resolve("reader-store");
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation,
                        "writer", -1L, controlPort, livePort, watermarkPort))) {
            writerTransport.positionProvider("store").init();
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final AeronStoreIntegrationIT.Root initial = new AeronStoreIntegrationIT.Root();
            final EmbeddedStorageFoundation<?> seedFoundation = AeronStoreIntegrationIT.foundation(writerPath);
            DistributedStorage.configureWriting(seedFoundation, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final EmbeddedStorageManager seed = seedFoundation.start(initial);
            seed.storeRoot();
            seed.shutdown();
            final ReplicationCursor baseline = writerTransport.positionProvider("store").latest();
            AeronStoreIntegrationIT.copyDirectory(writerPath, readerPath);

            final EmbeddedStorageFoundation<?> writerFoundation = AeronStoreIntegrationIT.foundation(writerPath);
            DistributedStorage.configureWriting(writerFoundation, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final EmbeddedStorageManager writer = writerFoundation.start();
            final AeronStoreIntegrationIT.Root writerRoot = writer.root();

            final Path readerRoot = root.resolve("reader");
            try (ClusterReplicationTransport readerTransport = new AeronClusterReplicationTransportProvider().create(
                    AeronStoreIntegrationIT.properties(readerRoot, clusterId, UUID.randomUUID(), generation, "reader",
                            -1L, controlPort, livePort, watermarkPort));
                 StoredReplicationCursorManager cursorManager = StoredReplicationCursorManager.NewAtomic(
                         readerRoot.resolve("cursor"))) {
                final EmbeddedStorageFoundation<?> readerFoundation = AeronStoreIntegrationIT.foundation(readerPath);
                final EmbeddedStorageManager reader = readerFoundation.start();
                final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(
                        readerFoundation.getConnectionFoundation(), reader.createConnection(),
                        ObjectGraphUpdateHandler.Synchronized(), 0L, 1L);
                final StorageBinaryDataPacketAcceptor acceptor = StorageBinaryDataPacketAcceptor.New(merger);
                final AtomicLong resolved = new AtomicLong(baseline.logicalSequence());
                final StorageBinaryDataClient client = readerTransport.client(acceptor, "store",
                        new AfterDataMessageConsumedListener() {
                            @Override
                            public void onApplied(final ReplicationCursor cursor) {
                                cursorManager.set(cursor);
                                resolved.set(cursor.logicalSequence());
                            }

                            @Override
                            public void close() {
                            }
                        }, baseline, false);
                try {
                    client.start();
                    awaitLive(client);
                    /* Reuse one application payload during warmup and measurement. A new
                     * byte[] per transaction would make the benchmark mostly measure its
                     * own fixture allocation rather than Store/Aeron transport work. */
                    final byte[] payload = new byte[payloadBytes];
                    for (int i = 0; i < warmup; i++)
                        publishAndAwait(
                                writer, writerRoot, payload, i, writerTransport, resolved);
                    final AllocationSnapshot allocation = AllocationSnapshot.capture();
                    final long directBefore = poolBytes("direct");
                    final long mappedBefore = poolBytes("mapped");
                    final long[] latencies = new long[iterations];
                    final long started = System.nanoTime();
                    for (int i = 0; i < iterations; i++) {
                        final long transactionStart = System.nanoTime();
                        publishAndAwait(writer, writerRoot, payload, warmup + i, writerTransport, resolved);
                        latencies[i] = System.nanoTime() - transactionStart;
                    }
                    final long elapsed = System.nanoTime() - started;
                    final long allocated = allocation.bytesSinceCapture();
                    Arrays.sort(latencies);
                    client.stopAtLatestMessage();
                    awaitStopped(client);
                    acceptor.awaitApplied();
                    return new Result(payloadBytes, iterations, elapsed,
                            latencies[iterations / 2], latencies[Math.min(iterations - 1, (int) Math.ceil(iterations * 0.99) - 1)],
                            allocated < 0 ? -1L : allocated / iterations,
                            poolBytes("direct") - directBefore, poolBytes("mapped") - mappedBefore);
                } finally {
                    client.dispose();
                    acceptor.dispose();
                    reader.shutdown();
                    writer.shutdown();
                }
            }
        } finally {
            AeronStoreIntegrationIT.delete(root);
        }
    }

    private static void publishAndAwait(final EmbeddedStorageManager writer,
                                        final AeronStoreIntegrationIT.Root root, final byte[] payload, final int iteration,
                                        final ClusterReplicationTransport transport, final AtomicLong resolved) {
        Arrays.fill(payload, (byte) iteration);
        root.payload = payload;
        writer.store(root);
        final long target = transport.positionProvider("store").latestSequence();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (resolved.get() < target && System.nanoTime() < deadline) LockSupport.parkNanos(50_000L);
        if (resolved.get() != target) throw new IllegalStateException("reader did not apply sequence %s".formatted(target));
    }

    private static void awaitLive(final StorageBinaryDataClient client) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!client.isLive() && client.failure() == null && System.nanoTime() < deadline) LockSupport.parkNanos(100_000L);
        if (client.failure() != null) throw client.failure();
        if (!client.isLive()) throw new IllegalStateException("reader did not join the live stream");
    }

    private static void awaitStopped(final StorageBinaryDataClient client) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (client.isRunning() && System.nanoTime() < deadline) LockSupport.parkNanos(100_000L);
        if (client.isRunning()) throw new IllegalStateException("reader did not stop at live tail");
    }

    private static long poolBytes(final String name) {
        return ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                .filter(pool -> name.equals(pool.getName())).mapToLong(BufferPoolMXBean::getMemoryUsed).findFirst().orElse(0L);
    }

    record Result(int payloadBytes, int iterations, long elapsedNanos, long p50Nanos, long p99Nanos,
                  long heapBytesPerTransaction, long directMemoryDelta, long mappedMemoryDelta) {
        double transactionsPerSecond() {
            return iterations / (elapsedNanos / 1_000_000_000.0);
        }

        double mebibytesPerSecond() {
            return payloadBytes * (double) iterations / (elapsedNanos / 1_000_000_000.0) / (1024.0 * 1024.0);
        }
    }

    private record AllocationSnapshot(com.sun.management.ThreadMXBean bean, Map<Long, Long> allocated) {
        static AllocationSnapshot capture() {
            if (!(ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean bean) ||
                !bean.isThreadAllocatedMemorySupported()) return new AllocationSnapshot(null, Map.of());
            if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
            final Map<Long, Long> values = new HashMap<>();
            for (final long id : bean.getAllThreadIds()) values.put(id, bean.getThreadAllocatedBytes(id));
            return new AllocationSnapshot(bean, values);
        }

        long bytesSinceCapture() {
            if (this.bean == null) return -1L;
            long total = 0L;
            for (final long id : this.bean.getAllThreadIds()) {
                final Long before = this.allocated.get(id);
                final long after = this.bean.getThreadAllocatedBytes(id);
                if (before != null && before >= 0 && after >= before) total += after - before;
            }
            return total;
        }
    }
}
