package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Regression budget for merger hot-path heap allocation.
///
/// Runs real import transactions through [StorageBinaryDataMerger] and asserts
/// the average heap allocation per transaction stays under an explicit budget.
/// The budget covers the merger, the chunker scratch reuse, the Store import
/// plus materialization, and the post-materialization index-policy scan —
///
/// not the one-time writer capture or Store startup. It is deliberately
/// generous (a multiple of the measured steady state) so ordinary JDK/GC noise
/// cannot flake it, while a hot-path allocation regression (a per-transaction
/// list, lambda, or copy) still trips it.
///
/// Measured steady state on the reference machine (JDK 26, Apple Silicon) was
/// roughly 31 KiB per transaction, stable within tens of bytes across runs;
/// the budget below was 8x that. After the per-batch allocations were removed
/// (reused drain scratch, no per-batch scope/list/flag/Duration, reused
/// validation scratch), the measured steady state is roughly 20 KiB per
/// transaction. The budget is deliberately left unchanged: it still bounds the
/// same hot path with ample headroom for JDK/GC noise, and tightening it
/// would buy nothing but flake risk on other machines.
class StorageBinaryDataMergerAllocationTest {
    private static final long BUDGET_BYTES_PER_TRANSACTION = 256L * 1024L;
    private static final int WARMUP_TRANSACTIONS = 10;
    private static final int MEASURED_TRANSACTIONS = 40;

    /// Verifies replaying captured transactions keeps average merger heap allocation per transaction within budget.
    @Test
    void perTransactionHeapAllocationStaysWithinBudget() throws Exception {
        final Path root = Files.createTempDirectory("datagrid-merger-allocation-");
        try {
            final CapturingDistributor capture = new CapturingDistributor();
            final Root initial = new Root();
            initial.values.add("seed");
            final EmbeddedStorageFoundation<?> writerFoundation = foundation(root);
            DistributedStorage.configureWriting(writerFoundation, capture, new ReplicatingTargetFactory(capture));
            final EmbeddedStorageManager storage;
            try {
                storage = writerFoundation.start(initial);
            } catch (final RuntimeException startupFailure) {
                throw new IllegalStateException("cannot start allocation fixture Store", startupFailure);
            }
            final List<BufferSnapshot> transaction;
            try {
                storage.storeRoot();
                for (int i = 0; i < 4; i++) {
                    initial.values.add("allocation-probe-%s".formatted(i));
                    storage.store(initial.values);
                }
                assertFalse(capture.transactions.isEmpty(), "writer must produce replayable transactions");
                transaction = capture.transactions.getLast();
                assertFalse(transaction.isEmpty(), "captured transaction must carry buffers");
            } finally {
                storage.shutdown();
            }

            final EmbeddedStorageManager reader = foundation(root).start();
            try {
                final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(StorageBinaryDataMergerTestSupport.configuration(StorageBinaryDataMergerTestSupport.foundation(), reader.createConnection(), ObjectGraphUpdateHandler.PerStore(new StorageGraphCoordinator()), 0L, 1_000_000L, 60_000L));
                try {
                    for (int i = 0; i < WARMUP_TRANSACTIONS; i++) {
                        merger.receiveDataOwned(transactionBinary(transaction));
                        merger.awaitApplied();
                    }
                    final AllocationSnapshot before = AllocationSnapshot.capture();
                    for (int i = 0; i < MEASURED_TRANSACTIONS; i++) {
                        merger.receiveDataOwned(transactionBinary(transaction));
                        merger.awaitApplied();
                    }
                    final long total = before.bytesSinceCapture();
                    assertTrue(total >= 0L, "thread allocation accounting is unavailable on this runtime");
                    final long perTransaction = total / MEASURED_TRANSACTIONS;
                    System.out.printf("merger heap allocation per transaction: %s bytes (budget %s bytes)%n", perTransaction, BUDGET_BYTES_PER_TRANSACTION);
                    assertTrue(perTransaction <= BUDGET_BYTES_PER_TRANSACTION,
                            "heap allocation per transaction %s exceeds budget %s"
                                    .formatted(perTransaction, BUDGET_BYTES_PER_TRANSACTION));
                } finally {
                    merger.dispose();
                }
            } finally {
                reader.shutdown();
            }
        } finally {
            delete(root);
        }
    }

    private static Binary transactionBinary(final List<BufferSnapshot> transaction) {
        final ByteBuffer[] buffers = new ByteBuffer[transaction.size()];
        for (int i = 0; i < transaction.size(); i++) {
            buffers[i] = transaction.get(i).restore();
        }
        return ChunksWrapper.New(buffers);
    }

    private static EmbeddedStorageFoundation<?> foundation(final Path path) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        return EmbeddedStorage.Foundation(configuration);
    }

    private static void delete(final Path path) throws Exception {
        if (Files.exists(path)) {
            try (var paths = Files.walk(path)) {
                paths.sorted(Comparator.reverseOrder()).forEach(value ->
                {
                    try {
                        Files.deleteIfExists(value);
                    } catch (final Exception ignored) {
                    }
                });
            }
        }
    }

    private record BufferSnapshot(int capacity, int position, int limit, byte[] content) {
        ByteBuffer restore() {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(this.capacity);
            buffer.put(this.content);
            buffer.position(this.position);
            buffer.limit(this.limit);
            return buffer;
        }
    }

    private static final class CapturingDistributor implements StorageBinaryDataDistributor {
        final List<List<BufferSnapshot>> transactions = new ArrayList<>();

        @Override
        public synchronized void distributeData(final Binary data) {
            final List<BufferSnapshot> snapshots = new ArrayList<>();
            data.iterateChannelChunks(chunk ->
            {
                for (final ByteBuffer buffer : chunk.buffers()) {
                    final ByteBuffer view = buffer.duplicate();
                    final int position = view.position();
                    final int limit = view.limit();
                    final byte[] content = new byte[view.capacity()];
                    view.clear();
                    view.get(content);
                    snapshots.add(new BufferSnapshot(view.capacity(), position, limit, content));
                }
            });
            this.transactions.add(snapshots);
        }

        @Override
        public synchronized void distributeTypeDictionary(final String ignored) {
        }

        @Override
        public void dispose() {
        }
    }

    private record AllocationSnapshot(com.sun.management.ThreadMXBean bean, Map<Long, Long> allocated) {
        static AllocationSnapshot capture() {
            if (!(ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean bean) ||
                !bean.isThreadAllocatedMemorySupported()) {
                return new AllocationSnapshot(null, Map.of());
            }
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

    public static final class Root {
        public final List<String> values = new ArrayList<>();
    }
}
