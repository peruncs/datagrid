package peruncs.datagrid.cluster.storage.aeron.reader;

import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelopeTestSupport;
import peruncs.datagrid.cluster.storage.binary.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


/// Contract tests for the batched delivery barrier: staging, window-size
/// auto-flush, idle flush by the polling loop, and the delivery-listener
/// marker opening exactly once per barrier.
class TransactionAssemblerBarrierTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final long EPOCH = 17;

    private static TransactionAssembler assembler(final int window, final StorageBinaryDataReceiver receiver,
                                                  final Runnable resolved, final ReaderDeliveryListener listener) {
        return TransactionAssemblerTestSupport.New(
                AeronReplicationConfiguration.builder()
                        .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024)
                        .readerBarrierMaxTransactions(window)
                        .build(),
                CLUSTER, EPOCH, -1L, receiver, resolved, listener);
    }

    private static StorageBinaryDataReceiver swallowingReceiver() {
        return new StorageBinaryDataReceiver() {
            @Override
            public void receiveData(final Binary value) {
            }

            @Override
            public void receiveTypeDictionary(final String value) {
            }
        };
    }

    private static void accept(final TransactionAssembler assembler, final byte[] bytes) {
        assembler.onFragment(new UnsafeBuffer(bytes), 0, bytes.length, null);
    }

    private static void commitOne(final TransactionAssembler assembler, final long sequence) {
        final byte[] data = {(byte) sequence};
        accept(assembler, AeronReplicationEnvelopeTestSupport.encode(CLUSTER, EPOCH, 1L, sequence,
                AeronReplicationEnvelope.Kind.STORE_BINARY, data.length, 0, 1, 0, 0, data));
        accept(assembler, AeronReplicationEnvelopeTestSupport.encode(CLUSTER, EPOCH, 1L, sequence,
                AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
                AeronReplicationEnvelope.crc32c(data), new byte[0]));
    }

    /// A durability callback that blocks or fails must leave status and the
    /// cursor snapshot on the previous durable boundary, keep the marker
    /// open, and stay retryable: the staged barrier is not drained until the
    /// callback reports the new boundary durable.
    @Test
    void blockedOrFailedCallbackKeepsPreviousDurableBoundary() throws Exception {
        final CountDownLatch callbackEntered = new CountDownLatch(1);
        final CountDownLatch releaseCallback = new CountDownLatch(1);
        final var blockCallback = new java.util.concurrent.atomic.AtomicBoolean(false);
        final var failCallback = new java.util.concurrent.atomic.AtomicBoolean(false);
        final TransactionAssembler assembler = assembler(8, swallowingReceiver(), () ->
        {
            if (blockCallback.get()) {
                callbackEntered.countDown();
                try {
                    releaseCallback.await(10, TimeUnit.SECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (failCallback.getAndSet(false)) {
                throw new IllegalStateException("transient cursor force failure");
            }
        }, null);

        commitOne(assembler, 0);
        assembler.flushDeliveries();
        assertEquals(0L, assembler.lastResolvedSequence(), "the first barrier is durable");
        commitOne(assembler, 1);

        /* The blocked callback parks the flush on a helper thread while a
         * concurrent reader must keep seeing the previous durable boundary. */
        final var flusher = Executors.newSingleThreadExecutor();
        try {
            blockCallback.set(true);
            final var flushed = flusher.submit(assembler::flushDeliveries);
            assertTrue(callbackEntered.await(5, TimeUnit.SECONDS), "the callback never started");
            assertEquals(0L, assembler.lastResolvedSequence(),
                    "status must keep the previous boundary while the callback runs");
            assertEquals(0L, assembler.cursorSnapshot().sequence(),
                    "the cursor snapshot must keep the previous boundary while the callback runs");
            assertEquals(1, assembler.unflushedDeliveryCount(),
                    "a callback in flight must not drain the staged barrier");

            /* A first failed callback must also stay retryable. */
            failCallback.set(true);
            blockCallback.set(false);
            releaseCallback.countDown();
            assertThrows(java.util.concurrent.ExecutionException.class, flushed::get);
            assertEquals(0L, assembler.lastResolvedSequence(), "a failed callback must not publish the boundary");
            assertEquals(1, assembler.unflushedDeliveryCount(), "a failed callback must keep the staged barrier");

            /* The retry persists the boundary and drains. */
            assembler.flushDeliveries();
            assertEquals(1L, assembler.lastResolvedSequence());
            assertEquals(1L, assembler.cursorSnapshot().sequence());
            assertEquals(0, assembler.unflushedDeliveryCount());
        } finally {
            blockCallback.set(false);
            releaseCallback.countDown();
            flusher.shutdownNow();
        }
    }

    /// Transactions under the window stay staged until an explicit flush.
    @Test
    void stagedTransactionsPublishOnlyOnFlush() {
        final AtomicInteger resolved = new AtomicInteger();
        final AtomicInteger before = new AtomicInteger();
        final AtomicInteger after = new AtomicInteger();
        final TransactionAssembler assembler = assembler(4, swallowingReceiver(), resolved::incrementAndGet,
                new ReaderDeliveryListener() {
                    @Override
                    public void beforeStoreImport(final long sequence, final long position, final int dataLength,
                                                  final int dataChunkCount, final int crc32c) {
                        before.incrementAndGet();
                    }

                    @Override
                    public void afterStoreImport() {
                        after.incrementAndGet();
                    }
                });

        commitOne(assembler, 0);
        commitOne(assembler, 1);
        commitOne(assembler, 2);
        assertEquals(-1, assembler.lastResolvedSequence(), "staged transactions must not resolve early");
        assertEquals(3, assembler.unflushedDeliveryCount());
        assertEquals(1, before.get(), "the barrier opens exactly one uncertainty marker");
        assertEquals(0, after.get(), "the marker stays open until the barrier flushes");
        assertEquals(0, resolved.get(), "no resolved callback before the flush");

        assembler.flushDeliveries();
        assertEquals(2, assembler.lastResolvedSequence());
        assertEquals(2, assembler.lastAppliedSequence());
        assertEquals(0, assembler.unflushedDeliveryCount());
        assertEquals(1, resolved.get(), "one durable callback represents the barrier tail");
        assertEquals(1, before.get());
        assertEquals(1, after.get());
    }

    /// A full window reports full, and the reader loop's flush publishes it.
    @Test
    void fullWindowFlushesFromTheReaderLoop() {
        final AtomicInteger resolved = new AtomicInteger();
        final TransactionAssembler assembler = assembler(4, swallowingReceiver(), resolved::incrementAndGet, null);

        commitOne(assembler, 0);
        commitOne(assembler, 1);
        commitOne(assembler, 2);
        assertEquals(0, resolved.get());
        assertFalse(assembler.deliveryBarrierFull(), "a partially filled window must not report full");
        commitOne(assembler, 3);
        /* The staging no longer flushes inside the fragment callback: the
         * window reports full, the reader breaks the poll, and the flush
         * below is exactly what the reader loop performs. */
        assertTrue(assembler.deliveryBarrierFull(), "the fourth commit fills the window");
        assembler.flushDeliveries();
        assertEquals(3, assembler.lastResolvedSequence(), "a full window flush publishes the durable tail");
        assertEquals(1, resolved.get(), "a full window publishes one durable tail");
        assertEquals(0, assembler.unflushedDeliveryCount());
    }

    /// A latched failure makes the flush a no-op and the barrier stays unflushed.
    @Test
    void latchedFailureSkipsFlush() {
        final AtomicInteger resolved = new AtomicInteger();
        final TransactionAssembler assembler = assembler(4, swallowingReceiver(), resolved::incrementAndGet, null);

        commitOne(assembler, 0);
        assembler.failure(new IllegalStateException("driver stopped"));
        assembler.flushDeliveries();
        assertEquals(-1, assembler.lastResolvedSequence(),
                "a failed assembler never advances the resolved boundary");
        assertEquals(1, assembler.unflushedDeliveryCount(), "staged entries stay until disposal");
        assertEquals(0, resolved.get());

        assembler.dispose();
        assertEquals(0, assembler.unflushedDeliveryCount(), "dispose clears the staged barrier");
    }

    /// Status remains at the durable boundary while cursor persistence is blocked or fails.
    @Test
    void resolvedBoundaryPublishesOnlyAfterDurabilityCallback() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final IllegalStateException failure = new IllegalStateException("cursor force failed");
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024)
                .readerBarrierMaxTransactions(4).build();
        final TransactionAssembler assembler = new TransactionAssembler(
                configuration, CLUSTER, EPOCH, -1L, -1L, swallowingReceiver(), ignored -> {
                    entered.countDown();
                    try {
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                    throw failure;
                }, null, AeronReplicationEnvelope.defaultWireNonce(CLUSTER));
        commitOne(assembler, 0L);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final var flush = executor.submit(assembler::flushDeliveries);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertEquals(-1L, assembler.lastResolvedSequence());
            assertEquals(new CursorSnapshot(-1L, -1L), assembler.cursorSnapshot());
            release.countDown();
            final var thrown = assertThrows(java.util.concurrent.ExecutionException.class, flush::get);
            assertEquals(failure, thrown.getCause());
            assertEquals(-1L, assembler.lastResolvedSequence());
        } finally {
            release.countDown();
            assembler.dispose();
        }
    }
}
