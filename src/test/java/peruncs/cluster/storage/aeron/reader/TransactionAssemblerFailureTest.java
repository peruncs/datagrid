package peruncs.cluster.storage.aeron.reader;

import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelopeTestSupport;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the failure path stays lock-free while a delivery is blocked in the
/// Store import, because Aeron invokes it from the client conductor thread.
///
/// A conductor that waits on the delivery monitor would stall past the driver
/// timeout and tear down the whole client. Failure must therefore only latch a
/// result; incomplete-transaction cleanup happens on the polling thread.
class TransactionAssemblerFailureTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final long EPOCH = 17;

    private static TransactionAssembler assembler(final StorageBinaryDataReceiver receiver) {
        return TransactionAssemblerTestSupport.New(
                AeronReplicationConfiguration.builder()/* direct-accept fixture: keep the barrier at one transaction */.readerBarrierMaxTransactions(1)                        .termLength(64 * 1024)
                        .chunkSize(256)
                        .maxTransactionBytes(1024)
                        .build(),
                CLUSTER,
                EPOCH,
                receiver);
    }

    private static void accept(final TransactionAssembler assembler, final byte[] bytes) {
        assembler.onFragment(new UnsafeBuffer(bytes), 0, bytes.length, null);
        if (assembler.deliveryBarrierFull()) {
            assembler.flushDeliveries();
        }
    }

    /// Proves `failure()` returns promptly while the delivery thread is parked in `awaitApplied()`.
    @Test
    void failureReturnsPromptlyWhileDeliveryIsInsideAwaitApplied() throws Exception {
        final CountDownLatch importEntered = new CountDownLatch(1);
        final CountDownLatch releaseImport = new CountDownLatch(1);
        final StorageBinaryDataReceiver receiver = new StorageBinaryDataReceiver() {
            @Override
            public void receiveData(final Binary value) {
                // The JSON import itself is not the blocking part under test.
            }

            @Override
            public void awaitApplied() {
                importEntered.countDown();
                try {
                    assertTrue(releaseImport.await(10, TimeUnit.SECONDS));
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }

            @Override
            public void receiveTypeDictionary(final String value) {
            }
        };
        final TransactionAssembler assembler = assembler(receiver);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final byte[] data = {1, 2, 3};
            final Future<?> delivery = executor.submit(() ->
            {
                accept(assembler, AeronReplicationEnvelopeTestSupport.encode(
                        CLUSTER, EPOCH, 1L, 0, AeronReplicationEnvelope.Kind.STORE_BINARY,
                        data.length, 0, 1, 0, 0, data));
                accept(assembler, AeronReplicationEnvelopeTestSupport.encode(
                        CLUSTER, EPOCH, 1L, 0, AeronReplicationEnvelope.Kind.COMMIT,
                        data.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(data), new byte[0]));
            });
            assertTrue(importEntered.await(5, TimeUnit.SECONDS), "delivery must reach awaitApplied");

            final IllegalStateException expected = new IllegalStateException("driver stopped");
            final Future<?> failed = executor.submit(() -> assembler.failure(expected));
            /* Without releasing the blocked import, a failure path that waited on the
             * delivery monitor could never complete. The bounded get is the proof. */
            failed.get(2, TimeUnit.SECONDS);
            assertSame(expected, assembler.failure());

            releaseImport.countDown();
            delivery.get(5, TimeUnit.SECONDS);
            /* The in-flight delivery was already past its final failure check, so it
             * completes; the latched failure still blocks every later frame. */
            assertSame(expected, assembler.failure());
        } finally {
            releaseImport.countDown();
            executor.shutdownNow();
            assembler.dispose();
        }
    }

    /// Proves disposal only needs the short assembler monitor, never the blocked delivery monitor.
    @Test
    void disposeReturnsWhileDeliveryIsInsideAwaitApplied() throws Exception {
        final CountDownLatch importEntered = new CountDownLatch(1);
        final CountDownLatch releaseImport = new CountDownLatch(1);
        final StorageBinaryDataReceiver receiver = new StorageBinaryDataReceiver() {
            @Override
            public void receiveData(final Binary value) {
            }

            @Override
            public void awaitApplied() {
                importEntered.countDown();
                try {
                    assertTrue(releaseImport.await(10, TimeUnit.SECONDS));
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }

            @Override
            public void receiveTypeDictionary(final String value) {
            }
        };
        final TransactionAssembler assembler = assembler(receiver);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final byte[] data = {4, 5};
            final Future<?> delivery = executor.submit(() ->
            {
                accept(assembler, AeronReplicationEnvelopeTestSupport.encode(
                        CLUSTER, EPOCH, 1L, 0, AeronReplicationEnvelope.Kind.STORE_BINARY,
                        data.length, 0, 1, 0, 0, data));
                accept(assembler, AeronReplicationEnvelopeTestSupport.encode(
                        CLUSTER, EPOCH, 1L, 0, AeronReplicationEnvelope.Kind.COMMIT,
                        data.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(data), new byte[0]));
            });
            assertTrue(importEntered.await(5, TimeUnit.SECONDS));
            final Future<?> disposed = executor.submit(assembler::dispose);
            disposed.get(2, TimeUnit.SECONDS);
            releaseImport.countDown();
            delivery.get(5, TimeUnit.SECONDS);
        } finally {
            releaseImport.countDown();
            executor.shutdownNow();
            assembler.dispose();
        }
    }

    /// Proves the polling thread releases an incomplete transaction after a concurrent failure.
    @Test
    void pollingThreadReleasesIncompleteTransactionAfterFailure() {
        final TransactionAssembler assembler = assembler(new StorageBinaryDataReceiver() {
            @Override
            public void receiveData(final Binary value) {
            }

            @Override
            public void receiveTypeDictionary(final String value) {
            }
        });
        try {
            accept(assembler, AeronReplicationEnvelopeTestSupport.encode(
                    CLUSTER, EPOCH, 1L, 0, AeronReplicationEnvelope.Kind.STORE_BINARY,
                    2, 0, 1, 0, 0, new byte[]{1, 2}));
            assertTrue(assembler.hasIncompleteTransaction());

            assembler.failure(new IllegalStateException("conductor stopped"));
            /* Failure is lock-free, so the next poll is where the incomplete
             * transaction is released. A later frame must be ignored entirely. */
            accept(assembler, AeronReplicationEnvelopeTestSupport.encode(
                    CLUSTER, EPOCH, 1L, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
                    1, 0, 1, 0, 0, new byte[]{3}));
            assertFalse(assembler.hasIncompleteTransaction());
            assertEquals(-1L, assembler.lastResolvedSequence());
        } finally {
            assembler.dispose();
        }
    }

    /// Proves a zero wire nonce is rejected by the canonical constructor.
    @Test
    void zeroWireNonceIsRejectedByTheCanonicalConstructor() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()/* direct-accept fixture: keep the barrier at one transaction */.readerBarrierMaxTransactions(1)                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        final StorageBinaryDataReceiver receiver = new StorageBinaryDataReceiver() {
            @Override
            public void receiveData(final Binary value) {
            }

            @Override
            public void receiveTypeDictionary(final String value) {
            }
        };
        assertThrows(IllegalArgumentException.class, () -> new TransactionAssembler(
                configuration, CLUSTER, EPOCH, -1, -1, receiver, ignored -> {
        }, null, 0L, TransactionAssembler.CommitDurabilityGate.ALWAYS));
    }
}
