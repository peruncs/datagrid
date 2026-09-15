package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies ordered publication, retry deadlines, and abandoned transactions.
class AeronReplicationPublisherTest {
    private static final UUID CLUSTER = UUID.randomUUID();

    private static byte[] preparedData(final byte[] message) {
        final AeronReplicationEnvelope.Envelope envelope = AeronReplicationEnvelope.decode(
                new org.agrona.concurrent.UnsafeBuffer(message), 0, message.length);
        return envelope.payload();
    }

    private static AeronReplicationConfiguration configuration(final long timeout) {
        return AeronReplicationConfiguration.builder()
                .termLength(64 * 1024)
                .chunkSize(256)
                .maxTransactionBytes(1024)
                .offerTimeoutNanos(timeout)
                .build();
    }

        /// Verifies retries back pressure and preserves source position.
    @Test
    void retriesBackPressureAndPreservesSourcePosition() {
        final AtomicInteger calls = new AtomicInteger();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> calls.getAndIncrement() < 2
                        ? Publication.BACK_PRESSURED
                        : calls.get(),
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0
        )) {
            final ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2, 3});
            final int position = source.position();
            publisher.publishTransaction(null, new ByteBuffer[]{source});
            assertTrue(calls.get() >= 4, "data and commit must both be offered after back pressure");
            assertEquals(source.position(), position, "publisher must not consume caller buffers");
        }
    }

        /// Verifies times out and fails closed after persistent back pressure.
    @Test
    void timesOutAndFailsClosedAfterPersistentBackPressure() {
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> Publication.BACK_PRESSURED,
                configuration(1_000_000L).maxMessageLength(), configuration(1_000_000L), CLUSTER, 1, 0
        )) {
            assertThrows(IllegalStateException.class,
                    () -> publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
            assertThrows(IllegalStateException.class,
                    () -> publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{2})}));
        }
    }

        /// Verifies timeout diagnostics identify the Aeron status and connectivity.
    @Test
    void timeoutDiagnosticsIdentifyNotConnectedPublication() {
        final AeronReplicationConfiguration configuration = configuration(1_000_000L);
        final AeronOfferRetryer.Offerer offerer = new AeronOfferRetryer.Offerer() {
            @Override
            public long offer(final org.agrona.DirectBuffer buffer, final int offset, final int length) {
                return Publication.NOT_CONNECTED;
            }

            @Override
            public boolean isConnected() {
                return false;
            }
        };
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new AeronOfferRetryer(offerer, configuration).offer(
                        new org.agrona.concurrent.UnsafeBuffer(new byte[]{1}), 1));
        assertTrue(failure.getMessage().contains("NOT_CONNECTED"));
        assertTrue(failure.getMessage().contains("connected=false"));
    }

        /// Verifies closed and max position statuses are fatal.
    @Test
    void closedAndMaxPositionStatusesAreFatal() {
        for (final long status : new long[]{Publication.CLOSED, Publication.MAX_POSITION_EXCEEDED}) {
            try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                    (buffer, offset, length) -> status,
                    configuration(50_000_000L).maxMessageLength(), configuration(50_000_000L), CLUSTER, 1, 0
            )) {
                assertThrows(IllegalStateException.class,
                        () -> publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
            }
        }
    }

        /// Verifies rejection of message limit larger than configuration.
    @Test
    void rejectsMessageLimitLargerThanConfiguration() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength() + 1,
                configuration, CLUSTER, 1, 0));
    }

        /// Verifies emits dictionary chunks before data and commit.
    @Test
    void emitsDictionaryChunksBeforeDataAndCommit() {
        final List<byte[]> messages = new ArrayList<>();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) ->
                {
                    final byte[] copy = new byte[length];
                    buffer.getBytes(offset, copy);
                    messages.add(copy);
                    return messages.size();
                }, configuration.maxMessageLength(), configuration, CLUSTER, 4, 10
        )) {
            final byte[] data = {1, 2, 3};
            final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                    new byte[]{9, 8}, new ByteBuffer[]{ByteBuffer.wrap(data)});
            assertEquals(10, prepared.sequence());
            assertEquals(3, prepared.dataLength());
            assertEquals(2, messages.size());
            assertEquals(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY,
                    AeronReplicationEnvelope.decode(new org.agrona.concurrent.UnsafeBuffer(messages.getFirst()), 0,
                            messages.getFirst().length).kind());
            assertEquals(AeronReplicationEnvelope.Kind.STORE_BINARY,
                    AeronReplicationEnvelope.decode(new org.agrona.concurrent.UnsafeBuffer(messages.get(1)), 0,
                            messages.get(1).length).kind());
            publisher.commit(prepared);
            assertEquals(3, messages.size());
            final AeronReplicationEnvelope.Envelope commit = AeronReplicationEnvelope.decode(
                    new org.agrona.concurrent.UnsafeBuffer(messages.get(2)), 0, messages.get(2).length);
            assertEquals(AeronReplicationEnvelope.Kind.COMMIT, commit.kind());
            assertEquals(AeronReplicationEnvelope.crc32c(data), commit.commitCrc32c());
            assertArrayEquals(data, preparedData(messages.get(1)));
        }
    }

        /// Verifies publishes across multiple source buffers without changing their positions.
    @Test
    void publishesAcrossMultipleSourceBuffersWithoutChangingTheirPositions() {
        final List<byte[]> messages = new ArrayList<>();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) ->
                {
                    final byte[] copy = new byte[length];
                    buffer.getBytes(offset, copy);
                    messages.add(copy);
                    return messages.size();
                }, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final ByteBuffer first = ByteBuffer.wrap(new byte[]{1, 2});
            final ByteBuffer second = ByteBuffer.wrap(new byte[]{3, 4});
            final int firstPosition = first.position();
            final int secondPosition = second.position();
            publisher.publishTransaction(null, new ByteBuffer[]{first, second});
            assertEquals(firstPosition, first.position());
            assertEquals(secondPosition, second.position());
            assertArrayEquals(new byte[]{1, 2, 3, 4}, preparedData(messages.getFirst()));
        }
    }

        /// Verifies closing an abandoned prepared transaction publishes an abort marker.
    @Test
    void closingAnAbandonedPreparedTransactionPublishesAnAbortMarker() {
        final List<byte[]> messages = new ArrayList<>();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) ->
                {
                    final byte[] copy = new byte[length];
                    buffer.getBytes(offset, copy);
                    messages.add(copy);
                    return messages.size();
                }, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            try (AeronReplicationPublisher.PreparedTransaction ignored = publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{7})})) {
                assertNotNull(ignored);
            }
            assertEquals(AeronReplicationEnvelope.Kind.ABORT,
                    AeronReplicationEnvelope.decode(new org.agrona.concurrent.UnsafeBuffer(messages.get(1)), 0,
                            messages.get(1).length).kind());
        }
    }

        /// Verifies publisher shutdown aborts an outstanding token and invokes its abort callback.
    @Test
    void publisherShutdownInvokesPendingAbortCallback() {
        final AtomicInteger abortCallbacks = new AtomicInteger();
        final java.util.concurrent.atomic.AtomicLong abortPosition = new java.util.concurrent.atomic.AtomicLong(-1);
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length,
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
        final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{6})});
        prepared.onAbort(position -> {
            abortPosition.set(position);
            abortCallbacks.incrementAndGet();
        });

        publisher.close();

        assertEquals(1, abortCallbacks.get());
        assertTrue(abortPosition.get() >= 0);
    }

        /// A transient abort offer failure keeps the token retryable and never emits two aborts.
    @Test
    void closeCanRetryPendingAbortAfterNotConnectedFailure() {
        final AtomicInteger offers = new AtomicInteger();
        final AtomicBoolean failAbort = new AtomicBoolean(true);
        final AeronReplicationConfiguration configuration = configuration(1_000_000L);
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> offers.getAndIncrement() == 0 || !failAbort.get()
                        ? length : Publication.NOT_CONNECTED,
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
        final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{6})});

        assertThrows(IllegalStateException.class, publisher::close);
        assertTrue(publisher.hasPendingTransaction(), "a marker that was never accepted must remain retryable");
        failAbort.set(false);
        assertDoesNotThrow(publisher::close);
        assertFalse(publisher.hasPendingTransaction());
        assertEquals(0L, prepared.sequence());
    }

        /// Verifies a direct publisher abort invokes the same callback as shutdown abort.
    @Test
    void directAbortInvokesPendingAbortCallback() {
        final AtomicInteger abortCallbacks = new AtomicInteger();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{6})});
            prepared.onAbort(position -> abortCallbacks.incrementAndGet());
            assertTrue(publisher.abort(prepared) >= 0);
            assertEquals(1, abortCallbacks.get());
        }
    }

        /// A publisher cannot overwrite an outstanding token with a second reservation.
    @Test
    void rejectsSecondPreparedTransactionUntilTheFirstIsTerminal() {
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final AeronReplicationPublisher.PreparedTransaction first = publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})});
            assertThrows(IllegalStateException.class, () -> publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{2})}));
            first.close();
            assertDoesNotThrow(() -> publisher.publishTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{2})}));
        }
    }

        /// Verifies a crash after token creation cannot publish a duplicate abort on shutdown.
    @Test
    void prepareCrashDoesNotPublishDuplicateAbortOnShutdown() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) ->
                {
                    kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
                    return length;
                }, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            CrashHook.runWithHook((name, ignored) ->
            {
                if ("AFTER_PREPARE".equals(name)) throw new IllegalStateException("after prepare");
            }, () -> assertThrows(IllegalStateException.class, () -> publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})})));
            publisher.close();
            assertEquals(List.of(
                    AeronReplicationEnvelope.Kind.STORE_BINARY,
                    AeronReplicationEnvelope.Kind.ABORT
            ), kinds);
        }
    }

        /// Verifies publishes an explicit empty store chunk.
    @Test
    void publishesAnExplicitEmptyStoreChunk() {
        final List<byte[]> messages = new ArrayList<>();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) ->
                {
                    final byte[] copy = new byte[length];
                    buffer.getBytes(offset, copy);
                    messages.add(copy);
                    return messages.size();
                }, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.allocate(0)});
            assertEquals(2, messages.size());
            final AeronReplicationEnvelope.Envelope data = AeronReplicationEnvelope.decode(
                    new org.agrona.concurrent.UnsafeBuffer(messages.getFirst()), 0, messages.getFirst().length);
            assertEquals(AeronReplicationEnvelope.Kind.STORE_BINARY, data.kind());
            assertEquals(0, data.payloadLength());
            assertEquals(0, data.payload().length);
        }
    }

        /// Verifies rejection of transaction larger than configured limit before offering.
    @Test
    void rejectsTransactionLargerThanConfiguredLimitBeforeOffering() {
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> {
                    offers.incrementAndGet();
                    return 1;
                },
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            assertThrows(IllegalArgumentException.class, () -> publisher.prepareTransaction(
                    new byte[300], new ByteBuffer[]{ByteBuffer.wrap(new byte[300])}));
            assertEquals(0, offers.get());
        }
    }

        /// Verifies failed commit leaves publisher failed closed.
    @Test
    void failedCommitLeavesPublisherFailedClosed() {
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? 1 : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})});
            assertThrows(IllegalStateException.class, () -> publisher.commit(prepared));
            assertThrows(IllegalStateException.class, () -> publisher.abort(prepared));
        }
    }

        /// Verifies partial prepare failure is terminal and cannot skip the reserved sequence.
    @Test
    void partialPrepareFailureIsTerminalAndCannotSkipTheReservedSequence() {
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            assertThrows(IllegalStateException.class, () -> publisher.prepareTransaction(
                    new byte[]{9}, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
            assertTrue(offers.get() >= 2, "the failure must occur after a partial publication");
            assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{2})}));
        }
    }

        /// Verifies abort failure fails closed after prepared chunks were published.
    @Test
    void abortFailureFailsClosedAfterPreparedChunksWerePublished() {
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})});
            assertThrows(IllegalStateException.class, () -> publisher.abort(prepared));
            assertThrows(IllegalStateException.class, () -> publisher.commit(prepared));
        }
    }

        /// Verifies close is idempotent and prevents further offers.
    @Test
    void closeIsIdempotentAndPreventsFurtherOffers() {
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            publisher.close();
            publisher.close();
            assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
        }
    }

        /// Verifies commit waits for and returns archive recorded position.
    @Test
    void commitWaitsForAndReturnsArchiveRecordedPosition() {
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        final AtomicInteger offers = new AtomicInteger();
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> offers.incrementAndGet(), configuration.maxMessageLength(), configuration,
                CLUSTER, 1, 0, offeredPosition -> offeredPosition + 100
        )) {
            assertEquals(102, publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
        }
    }

        /// Verifies sequence exhaustion is detected before the next reservation wraps.
    @Test
    void sequenceExhaustionIsDetectedBeforeWraparound() {
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                CLUSTER, 1, Long.MAX_VALUE - 1)) {
            assertDoesNotThrow(() -> publisher.publishTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
            assertThrows(IllegalStateException.class, () -> publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{2})}));
        }
    }

        /// A durable reservation cannot be bypassed by an independent publisher call.
    @Test
    void reservationMustBeConsumedOrReleasedBeforeAnotherPublication() {
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final long reserved = publisher.reserveSequence();
            assertEquals(0L, reserved);
            assertTrue(publisher.hasSequenceReservation());
            assertThrows(IllegalStateException.class,
                    () -> publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
            assertThrows(IllegalStateException.class,
                    () -> publisher.synchronizeNextSequence(2L));
            publisher.releaseReservedSequence(reserved);
            assertFalse(publisher.hasSequenceReservation());
            assertDoesNotThrow(() -> publisher.publishTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
        }
    }

        /// Explicit preparation consumes exactly the reservation returned by reserveSequence.
    @Test
    void explicitPreparationConsumesTheMatchingReservation() {
        final AeronReplicationConfiguration configuration = configuration(50_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final ByteBuffer[] buffers = {ByteBuffer.wrap(new byte[]{3, 4})};
            final AeronReplicationPublisher.TransactionMetadata metadata = publisher.transactionMetadata(buffers, 1);
            final long reserved = publisher.reserveSequence();
            final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                    null, buffers, reserved, metadata);
            assertFalse(publisher.hasSequenceReservation());
            assertEquals(reserved, prepared.sequence());
            publisher.commit(prepared);
        }
    }

        /// A commit marker that retries under back pressure must not hold the
        /// publisher state monitor: monitoring and close() must stay responsive
        /// while the offer is in flight.
    @Test
    void slowCommitOfferDoesNotBlockStateAccessors() throws Exception {
        final CountDownLatch commitOfferEntered = new CountDownLatch(1);
        final CountDownLatch releaseOffer = new CountDownLatch(1);
        final AtomicInteger offers = new AtomicInteger();
        final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
        final AeronReplicationConfiguration configuration = configuration(30_000_000_000L);
        try (final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) ->
                {
                    if (offers.incrementAndGet() == 1) {
                        return length;
                    }
                    commitOfferEntered.countDown();
                    try {
                        if (!releaseOffer.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                            throw new IllegalStateException("commit offer was never released");
                        }
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("commit offer wait was interrupted", interrupted);
                    }
                    return length;
                },
                configuration.maxMessageLength(), configuration, CLUSTER, 1, 0)) {
            final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
                    null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})});
            final Thread committing = Thread.ofVirtual().start(() ->
            {
                try {
                    publisher.commit(prepared);
                } catch (final Throwable failure) {
                    commitFailure.set(failure);
                }
            });

            assertTrue(commitOfferEntered.await(10, java.util.concurrent.TimeUnit.SECONDS),
                    "commit offer never started");
            final long start = System.nanoTime();
            assertFalse(publisher.isFailed(), "monitoring must observe state during the offer");
            publisher.nextSequence();
            publisher.isClosed();
            publisher.hasPendingTransaction();
            final long elapsedMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMillis < 5_000L,
                    "state accessors stalled behind the commit offer for %s ms".formatted(elapsedMillis));

            releaseOffer.countDown();
            committing.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
            assertFalse(committing.isAlive(), "commit did not finish after the offer was released");
            assertNull(commitFailure.get(), "commit must succeed after the offer completes");
            assertFalse(publisher.isFailed(), "a successful commit must not fail the publisher");
        }
    }
}
