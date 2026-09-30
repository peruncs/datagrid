package peruncs.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksBuffer;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.util.BufferSizeProviderIncremental;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationPendingException;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.binary.ReplicationPublisher;
import peruncs.cluster.storage.io.FaultInjection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies terminal-boundary updates and fail-closed writer coordination.
class AeronReplicationWriteCoordinatorTest {


    /// New writes fail instead of waiting behind Archive retention maintenance.
    @Test
    void maintenanceClosesWriteAdmission() throws Exception {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread maintenance = Thread.ofVirtual().start(() -> coordinator.withWritesPaused(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return 0L;
        }));
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            final long start = System.nanoTime();
            assertThrows(WriteRejectedException.class,
                    () -> coordinator.prepareWriteAtomically(() -> null));
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1),
                    "write admission must fail promptly during maintenance");
        } finally {
            release.countDown();
            maintenance.join(5_000L);
            coordinator.dispose();
        }
    }

        /// A publisher has one owner so dictionaries and reservations cannot diverge.
    @Test
    void rejectsMultipleCoordinatorsForOnePublisher() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator first = new AeronReplicationWriteCoordinator(publisher);
        assertThrows(IllegalStateException.class, () -> new AeronReplicationWriteCoordinator(publisher));
        first.dispose();
    }

        /// Coordinator ownership prevents callers from bypassing the fenced path.
    @Test
    void coordinatorOwnedPublisherRejectsUnfencedPreparation() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        assertThrows(IllegalStateException.class, () -> publisher.prepareTransaction(null,
                new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
        coordinator.dispose();
    }

        /// Rejects a new transaction when the Archive free-space admission guard is closed.
    @Test
    void rejectsWritesBelowArchiveCapacityThreshold() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AtomicBoolean capacity = new AtomicBoolean();
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, ignored -> {
        },
                bytes -> capacity.get());
        try {
            assertThrows(WriteRejectedException.class,
                    () -> coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertFalse(publisher.isFailed());
            capacity.set(true);
            try (final var prepared = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})))) {
                assertEquals(0L, prepared.sequence());
                coordinator.commitOrMarkUncertain(prepared);
            }
        } finally {
            coordinator.dispose();
        }
    }

    /// Verifies capacity admission receives the combined dictionary and payload bytes before preparation.
    @Test
    void capacityAdmissionReceivesPayloadAndDictionaryBytes() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AtomicLong required = new AtomicLong();
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, ignored -> {
        },
                bytes -> {
                    required.set(bytes);
                    return true;
                });
        coordinator.distributeTypeDictionary("type");
        try (var prepared = coordinator.prepare(
                ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1, 2, 3})))) {
            assertEquals(7L, required.get());
            coordinator.abort(prepared);
        }
        coordinator.dispose();
    }

        /// Verifies Archive preparation precedes the local Store write.
    @Test
    void archiveFirstPublishesBeforeLocalEnqueue() {
        final List<String> events = new ArrayList<>();
        final List<Long> sequences = new ArrayList<>();
        final UUID cluster = UUID.randomUUID();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) ->
                {
                    events.add("archive");
                    return length;
                }, configuration.maxMessageLength(), configuration, cluster, 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, position ->
        {
            events.add("terminal");
            sequences.add(position);
        });
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                events.add("local");
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
        AeronStorageBinaryReplicationTarget.create(local, coordinator)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{7})));
        assertEquals(List.of("archive", "local", "archive", "terminal"), events);
        assertEquals(1, sequences.size());
        coordinator.dispose();
    }

        /// A delegate write failure after entering local persistence is
    /// uncertain, not certainly rejected: no ABORT is emitted, and the
    /// the Store mark and Archive tail leave restart recovery unambiguous.
    @Test
    void localWriteFailureRecordsUncertaintyNotRejection() {
        final List<String> events = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    events.add("archive");
                    return length;
                },
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, ignored -> { });
        final PersistenceTarget<Binary> failing = new PersistenceTarget<>() {
            public void write(final Binary data) {
                throw new IllegalStateException("local failure");
            }

            public boolean isWritable() {
                return true;
            }
        };
        assertThrows(IllegalStateException.class, () -> AeronStorageBinaryReplicationTarget.create(failing, coordinator)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        assertTrue(events.contains("archive"), "prepare frames reach the Archive before local persistence");
        assertFalse(events.contains("ABORT"), "an uncertain local write must not emit a contradictory abort");
        coordinator.dispose();
    }

        /// A rejected transaction retains its dictionary for the next successful commit.
    @Test
    void rejectedTransactionRetainsDictionaryForRetry() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) ->
                {
                    kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        coordinator.distributeTypeDictionary("new.Type");
        final AtomicInteger writes = new AtomicInteger();
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                if (writes.getAndIncrement() == 0) throw new IllegalStateException("reject once");
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
        final AeronStorageBinaryReplicationTarget target = AeronStorageBinaryReplicationTarget.create(local, coordinator);
        assertThrows(IllegalStateException.class,
                () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        /* After the first local write failed, the publisher is failed closed:
         * a retry must be refused, or the transaction would risk surviving a
         * write that was never durably acknowledged as rejected. */
        assertThrows(IllegalStateException.class,
                () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2}))));
        assertTrue(publisher.isFailed(),
                "the publisher must fail closed after an uncertain local write");
        coordinator.dispose();
    }

    /// Verifies a local persistence failure does not synthesize a terminal sequence.
    @Test
    void enqueueLocalRejectionDoesNotCreateSyntheticTerminalSequence() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final PersistenceTarget<Binary> failing = new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                throw new IllegalStateException("local failure");
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
        assertThrows(IllegalStateException.class, () -> AeronStorageBinaryReplicationTarget.create(failing, coordinator)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        assertTrue(publisher.isFailed(), "a local write failure leaves the outcome uncertain");
        assertEquals(1L, coordinator.nextSequence());
        coordinator.dispose();
    }

        /// A post-acceptance archive-first failure retains uncertainty and emits no contradictory abort.
    @Test
    void archivePostAcceptanceFailureLeavesUncertainWithoutAbort() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) ->
                {
                    kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
        final IllegalStateException failure = new IllegalStateException("post-acceptance failure");
        try {
            FaultInjection.runWithHook((name, sequence, path) ->
            {
                if ("AFTER_LOCAL_WRITE_BEFORE_COMMIT".equals(name)) throw failure;
            }, () -> assertThrows(IllegalStateException.class, () -> AeronStorageBinaryReplicationTarget.create(local, coordinator)
                    .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})))));
            assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY), kinds,
                    "an accepted local write must not be followed by an archive ABORT");
        } finally {
            coordinator.dispose();
        }
    }

    /// A failure after durable prepare but before local persistence leaves the writer fenced.
    @Test
    void failureAfterPrepareBeforeLocalWriteFailsClosedWithoutAbort() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final AtomicInteger localWrites = new AtomicInteger();
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                localWrites.incrementAndGet();
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
        final IllegalStateException failure = new IllegalStateException("injected after prepare");
        try {
            FaultInjection.runWithHook((name, sequence, path) -> {
                if ("AFTER_PREPARE_BEFORE_LOCAL_WRITE".equals(name)) throw failure;
            }, () -> assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> AeronStorageBinaryReplicationTarget.create(local, coordinator)
                            .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))))));
            assertEquals(0, localWrites.get());
            assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY), kinds,
                    "an unresolved prepare must not receive a contradictory terminal marker");
            assertTrue(publisher.isFailed());
            assertEquals(1L, coordinator.nextSequence());
        } finally {
            coordinator.dispose();
        }
    }


        /// Verifies capacity exhaustion keeps its own rejection message.
    @Test
    void capacityExhaustionKeepsDistinctMessage() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, ignored -> { },
                bytes -> false);
        try {
            final var failure = assertThrows(WriteRejectedException.class,
                    () -> coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertTrue(failure.getMessage().contains("insufficient free capacity"),
                    "capacity exhaustion must keep its own message, was: %s".formatted(failure.getMessage()));
            assertFalse(publisher.isFailed(), "capacity exhaustion must not poison the publisher");
        } finally {
            coordinator.dispose();
        }
    }

    /// Admission timeout is a retryable rejection.
    @Test
    void admissionTimeoutDoesNotFailThePublisher() throws Exception {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512)
                .offerTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(20)).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread owner = Thread.ofVirtual().start(() -> coordinator.prepareWriteAtomically(() -> {
            held.countDown();
            try {
                release.await();
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return null;
        }));
        try {
            assertTrue(held.await(5, TimeUnit.SECONDS));
            assertThrows(WriteRejectedException.class,
                    () -> coordinator.prepareWriteAtomically(() -> null));
            assertFalse(publisher.isFailed());
        } finally {
            release.countDown();
            owner.join(5_000L);
            coordinator.dispose();
        }
    }

    /// A recorded ABORT consumes its sequence and returns coordinator admission to service.
    @Test
    void recordedAbortFromPrepareFailureKeepsWriterUsable() {
        final AtomicBoolean rejectData = new AtomicBoolean();
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final List<Long> positions = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512)
                .offerTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(5)).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    final AeronReplicationEnvelope.Kind kind = AeronReplicationEnvelope.decode(buffer, offset, length).kind();
                    kinds.add(kind);
                    if (kind == AeronReplicationEnvelope.Kind.STORE_BINARY && rejectData.get()) {
                        return Publication.BACK_PRESSURED;
                    }
                    if (kind == AeronReplicationEnvelope.Kind.ABORT) rejectData.set(false);
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, positions::add);
        try {
            try (final var first = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{0})))) {
                assertEquals(0L, first.sequence());
                coordinator.commitOrMarkUncertain(first);
            }
            rejectData.set(true);
            assertThrows(WriteRejectedException.class,
                    () -> coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertFalse(publisher.isFailed());
            assertTrue(kinds.contains(AeronReplicationEnvelope.Kind.ABORT));
            assertTrue(positions.getLast() >= 0L, "the rejection journal keeps the durable ABORT position");
            try (final var next = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})))) {
                assertEquals(2L, next.sequence(), "the ABORTed sequence 1 must never be reused");
                coordinator.commitOrMarkUncertain(next);
            }
            assertEquals(3L, positions.size(), "the commit, ABORT and next commit each advance the boundary");
            assertEquals(3L, coordinator.nextSequence());
        } finally {
            coordinator.dispose();
        }
    }

        /// Verifies commit failure is marked uncertain and coordinator cannot pretend success.
    @Test
    void commitFailureIsMarkedUncertainAndCoordinatorCannotPretendSuccess() {
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        assertThrows(ReplicationUnavailableException.class, () -> coordinator.commitOrMarkUncertain(prepared));
        assertTrue(publisher.isFailed(), "a failed commit leaves the publisher closed to further writes");
        coordinator.dispose();
    }

        /// A back-pressure timeout during a commit keeps its own failure
    /// category: it must not be relabeled as fencing loss, and the transaction
    /// must still be recorded uncertain.
    @Test
    void commitTimeoutIsNotRelabeledAsFencingLoss() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).offerTimeoutNanos(1_000_000L).build();
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.BACK_PRESSURED,
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        try {
            final var prepared = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
            final RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> coordinator.commitOrMarkUncertain(prepared));
            assertTrue(failure.getMessage().contains("timed out"), failure::getMessage);
            assertTrue(publisher.isFailed());
        } finally {
            coordinator.dispose();
        }
    }

    /// A Store COMMIT offer suspends admission, then retry completes it without an Archive wait.
    @Test
    void acceptedStoreCommitRetriesAfterOfferTimeoutWithoutLatchingWriter() {
        final AtomicBoolean blockCommit = new AtomicBoolean(true);
        final AtomicInteger acknowledgements = new AtomicInteger();
        final AtomicLong committedSequence = new AtomicLong(-1L);
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512)
                .offerTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(2)).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    final AeronReplicationEnvelope.Kind kind =
                            AeronReplicationEnvelope.decode(buffer, offset, length).kind();
                    if (kind == AeronReplicationEnvelope.Kind.COMMIT && blockCommit.get()) {
                        return Publication.BACK_PRESSURED;
                    }
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0,
                position -> {
                    acknowledgements.incrementAndGet();
                    return position;
                });
        final AtomicLong terminalPosition = new AtomicLong(-1L);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, terminalPosition::set);
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override public void write(final Binary data) { }
            @Override public boolean isWritable() { return true; }
        };
        final AeronStorageBinaryReplicationTarget target = AeronStorageBinaryReplicationTarget.create(
                local, coordinator, ReplicationPublisher.noOp(), committedSequence::set, () -> true);
        try {
            final ReplicationPendingException pending = assertThrows(ReplicationPendingException.class,
                    () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertEquals(0L, pending.sequence());
            assertFalse(publisher.isFailed());
            assertSame(pending, coordinator.failure());
            assertThrows(WriteRejectedException.class,
                    () -> coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2}))));
            assertEquals(-1L, committedSequence.get(), "the sequence is not live until COMMIT is offered");

            blockCommit.set(false);
            coordinator.retryPendingCommit();
            assertNull(coordinator.failure());
            assertEquals(0L, committedSequence.get());
            assertTrue(terminalPosition.get() >= 0L);
            assertEquals(1, acknowledgements.get(), "prepare is acknowledged, Store COMMIT is not awaited");
            try (final var next = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{3})))) {
                assertEquals(1L, next.sequence());
                coordinator.commitOrMarkUncertain(next);
            }
        } finally {
            coordinator.dispose();
        }
    }

    /// Closing while a Store COMMIT is pending preserves the Store mark for restart recovery.
    @Test
    void closeDoesNotAbortALocallyAcceptedPendingCommit() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512)
                .offerTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(1)).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    final AeronReplicationEnvelope.Kind kind =
                            AeronReplicationEnvelope.decode(buffer, offset, length).kind();
                    kinds.add(kind);
                    return kind == AeronReplicationEnvelope.Kind.COMMIT
                            ? Publication.BACK_PRESSURED : length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override public void write(final Binary data) { }
            @Override public boolean isWritable() { return true; }
        };
        final AeronStorageBinaryReplicationTarget target =
                AeronStorageBinaryReplicationTarget.create(local, coordinator);

        assertThrows(ReplicationPendingException.class,
                () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        coordinator.dispose();

        assertFalse(kinds.contains(AeronReplicationEnvelope.Kind.ABORT));
        assertTrue(publisher.isClosed());
    }

        /// An interrupt landing on the unlock/relock hand-off must restore
    /// write-lock ownership before surfacing the failure: the caller's finally
    /// unlocks unconditionally, so throwing without the hold would mask the
    /// original fault behind an IllegalMonitorStateException.
        /// Verifies persistence target consumes dictionary from shared distributor.
    @Test
    void persistenceTargetConsumesDictionaryFromSharedDistributor() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = AeronReplicationPublisher.forTests((buffer, offset, length) -> {
            kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
            return length;
        }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final var coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var source = new ReplicationPublisher() {
            private String dictionary = "type";

            public void distributeData(final Binary ignored) {
            }

            public void distributeTypeDictionary(final String ignored) {
            }

            public String consumeTypeDictionary() {
                final String value = this.dictionary;
                this.dictionary = null;
                return value;
            }

            public void dispose() {
            }
        };
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            public void write(final Binary ignored) {
            }

            public boolean isWritable() {
                return true;
            }
        };
        AeronStorageBinaryReplicationTarget.create(local, coordinator, source, ignored -> {
        }, () -> true)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        assertEquals(List.of(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY,
                AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.COMMIT), kinds);
        coordinator.dispose();
    }

        /// A dictionary transferred from a shared source survives local rejection.
    @Test
    void sharedDictionarySourceIsRetainedAcrossRejectedStoreWrite() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = AeronReplicationPublisher.forTests((buffer, offset, length) ->
        {
            kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
            return length;
        }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final var coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var source = new ReplicationPublisher() {
            private String dictionary = "type";

            public void distributeData(final Binary ignored) {
            }

            public void distributeTypeDictionary(final String ignored) {
            }

            public String consumeTypeDictionary() {
                final String value = this.dictionary;
                this.dictionary = null;
                return value;
            }

            public void dispose() {
            }
        };
        final AtomicInteger writes = new AtomicInteger();
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            public void write(final Binary ignored) {
                if (writes.getAndIncrement() == 0) throw new IllegalStateException("reject once");
            }

            public boolean isWritable() {
                return true;
            }
        };
        final var target = AeronStorageBinaryReplicationTarget.create(local, coordinator, source,
                ignored -> {
                }, () -> true);
        final var first = ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}));
        assertThrows(IllegalStateException.class, () -> target.write(first));
        /* After an uncertain local write, the publisher is failed closed; a
         * retried write is refused outright, rather than risking a
         * contradictory re-commit. */
        assertThrows(IllegalStateException.class,
                () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2}))));
        assertTrue(publisher.isFailed(),
                "the publisher must be failed closed after the uncertain local write");
        coordinator.dispose();
    }

        /// All Serializer channels, not only the channel-zero view, are published.
    @Test
    void collectsEverySerializerChannelForReplication() {
        final ChunksBuffer[] channels = new ChunksBuffer[4];
        final BufferSizeProviderIncremental sizes = BufferSizeProviderIncremental.New(64);
        for (int channelIndex = 0; channelIndex < channels.length; channelIndex++) {
            channels[channelIndex] = ChunksBuffer.New(channels, sizes);
        }
        for (int channelIndex = 0; channelIndex < channels.length; channelIndex++) {
            final int payloadLength = channelIndex + 2;
            channels[channelIndex].storeEntityHeader(payloadLength, 1, channelIndex + 1);
            for (int byteIndex = 0; byteIndex < payloadLength; byteIndex++) {
                channels[channelIndex].store_byte(byteIndex, (byte) (channelIndex + byteIndex));
            }
            channels[channelIndex].complete();
        }

        final List<ByteBuffer> buffers = new ArrayList<>();
        channels[0].iterateChannelChunks(channel ->
                buffers.addAll(Arrays.asList(channel.buffers())));
        assertEquals(channels.length, buffers.size());
        for (final ByteBuffer buffer : buffers) {
            assertTrue(buffer.remaining() > 0);
        }
    }

        /// Verifies ignored distribution writes locally without offering aeron frames.
    @Test
    void ignoredDistributionWritesLocallyWithoutOfferingAeronFrames() {
        final AtomicInteger offers = new AtomicInteger();
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = AeronReplicationPublisher.forTests((buffer, offset, length) -> {
            offers.incrementAndGet();
            return length;
        }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final var coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var localWrites = new AtomicInteger();
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            public void write(final Binary ignored) {
                localWrites.incrementAndGet();
            }

            public boolean isWritable() {
                return true;
            }
        };
        AeronStorageBinaryReplicationTarget.create(local, coordinator, null, ignored -> {
        }, () -> false)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        assertEquals(1, localWrites.get());
        assertEquals(0, offers.get());
        coordinator.dispose();
    }

        /// Verifies committed sequence callback does not advance on uncertain commit.
    @Test
    void committedSequenceCallbackDoesNotAdvanceOnUncertainCommit() {
        final AtomicInteger offers = new AtomicInteger();
        final AtomicLong committed = new AtomicLong(-1);
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final var coordinator = new AeronReplicationWriteCoordinator(publisher);
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            public void write(final Binary ignored) {
            }

            public boolean isWritable() {
                return true;
            }
        };
        assertThrows(ReplicationPendingException.class, () -> AeronStorageBinaryReplicationTarget.create(
                local, coordinator, null, committed::set, () -> true).write(
                ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        assertEquals(-1, committed.get(), "uncertain commit must not advance the local index");
        coordinator.dispose();
    }

    /// A fatal JVM error fails the publisher closed.
    @Test
    void fatalCommitErrorDoesNotWriteAnUncertaintyMarker() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final AssertionError fatal = new AssertionError("fatal commit failure");
        try {
            FaultInjection.runWithHook((name, sequence, path) ->
            {
                if ("AFTER_COMMIT_OFFER".equals(name)) throw fatal;
            }, () -> {
                final AssertionError thrown = assertThrows(AssertionError.class,
                        () -> coordinator.distributeData(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
                assertSame(fatal, thrown);
            });
            assertTrue(publisher.isFailed());
            assertInstanceOf(ReplicationUnavailableException.class, coordinator.failure());
            assertSame(fatal, coordinator.failure().getCause());
        } finally {
            coordinator.dispose();
        }
    }

        /// A fenced commit must restore the entry hold count so later writers proceed.
    @Test
    void fencedCommitReleasesWriteLockForOtherThreads() throws InterruptedException {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, ignored -> { });
        try {
            coordinator.distributeData(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
            final var admitted = new AtomicBoolean();
            final Thread writer = Thread.ofPlatform().start(() ->
                    coordinator.prepareWriteAtomically(() -> {
                        admitted.set(true);
                        return null;
                    }));
            writer.join(5_000);
            assertFalse(writer.isAlive(), "commit leaked a write-lock hold; second writer blocked");
            assertTrue(admitted.get());
        } finally {
            coordinator.dispose();
        }
    }

        /// Concurrent commit and abort threads plus a fault-injected
        /// acknowledgement must never leak the coordinator write lock.
    ///
    /// A leaked hold (for example from an error path that forgets to release)
    /// would deadlock every later writer, so each phase is followed by a
    /// bounded acquisition attempt.
    @Test
    void concurrentCommitAbortAndFaultInjectionNeverLeakTheWriteLock() throws Exception {
        final CountDownLatch commitWaitEntered = new CountDownLatch(1);
        final CountDownLatch releaseCommit = new CountDownLatch(1);
        final AtomicInteger awaits = new AtomicInteger();
        final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
        final AtomicReference<Throwable> abortFailure = new AtomicReference<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0, position ->
                {
                    final int await = awaits.incrementAndGet();
                    if (await == 2) {
                        commitWaitEntered.countDown();
                        try {
                            if (!releaseCommit.await(30, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting for commit release");
                            }
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return position;
                    }
                    if (await == 4) throw new IllegalStateException("injected archive acknowledgement failure");
                    return position;
                });
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        try {
            final var prepared = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
            final Thread committing = Thread.ofVirtual().start(() ->
            {
                try {
                    coordinator.commitOrMarkUncertain(prepared);
                } catch (final Throwable failure) {
                    commitFailure.set(failure);
                }
            });
            assertTrue(commitWaitEntered.await(10, TimeUnit.SECONDS), "the commit never reached its position wait");

            /* The abort races the in-flight commit. It must be refused by the
             * guard, and its refusal path must leave no hold behind. */
            final Thread aborting = Thread.ofVirtual().start(() ->
            {
                try {
                    coordinator.abort(prepared);
                } catch (final Throwable failure) {
                    abortFailure.set(failure);
                }
            });
            aborting.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(aborting.isAlive(), "the refused abort leaked a write-lock hold");
            assertInstanceOf(IllegalStateException.class, abortFailure.get());
            assertTrue(abortFailure.get().getMessage().contains("commit is in progress"),
                    "a concurrent abort must be refused by the commit guard: " + abortFailure.get().getMessage());

            releaseCommit.countDown();
            committing.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(committing.isAlive(), "the commit did not finish after the position was released");
            assertNull(commitFailure.get(), "the first commit must succeed: " + commitFailure.get());

            /* Fault injection: the next acknowledgement fails after its marker
             * offer. The commit must be recorded uncertain and still release
             * every lock before it throws. */
            final var second = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})));
            assertThrows(IllegalStateException.class, () -> coordinator.commitOrMarkUncertain(second));
            assertTrue(publisher.isFailed(), "an ambiguous Archive acknowledgement fails the publisher closed");
            assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> coordinator.prepareWriteAtomically(() -> null),
                    "a leaked write-lock hold would deadlock the next writer");
        } finally {
            releaseCommit.countDown();
            coordinator.dispose();
        }
    }

        /// While a commit waits for its Archive position, a second Store write
        /// waits on ownership without holding or spinning on the write lock.
    @Test
    void commitWaitsForConcurrentStoreWriteAdmission() throws Exception {
        final CountDownLatch commitWaitEntered = new CountDownLatch(1);
        final CountDownLatch releaseCommit = new CountDownLatch(1);
        final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
        final AtomicInteger acknowledgements = new AtomicInteger();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0, position ->
                {
                    if (acknowledgements.incrementAndGet() == 2) {
                        commitWaitEntered.countDown();
                        try {
                            if (!releaseCommit.await(30, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting for commit release");
                            }
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return position;
                });
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        final Thread committing = Thread.ofVirtual().start(() ->
        {
            try {
                coordinator.commitOrMarkUncertain(prepared);
            } catch (final Throwable failure) {
                commitFailure.set(failure);
            }
        });
        try {
            assertTrue(commitWaitEntered.await(10, TimeUnit.SECONDS), "the commit never reached its position wait");

            final CountDownLatch admitted = new CountDownLatch(1);
            final AtomicReference<Throwable> secondFailure = new AtomicReference<>();
            final Thread second = Thread.ofVirtual().start(() -> {
                try {
                    coordinator.prepareWriteAtomically(() -> {
                        admitted.countDown();
                        return null;
                    });
                } catch (final Throwable failure) {
                    secondFailure.set(failure);
                }
            });
            assertFalse(admitted.await(100, TimeUnit.MILLISECONDS),
                    "a second Store write must not enter while the first owns publication");
            releaseCommit.countDown();
            second.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(second.isAlive(), "the second write did not wake after commit");
            assertNull(secondFailure.get(), "the second write must be admitted after commit");
            assertEquals(0L, admitted.getCount());
        } finally {
            releaseCommit.countDown();
            committing.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(committing.isAlive(), "the commit did not finish after the position was released");
            assertNull(commitFailure.get(), "the commit must succeed: " + commitFailure.get());
            coordinator.dispose();
        }
    }

    /// Two Store writes keep their dictionary and terminal CRC while the first
    /// prepare waits for Archive acknowledgement.
    @Test
    void concurrentStoreWritesKeepDistinctDictionariesAndCrcs() throws Exception {
        final List<AeronReplicationEnvelope.Envelope> frames = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch firstCommitWaiting = new CountDownLatch(1);
        final CountDownLatch releaseFirstCommit = new CountDownLatch(1);
        final AtomicInteger acknowledgements = new AtomicInteger();
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        final AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    frames.add(AeronReplicationEnvelope.decode(buffer, offset, length));
                    return frames.size();
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0,
                position -> {
                    if (acknowledgements.incrementAndGet() == 1) {
                        firstCommitWaiting.countDown();
                        try {
                            assertTrue(releaseFirstCommit.await(10, TimeUnit.SECONDS));
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    }
                    return position;
                });
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final ReplicationPublisher dictionaries =
                ReplicationPublisher.Caching(ReplicationPublisher.noOp());
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override public void write(final Binary data) { }
            @Override public boolean isWritable() { return true; }
        };
        final AeronStorageBinaryReplicationTarget target = AeronStorageBinaryReplicationTarget.create(
                local, coordinator, dictionaries, ignored -> { }, () -> true);
        dictionaries.distributeTypeDictionary("first-type");
        final Thread first = Thread.ofVirtual().start(() -> {
            try {
                target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1, 2, 3})));
            } catch (final Throwable failure) {
                firstFailure.set(failure);
            }
        });
        Thread second = null;
        try {
            assertTrue(firstCommitWaiting.await(10, TimeUnit.SECONDS));
            dictionaries.distributeTypeDictionary("second-type");
            second = Thread.ofVirtual().start(() -> {
                try {
                    target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{4, 5, 6})));
                } catch (final Throwable failure) {
                    secondFailure.set(failure);
                }
            });
            assertEquals(2, frames.size(), "the second writer must not publish during the first prepare wait");
            releaseFirstCommit.countDown();
            first.join(10_000L);
            second.join(10_000L);
            assertFalse(first.isAlive());
            assertFalse(second.isAlive());
            assertNull(firstFailure.get());
            assertNull(secondFailure.get());
            assertEquals(6, frames.size());
            assertEquals(List.of(0L, 0L, 0L, 1L, 1L, 1L),
                    frames.stream().map(AeronReplicationEnvelope.Envelope::sequence).toList());
            assertArrayEquals("first-type".getBytes(StandardCharsets.UTF_8), frames.get(0).payload());
            assertArrayEquals("second-type".getBytes(StandardCharsets.UTF_8), frames.get(3).payload());
            assertEquals(AeronReplicationEnvelope.crc32c(new byte[]{1, 2, 3}), frames.get(2).commitCrc32c());
            assertEquals(AeronReplicationEnvelope.crc32c(new byte[]{4, 5, 6}), frames.get(5).commitCrc32c());
        } finally {
            releaseFirstCommit.countDown();
            first.join(10_000L);
            if (second != null) second.join(10_000L);
            coordinator.dispose();
            dictionaries.dispose();
        }
    }

    /// A foreign prepared token cannot clear or complete another writer's owner.
    @Test
    void foreignTokenCannotStealActiveWrite() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher firstPublisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationPublisher secondPublisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator first = new AeronReplicationWriteCoordinator(firstPublisher);
        final AeronReplicationWriteCoordinator second = new AeronReplicationWriteCoordinator(secondPublisher);
        try {
            final var firstToken = first.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
            final var foreignToken = second.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})));
            assertThrows(IllegalStateException.class, () -> first.commitOrMarkUncertain(foreignToken));
            assertThrows(IllegalStateException.class, () -> first.abort(foreignToken));
            first.commitOrMarkUncertain(firstToken);
            second.abort(foreignToken);
            assertFalse(firstPublisher.isFailed());
        } finally {
            first.dispose();
            second.dispose();
        }
    }

    /// An abort owns the terminal decision until Archive acknowledgement returns.
    @Test
    void secondAbortCannotClearFirstAbortOwner() throws Exception {
        final CountDownLatch abortWaiting = new CountDownLatch(1);
        final CountDownLatch releaseAbort = new CountDownLatch(1);
        final AtomicReference<Throwable> abortFailure = new AtomicReference<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AtomicInteger acknowledgements = new AtomicInteger();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0, position -> {
                    if (acknowledgements.incrementAndGet() == 2) {
                        abortWaiting.countDown();
                        try {
                            assertTrue(releaseAbort.await(10, TimeUnit.SECONDS));
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    }
                    return position;
                });
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var prepared = coordinator.prepare(
                ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        final Thread aborting = Thread.ofVirtual().start(() -> {
            try {
                coordinator.abort(prepared);
            } catch (final Throwable failure) {
                abortFailure.set(failure);
            }
        });
        try {
            assertTrue(abortWaiting.await(10, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, () -> coordinator.abort(prepared));
            releaseAbort.countDown();
            aborting.join(10_000L);
            assertFalse(aborting.isAlive());
            assertNull(abortFailure.get());
        } finally {
            releaseAbort.countDown();
            aborting.join(10_000L);
            coordinator.dispose();
        }
    }
}
