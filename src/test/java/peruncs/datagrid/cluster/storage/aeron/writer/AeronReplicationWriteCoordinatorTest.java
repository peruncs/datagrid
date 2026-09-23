package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksBuffer;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.util.BufferSizeProviderIncremental;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.WriterFencedException;
import peruncs.datagrid.cluster.storage.ReplicationDurabilityMode;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.binary.ReplicationPublisher;

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
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies checkpoint transitions and fail-closed writer coordination.
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
            assertThrows(IllegalStateException.class,
                    () -> coordinator.executeWriteAtomically(() -> null));
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
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> {
        },
                bytes -> false);
        assertThrows(IllegalStateException.class,
                () -> coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        coordinator.dispose();
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
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> {
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

        /// Verifies reporting of prepare commit and abort checkpoint states.
    @Test
    void reportsPrepareCommitAndAbortCheckpointStates() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final List<Long> sequences = new ArrayList<>();
        final List<Integer> crcs = new ArrayList<>();
        final UUID cluster = UUID.randomUUID();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, 1024,
                AeronReplicationConfiguration.builder().chunkSize(256).maxTransactionBytes(512).build(), cluster, 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) ->
        {
            states.add(state);
            sequences.add(sequence);
            crcs.add(crc);
        });
        final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        coordinator.commitOrMarkUncertain(prepared);
        final var second = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})));
        coordinator.abort(second);
        assertEquals(List.of(
                AeronReplicationCheckpoint.State.PREPARING,
                AeronReplicationCheckpoint.State.COMMITTED,
                AeronReplicationCheckpoint.State.PREPARING,
                AeronReplicationCheckpoint.State.REJECTED), states);
        assertEquals(List.of(0L, 0L, 1L, 1L), sequences);
        assertEquals(AeronReplicationEnvelope.crc32c(new byte[]{1}), crcs.get(0));
        assertEquals(crcs.get(0), crcs.get(1));
        assertEquals(AeronReplicationEnvelope.crc32c(new byte[]{2}), crcs.get(2));
        assertEquals(crcs.get(2), crcs.get(3));
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
                .durabilityMode(ReplicationDurabilityMode.ARCHIVE_FIRST)
                .build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) ->
                {
                    events.add("archive");
                    return length;
                }, configuration.maxMessageLength(), configuration, cluster, 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) ->
        {
            events.add(state.name());
            sequences.add(sequence);
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
        assertEquals(List.of("PREPARING", "archive", "local", "archive", "COMMITTED"), events);
        assertEquals(List.of(0L, 0L), sequences);
        coordinator.dispose();
    }

        /// Verifies local rejection emits abort and never commits.
    @Test
    void localRejectionEmitsAbortAndNeverCommits() {
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
                publisher, (state, sequence, length, chunks, crc, position) -> events.add(state.name()));
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
        assertEquals(List.of("PREPARING", "archive", "archive", "REJECTED"), events);
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
        target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})));
        assertEquals(2, kinds.stream().filter(kind -> kind == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY).count());
        coordinator.dispose();
    }

        /// Verifies a local rejection records a durable Archive rejection.
    @Test
    void enqueueLocalRejectionDoesNotCreateSyntheticTerminalSequence() {
        final List<String> events = new ArrayList<>();
        final AtomicBoolean fenceCleared = new AtomicBoolean();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ARCHIVE_FIRST)
                .build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), new AeronArchiveReplicationPublisher.CheckpointWriter() {
            @Override
            public void onState(final AeronReplicationCheckpoint.State state, final long sequence,
                                final int length, final int chunks, final int crc, final long position) {
                events.add("%s:%s".formatted(state, sequence));
            }

            @Override
            public void clearEnqueueFence() {
                fenceCleared.set(true);
            }
        });
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
        assertEquals(List.of("PREPARING:0", "REJECTED:0"), events);
        assertFalse(fenceCleared.get());
        assertEquals(1L, coordinator.nextSequence());
        coordinator.dispose();
    }

        /// A post-acceptance archive-first failure retains uncertainty and emits no contradictory abort.
    @Test
    void archivePostAcceptanceFailureLeavesUncertainWithoutAbort() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) ->
                {
                    kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
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
            CrashHook.runWithHook((name, ignored) ->
            {
                if ("AFTER_LOCAL_WRITE_BEFORE_COMMIT".equals(name)) throw failure;
            }, () -> assertThrows(IllegalStateException.class, () -> AeronStorageBinaryReplicationTarget.create(local, coordinator)
                    .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})))));
            assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
                    AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
            assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY), kinds,
                    "an accepted local write must not be followed by an archive ABORT");
        } finally {
            coordinator.dispose();
        }
    }

        /// A lease stolen before the marker offer must never reach Aeron.
    ///
    /// The gate pauses at the ownership boundary while a successor steals the
    /// lease; when released, ownership verification fails and the offer never
    /// runs. A true cross-process forked variant would add file-lock coverage;
    /// this test covers the gate logic deterministically in-JVM.
    @Test
    void stolenLeaseAtCommitGateNeverOffersMarker() throws Exception {
            final AtomicInteger offers = new AtomicInteger();
            final AtomicInteger commitMarkers = new AtomicInteger();
            final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                    .chunkSize(256).maxTransactionBytes(512).build();
            final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                    (buffer, offset, length) -> {
                        offers.incrementAndGet();
                        try {
                            if (AeronReplicationEnvelope.decode(buffer, offset, length).kind()
                                    == AeronReplicationEnvelope.Kind.COMMIT) {
                                commitMarkers.incrementAndGet();
                            }
                        } catch (final RuntimeException notEnvelope) {
                            /* Data-chunk frames share the offer path; only the
                             * terminal marker proves the commit escaped. */
                        }
                        return length;
                    }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final CountDownLatch atGate = new CountDownLatch(1);
        final CountDownLatch releaseGate = new CountDownLatch(1);
        final AtomicBoolean stolen = new AtomicBoolean();
        final AtomicInteger gatedOffers = new AtomicInteger();
        final WriterLeaseGate gate = new WriterLeaseGate() {
            @Override
            public boolean isValid() {
                return !stolen.get();
            }

            @Override
            public long offerUnderOwnership(final LongSupplier offer) {
                if (stolen.get()) throw new WriterFencedException("writer fencing lease lost before commit");
                return offer.getAsLong();
            }

            @Override
            public long offerUnderOwnership(final WriterLeaseGate.OwnedOffer offer) {
                if (gatedOffers.getAndIncrement() == 0) return offer.offer(() -> !stolen.get());
                atGate.countDown();
                try {
                    assertTrue(releaseGate.await(10, TimeUnit.SECONDS), "gate test timed out");
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                if (stolen.get()) {
                    throw new WriterFencedException("writer fencing lease lost before commit; this writer is fenced");
                }
                return offer.offer(() -> !stolen.get());
            }
        };
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> {
        },
                bytes -> true, gate);
        try {
            final var prepared = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
            final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
            final Thread committing = Thread.ofVirtual().start(() -> {
                try {
                    coordinator.commitOrMarkUncertain(prepared);
                } catch (final Throwable failure) {
                    commitFailure.set(failure);
                }
            });
            try {
                assertTrue(atGate.await(10, TimeUnit.SECONDS), "commit never reached the lease gate");
                stolen.set(true);
                releaseGate.countDown();
                committing.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse(committing.isAlive(), "commit did not finish after the steal");
                assertInstanceOf(WriterFencedException.class, commitFailure.get(),
                        "a stolen lease must fail the commit as a fencing loss");
                assertEquals(0, commitMarkers.get(), "a deposed writer must never offer its commit marker");
                assertTrue(publisher.isFailed(), "a fenced writer must fail its publisher closed");
            } finally {
                releaseGate.countDown();
                try {
                    committing.join(TimeUnit.SECONDS.toMillis(10));
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            coordinator.dispose();
        }
    }

    /// Verifies a lost lease prevents the abort marker from being offered, even during shutdown.
    @Test
    void lostLeasePreventsAbortMarkerEvenDuringShutdown() {
        final AtomicInteger abortMarkers = new AtomicInteger();
        final AtomicBoolean leaseValid = new AtomicBoolean(true);
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> {
                    if (AeronReplicationEnvelope.decode(buffer, offset, length).kind()
                            == AeronReplicationEnvelope.Kind.ABORT) abortMarkers.incrementAndGet();
                    return length;
                }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(),
                (state, sequence, length, chunks, crc, position) -> {}, bytes -> true, leaseValid::get);
        final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        leaseValid.set(false);
        assertThrows(IllegalStateException.class, () -> coordinator.abort(prepared));
        coordinator.dispose();
        assertEquals(0, abortMarkers.get());
    }

        /// Verifies a lost lease fails admission as fenced, never as capacity exhaustion.
    @Test
    void lostLeaseAdmissionFailsClosedWithLeaseError() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> {
        },
                bytes -> false, () -> false);
        try {
            final var failure = assertThrows(IllegalStateException.class,
                    () -> coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertTrue(failure.getMessage().contains("lease lost"),
                    "lost lease must not masquerade as capacity exhaustion, was: %s".formatted(failure.getMessage()));
            assertTrue(publisher.isFailed(), "a fenced writer must fail its publisher closed");
        } finally {
            coordinator.dispose();
        }
    }

        /// Verifies capacity exhaustion keeps its own message while the lease is valid.
    @Test
    void capacityExhaustionKeepsDistinctMessageWhileLeaseIsValid() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> {
        },
                bytes -> false, () -> true);
        try {
            final var failure = assertThrows(IllegalStateException.class,
                    () -> coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertTrue(failure.getMessage().contains("insufficient free capacity"),
                    "capacity exhaustion must keep its own message, was: %s".formatted(failure.getMessage()));
            assertFalse(publisher.isFailed(), "capacity exhaustion must not poison the publisher");
        } finally {
            coordinator.dispose();
        }
    }

        /// Verifies commit failure is marked uncertain and coordinator cannot pretend success.
    @Test
    void commitFailureIsMarkedUncertainAndCoordinatorCannotPretendSuccess() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
        final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING), states);
        assertThrows(IllegalStateException.class, () -> coordinator.commitOrMarkUncertain(prepared));
        assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states,
                "a failed commit must record its uncertainty without a second caller step");
        coordinator.dispose();
    }

        /// A back-pressure timeout during a commit keeps its own failure
    /// category: it must not be relabeled as fencing loss, and the transaction
    /// must still be recorded uncertain.
    @Test
    void commitTimeoutIsNotRelabeledAsFencingLoss() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).offerTimeoutNanos(1_000_000L).build();
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.BACK_PRESSURED,
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
        try {
            final var prepared = coordinator.prepare(
                    ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
            final RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> coordinator.commitOrMarkUncertain(prepared));
            assertFalse(failure instanceof WriterFencedException,
                    "a back-pressure timeout must not be reported as fencing loss");
            assertTrue(failure.getMessage().contains("timed out"), failure::getMessage);
            assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
                    AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
        } finally {
            coordinator.dispose();
        }
    }

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
        target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})));
        assertEquals(2, kinds.stream().filter(kind -> kind == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY).count());
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
        assertThrows(IllegalStateException.class, () -> AeronStorageBinaryReplicationTarget.create(
                local, coordinator, null, committed::set, () -> true).write(
                ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        assertEquals(-1, committed.get(), "uncertain commit must not advance the local index");
        coordinator.dispose();
    }

        /// A fatal JVM error never triggers a second checkpoint write from cleanup.
    @Test
    void fatalCommitErrorDoesNotWriteAnUncertaintyMarker() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
        final AssertionError fatal = new AssertionError("fatal commit failure");
        try {
            CrashHook.runWithHook((name, ignored) ->
            {
                if ("AFTER_COMMIT_OFFER".equals(name)) throw fatal;
            }, () -> {
                final AssertionError thrown = assertThrows(AssertionError.class,
                        () -> coordinator.distributeData(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
                assertSame(fatal, thrown);
            });
            assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING), states,
                    "fatal errors must leave the existing fence unresolved rather than writing a marker");
            assertTrue(publisher.isFailed());
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
                publisher, (state, sequence, length, chunks, crc, position) -> {
        });
        try {
            coordinator.distributeData(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
            final var admitted = new AtomicBoolean();
            final Thread writer = Thread.ofPlatform().start(() ->
                    coordinator.executeWriteAtomically(() -> {
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
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0, position ->
                {
                    if (awaits.incrementAndGet() == 1) {
                        commitWaitEntered.countDown();
                        try {
                            releaseCommit.await(30, TimeUnit.SECONDS);
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return position;
                    }
                    throw new IllegalStateException("injected archive acknowledgement failure");
                });
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
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
            assertEquals(List.of(
                    AeronReplicationCheckpoint.State.PREPARING,
                    AeronReplicationCheckpoint.State.COMMITTED,
                    AeronReplicationCheckpoint.State.PREPARING,
                    AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states,
                    "the injected acknowledgement failure must be recorded as uncertain");
            assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> coordinator.executeWriteAtomically(() -> null),
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
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0, position ->
                {
                    commitWaitEntered.countDown();
                    try {
                        releaseCommit.await(30, TimeUnit.SECONDS);
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
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
                    coordinator.executeWriteAtomically(() -> {
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

    /// Two Store writes keep their dictionary and terminal CRC even when the
    /// first is waiting for Archive acknowledgement as the second arrives.
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
            assertEquals(3, frames.size(), "the second writer must not publish during the first commit wait");
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
            assertThrows(IllegalStateException.class, () -> first.markCommittingUncertain(foreignToken));
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
        final AeronReplicationPublisher publisher = AeronReplicationPublisher.forTests(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0, position -> {
                    abortWaiting.countDown();
                    try {
                        assertTrue(releaseAbort.await(10, TimeUnit.SECONDS));
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
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
            assertThrows(IllegalStateException.class, () -> coordinator.markCommittingUncertain(prepared));
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
