package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksBuffer;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.util.BufferSizeProviderIncremental;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.aeron.writer.*;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies checkpoint transitions and fail-closed writer coordination. */
class AeronReplicationWriteCoordinatorTest {
    /** A publisher has one owner so dictionaries and reservations cannot diverge. */
    @Test
    void rejectsMultipleCoordinatorsForOnePublisher() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator first = new AeronReplicationWriteCoordinator(publisher);
        assertThrows(IllegalStateException.class, () -> new AeronReplicationWriteCoordinator(publisher));
        first.dispose();
    }

    /** Coordinator ownership prevents callers from bypassing the fenced path. */
    @Test
    void coordinatorOwnedPublisherRejectsUnfencedPreparation() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(publisher);
        assertThrows(IllegalStateException.class, () -> publisher.prepareTransaction(null,
                new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})}));
        coordinator.dispose();
    }

    /** Rejects a new transaction when the Archive free-space admission guard is closed. */
    @Test
    void rejectsWritesBelowArchiveCapacityThreshold() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
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

    @Test
    void capacityAdmissionReceivesPayloadAndDictionaryBytes() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
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

    /** Verifies reporting of prepare commit and abort checkpoint states. */
    @Test
    void reportsPrepareCommitAndAbortCheckpointStates() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final List<Long> sequences = new ArrayList<>();
        final List<Integer> crcs = new ArrayList<>();
        final UUID cluster = UUID.randomUUID();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
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
        coordinator.commit(prepared);
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

    /** Verifies enqueue then archive does not publish before local enqueue. */
    @Test
    void enqueueThenArchiveDoesNotPublishBeforeLocalEnqueue() {
        final List<String> events = new ArrayList<>();
        final List<Long> sequences = new ArrayList<>();
        final UUID cluster = UUID.randomUUID();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                .build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
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
        new AeronStorageBinaryTargetDistributing(local, coordinator)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{7})));
        assertEquals(List.of("ENQUEUED", "local", "archive", "archive", "COMMITTED"), events);
        assertEquals(List.of(0L, 0L), sequences);
        coordinator.dispose();
    }

    /** Verifies enqueue prepare failure records the failed transaction dimensions. */
    @Test
    void enqueuePrepareFailureRecordsTheFailedTransactionDimensions() {
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                .build();
        final AtomicInteger uncertainLength = new AtomicInteger(-1);
        final AtomicInteger uncertainChunks = new AtomicInteger(-1);
        final AtomicInteger uncertainCrc = new AtomicInteger(-1);
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> offers.getAndIncrement() == 0 ? length : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) ->
        {
            if (state == AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN) {
                uncertainLength.set(length);
                uncertainChunks.set(chunks);
                uncertainCrc.set(crc);
            }
        });
        final PersistenceTarget<Binary> local = new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
            }

            @Override
            public boolean isWritable() {
                return true;
            }
        };
        coordinator.distributeTypeDictionary("type");
        assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(local, coordinator)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1, 2, 3}))));
        assertEquals(3, uncertainLength.get());
        assertEquals(1, uncertainChunks.get());
        assertEquals(AeronReplicationEnvelope.crc32c(new byte[]{1, 2, 3}), uncertainCrc.get());
        coordinator.dispose();
    }

    /** Verifies local rejection emits abort and never commits. */
    @Test
    void localRejectionEmitsAbortAndNeverCommits() {
        final List<String> events = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
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
        assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(failing, coordinator)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        assertEquals(List.of("PREPARING", "archive", "archive", "REJECTED"), events);
        coordinator.dispose();
    }

    /** A rejected transaction retains its dictionary for the next successful commit. */
    @Test
    void rejectedTransactionRetainsDictionaryForRetry() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
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
        final AeronStorageBinaryTargetDistributing target = new AeronStorageBinaryTargetDistributing(local, coordinator);
        assertThrows(IllegalStateException.class,
                () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})));
        assertEquals(2, kinds.stream().filter(kind -> kind == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY).count());
        coordinator.dispose();
    }

    /** Verifies an ENQUEUE local rejection removes only the fence and never advances a sequence. */
    @Test
    void enqueueLocalRejectionDoesNotCreateSyntheticTerminalSequence() {
        final List<String> events = new ArrayList<>();
        final AtomicBoolean fenceCleared = new AtomicBoolean();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                .build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), new AeronArchiveReplicationPublisher.CheckpointWriter() {
            @Override
            public void onState(final AeronReplicationCheckpoint.State state, final long sequence,
                                final int length, final int chunks, final int crc, final long position) {
                events.add(state + ":" + sequence);
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
        assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(failing, coordinator)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        assertEquals(List.of("ENQUEUED:0"), events);
        assertTrue(fenceCleared.get());
        assertEquals(0L, coordinator.nextSequence());
        coordinator.dispose();
    }

    /** A post-acceptance failure retains the enqueue fence instead of fabricating an abort. */
    @Test
    void enqueuePostAcceptanceFailureLeavesUncertainFence() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                .build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> states.add(state));
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
            CrashHook.install((name, ignored) ->
            {
                if ("AFTER_ENQUEUE_BEFORE_PREPARE".equals(name)) throw failure;
            });
            assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(local, coordinator)
                    .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertEquals(List.of(AeronReplicationCheckpoint.State.ENQUEUED,
                    AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
            assertEquals(1L, coordinator.nextSequence(), "the accepted sequence must remain consumed");
            assertFalse(publisher.hasSequenceReservation(),
                    "an uncertainty fence must consume the reservation even when preparation never started");
        } finally {
            CrashHook.clear();
            coordinator.dispose();
        }
    }

    /** A post-acceptance archive-first failure retains uncertainty and emits no contradictory abort. */
    @Test
    void archivePostAcceptanceFailureLeavesUncertainWithoutAbort() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
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
            CrashHook.install((name, ignored) ->
            {
                if ("AFTER_LOCAL_WRITE_BEFORE_COMMIT".equals(name)) throw failure;
            });
            assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(local, coordinator)
                    .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2}))));
            assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
                    AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
            assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY), kinds,
                    "an accepted local write must not be followed by an archive ABORT");
        } finally {
            CrashHook.clear();
            coordinator.dispose();
        }
    }

    /** Verifies a failed fence cleanup still releases its reserved sequence. */
    @Test
    void enqueueFenceCleanupReleasesSequenceWhenCheckpointCleanupFails() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
                .durabilityMode(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
                .build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final IllegalStateException cleanupFailure = new IllegalStateException("checkpoint cleanup failed");
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, configuration.durabilityMode(), new AeronArchiveReplicationPublisher.CheckpointWriter() {
            @Override
            public void onState(final AeronReplicationCheckpoint.State state, final long sequence,
                                final int length, final int chunks, final int crc, final long position) {
            }

            @Override
            public void clearEnqueueFence() {
                throw cleanupFailure;
            }
        });
        try {
            final Binary data = ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}));
            coordinator.markLocalEnqueue(data);
            assertThrows(IllegalStateException.class, coordinator::clearLocalEnqueue);
            assertEquals(0L, coordinator.nextSequence());
        } finally {
            coordinator.dispose();
        }
    }

    /** Verifies commit failure is marked uncertain and coordinator cannot pretend success. */
    @Test
    void commitFailureIsMarkedUncertainAndCoordinatorCannotPretendSuccess() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AtomicInteger offers = new AtomicInteger();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
                configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
        final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING), states);
        assertThrows(IllegalStateException.class, () -> coordinator.commit(prepared));
        coordinator.markCommittingUncertain(prepared);
        assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
        coordinator.dispose();
    }

    /** Verifies persistence target consumes dictionary from shared distributor. */
    @Test
    void persistenceTargetConsumesDictionaryFromSharedDistributor() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = new AeronReplicationPublisher((buffer, offset, length) -> {
            kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
            return length;
        }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final var coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var source = new StorageBinaryDataDistributor() {
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
        new AeronStorageBinaryTargetDistributing(local, coordinator, source, ignored -> {
        }, () -> true)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        assertEquals(List.of(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY,
                AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.COMMIT), kinds);
        coordinator.dispose();
    }

    /** A dictionary transferred from a shared source survives local rejection. */
    @Test
    void sharedDictionarySourceIsRetainedAcrossRejectedStoreWrite() {
        final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = new AeronReplicationPublisher((buffer, offset, length) ->
        {
            kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
            return length;
        }, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
        final var coordinator = new AeronReplicationWriteCoordinator(publisher);
        final var source = new StorageBinaryDataDistributor() {
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
        final var target = new AeronStorageBinaryTargetDistributing(local, coordinator, source,
                ignored -> {
                }, () -> true);
        final var first = ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}));
        assertThrows(IllegalStateException.class, () -> target.write(first));
        target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{2})));
        assertEquals(2, kinds.stream().filter(kind -> kind == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY).count());
        coordinator.dispose();
    }

    /** All Serializer channels, not only the channel-zero view, are published. */
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

        final List<java.nio.ByteBuffer> buffers = new ArrayList<>();
        channels[0].iterateChannelChunks(channel ->
                buffers.addAll(Arrays.asList(channel.buffers())));
        assertEquals(channels.length, buffers.size());
        for (final java.nio.ByteBuffer buffer : buffers) {
            assertTrue(buffer.remaining() > 0);
        }
    }

    /** Verifies ignored distribution writes locally without offering aeron frames. */
    @Test
    void ignoredDistributionWritesLocallyWithoutOfferingAeronFrames() {
        final AtomicInteger offers = new AtomicInteger();
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = new AeronReplicationPublisher((buffer, offset, length) -> {
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
        new AeronStorageBinaryTargetDistributing(local, coordinator, null, ignored -> {
        }, () -> false)
                .write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1})));
        assertEquals(1, localWrites.get());
        assertEquals(0, offers.get());
        coordinator.dispose();
    }

    /** Verifies committed sequence callback does not advance on uncertain commit. */
    @Test
    void committedSequenceCallbackDoesNotAdvanceOnUncertainCommit() {
        final AtomicInteger offers = new AtomicInteger();
        final AtomicLong committed = new AtomicLong(-1);
        final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
                .chunkSize(256).maxTransactionBytes(1024).build();
        final var publisher = new AeronReplicationPublisher(
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
        assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(
                local, coordinator, null, committed::set, () -> true).write(
                ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
        assertEquals(-1, committed.get(), "uncertain commit must not advance the local index");
        coordinator.dispose();
    }

    /** A fatal JVM error never triggers a second checkpoint write from cleanup. */
    @Test
    void fatalCommitErrorDoesNotWriteAnUncertaintyMarker() {
        final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
        final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
                (buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
                UUID.randomUUID(), 1, 0);
        final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
        final AssertionError fatal = new AssertionError("fatal commit failure");
        try {
            CrashHook.install((name, ignored) ->
            {
                if ("AFTER_COMMIT_OFFER".equals(name)) throw fatal;
            });
            final AssertionError thrown = assertThrows(AssertionError.class,
                    () -> coordinator.distributeData(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1}))));
            assertSame(fatal, thrown);
            assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING), states,
                    "fatal errors must leave the existing fence unresolved rather than writing a marker");
            assertTrue(publisher.isFailed());
        } finally {
            CrashHook.clear();
            coordinator.dispose();
        }
    }
}
