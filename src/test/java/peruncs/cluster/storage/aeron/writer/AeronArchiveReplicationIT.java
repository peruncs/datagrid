package peruncs.cluster.storage.aeron.writer;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.crashtest.ArchiveArtifactMutator;
import peruncs.cluster.storage.aeron.crashtest.RecordingInspector;
import peruncs.cluster.storage.aeron.reader.AeronArchiveReader;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.io.File;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that an Archive recording can be written, inspected, and extended.
class AeronArchiveReplicationIT {
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";

    private static long awaitRecordingId(final AeronArchiveReplicationPublisher publisher) {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        long recordingId;
        do {
            recordingId = publisher.recordingId();
            if (recordingId >= 0) {
                return recordingId;
            }
            LockSupport.parkNanos(1_000_000L);
        }
        while (System.nanoTime() < deadline);
        throw new AssertionError("recording counter was not created");
    }

    private static ArchiveFixture createStoppedRecording(final String prefix) throws Exception {
        final int controlPort = freePort();
        final Path root = Files.createTempDirectory(prefix);
        final Path aeronDirectory = root.resolve("aeron");
        final Path archiveDirectory = root.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
        final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024).offerTimeoutNanos(10_000_000_000L).build();
        final UUID clusterId = UUID.randomUUID();
        final MediaDriver.Context media = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectory.toString()).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        final AeronArchive.Context client = new AeronArchive.Context()
                .aeronDirectoryName(aeronDirectory.toString()).controlRequestChannel(controlChannel)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(10_000_000_000L);
        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(aeronDirectory.toString()).archiveDir(archiveDirectory.toFile())
                .deleteArchiveOnStart(true).threadingMode(ArchiveThreadingMode.SHARED)
                .controlChannel(controlChannel).replicationChannel("aeron:udp?endpoint=localhost:0");
        long recordingId;
        long startPosition;
        long stopPosition;
        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(media, archiveContext);
             AeronArchive archive = AeronArchive.connect(client)) {
            final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.create(
                    archive, liveChannel, 1001, configuration, clusterId, 2, 0, AeronReplicationEnvelope.defaultWireNonce(clusterId));
            await(publisher.publication()::isConnected, 10_000);
            publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[70_000])});
            recordingId = awaitRecordingId(publisher);
            publisher.close();
            final long stoppedRecording = recordingId;
            await(() -> archive.getStopPosition(stoppedRecording) >= 0, 10_000);
            startPosition = archive.getStartPosition(recordingId);
            stopPosition = archive.getStopPosition(recordingId);
        }
        return new ArchiveFixture(root, aeronDirectory.toString(), archiveDirectory,
                controlChannel, recordingId, clusterId, startPosition, stopPosition);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void await(final Check check, final long timeoutMillis) {
        final long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (!check.value()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("timed out waiting for Archive state");
            }
            LockSupport.parkNanos(1_000_000L);
        }
    }

    private static void delete(final File file) throws Exception {
        if (file.exists()) {
            try (var paths = Files.walk(file.toPath())) {
                paths.sorted(Comparator.reverseOrder()).forEach(path ->
                {
                    try {
                        Files.deleteIfExists(path);
                    } catch (final Exception ignored) {
                    }
                });
            }
        }
    }

        /// Verifies a stalled recording with live delivery running ahead: the
    /// reader withholds the live COMMIT while the Archive cannot prove
    /// coverage — no staging, no cursor advance — and fails closed once the
    /// reader stop budget expires.
    @Test
    void stalledRecordingWithholdsLiveCommits() throws Exception {
        final int controlPort = freePort();
        final String directory = Files.createTempDirectory("datagrid-aeron-stall-").toString();
        final File archiveDirectory = new File(directory, "archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
        final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
        final long stallBudgetNanos = 400_000_000L;
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024).offerTimeoutNanos(2_000_000_000L)
                .recordedPositionTimeoutNanos(1_000_000_000L)
                .readerStopTimeoutNanos(stallBudgetNanos)
                .build();
        final UUID clusterId = UUID.randomUUID();
        final RecordingReceiver receiver = new RecordingReceiver();
        final MediaDriver.Context mediaContext = new MediaDriver.Context()
                .aeronDirectoryName(directory)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                .aeronDirectoryName(directory)
                .controlRequestChannel(controlChannel)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                .messageTimeoutNs(2_000_000_000L);
        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(directory)
                .archiveDir(archiveDirectory)
                .deleteArchiveOnStart(true)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .controlChannel(controlChannel)
                .replicationChannel("aeron:udp?endpoint=localhost:0");

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(mediaContext, archiveContext);
             AeronArchive archive = AeronArchive.connect(archiveClientContext);
             Aeron readerAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory))) {
            final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.create(
                    archive, liveChannel, 1001, configuration, clusterId, 2, 0, AeronReplicationEnvelope.defaultWireNonce(clusterId));
            await(publisher.publication()::isConnected, 10_000);
            final byte[] recorded = new byte[4 * 1024];
            publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(recorded)});
            final long recordingId = awaitRecordingId(publisher);

            final AeronArchiveReader client = AeronArchiveReader.create(
                    AeronArchiveReader.Configuration.builder()
                            .aeron(readerAeron)
                            .archiveContext(new AeronArchive.Context()
                                    .aeronDirectoryName(directory)
                                    .controlRequestChannel(controlChannel)
                                    .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                                    .messageTimeoutNs(2_000_000_000L))
                            .recordingId(recordingId)
                            .startPosition(PersistentSubscription.FROM_START)
                            .liveChannel(liveChannel).liveStreamId(1001)
                            .replayChannel("aeron:udp?endpoint=localhost:0").replayStreamId(1002)
                            .replicationConfiguration(configuration).clusterId(clusterId)
                            .wireNonce(AeronReplicationEnvelope.defaultWireNonce(clusterId)).epoch(2)
                            .initialSequence(-1).receiver(receiver)
                            .recordedPosition(() -> archive.getMaxRecordedPosition(recordingId)).build());
            client.start();
            /* The first commit replays and applies; the reader then joins the
             * live tail. */
            await(() -> client.lastResolvedSequence() == 0 || client.failure() != null, 15_000);
            assertNull(client.failure());
            await(client::isLive, 15_000);
            assertArrayEquals(recorded, receiver.data);

            /* Stall the recording while the live publication continues: the
             * next COMMIT can never become durable. The publication call below
             * fails on its own recorded-position wait; only its offered frames
             * matter to the reader. */
            assertTrue(archive.tryStopRecordingByIdentity(recordingId),
                    "the session-scoped recording must be running before the stall");
            final byte[] stalled = new byte[2 * 1024];
            final java.util.concurrent.atomic.AtomicReference<Throwable> publishFailure =
                    new java.util.concurrent.atomic.AtomicReference<>();
            final Thread publishing = Thread.ofVirtual().start(() ->
            {
                try {
                    publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(stalled)});
                } catch (final Throwable failure) {
                    publishFailure.set(failure);
                }
            });

            /* During the stall budget the reader must neither apply the
             * unrecorded transaction nor fail early. */
            final long stallStartNanos = System.nanoTime();
            LockSupport.parkNanos(stallBudgetNanos / 2L);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(150L));
            assertEquals(0L, client.lastResolvedSequence(),
                    "the withheld live COMMIT must not advance the cursor");
            assertArrayEquals(recorded, receiver.data,
                    "the withheld live COMMIT must not reach the Store");
            assertNull(client.failure(), "withholding is not failure before the budget expires");

            /* Past the budget the reader fails closed (fail closed, not a
             * silent application). */
            await(() -> client.failure() != null, TimeUnit.NANOSECONDS.toMillis(stallBudgetNanos) + 10_000L);
            assertNotNull(client.failure());
            assertEquals(0L, client.lastResolvedSequence(),
                    "a recorded-less live COMMIT must never become durable for the reader");
            assertArrayEquals(recorded, receiver.data);
            client.dispose();
            publishing.join(TimeUnit.SECONDS.toMillis(15L));
            assertNotNull(publishFailure.get(),
                    "the writer's own recorded-position wait must fail on a stalled recording");
            publisher.close();
        } finally {
            delete(archiveDirectory);
            delete(new File(directory));
        }
    }

    /// Verifies corrupted recording payload fails archive inspection.
    @Test
    void corruptedRecordingPayloadFailsArchiveInspection() throws Exception {
        final int controlPort = freePort();
        final String directory = Files.createTempDirectory("datagrid-aeron-corrupt-").toString();
        final String aeronDirectory = Path.of(directory, "aeron").toString();
        final File archiveDirectory = new File(directory, "archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
        final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024).offerTimeoutNanos(10_000_000_000L).build();
        final UUID clusterId = UUID.randomUUID();
        long recordingId;
        try {
            final MediaDriver.Context mediaContext = new MediaDriver.Context()
                    .aeronDirectoryName(aeronDirectory).threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
            final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                    .aeronDirectoryName(aeronDirectory).controlRequestChannel(controlChannel)
                    .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(10_000_000_000L);
            final Archive.Context archiveContext = new Archive.Context()
                    .aeronDirectoryName(aeronDirectory).archiveDir(archiveDirectory).deleteArchiveOnStart(true)
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .controlChannel(controlChannel).replicationChannel("aeron:udp?endpoint=localhost:0");
            try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(mediaContext, archiveContext);
                 AeronArchive archive = AeronArchive.connect(archiveClientContext)) {
                try (AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.create(
                        archive, liveChannel, 1001, configuration, clusterId, 2, 0, AeronReplicationEnvelope.defaultWireNonce(clusterId))) {
                    await(publisher.publication()::isConnected, 10_000);
                    publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[70_000])});
                    recordingId = awaitRecordingId(publisher);
                }
            }
            final Path segment = ArchiveArtifactMutator.segments(archiveDirectory.toPath(), recordingId).getFirst();
            ArchiveArtifactMutator.corruptFirstEnvelopePayload(segment);

            Throwable failure = null;
            try {
                final MediaDriver.Context restartMedia = new MediaDriver.Context()
                        .aeronDirectoryName(aeronDirectory).threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
                final Archive.Context restartArchive = new Archive.Context()
                        .aeronDirectoryName(aeronDirectory).archiveDir(archiveDirectory).deleteArchiveOnStart(false)
                        .threadingMode(ArchiveThreadingMode.SHARED)
                        .controlChannel(controlChannel).replicationChannel("aeron:udp?endpoint=localhost:0");
                try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(restartMedia, restartArchive);
                     AeronArchive archive = AeronArchive.connect(archiveClientContext)) {
                    RecordingInspector.inspect(archive, recordingId, "aeron:udp?endpoint=localhost:%s".formatted(freePort()),
                            1001, clusterId, 2, 10_000);
                }
            } catch (final Throwable corruptionDetected) {
                failure = corruptionDetected;
            }
            assertNotNull(failure, "corrupted Archive recording was accepted");
        } finally {
            delete(archiveDirectory);
            delete(new File(directory));
        }
    }

        /// Verifies truncated archive catalog cannot silently create a replacement recording.
    @Test
    void truncatedArchiveCatalogCannotSilentlyCreateAReplacementRecording() throws Exception {
        final ArchiveFixture fixture = createStoppedRecording("datagrid-aeron-catalog-");
        try {
            ArchiveArtifactMutator.truncateCatalog(fixture.archiveDirectory());
            Throwable failure = null;
            try {
                final MediaDriver.Context media = new MediaDriver.Context()
                        .aeronDirectoryName(fixture.aeronDirectory()).threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
                final Archive.Context archiveContext = new Archive.Context()
                        .aeronDirectoryName(fixture.aeronDirectory()).archiveDir(fixture.archiveDirectory().toFile())
                        .deleteArchiveOnStart(false).threadingMode(ArchiveThreadingMode.SHARED)
                        .controlChannel(fixture.controlChannel()).replicationChannel("aeron:udp?endpoint=localhost:0");
                final AeronArchive.Context clientContext = new AeronArchive.Context()
                        .aeronDirectoryName(fixture.aeronDirectory()).controlRequestChannel(fixture.controlChannel())
                        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(10_000_000_000L);
                try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(media, archiveContext);
                     AeronArchive archive = AeronArchive.connect(clientContext)) {
                    assertTrue(archive.getStartPosition(fixture.recordingId()) < 0,
                            "truncated catalog exposed the old recording as valid");
                }
            } catch (final Throwable corruptionDetected) {
                failure = corruptionDetected;
            }
            assertNotNull(failure, "truncated Archive catalog was accepted");
        } finally {
            delete(fixture.archiveDirectory().toFile());
            delete(fixture.root().toFile());
        }
    }

        /// Verifies truncated recording frame fails archive inspection.
    @Test
    void truncatedRecordingFrameFailsArchiveInspection() throws Exception {
        final ArchiveFixture fixture = createStoppedRecording("datagrid-aeron-tail-");
        try {
            final List<Path> segments = ArchiveArtifactMutator.segments(
                    fixture.archiveDirectory(), fixture.recordingId());
            ArchiveArtifactMutator.truncateFinalFrame(
                    segments.getLast(),
                    fixture.startPosition(), fixture.stopPosition());
            Throwable failure = null;
            try {
                final MediaDriver.Context media = new MediaDriver.Context()
                        .aeronDirectoryName(fixture.aeronDirectory()).threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
                final Archive.Context archiveContext = new Archive.Context()
                        .aeronDirectoryName(fixture.aeronDirectory()).archiveDir(fixture.archiveDirectory().toFile())
                        .deleteArchiveOnStart(false).threadingMode(ArchiveThreadingMode.SHARED)
                        .controlChannel(fixture.controlChannel()).replicationChannel("aeron:udp?endpoint=localhost:0");
                final AeronArchive.Context clientContext = new AeronArchive.Context()
                        .aeronDirectoryName(fixture.aeronDirectory()).controlRequestChannel(fixture.controlChannel())
                        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(10_000_000_000L);
                try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(media, archiveContext);
                     AeronArchive archive = AeronArchive.connect(clientContext)) {
                    RecordingInspector.inspect(archive, fixture.recordingId(),
                            "aeron:udp?endpoint=localhost:%s".formatted(freePort()), 1001, fixture.clusterId(), 2, 10_000,
                            fixture.stopPosition());
                }
            } catch (final Throwable corruptionDetected) {
                failure = corruptionDetected;
            }
            assertNotNull(failure, "truncated Archive recording was accepted");
        } finally {
            delete(fixture.archiveDirectory().toFile());
            delete(fixture.root().toFile());
        }
    }

        /// Verifies fragmented data and its terminal marker are inspected across poll boundaries.
    @Test
    void fragmentedTailIsInspectedAcrossPollBoundaries() throws Exception {
        final ArchiveFixture fixture = createStoppedRecording("datagrid-aeron-fragmented-tail-");
        try {
            final MediaDriver.Context media = new MediaDriver.Context()
                    .aeronDirectoryName(fixture.aeronDirectory()).threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
            final Archive.Context archiveContext = new Archive.Context()
                    .aeronDirectoryName(fixture.aeronDirectory()).archiveDir(fixture.archiveDirectory().toFile())
                    .deleteArchiveOnStart(false).threadingMode(ArchiveThreadingMode.SHARED)
                    .controlChannel(fixture.controlChannel()).replicationChannel("aeron:udp?endpoint=localhost:0");
            final AeronArchive.Context clientContext = new AeronArchive.Context()
                    .aeronDirectoryName(fixture.aeronDirectory()).controlRequestChannel(fixture.controlChannel())
                    .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(10_000_000_000L);
            try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(media, archiveContext);
                 AeronArchive archive = AeronArchive.connect(clientContext)) {
                final RecordingInspector.RecordingEvidence evidence = RecordingInspector.inspect(
                        archive, fixture.recordingId(), "aeron:udp?endpoint=localhost:%s".formatted(freePort()), 1001,
                        fixture.clusterId(), 2, 10_000, fixture.stopPosition(), 1);
                assertEquals(AeronReplicationEnvelope.Kind.COMMIT,
                        evidence.terminalBySequence().get(0L));
                assertEquals(0L, evidence.orphanTailLength());
            }
        } finally {
            delete(fixture.archiveDirectory().toFile());
            delete(fixture.root().toFile());
        }
    }

        /// Verifies replays recorded udp messages and joins live.
    @Test
    void replaysRecordedUdpMessagesAndJoinsLive() throws Exception {
        final int controlPort = freePort();
        final String directory = Files.createTempDirectory("datagrid-aeron-").toString();
        final File archiveDirectory = new File(directory, "archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
        // Archive control and replay are UDP; IPC is used only for the local
        // recorded publication so this test is deterministic on CI hosts.
        final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024)
                .mtuLength(1408)
                .chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024)
                .offerTimeoutNanos(10_000_000_000L)
                .build();
        final UUID clusterId = UUID.randomUUID();
        final RecordingReceiver receiver = new RecordingReceiver();

        final MediaDriver.Context mediaContext = new MediaDriver.Context()
                .aeronDirectoryName(directory)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                .aeronDirectoryName(directory)
                .controlRequestChannel(controlChannel)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                .messageTimeoutNs(10_000_000_000L);
        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(directory)
                .archiveDir(archiveDirectory)
                .deleteArchiveOnStart(true)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .controlChannel(controlChannel)
                .replicationChannel("aeron:udp?endpoint=localhost:0");

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(mediaContext, archiveContext);
             AeronArchive archive = AeronArchive.connect(archiveClientContext)) {
            final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.create(
                    archive, liveChannel, 1001, configuration, clusterId, 2, 0, AeronReplicationEnvelope.defaultWireNonce(clusterId));
            await(publisher.publication()::isConnected, 10_000);
            final byte[] data = new byte[70_000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i * 13);
            }
            publisher.publishTransaction(
                    "recorded.Type".getBytes(StandardCharsets.UTF_8),
                    new ByteBuffer[]{ByteBuffer.wrap(data)}
            );
            final long recordingId = awaitRecordingId(publisher);
            final long firstStop = publisher.publication().position();
            assertTrue(publisher.recordingIsActive(), "local Archive recording must be active without an external reader");
            assertTrue(publisher.publication().position() > 0, "writer publication must progress with zero external readers");
            publisher.close();
            await(() -> archive.getStopPosition(recordingId) >= firstStop, 10_000);
            assertThrows(IllegalArgumentException.class, () -> AeronArchiveReplicationPublisher.extend(
                    archive, recordingId, 1002, configuration, clusterId, 2, 1, AeronReplicationEnvelope.defaultWireNonce(clusterId)));
            final byte[] resumedData = new byte[]{8, 6, 7, 5};
            final AeronArchiveReplicationPublisher resumed = AeronArchiveReplicationPublisher.extend(
                    archive, recordingId, 1001, configuration, clusterId, 2, 1, AeronReplicationEnvelope.defaultWireNonce(clusterId));
            await(resumed.publication()::isConnected, 10_000);
            resumed.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(resumedData)});
            assertEquals(recordingId, awaitRecordingId(resumed));
            final AeronArchiveReader client = AeronArchiveReader.create(
                    AeronArchiveReader.Configuration.builder()
                    .aeron(archive.context().aeron())
                    .archiveContext(new AeronArchive.Context()
                            .aeronDirectoryName(directory)
                            .controlRequestChannel(controlChannel)
                            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                            .messageTimeoutNs(10_000_000_000L))
                    .recordingId(recordingId)
                    .startPosition(PersistentSubscription.FROM_START)
                    .liveChannel(liveChannel).liveStreamId(1001)
                    .replayChannel("aeron:udp?endpoint=localhost:0").replayStreamId(1002)
                    .replicationConfiguration(configuration).clusterId(clusterId)
                    .wireNonce(AeronReplicationEnvelope.defaultWireNonce(clusterId)).epoch(2)
                    .initialSequence(-1).receiver(receiver)
                    .recordedPosition(() -> archive.getMaxRecordedPosition(recordingId)).build());
            client.start();

            await(() -> client.lastResolvedSequence() == 1 || client.failure() != null, 15_000);
            if (client.failure() != null) {
                throw client.failure();
            }
            assertEquals("recorded.Type", receiver.dictionary);
            assertArrayEquals(resumedData, receiver.data);
            assertNull(client.failure());
            final long restartPosition = client.lastResolvedPosition();
            final long restartSequence = client.lastResolvedSequence();
            client.dispose();
            final RecordingReceiver restartedReceiver = new RecordingReceiver();
            final AeronArchiveReader restarted = AeronArchiveReader.create(
                    AeronArchiveReader.Configuration.builder()
                    .aeron(archive.context().aeron())
                    .archiveContext(new AeronArchive.Context().aeronDirectoryName(directory)
                            .controlRequestChannel(controlChannel).controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                            .messageTimeoutNs(10_000_000_000L))
                    .recordingId(recordingId).startPosition(restartPosition)
                    .liveChannel(liveChannel).liveStreamId(1001)
                    .replayChannel("aeron:udp?endpoint=localhost:0").replayStreamId(1002)
                    .replicationConfiguration(configuration).clusterId(clusterId)
                    .wireNonce(AeronReplicationEnvelope.defaultWireNonce(clusterId)).epoch(2)
                    .initialSequence(restartSequence).initialPosition(restartPosition)
                    .receiver(restartedReceiver)
                    .recordedPosition(() -> archive.getMaxRecordedPosition(recordingId)).build());
            restarted.start();
            final byte[] thirdData = new byte[]{1, 3, 3, 7};
            resumed.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(thirdData)});
            await(() -> restarted.lastResolvedSequence() == 2 || restarted.failure() != null, 15_000);
            if (restarted.failure() != null) {
                throw restarted.failure();
            }
            assertArrayEquals(thirdData, restartedReceiver.data);
            restarted.dispose();
            final long finalStop = resumed.publication().position();
            assertTrue(finalStop > 0, "resumed publication did not advance");
            resumed.close();
            await(() ->
                    archive.getStopPosition(recordingId) >= finalStop &&
                    archive.getStopPosition(recordingId) > archive.getStartPosition(recordingId), 10_000);
            final RecordingInspector.RecordingEvidence evidence = RecordingInspector.inspect(
                    archive, recordingId, "aeron:udp?endpoint=localhost:%s".formatted(freePort()), 1001, clusterId, 2, 10_000);
            assertEquals(AeronReplicationEnvelope.Kind.COMMIT, evidence.terminalBySequence().get(0L),
                    "terminals=%s payloads=%s".formatted(evidence.terminalBySequence(), evidence.payloadCrcBySequence()));
            assertEquals(AeronReplicationEnvelope.Kind.COMMIT, evidence.terminalBySequence().get(1L));
            assertEquals(AeronReplicationEnvelope.Kind.COMMIT, evidence.terminalBySequence().get(2L));
            assertEquals(0L, evidence.orphanTailLength());
        } finally {
            delete(archiveDirectory);
            delete(new File(directory));
        }
    }

        /// A closed Archive must not make the publisher report a successful close while
    /// its recording may still be active. The wrapper is retained so a caller can
    /// retry the Archive stop after reconnecting the control client.
    @Test
    void archiveStopFailureKeepsPublisherOpenForRetry() throws Exception {
        final int controlPort = freePort();
        final String directory = Files.createTempDirectory("datagrid-aeron-close-").toString();
        final File archiveDirectory = new File(directory, "archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
        final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024).offerTimeoutNanos(2_000_000_000L).build();
        final MediaDriver.Context mediaContext = new MediaDriver.Context()
                .aeronDirectoryName(directory).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                .aeronDirectoryName(directory).controlRequestChannel(controlChannel)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(10_000_000_000L);
        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(directory).archiveDir(archiveDirectory).deleteArchiveOnStart(true)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .controlChannel(controlChannel).replicationChannel("aeron:udp?endpoint=localhost:0");
        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(mediaContext, archiveContext)) {
            final AeronArchive archive = AeronArchive.connect(archiveClientContext);
            final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.create(
                    archive, liveChannel, 1001, configuration, UUID.randomUUID(), 1, 0, AeronReplicationEnvelope.defaultWireNonce(UUID.randomUUID()));
            try {
                await(publisher.publication()::isConnected, 10_000);
                publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1, 2, 3})});
                awaitRecordingId(publisher);
                archive.close();
                assertThrows(IllegalStateException.class, publisher::close);
                assertFalse(publisher.isClosed(), "an unconfirmed Archive stop must remain retryable");
            } finally {
                if (!publisher.isClosed()) {
                    try {
                        publisher.close();
                    } catch (final RuntimeException ignored) {
                        // The control client was deliberately closed; the driver owns cleanup.
                    }
                }
            }
        } finally {
            delete(archiveDirectory);
            delete(new File(directory));
        }
    }

    /// Verifies a mid-replay Archive restart is absorbed by the reader's bounded
    /// reconnect: the lost response channel swaps for a fresh subscription
    /// resuming at the last resolved position, and replay completes instead of
    /// dying on a raw ArchiveException (soak finding #1).
    @Test
    void archiveRestartMidReplayTriggersBoundedReconnect() throws Exception {
        final int controlPort = freePort();
        final String directory = Files.createTempDirectory("datagrid-aeron-reconnect-").toString();
        final File archiveDirectory = new File(directory, "archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
        final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024).offerTimeoutNanos(10_000_000_000L).build();
        final UUID clusterId = UUID.randomUUID();
        /* Enough history that replay is still in flight when the Archive goes
         * away, so the disconnect lands mid-replay rather than on a live
         * subscription. */
        final int transactions = 256;
        /* Standalone MediaDriver and Archive, so the Archive can restart like
         * the writer's embedded one while the MediaDriver stays up. */
        final MediaDriver.Context mediaContext = new MediaDriver.Context()
                .aeronDirectoryName(directory).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                .aeronDirectoryName(directory).controlRequestChannel(controlChannel)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(2_000_000_000L);
        try (MediaDriver mediaDriver = MediaDriver.launch(mediaContext)) {
            final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory));
            try (aeron;
                 Archive archive = Archive.launch(
                         archiveContext(directory, archiveDirectory, controlChannel, true));
                 AeronArchive archiveClient = connectArchive(
                         archiveClientContext.clone().aeron(aeron).ownsAeronClient(false))) {
                final AeronArchiveReplicationPublisher publisher;
                final long recordingId;
                {
                    publisher = AeronArchiveReplicationPublisher.create(
                            archiveClient, liveChannel, 1001, configuration, clusterId, 2, 0, AeronReplicationEnvelope.defaultWireNonce(clusterId));
                    try {
                        await(publisher.publication()::isConnected, 10_000);
                        for (int i = 0; i < transactions; i++) {
                            publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[32 * 1024])});
                        }
                        recordingId = awaitRecordingId(publisher);
                    } finally {
                        publisher.close();
                    }
                    await(() -> archiveClient.getStopPosition(recordingId) >= 0, 10_000);
                    final CountingReceiver receiver = new CountingReceiver();
                    final AeronArchiveReader reader = AeronArchiveReader.create(
                            AeronArchiveReader.Configuration.builder()
                                    .aeron(aeron)
                                    .archiveContext(archiveClientContext)
                                    .recordingId(recordingId)
                                    .startPosition(PersistentSubscription.FROM_START)
                                    .liveChannel(liveChannel).liveStreamId(1001)
                                    .replayChannel("aeron:udp?endpoint=localhost:0").replayStreamId(1002)
                                    .replicationConfiguration(configuration).clusterId(clusterId)
                    .wireNonce(AeronReplicationEnvelope.defaultWireNonce(clusterId)).epoch(2)
                                    .initialSequence(-1).receiver(receiver)
                    .recordedPosition(() -> archiveClient.getMaxRecordedPosition(recordingId)).build());
                    try {
                        reader.start();
                        /* Wait until replay demonstrably started, then restart
                         * the Archive mid-replay like the writer's embedded
                         * Archive restart that killed readers in the soak. */
                        await(() -> reader.lastResolvedSequence() >= 4 || reader.failure() != null, 15_000);
                        assertNull(reader.failure());
                        archive.close();
                        /* The reader is now inside its bounded reconnect budget;
                         * bring the Archive back so the replacement subscription
                         * can attach to the same recording and catalog. */
                        try (Archive restarted = Archive.launch(
                                archiveContext(directory, archiveDirectory, controlChannel, false))) {
                            await(() -> reader.lastResolvedSequence() == transactions - 1 ||
                                        reader.failure() != null, 30_000);
                            assertNull(reader.failure(),
                                    "a mid-replay Archive restart must stay within the bounded reconnect path");
                            assertEquals(transactions, receiver.count(),
                                    "reconnect must resume exactly at the last resolved boundary");
                            assertEquals(ReplicationApplier.StopOutcome.RUNNING, reader.stopOutcome());
                        }
                    } finally {
                        reader.dispose();
                    }
                }
            }
        } finally {
            delete(archiveDirectory);
            delete(new File(directory));
        }
    }

    /// Verifies an Archive response channel that never returns within the
    /// reader's reconnect budget fails closed with a typed RESEED_REQUIRED
    /// signal instead of surfacing a raw ArchiveException.
    @Test
    void archiveGoneBeyondReconnectBudgetLatchesTypedReseedFailure() throws Exception {
        final int controlPort = freePort();
        final String directory = Files.createTempDirectory("datagrid-aeron-reseed-").toString();
        final File archiveDirectory = new File(directory, "archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
        final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024).offerTimeoutNanos(10_000_000_000L)
                .readerStopTimeoutNanos(1_000_000_000L).build();
        final UUID clusterId = UUID.randomUUID();
        /* Short control timeouts so each reconnect attempt fails fast while
         * the Archive is down and the 1s reconnect budget expires quickly. */
        final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                .aeronDirectoryName(directory).controlRequestChannel(controlChannel)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).messageTimeoutNs(500_000_000L);
        final MediaDriver.Context mediaContext = new MediaDriver.Context()
                .aeronDirectoryName(directory).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        try (MediaDriver mediaDriver = MediaDriver.launch(mediaContext)) {
            final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory));
            try (aeron;
                 Archive archive = Archive.launch(
                         archiveContext(directory, archiveDirectory, controlChannel, true));
                 AeronArchive archiveClient =
                         connectArchive(archiveClientContext.clone().aeron(aeron).ownsAeronClient(false))) {
                final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.create(
                        archiveClient, liveChannel, 1001, configuration, clusterId, 2, 0, AeronReplicationEnvelope.defaultWireNonce(clusterId));
                final long recordingId;
                try {
                    await(publisher.publication()::isConnected, 10_000);
                    /* Enough history that replay is still in flight when the
                     * Archive goes away, so the disconnect lands mid-replay. */
                    for (int i = 0; i < 128; i++) {
                        publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[32 * 1024])});
                    }
                    recordingId = awaitRecordingId(publisher);
                } finally {
                    publisher.close();
                }
                await(() -> archiveClient.getStopPosition(recordingId) >= 0, 10_000);
                final AeronArchiveReader reader = AeronArchiveReader.create(
                        AeronArchiveReader.Configuration.builder()
                                .aeron(aeron)
                                .archiveContext(archiveClientContext)
                                .recordingId(recordingId)
                                .startPosition(PersistentSubscription.FROM_START)
                                .liveChannel(liveChannel).liveStreamId(1001)
                                .replayChannel("aeron:udp?endpoint=localhost:0").replayStreamId(1002)
                                .replicationConfiguration(configuration).clusterId(clusterId)
                    .wireNonce(AeronReplicationEnvelope.defaultWireNonce(clusterId)).epoch(2)
                                .initialSequence(-1).receiver(new CountingReceiver())
                                .recordedPosition(() -> archiveClient.getMaxRecordedPosition(recordingId)).build());
                try {
                    reader.start();
                    await(() -> reader.lastResolvedSequence() >= 2 || reader.failure() != null, 15_000);
                    assertNull(reader.failure());
                    /* The Archive goes away and never returns: the reconnect
                     * budget must expire into the typed reseed signal. */
                    archive.close();
                    await(() -> reader.failure() != null, 15_000);
                    assertInstanceOf(ReseedRequiredException.class, reader.failure(),
                            "an unrecoverable Archive loss must surface as RESEED_REQUIRED, not a raw ArchiveException");
                    assertEquals(ReplicationApplier.StopOutcome.FAILED, reader.stopOutcome());
                } finally {
                    reader.dispose();
                }
            }
        } finally {
            delete(archiveDirectory);
            delete(new File(directory));
        }
    }

    /// Connects an Archive control client with bounded retries, since an
    /// Archive launched in-process publishes its control endpoint
    /// asynchronously after `Archive.launch` returns.
    private static AeronArchive connectArchive(final AeronArchive.Context context) {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (true) {
            try {
                return AeronArchive.connect(context);
            } catch (final RuntimeException connectFailure) {
                if (System.nanoTime() >= deadline) throw connectFailure;
                LockSupport.parkNanos(50_000_000L);
            }
        }
    }

    private static Archive.Context archiveContext(
            final String aeronDirectory,
            final File archiveDirectory,
            final String controlChannel,
            final boolean deleteOnStart) {
        return new Archive.Context()
                .aeronDirectoryName(aeronDirectory).archiveDir(archiveDirectory)
                .deleteArchiveOnStart(deleteOnStart).threadingMode(ArchiveThreadingMode.SHARED)
                .controlChannel(controlChannel).replicationChannel("aeron:udp?endpoint=localhost:0");
    }

    @FunctionalInterface
    private interface Check {
        boolean value();
    }

    private record ArchiveFixture(
            Path root,
            String aeronDirectory,
            Path archiveDirectory,
            String controlChannel,
            long recordingId,
            UUID clusterId,
            long startPosition,
            long stopPosition
    ) {
    }

    private static final class CountingReceiver implements StorageBinaryDataReceiver {
        private final java.util.concurrent.atomic.AtomicInteger applied = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public void receiveData(final Binary value) {
            this.applied.incrementAndGet();
        }

        @Override
        public void receiveTypeDictionary(final String value) {
        }

        int count() {
            return this.applied.get();
        }
    }

    private static final class RecordingReceiver implements StorageBinaryDataReceiver {
        private volatile String dictionary;
        private volatile byte[] data;

        @Override
        public void receiveData(final Binary value) {
            final ByteBuffer source = value.buffers()[0].duplicate();
            source.flip();
            this.data = new byte[source.remaining()];
            source.get(this.data);
        }

        @Override
        public void receiveTypeDictionary(final String value) {
            this.dictionary = value;
        }
    }
}
