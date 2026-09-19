package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.crashtest.ArchiveArtifactMutator;
import peruncs.datagrid.cluster.storage.aeron.crashtest.RecordingInspector;
import peruncs.datagrid.cluster.storage.aeron.reader.AeronArchiveReader;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataReceiver;

import java.io.File;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
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
            final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
                    archive, liveChannel, 1001, configuration, clusterId, 2, 0);
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
                try (AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
                        archive, liveChannel, 1001, configuration, clusterId, 2, 0)) {
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
            final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
                    archive, liveChannel, 1001, configuration, clusterId, 2, 0
            );
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
            assertThrows(IllegalArgumentException.class, () -> AeronArchiveReplicationPublisher.Extend(
                    archive, recordingId, 1002, configuration, clusterId, 2, 1));
            final byte[] resumedData = new byte[]{8, 6, 7, 5};
            final AeronArchiveReplicationPublisher resumed = AeronArchiveReplicationPublisher.Extend(
                    archive, recordingId, 1001, configuration, clusterId, 2, 1
            );
            await(resumed.publication()::isConnected, 10_000);
            resumed.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(resumedData)});
            assertEquals(recordingId, awaitRecordingId(resumed));
            final AeronArchiveReader client = AeronArchiveReader.New(
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
                    .replicationConfiguration(configuration).clusterId(clusterId).epoch(2)
                    .initialSequence(-1).receiver(receiver).build());
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
            final AeronArchiveReader restarted = AeronArchiveReader.New(
                    AeronArchiveReader.Configuration.builder()
                    .aeron(archive.context().aeron())
                    .archiveContext(new AeronArchive.Context().aeronDirectoryName(directory)
                            .controlRequestChannel(controlChannel).controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                            .messageTimeoutNs(10_000_000_000L))
                    .recordingId(recordingId).startPosition(restartPosition)
                    .liveChannel(liveChannel).liveStreamId(1001)
                    .replayChannel("aeron:udp?endpoint=localhost:0").replayStreamId(1002)
                    .replicationConfiguration(configuration).clusterId(clusterId).epoch(2)
                    .initialSequence(restartSequence).initialPosition(restartPosition)
                    .receiver(restartedReceiver).build());
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
            final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
                    archive, liveChannel, 1001, configuration, UUID.randomUUID(), 1, 0);
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
