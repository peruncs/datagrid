package peruncs.datagrid.cluster.storage.aeron.crashtest;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.writer.AeronArchiveReplicationPublisher;
import peruncs.datagrid.cluster.storage.aeron.writer.RawArchivePublisher;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises reader restart boundaries with a real child process and Archive. */
class AeronReaderCrashMatrixIT {
    private static final UUID CLUSTER_ID = UUID.nameUUIDFromBytes("reader-crash-cluster".getBytes(StandardCharsets.UTF_8));
    private static final long EPOCH = 2L;
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String LIVE_CHANNEL = "aeron:ipc?term-length=1048576|mtu=1408";
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final long RECORDING_ID_TIMEOUT_MILLIS = 15_000L;
    private static final long CHILD_FILE_TIMEOUT_MILLIS = 30_000L;

    private static Process launch(final Path base, final String mode, final String point, final long recordingId,
                                  final String controlChannel, final Path aeronDirectory) throws IOException {
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        Files.deleteIfExists(control.resolve("ready"));
        Files.deleteIfExists(control.resolve("outcome"));
        Files.deleteIfExists(control.resolve("milestone.reached"));
        Files.deleteIfExists(control.resolve("release"));
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.reader.base=" + base,
                "-Ddg.reader.mode=" + mode,
                "-Ddg.reader.barrier=" + point,
                "-Ddg.reader.recordingId=" + recordingId,
                "-Ddg.reader.controlChannel=" + controlChannel,
                "-Ddg.reader.controlResponseChannel=" + CONTROL_RESPONSE_CHANNEL,
                "-Ddg.reader.liveChannel=" + LIVE_CHANNEL,
                "-Ddg.reader.replayChannel=" + REPLAY_CHANNEL,
                "-Ddg.reader.aeronDirectory=" + aeronDirectory,
                "-Ddg.reader.sharedDriver=true",
                ReaderCrashChildMain.class.getName())
                .redirectOutput(control.resolve(mode + "-stdout.log").toFile())
                .redirectError(control.resolve(mode + "-stderr.log").toFile())
                .start();
    }

    private static byte[] payload(final int sequence) {
        final byte[] result = new byte[64];
        final byte[] value = ("reader-crash:" + sequence).getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < result.length; i++) result[i] = value[i % value.length];
        return result;
    }

    private static long awaitRecordingId(final AeronArchiveReplicationPublisher publisher) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RECORDING_ID_TIMEOUT_MILLIS);
        while (System.nanoTime() < deadline) {
            final long recordingId = publisher.recordingId();
            if (recordingId >= 0) return recordingId;
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
        throw new AssertionError("recording id not available");
    }

    private static void awaitFile(final Path path, final Process process) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CHILD_FILE_TIMEOUT_MILLIS);
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                final Path control = path.getParent();
                throw new AssertionError("child exited before " + path + "\n" +
                                         readIfExists(control.resolve("phase1-stderr.log")) + "\n" +
                                         readIfExists(control.resolve("phase1-stdout.log")) + "\nclasspath=" +
                                         System.getProperty("java.class.path") + "\nmodulepath=" +
                                         System.getProperty("jdk.module.path"));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
        assertTrue(Files.exists(path), "timed out waiting for " + path);
    }

    private static String readIfExists(final Path path) {
        try {
            return Files.exists(path) ? Files.readString(path) : "<missing " + path + ">";
        } catch (final IOException failure) {
            return failure.toString();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) return;
        IOException failure = null;
        try (var paths = Files.walk(root)) {
            for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException deleteFailure) {
                    if (failure == null) failure = deleteFailure;
                    else failure.addSuppressed(deleteFailure);
                }
            }
        }
        if (failure != null) throw failure;
    }

    /** Verifies replay before import leaves uncertain marker and requires reseed. */
    @Test
    void replayBeforeImportLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("REPLAY_BEFORE_FIRST_IMPORT", false);
    }

    /** Verifies import boundary leaves uncertain marker and requires reseed. */
    @Test
    void importBoundaryLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("DURING_STORE_IMPORT", false);
    }

    /** Verifies an injected Store import failure leaves the reader uncertain. */
    @Test
    void importFailureLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("DURING_STORE_IMPORT_FAILURE", false);
    }

    /** Verifies import before cursor boundary leaves store record and requires reseed. */
    @Test
    void importBeforeCursorBoundaryLeavesStoreRecordAndRequiresReseed() throws Exception {
        this.assertReseed("AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE", true);
    }

    /** Verifies a crash during cursor encoding retains the uncertainty marker. */
    @Test
    void cursorWriteFailureLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("DURING_CURSOR_FILE_WRITE", true);
    }

    /** Verifies a crash after cursor force but before replacement retains the prior cursor. */
    @Test
    void cursorRenameWindowLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("AFTER_CURSOR_TEMP_WRITE_BEFORE_RENAME", true);
    }

    /** Verifies a crash after cursor replacement but before directory sync is fail-closed. */
    @Test
    void cursorDirectorySyncWindowLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("AFTER_CURSOR_RENAME_BEFORE_DIRECTORY_SYNC", true);
    }

    private void assertReseed(final String point, final boolean expectStoreRecord)
            throws IOException, InterruptedException {
        final Path base = Files.createTempDirectory("dg-reader-crash-");
        final int controlPort = freePort();
        final Path mediaDirectory = base.resolve("archive-aeron");
        final Path archiveDirectory = base.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:" + controlPort;
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024).offerTimeoutNanos(10_000_000_000L).build();
        final MediaDriver.Context mediaContext = new MediaDriver.Context()
                .aeronDirectoryName(mediaDirectory.toString())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(mediaDirectory.toString())
                .archiveDir(archiveDirectory.toFile())
                .deleteArchiveOnStart(true)
                .threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED)
                .controlChannel(controlChannel)
                .replicationChannel(REPLAY_CHANNEL);
        Process child = null;
        Process recovery = null;
        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(mediaContext, archiveContext);
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                     .aeronDirectoryName(mediaDirectory.toString())
                     .controlRequestChannel(controlChannel)
                     .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                     .messageTimeoutNs(configuration.offerTimeoutNanos()))) {
            try (final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
                    archive, LIVE_CHANNEL, 1001, configuration, CLUSTER_ID, EPOCH, 0)) {
                RawArchivePublisher.publish(publisher, null, new ByteBuffer[]{ByteBuffer.wrap(payload(0))});
                RawArchivePublisher.publish(publisher, null, new ByteBuffer[]{ByteBuffer.wrap(payload(1))});
                final long recordingId = awaitRecordingId(publisher);
                child = launch(base, "phase1", point, recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/ready"), child);
                final Path milestonePath = base.resolve("control/milestone.reached");
                awaitFile(milestonePath, child);
                final ReaderMilestone milestone = ReaderMilestone.read(milestonePath);
                assertEquals(point, milestone.point(), "unexpected reader milestone " + milestone);
                assertTrue(milestone.sequence() >= 0, "reader milestone has no sequence");
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "reader child did not exit after kill");
                recovery = launch(base, "phase2", "NONE", recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/outcome"), recovery);
                assertTrue(recovery.waitFor(15, TimeUnit.SECONDS), "reader recovery child did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                assertTrue(outcome.lines().anyMatch(line -> line.equals("OUTCOME=RESEED_REQUIRED")), outcome);
                final Path uncertainty = base.resolve("reader.reader-inflight");
                assertTrue(Files.exists(uncertainty), "uncertainty marker must survive the crash");
                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(uncertainty);
                assertEquals(AeronReplicationCheckpoint.RecordType.READER_CURSOR, checkpoint.recordType());
                assertEquals(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, checkpoint.state());
                assertEquals(milestone.sequence(), checkpoint.transactionSequence());
                assertEquals(milestone.position(), checkpoint.recordingPosition());
                assertEquals(Files.exists(base.resolve("reader.store")), expectStoreRecord, "unexpected Store fixture state for " + point);
            }
        } finally {
            if (child != null && child.isAlive()) child.destroyForcibly();
            if (recovery != null && recovery.isAlive()) recovery.destroyForcibly();
            deleteTree(base);
        }
    }

}
