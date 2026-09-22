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
import peruncs.datagrid.cluster.test.ChildJava;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/// Crash matrix for the replication reader: proves every kill boundary on the
/// reader's apply path ends in a safe recovery outcome, never in silent data
/// divergence.
///
/// The reader has exactly one durability contract: a replicated transaction is
/// durable only when the Store import has completed AND the durable cursor has
/// been atomically replaced AND its directory synced. Killing the process at
/// any earlier point must be detectable after restart, and detection must
/// refuse to continue until reseeded rather than risk applying the same
/// transaction twice or serving a half-imported Store. These tests encode that
/// contract, one kill point per method.
///
/// Each cell is a two-process choreography with a real SIGKILL, not a
/// simulation:
///
/// - phase1 [`ReaderCrashChildMain`] replays a small recording, parks at the
///   named boundary (e.g. `DURING_STORE_IMPORT`), and writes a
///   CRC-checked `milestone.reached` control file. A reader-transaction
///   uncertainty marker (`reader.cursor` companion: `reader.reader-inflight`)
///   is written before any Store work and must survive the kill.
/// - The parent observes the milestone, then `destroyForcibly()` kills the
///   child mid-window — the kill cannot land anywhere else because the child
///   is parked.
/// - Some cells then mutate the on-disk remains (bit-flip the uncertainty
///   marker, delete the durable cursor) to prove corruption cannot launder an
///   uncertain transaction into a clean-looking recovery.
/// - phase2 is a fresh process over the same directories: it must report
///   `OUTCOME=RESEED_REQUIRED` (or `FAIL_CLOSED` for mutated state), never
///   continue, and the uncertainty marker must still be present.
///
/// The replay cells (`*RecoversByArchiveReplay`, `*NeverAppliesThePartialTransaction`)
/// cover the complementary contract: killed *before* any import ran, with the
/// durable cursor still at its validated boundary, so recovery is pure Archive
/// replay (`REPLAY_FROM_ARCHIVE`) and the fixture must contain exactly the
/// published records — never a torn multi-chunk transaction.
///
/// This is the reader-side companion of `ProviderCrashMatrixIT` (writer cells)
/// and shares its control-file protocol: `ready`, `milestone.reached`,
/// `release`, `outcome`, all under `control/` inside the per-test temp tree.
class AeronReaderCrashMatrixIT {
    private static final UUID CLUSTER_ID = UUID.nameUUIDFromBytes("reader-crash-cluster".getBytes(StandardCharsets.UTF_8));
    private static final long EPOCH = 2L;
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String LIVE_CHANNEL = "aeron:ipc?term-length=1048576|mtu=1408";
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final long RECORDING_ID_TIMEOUT_MILLIS = 15_000L;
    private static final long CHILD_FILE_TIMEOUT_MILLIS = 30_000L;

    /// Forks one child JVM running [`ReaderCrashChildMain`] for one phase.
    ///
    /// The child is fully parameterized via `-D` properties so no shared
    /// classpath state leaks in: the working tree, the phase name (`phase1`/
    /// `phase2` — only used to label logs), the kill boundary to park on
    /// (`NONE` for the recovery phase), the Archive recording to replay, and
    /// every channel/directory override. Stale control files from an earlier
    /// same-tree run are deleted first so the parent can never observe a
    /// previous phase's `ready`/`milestone` by accident. Child stdout/stderr
    /// land in per-phase log files which the parent surfaces verbatim on
    /// failure (see `awaitFile`'s diagnostics).
    ///
    /// @param base          per-test working tree for store, cursor, and
    ///                      control files
    /// @param mode          phase label used in log file names
    /// @param point         kill boundary name, or `NONE` for plain recovery
    /// @param recordingId   Archive recording the child's reader replays
    /// @param controlChannel Archive control channel of the parent driver
    /// @param aeronDirectory shared driver directory
    /// @return the running child process
    /// @throws IOException when the process cannot be spawned
    private static Process launch(final Path base, final String mode, final String point, final long recordingId,
                                  final String controlChannel, final Path aeronDirectory) throws IOException {
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        Files.deleteIfExists(control.resolve("ready"));
        Files.deleteIfExists(control.resolve("outcome"));
        Files.deleteIfExists(control.resolve("milestone.reached"));
        Files.deleteIfExists(control.resolve("release"));
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.reader.base=%s".formatted(base),
                "-Ddg.reader.mode=%s".formatted(mode),
                "-Ddg.reader.barrier=%s".formatted(point),
                "-Ddg.reader.recordingId=%s".formatted(recordingId),
                "-Ddg.reader.controlChannel=%s".formatted(controlChannel),
                "-Ddg.reader.controlResponseChannel=%s".formatted(CONTROL_RESPONSE_CHANNEL),
                "-Ddg.reader.liveChannel=%s".formatted(LIVE_CHANNEL),
                "-Ddg.reader.replayChannel=%s".formatted(REPLAY_CHANNEL),
                "-Ddg.reader.aeronDirectory=%s".formatted(aeronDirectory),
                "-Ddg.reader.sharedDriver=true",
                ReaderCrashChildMain.class.getName())
                .redirectOutput(control.resolve("%s-stdout.log".formatted(mode)).toFile())
                .redirectError(control.resolve("%s-stderr.log".formatted(mode)).toFile())
                .start();
    }

    /// Builds the 64-byte fixture payload for one transaction sequence.
    /// Content is a repeated, sequence-labeled pattern so the reader-side
    /// fixture CRC can only pass when the *exact* bytes of that sequence were
    /// applied — a length- or truncation-collision can never pass by accident.
    private static byte[] payload(final int sequence) {
        final byte[] result = new byte[64];
        final byte[] value = ("reader-crash:%s".formatted(sequence)).getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < result.length; i++) result[i] = value[i % value.length];
        return result;
    }

    /// Polls the publisher until the Archive recording id is assigned.
    /// A recording id only exists after the Archive has registered the
    /// publication, which is the earliest moment a reader child can replay.
    private static long awaitRecordingId(final AeronArchiveReplicationPublisher publisher) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RECORDING_ID_TIMEOUT_MILLIS);
        while (System.nanoTime() < deadline) {
            final long recordingId = publisher.recordingId();
            if (recordingId >= 0) return recordingId;
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
        throw new AssertionError("recording id not available");
    }

    /// Waits for one child control file with a bounded deadline, failing fast
    /// with the child's logs attached if the child dies first.
    ///
    /// The loop checks liveness before existence on every iteration, so a
    /// crashed child surfaces as an immediate, diagnosable failure (with
    /// stdout/stderr inline) rather than a bare 30 s timeout that hides the
    /// root cause.
    private static void awaitFile(final Path path, final Process process) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CHILD_FILE_TIMEOUT_MILLIS);
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                final Path control = path.getParent();
                throw new AssertionError("child exited before %s\n%s\n%s\nclasspath=%s\nmodulepath=%s".formatted(path, readIfExists(control.resolve("phase1-stderr.log")), readIfExists(control.resolve("phase1-stdout.log")), System.getProperty("java.class.path"), System.getProperty("jdk.module.path")));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
        assertTrue(Files.exists(path), "timed out waiting for %s".formatted(path));
    }

    /// Reads a diagnostic log best-effort: missing files and read errors are
    /// rendered inline so error reports never themselves throw.
    private static String readIfExists(final Path path) {
        try {
            return Files.exists(path) ? Files.readString(path) : "<missing %s>".formatted(path);
        } catch (final IOException failure) {
            return failure.toString();
        }
    }

    /// Reserves one free loopback port for the Archive control channel.
    /// There is an inherent close-then-bind TOCTOU between this probe and the
    /// child binding the port; acceptable locally because the crash matrix
    /// runs cells sequentially.
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /// Removes a test working tree, aggregating (not short-circuiting on)
    /// deletion failures so one locked file in a dying child does not hide
    /// the rest. Only called in `finally` after both children were reaped.
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

        /// Kill between the uncertainty marker and the first Store import call.
    /// The marker is already on disk but zero bytes of the transaction were
    /// applied. Recovery cannot prove "nothing was applied" from a cursor that
    /// still names the pre-transaction position, so reseed-not-replay is the
    /// only safe outcome. `expectStoreRecord=false`: the fixture must be empty.
    @Test
    void replayBeforeImportLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("REPLAY_BEFORE_FIRST_IMPORT", false);
    }

        /// Kill while the Store import is mid-apply. Partial graph state may
    /// exist in memory; nothing guarantees atomicity of the write, so the
    /// marker must gate recovery. `expectStoreRecord=false`: at this boundary
    /// the fixture write has not happened yet.
    @Test
    void importBoundaryLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("DURING_STORE_IMPORT", false);
    }

        /// Same window as the previous cell, but the child injects a real
    /// ImportException from `receiveData` instead of a parent-side SIGKILL:
    /// exercises the *failure* path (not the *kill* path) through the same
    /// uncertainty-marker machinery — a reader that crashed and a reader whose
    /// import simply failed must look identical to the next recovery.
    @Test
    void importFailureLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("DURING_STORE_IMPORT_FAILURE", false);
    }

        /// Kill in the most dangerous window: the import returned success and
    /// the fixture carries the record, but the durable cursor still names the
    /// pre-transaction position. Recovery that trusted the cursor alone would
    /// replay the transaction a second time over an already-applied record, so
    /// the uncertainty marker must dominate. `expectStoreRecord=true`: after
    /// this boundary the record legitimately exists on disk.
    @Test
    void importBeforeCursorBoundaryLeavesStoreRecordAndRequiresReseed() throws Exception {
        this.assertReseed("AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE", true);
    }

        /// Kill during the cursor write itself (the AtomicFileWriter
    /// temp-write phase). The new cursor may exist as a torn temp file; the
    /// durable cursor on disk is still the old one. Marker dominates again.
    @Test
    void cursorWriteFailureLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("DURING_CURSOR_FILE_WRITE", true);
    }

    /// Kill after the new cursor's temp file was forced but before the atomic
    /// rename. The rename is the one instruction that makes the transaction
    /// durable; killed before it, the visible cursor never advanced, so the
    /// marker must govern.
    @Test
    void cursorRenameWindowLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("AFTER_CURSOR_TEMP_WRITE_BEFORE_RENAME", true);
    }

    /// Kill after rename but before the parent-directory fsync. The rename may
    /// not survive a machine crash — on restart either cursor may be visible
    /// — so the marker must fail closed rather than trust the possibly-lost
    /// rename. The final window past the sync is covered by
    /// [#postSyncPreResolveReturnRequiresReseedAndNeverDoubleApplies].
    @Test
    void cursorDirectorySyncWindowLeavesUncertainMarkerAndRequiresReseed() throws Exception {
        this.assertReseed("AFTER_CURSOR_RENAME_BEFORE_DIRECTORY_SYNC", true);
    }

    /// Kill after the cursor rename AND its directory sync, but before
    /// `transactionResolved` returns. The transaction is fully durable here;
    /// what survives is the uncertainty marker, because the reader deletes it
    /// only *after* the resolver callback returns. Recovery must therefore
    /// fail closed with `RESEED_REQUIRED` — it may NOT trust the advanced
    /// cursor and silently continue, and it must never double-apply the
    /// already-imported record. This cell pins the current, conservative
    /// production contract: a fully durable commit killed before marker
    /// clearance still demands a reseed rather than replay-with-dedupe.
    @Test
    void postSyncPreResolveReturnRequiresReseedAndNeverDoubleApplies() throws Exception {
        this.assertReseed("AFTER_CURSOR_DIRECTORY_SYNC_BEFORE_RETURN", true,
                new byte[][]{payload(0)});
    }

    /// Kill inside the AtomicFileWriter mid-write phase of the uncertainty
    /// marker itself. The rename never happened, so the visible marker file
    /// is absent and only a torn `reader.reader-inflight.tmp-*` sibling
    /// remains. Because the marker write always precedes the first import,
    /// no Store bytes can exist either, and pure Archive replay is the
    /// provably safe outcome: the fixture must contain both published
    /// transactions exactly once.
    @Test
    void tornInflightMarkerWriteReplaysFromArchiveExactlyOnce() throws Exception {
        final String point = "DURING_INFLIGHT_MARKER_WRITE";
        final List<byte[]> payloads = List.of(payload(0), payload(1));
        final Path base = Files.createTempDirectory("dg-reader-crash-");
        final int controlPort = freePort();
        final Path mediaDirectory = base.resolve("archive-aeron");
        final Path archiveDirectory = base.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
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
        try (ArchivingMediaDriver _ = ArchivingMediaDriver.launch(mediaContext, archiveContext);
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                     .aeronDirectoryName(mediaDirectory.toString())
                     .controlRequestChannel(controlChannel)
                     .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                     .messageTimeoutNs(configuration.offerTimeoutNanos()))) {
            try (final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
                    archive, LIVE_CHANNEL, 1001, configuration, CLUSTER_ID, EPOCH, 0)) {
                for (byte[] payload : payloads) {
                    RawArchivePublisher.publish(publisher, null, new ByteBuffer[]{ByteBuffer.wrap(payload)});
                }
                final long recordingId = awaitRecordingId(publisher);
                child = launch(base, "phase1", point, recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/ready"), child);
                final Path milestonePath = base.resolve("control/milestone.reached");
                awaitFile(milestonePath, child);
                assertEquals(point, ReaderMilestone.read(milestonePath).point());
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "reader child did not exit after kill");
                /* The kill landed before the marker's atomic rename: the
                 * visible marker never appeared, the torn temp sibling is the
                 * only trace, and no Store import could have started. */
                assertFalse(Files.exists(base.resolve("reader.reader-inflight")),
                        "a marker killed mid-write must never become visible");
                try (var entries = Files.list(base)) {
                    assertTrue(entries.anyMatch(p -> p.getFileName().toString().startsWith("reader.reader-inflight.tmp-")),
                            "a torn temp sibling must remain as crash evidence");
                }
                assertFalse(Files.exists(base.resolve("reader.store")),
                        "no import may run before the marker write completed");
                recovery = launch(base, "phase2", "NONE", recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/outcome"), recovery);
                assertTrue(recovery.waitFor(15, TimeUnit.SECONDS), "reader recovery child did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                assertTrue(outcome.lines().anyMatch(line -> line.equals("OUTCOME=REPLAY_FROM_ARCHIVE")),
                        "with neither marker nor fixture, replay is the safe outcome\n%s".formatted(outcome));
                final List<byte[]> records = readFixtureRecords(base.resolve("reader.store"));
                assertEquals(payloads.size(), records.size(), "replayed fixture must hold every transaction once");
                for (int index = 0; index < payloads.size(); index++) {
                    assertArrayEquals(payloads.get(index), records.get(index),
                            "replayed record %s does not match the published payload".formatted(index));
                }
            }
        } finally {
            if (child != null && child.isAlive()) child.destroyForcibly();
            if (recovery != null && recovery.isAlive()) recovery.destroyForcibly();
            deleteTree(base);
        }
    }

    /// A second crash lands inside the recovery's own marker-parse window:
    /// the recovery child reads a torn uncertainty marker and dies before it
    /// can publish a verdict. The subsequent recovery must still fail closed
    /// (`RESEED_REQUIRED`) over the same torn state — a dead recovery changes
    /// nothing on disk by construction.
    @Test
    void recoveryKilledWhileParsingTornMarkerStillRequiresReseed() throws Exception {
        final Path base = Files.createTempDirectory("dg-reader-crash-");
        final int controlPort = freePort();
        final Path mediaDirectory = base.resolve("archive-aeron");
        final Path archiveDirectory = base.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
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
        Process crashedRecovery = null;
        Process recovery = null;
        try (ArchivingMediaDriver _ = ArchivingMediaDriver.launch(mediaContext, archiveContext);
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
                crashAt(base, "phase1", "REPLAY_BEFORE_FIRST_IMPORT", recordingId, controlChannel, mediaDirectory);
                assertFalse(Files.exists(base.resolve("reader.store")),
                        "no import may have run before the first crash");
                /* Tear the surviving marker into a parse-defeating fragment. */
                final Path uncertainty = base.resolve("reader.reader-inflight");
                final byte[] intact = Files.readAllBytes(uncertainty);
                Files.write(uncertainty, java.util.Arrays.copyOf(intact, intact.length / 2));
                assertThrows(IOException.class, () -> AeronReplicationCheckpointStore.read(uncertainty),
                        "the torn marker must defeat parsing");
                /* The first recovery child reads the torn marker and crashes
                 * itself inside the parse window: milestone observed, no
                 * outcome may ever be published. */
                crashedRecovery = launch(base, "phase2", "DURING_RECOVERY_CURSOR_PARSE",
                        recordingId, controlChannel, mediaDirectory);
                final Path milestonePath = base.resolve("control/milestone.reached");
                awaitFile(milestonePath, crashedRecovery);
                assertEquals("DURING_RECOVERY_CURSOR_PARSE", ReaderMilestone.read(milestonePath).point());
                assertTrue(crashedRecovery.waitFor(15, TimeUnit.SECONDS),
                        "the crashed recovery must terminate itself");
                assertNotEquals(0, crashedRecovery.exitValue(),
                        "the crashed recovery must die like a process kill");
                crashedRecovery = null;
                assertFalse(Files.exists(base.resolve("control/outcome")),
                        "a recovery killed mid-parse must not publish a verdict");
                /* The next recovery over unchanged state must fail closed. */
                recovery = launch(base, "phase2", "NONE", recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/outcome"), recovery);
                assertTrue(recovery.waitFor(15, TimeUnit.SECONDS), "reader recovery child did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                assertTrue(outcome.lines().anyMatch(line -> line.equals("OUTCOME=RESEED_REQUIRED")),
                        "the torn marker must still force a reseed\n%s".formatted(outcome));
            }
        } finally {
            if (crashedRecovery != null && crashedRecovery.isAlive()) crashedRecovery.destroyForcibly();
            if (recovery != null && recovery.isAlive()) recovery.destroyForcibly();
            deleteTree(base);
        }
    }

    /// N=4 crash loop with mixed barriers over one writer epoch: the reader
    /// process is killed at a write-side window, the first recovery is killed
    /// right after it validated the durable state, and the second recovery
    /// crashes itself inside its own marker-parse window. The final recovery
    /// must still reach the documented safe terminal outcome
    /// (`RESEED_REQUIRED`) with the fixture exactly as of the first crash and
    /// the surviving marker pinning the uncertain sequence.
    ///
    /// Note on the barriers used: a production reader's uncertainty marker
    /// gates the recovery entry point, so a recovery over marker-present
    /// state can only ever crash in its *read* window — a recovery can never
    /// again reach a cursor-write window while uncertainty is unresolved.
    /// The loop therefore mixes the only reachable combination: one
    /// write-side window followed by two read-side recovery windows.
    @Test
    void repeatedCrashLoopAlwaysTerminatesInFailClosedRecovery() throws Exception {
        final Path base = Files.createTempDirectory("dg-reader-crash-");
        final int controlPort = freePort();
        final Path mediaDirectory = base.resolve("archive-aeron");
        final Path archiveDirectory = base.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
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
        Process recovery = null;
        try (ArchivingMediaDriver _ = ArchivingMediaDriver.launch(mediaContext, archiveContext);
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
                /* Crash 1: write-side window, import applied, cursor not yet
                 * durable. The surviving marker names sequence 0. */
                final ReaderMilestone first = crashAt(base, "phase1", "DURING_CURSOR_FILE_WRITE",
                        recordingId, controlChannel, mediaDirectory);
                /* Crash 2: the recovery parsed the marker, validated the
                 * durable state, and was killed before publishing a verdict. */
                final ReaderMilestone second = crashAt(base, "phase2", "AFTER_RECOVERY_CURSOR_VALIDATED",
                        recordingId, controlChannel, mediaDirectory);
                assertEquals(first.sequence(), second.sequence(),
                        "both crashes must pin the same uncertain transaction");
                /* Crash 3: the next recovery died inside its own marker-parse
                 * window, again without publishing a verdict. */
                crashAt(base, "phase2", "DURING_RECOVERY_CURSOR_PARSE",
                        recordingId, controlChannel, mediaDirectory);
                /* The final recovery over unchanged state must fail closed. */
                recovery = launch(base, "phase2", "NONE", recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/outcome"), recovery);
                assertTrue(recovery.waitFor(15, TimeUnit.SECONDS), "reader recovery child did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                assertTrue(outcome.lines().anyMatch(line -> line.equals("OUTCOME=RESEED_REQUIRED")),
                        "three crashes must still terminate in a fail-closed recovery\n%s".formatted(outcome));
                /* The on-disk evidence must reflect the first crash exactly:
                 * the import record applied once, the marker pinning the first
                 * uncertain transaction, and no cursor advancement. */
                final List<byte[]> records = readFixtureRecords(base.resolve("reader.store"));
                assertEquals(1, records.size(), "only the first imported record may exist");
                assertArrayEquals(payload(0), records.getFirst(), "the surviving fixture record is torn or mutated");
                final AeronReplicationCheckpoint checkpoint =
                        AeronReplicationCheckpointStore.read(base.resolve("reader.reader-inflight"));
                assertEquals(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, checkpoint.state());
                assertEquals(first.sequence(), checkpoint.transactionSequence());
                assertEquals(first.position(), checkpoint.recordingPosition());
                assertFalse(Files.exists(base.resolve("reader.cursor")),
                        "no durable cursor may exist behind the uncertainty marker");
            }
        } finally {
            if (recovery != null && recovery.isAlive()) recovery.destroyForcibly();
            deleteTree(base);
        }
    }

    /// One crash-loop step: launches the child at `point`, waits for its
    /// milestone, kills it with SIGKILL semantics, and proves no verdict file
    /// was left behind. A self-halting child (the recovery parse window) is
    /// observed identically — `destroyForcibly` on an exiting process is a
    /// no-op race. Returns the milestone so callers can correlate the pinned
    /// transaction across crashes.
    private static ReaderMilestone crashAt(final Path base, final String mode, final String point,
                                           final long recordingId, final String controlChannel,
                                           final Path mediaDirectory) throws IOException, InterruptedException {
        final Path milestonePath = base.resolve("control/milestone.reached");
        final Process process = launch(base, mode, point, recordingId, controlChannel, mediaDirectory);
        try {
            awaitFile(milestonePath, process);
            final ReaderMilestone milestone = ReaderMilestone.read(milestonePath);
            assertEquals(point, milestone.point(), "unexpected crash milestone");
            process.destroyForcibly();
            assertTrue(process.waitFor(15, TimeUnit.SECONDS), "crashed child did not terminate");
            assertTrue(process.exitValue() != 0, "a crashed child must exit abnormally");
            assertFalse(Files.exists(base.resolve("control/outcome")),
                    "a child killed at %s must not publish a verdict".formatted(point));
            return milestone;
        } finally {
            if (process.isAlive()) process.destroyForcibly();
        }
    }


        /// Same boundary as the cursor-write cell, but after the kill the
    /// parent flips one byte in the middle of the uncertainty marker itself.
    /// Recovery reads a CRC-invalid marker and must still fail closed
    /// (RESEED_REQUIRED or FAIL_CLOSED), never treat a corrupt marker as the
    /// absence of one and happily replay.
    @Test
    void corruptedUncertaintyMarkerStillRequiresReseed() throws Exception {
        this.assertReseedAfterInflightCorruption();
    }

        /// Kill after the recovered cursor was validated but before any
    /// transaction was imported: no marker, no fixture record, and the
    /// durable cursor is trustworthy. This is the only cell where pure
    /// Archive replay is the correct answer — recovery must report
    /// `REPLAY_FROM_ARCHIVE`, reach the live tail, and the fixture must hold
    /// both published transactions exactly once.
    @Test
    void killedBeforeAnyImportRecoversByArchiveReplay() throws Exception {
        this.assertReplayOutcome("AFTER_RECOVERY_CURSOR_VALIDATED",
                List.of(payload(0), payload(1)));
    }

        /// Kill with a multi-chunk transaction still being assembled in the
    /// reader's native staging buffers (two of three chunks present — the
    /// 40 KiB payload over a 16 KiB chunk size guarantees it). No import ran,
    /// so this is still a replay cell, but the assertion that matters is
    /// content integrity: the fixture must contain the complete 40 KiB record
    /// byte-for-byte exactly once, never the partial 32 KiB prefix the dead
    /// child was holding.
    @Test
    void killedMidChunkAssemblyNeverAppliesThePartialTransaction() throws Exception {
        /* A payload larger than the 16 KiB chunk size spans three chunks, so
         * the assembly barrier parks with two chunks still missing. */
        this.assertReplayOutcome("DURING_CHUNK_ASSEMBLY",
                List.of(payload(0), multiChunkPayload()));
    }

    /// A 40 KiB deterministic payload: against the 16 KiB chunk size it spans
    /// three chunks, so the `DURING_CHUNK_ASSEMBLY` barrier parks the child
    /// while genuinely mid-transaction. The `i % 251` pattern makes every byte
    /// position-unique inside its period, so a truncated or offset-shifted
    /// record always fails the fixture compare — never silently "close enough".
    private static byte[] multiChunkPayload() {
        final byte[] payload = new byte[40_000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        return payload;
    }

        /// Runs one replay-cell: phase1 parks at `point` before any import,
    /// the parent kills it, and phase2 must recover by pure Archive replay.
    ///
    /// Per-cell assertions, beyond the shared milestone/kill choreography:
    /// no uncertainty marker exists after the kill (no import ever ran), the
    /// recovery outcome is exactly `REPLAY_FROM_ARCHIVE` with live health, and
    /// the Store fixture equals `payloads` element-wise — count and bytes —
    /// which simultaneously proves idempotent-once semantics and the absence
    /// of torn multi-chunk records.
    ///
    /// @param point    kill boundary the phase1 child parks on
    /// @param payloads the transactions published into the recording, in order
    private void assertReplayOutcome(final String point, final List<byte[]> payloads)
            throws IOException, InterruptedException {
        final Path base = Files.createTempDirectory("dg-reader-crash-");
        final int controlPort = freePort();
        final Path mediaDirectory = base.resolve("archive-aeron");
        final Path archiveDirectory = base.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
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
        try (ArchivingMediaDriver _ = ArchivingMediaDriver.launch(mediaContext, archiveContext);
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                     .aeronDirectoryName(mediaDirectory.toString())
                     .controlRequestChannel(controlChannel)
                     .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                     .messageTimeoutNs(configuration.offerTimeoutNanos()))) {
            try (final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
                    archive, LIVE_CHANNEL, 1001, configuration, CLUSTER_ID, EPOCH, 0)) {
                for (byte[] payload : payloads) {
                    RawArchivePublisher.publish(publisher, null, new ByteBuffer[]{ByteBuffer.wrap(payload)});
                }
                final long recordingId = awaitRecordingId(publisher);
                child = launch(base, "phase1", point, recordingId, controlChannel, mediaDirectory);
                /* Await the milestone, never `ready`: the
                 * AFTER_RECOVERY_CURSOR_VALIDATED barrier parks the child's
                 * main thread before the reader is started, so `ready` may
                 * legitimately never be written for these cells. */
                final Path milestonePath = base.resolve("control/milestone.reached");
                awaitFile(milestonePath, child);
                final ReaderMilestone milestone = ReaderMilestone.read(milestonePath);
                assertEquals(point, milestone.point(), "unexpected reader milestone %s".formatted(milestone));
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "reader child did not exit after kill");
                /* No import ever started, so no uncertainty marker and no
                 * partial Store record may exist after the kill. */
                assertFalse(Files.exists(base.resolve("reader.reader-inflight")),
                        "no import ran, so no uncertainty marker may exist");
                recovery = launch(base, "phase2", "NONE", recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/outcome"), recovery);
                assertTrue(recovery.waitFor(15, TimeUnit.SECONDS), "reader recovery child did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                assertTrue(outcome.lines().anyMatch(line -> line.equals("OUTCOME=REPLAY_FROM_ARCHIVE")),
                        "recovery must report a genuine archive replay, was:\n%s".formatted(outcome));
                assertTrue(outcome.lines().anyMatch(line -> line.equals("HEALTH=LIVE")),
                        "a completed replay must report live health, was:\n%s".formatted(outcome));
                /* The replayed Store fixture must hold every published
                 * transaction exactly once — never a torn partial record. */
                final List<byte[]> records = readFixtureRecords(base.resolve("reader.store"));
                assertEquals(payloads.size(), records.size(),
                        "replay must apply exactly the published transactions, fixture=%s".formatted(records.size()));
                for (int index = 0; index < payloads.size(); index++) {
                    assertArrayEquals(payloads.get(index), records.get(index), "replayed record %s does not match the published payload".formatted(index));
                }
            }
        } finally {
            if (child != null && child.isAlive()) child.destroyForcibly();
            if (recovery != null && recovery.isAlive()) recovery.destroyForcibly();
            deleteTree(base);
        }
    }

        /// Decodes the child's Store fixture (a length+CRC32C append log written
    /// by the fixture's receiver) into its records, asserting structural
    /// integrity as it goes: a torn header, torn body, or checksum mismatch is
    /// a hard failure here, not a silent decode artifact. This makes the
    /// fixture a trustworthy oracle for "applied exactly once, complete".
    private static List<byte[]> readFixtureRecords(final Path store) throws IOException {
        if (!Files.exists(store)) return List.of();
        final byte[] bytes = Files.readAllBytes(store);
        final List<byte[]> records = new ArrayList<>();
        int offset = 0;
        while (offset < bytes.length) {
            assertTrue(bytes.length - offset >= Integer.BYTES * 2, "torn fixture header");
            final int length = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
            final int crc = ByteBuffer.wrap(bytes, offset + Integer.BYTES, Integer.BYTES).getInt();
            assertTrue(length >= 0 && bytes.length - offset - Integer.BYTES * 2 >= length, "torn fixture record");
            final byte[] record = java.util.Arrays.copyOfRange(bytes, offset + Integer.BYTES * 2,
                    offset + Integer.BYTES * 2 + length);
            final CRC32C checksum = new CRC32C();
            checksum.update(record);
            assertEquals(crc, (int) checksum.getValue(), "fixture record failed its checksum");
            records.add(record);
            offset += Integer.BYTES * 2 + length;
        }
        return records;
    }

        /// Kill at the import-durable/cursor-not-yet boundary, then delete the
    /// durable cursor entirely. A marker-naive recovery would see "no cursor"
    /// and replay from position 0 over already-imported state; the surviving
    /// uncertainty marker must override that and demand a reseed instead.
    @Test
    void deletedCursorStillRequiresReseed() throws Exception {
        this.assertReseedAfterCursorRollback();
    }

    /// Mutation-backed cell: flip one byte in the middle of the persisted
    /// uncertainty marker — the strongest "corruption is not a clean state"
    /// probe in this suite, since the marker's own CRC must reject the
    /// tampered file.
    private void assertReseedAfterInflightCorruption()
            throws IOException, InterruptedException {
        this.assertReseedWithMutation(base -> {
            final Path uncertainty = base.resolve("reader.reader-inflight");
            assertTrue(Files.exists(uncertainty), "expected an uncertainty marker to corrupt");
            final byte[] bytes = Files.readAllBytes(uncertainty);
            assertTrue(bytes.length > 0, "uncertainty marker is empty");
            bytes[bytes.length / 2] ^= 0x01;
            Files.write(uncertainty, bytes);
        });
    }

    /// Mutation-backed cell: delete the durable cursor, rolling visible state
    /// behind the import boundary. Equivalent behavior is expected for any
    /// form of cursor rollback, not only deletion.
    private void assertReseedAfterCursorRollback()
            throws IOException, InterruptedException {
        this.assertReseedWithMutation(base -> {
            /* Roll the durable cursor behind the already-imported Store: the
             * next replay starts before the import boundary and must refuse
             * to re-apply it instead of continuing silently. */
            final Path cursor = base.resolve("reader.cursor");
            if (Files.exists(cursor)) Files.delete(cursor);
        });
    }

    /// Runs one mutation cell: phase1 parks at
    /// `AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE` (import applied, cursor not
    /// yet durable), the parent kills it, applies `mutation` to the on-disk
    /// remains, and phase2 must fail closed.
    ///
    /// The outcome assertion is deliberately two-valued: both
    /// `RESEED_REQUIRED` and `FAIL_CLOSED` are safe terminal answers to
    /// corrupted reader state; what is forbidden is silence. The uncertainty
    /// marker must survive the whole sequence, and the fixture must still be
    /// present as evidence.
    ///
    /// @param mutation post-kill mutation of the reader's on-disk state
    private void assertReseedWithMutation(final FixtureMutation mutation)
            throws IOException, InterruptedException {
        final String point = "AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE";
        final Path base = Files.createTempDirectory("dg-reader-crash-");
        final int controlPort = freePort();
        final Path mediaDirectory = base.resolve("archive-aeron");
        final Path archiveDirectory = base.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
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
        try (ArchivingMediaDriver _ = ArchivingMediaDriver.launch(mediaContext, archiveContext);
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
                assertEquals(point, milestone.point(), "unexpected reader milestone %s".formatted(milestone));
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "reader child did not exit after kill");
                mutation.apply(base);
                recovery = launch(base, "phase2", "NONE", recordingId, controlChannel, mediaDirectory);
                awaitFile(base.resolve("control/outcome"), recovery);
                assertTrue(recovery.waitFor(15, TimeUnit.SECONDS), "reader recovery child did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                /* Both fail-closed shapes are safe: an explicit reseed demand
                 * or a terminal failure. Silent continuation is the defect. */
                assertTrue(outcome.lines().anyMatch(line -> line.equals("OUTCOME=RESEED_REQUIRED"))
                                || outcome.lines().anyMatch(line -> line.equals("OUTCOME=FAIL_CLOSED")),
                        "corrupted reader state continued silently\n%s".formatted(outcome));
                assertTrue(Files.exists(base.resolve("reader.reader-inflight")),
                        "uncertainty marker must survive the crash");
                assertTrue(Files.exists(base.resolve("reader.store")),
                        "unexpected Store fixture state after mutation");
            }
        } finally {
            if (child != null && child.isAlive()) child.destroyForcibly();
            if (recovery != null && recovery.isAlive()) recovery.destroyForcibly();
            deleteTree(base);
        }
    }

        /// One post-kill mutation applied to the reader's on-disk state before
    /// recovery. Kept as an interface (rather than inline lambdas in the two
    /// callers) so mutation cells share the full kill/relaunch/assert
    /// choreography exactly once.
    @FunctionalInterface
    private interface FixtureMutation {
        void apply(Path base) throws IOException;
    }

    /// Runs one plain kill cell: phase1 parks at `point`, the parent SIGKILLs
    /// it, phase2 must report `RESEED_REQUIRED`. Beyond the outcome word, this
    /// is where the *semantic* invariants of the marker protocol are pinned:
    /// the surviving marker must decode as `COMMITTING_UNCERTAIN`, its
    /// recorded sequence/position must equal the killed transaction's
    /// milestone (so the marker names the exact transaction in doubt), and
    /// the store-fixture presence must match the boundary (`false` before any
    /// import finished, `true` once the import completed).
    ///
    /// @param point             kill boundary the phase1 child parks on
    /// @param expectStoreRecord whether the import's fixture record may be
    ///                          present at this boundary
    private void assertReseed(final String point, final boolean expectStoreRecord)
            throws IOException, InterruptedException {
        this.assertReseed(point, expectStoreRecord, null);
    }

    /// Like [#assertReseed(String, boolean)], and additionally asserts the
    /// exact fixture contents when `expectedFixture` is non-`null` — the
    /// strongest proof that recovery neither lost nor double-applied records.
    ///
    /// @param expectedFixture records the Store fixture must hold, in order,
    ///                        or `null` to skip the content assertion
    private void assertReseed(final String point, final boolean expectStoreRecord,
                              final byte[][] expectedFixture)
            throws IOException, InterruptedException {
        final Path base = Files.createTempDirectory("dg-reader-crash-");
        final int controlPort = freePort();
        final Path mediaDirectory = base.resolve("archive-aeron");
        final Path archiveDirectory = base.resolve("archive");
        final String controlChannel = "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
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
        try (ArchivingMediaDriver _ = ArchivingMediaDriver.launch(mediaContext, archiveContext);
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
                assertEquals(point, milestone.point(), "unexpected reader milestone %s".formatted(milestone));
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
                assertEquals(Files.exists(base.resolve("reader.store")), expectStoreRecord, "unexpected Store fixture state for %s".formatted(point));
                if (expectedFixture != null) {
                    final List<byte[]> records = readFixtureRecords(base.resolve("reader.store"));
                    assertEquals(expectedFixture.length, records.size(),
                            "fixture record count drifted for %s".formatted(point));
                    for (int index = 0; index < expectedFixture.length; index++) {
                        assertArrayEquals(expectedFixture[index], records.get(index),
                                "fixture record %s is not byte-identical to the published payload".formatted(index));
                    }
                }
            }
        } finally {
            if (child != null && child.isAlive())
                child.destroyForcibly();
            if (recovery != null && recovery.isAlive())
                recovery.destroyForcibly();

            deleteTree(base);
        }
    }

}
