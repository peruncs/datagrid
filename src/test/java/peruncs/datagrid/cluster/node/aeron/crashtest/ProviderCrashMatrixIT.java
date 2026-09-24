package peruncs.datagrid.cluster.node.aeron.crashtest;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.datagrid.cluster.storage.aeron.crashtest.ArchiveArtifactMutator;
import peruncs.datagrid.cluster.storage.aeron.crashtest.ArchiveArtifactMutator.HeaderField;
import peruncs.datagrid.cluster.storage.aeron.crashtest.ArchiveArtifactMutator.Mutation;
import peruncs.datagrid.cluster.storage.aeron.crashtest.CrashPayloads;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.test.ChildJava;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// Child-process crash matrix for the writer, embedded Archive, checkpoint,
/// and recovery boundaries. The parent kills a forked child only after the
/// requested milestone is written to the control file, so each cell proves
/// the crash happened at the intended boundary rather than timing out during
/// setup.
///
/// A normal provider cell uses this protocol:
///
/// 1. Create an isolated directory layout with fresh Aeron live/control ports.
/// 2. Launch [ProviderCrashChildMain] in phase 1. It writes the requested
///    transaction payloads and emits `control/ready` followed by the exact
///    `control/milestone.reached` marker.
/// 3. Validate the phase-1 Store prefix and kill the child. The expected prefix
///    is derived from the crash point, so a cell cannot pass merely because the
///    process died somewhere in the setup path.
/// 4. Relaunch the same directory in phase 2, retrying only transient
///    "active driver" startup failures. Parse `control/outcome` and require
///    the declared safe policy, health, checkpoint identity, CRC, and exact
///    Store contents.
///
/// `CONTINUE` means the checkpoint and Archive boundary are unambiguous: the
/// node must be `LIVE` and replay must append the missing transaction exactly
/// once. `RESEED_REQUIRED` means recovery cannot prove the boundary: the node
/// must fail closed, report an error, and never silently apply the ambiguous
/// tail. Harness errors, missing milestones, invalid child outcomes, mutated
/// Store prefixes, and unexpected recovery policies are test failures.
///
/// The deterministic cells cover publication, prepare/local-write/commit
/// seams, archive-first ordering, prepare rejection,
/// backpressure with no subscriber, checkpoint temp-write/rename/directory-sync
/// seams, payload sizes from one byte through 200 KiB, chunk boundaries,
/// checkpoint and Archive-tail corruption, reader-side corruption, deleted
/// cursors, and double/triple recovery crashes. The seeded budget and process
/// kill cells sample those same decision points between named milestones.
///
/// Each cell appends selection, milestone, mutation, and outcome records to
/// `control/events.jsonl`. On failure [DiagnosticCollector] preserves the
/// control files and child logs, plus a reason and directory listing covering
/// the Store, checkpoint, and Archive fixture. The matrix tests fail closed;
/// they do not treat a crash before the requested milestone as a valid result.
class ProviderCrashMatrixIT {
    /// Transient startup race the phase-2 recovery may legitimately retry:
    /// the SIGKILLed phase-1 child left its embedded driver's mark file behind
    /// and Aeron may refuse startup while a driver or Archive mark is active.
    /// The Archive's MarkFile currently throws a plain IllegalStateException,
    /// so that case also requires its exact message and archive-mark filename.
    /// Unrelated IllegalStateExceptions remain real failures.
    static boolean isActiveDriverRetry(final String outcome) {
        final CrashOutcome parsed;
        try {
            parsed = CrashOutcome.parse(outcome);
        } catch (final RuntimeException unparseable) {
            return false;
        }
        if (parsed.errorTypes().stream().anyMatch(RETRYABLE_STARTUP_TYPES::contains)) return true;
        return parsed.policy() == RecoveryPolicy.FAIL_CLOSED && parsed.storeValid() &&
               parsed.errorTypes().equals(java.util.List.of(IllegalStateException.class.getName())) &&
               parsed.error() != null &&
               parsed.error().replace('\\', '/').matches(
                       "active mark file detected: .*/archive/archive-mark\\.dat");
    }

    /// Exception types considered evidence of the stale active-driver race.
    private static final java.util.Set<String> RETRYABLE_STARTUP_TYPES = java.util.Set.of(
            io.aeron.driver.exceptions.ActiveDriverException.class.getName(),
            io.aeron.exceptions.DriverTimeoutException.class.getName());

    @Test
    void activeArchiveMarkRetryDoesNotHideOtherIllegalStateFailures() {
        final String base = "HEALTH=FAILED\nOUTCOME=FAIL_CLOSED\nPROOF_STORE_VALID=true\n" +
                "ERROR_TYPE=java.lang.IllegalStateException\nERROR=";
        assertTrue(isActiveDriverRetry(base +
                "active mark file detected: /tmp/archive/archive-mark.dat\n"));
        assertFalse(isActiveDriverRetry(base + "checkpoint is invalid\n"));
        assertFalse(isActiveDriverRetry(base +
                "active mark file detected: /tmp/unrelated/archive-mark.dat\n"));
    }

    private static void assertNotHarnessError(final CrashOutcome outcome, final String raw) {
        if (outcome.policy() == RecoveryPolicy.HARNESS_ERROR) {
            throw new AssertionError("crash barrier was not reached; verify hook installation and point mapping\n%s".formatted(raw));
        }
    }

    private static String diagnostics(final Path control) {
        try {
            final StringBuilder result = new StringBuilder();
            if (Files.exists(control)) {
                try (var paths = Files.list(control)) {
                    paths.sorted().forEach(path ->
                    {
                        try {
                            result.append(path.getFileName()).append('=').append(Files.readString(path)).append('\n');
                        } catch (final IOException failure) {
                            result.append(path).append('=').append(failure).append('\n');
                        }
                    });
                }
            }
            return result.toString();
        } catch (final IOException failure) {
            return failure.toString();
        }
    }

    private static byte[] payload(final int sequence) {
        return CrashPayloads.sized(sequence, CrashPayloads.DEFAULT_SIZE, CrashPayloads.DEFAULT_KIND);
    }

    /// Mirrors the forked child's generator byte-for-byte: the parent and the
    /// child agree on expected Store contents without sharing heap state.
    private static byte[] payload(final int sequence, final int size, final String kind) {
        return CrashPayloads.sized(sequence, size, kind);
    }

    private static long budget(final String property, final long fallback) {
        final String value = System.getProperty(property);
        if (value == null || value.isBlank()) return fallback;
        try {
            return Math.max(1L, Long.parseLong(value));
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException("invalid crash budget %s%s%s".formatted(property, '=', value), failure);
        }
    }

    private static int crc(final byte[] payload) {
        final java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }

    private static long fileSize(final Path path) {
        try {
            return Files.exists(path) ? Files.size(path) : 0L;
        } catch (final IOException failure) {
            throw new AssertionError("cannot inspect Store fixture %s".formatted(path), failure);
        }
    }

        /// Verifies prepared data tail requires reseed.
    @Test
    void preparedDataTailRequiresReseed() throws Exception {
        this.assertReseed("AFTER_DATA_CHUNKS", false);
    }

        /// Verifies dictionary data tail requires reseed.
    @Test
    void dictionaryDataTailRequiresReseed() throws Exception {
        this.assertReseed("AFTER_DICTIONARY_CHUNKS", false);
    }

        /// Verifies prepared before local write requires reseed.
    @Test
    void preparedBeforeLocalWriteRequiresReseed() throws Exception {
        this.assertReseed("AFTER_PREPARE_BEFORE_LOCAL_WRITE", false);
    }

        /// Verifies the pre-publication fence refuses an ambiguous restart.
    @Test
    void crashBeforePrepareRequiresReseed() throws Exception {
        this.assertOutcome("BEFORE_PREPARE", false, false, "RESEED_REQUIRED");
    }

        /// Verifies a crash before the first publication connection leaves no writer state.
    @Test
    void beforePublicationConnectionLeavesNoWriterState() throws Exception {
        this.assertOutcome("BEFORE_PUBLICATION_CONNECTED", false, false, "CONTINUE");
    }

        /// Verifies local rejection after abort offer requires reseed.
    @Test
    void localRejectionAfterAbortOfferRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_ABORT_OFFERED", false, true,
                "RESEED_REQUIRED");
    }

        /// Verifies archive recorded before checkpoint requires reseed.
    @Test
    void archiveRecordedBeforeCheckpointRequiresReseed() throws Exception {
        this.assertReseed("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", false);
    }

        /// Verifies ambiguous commit offer requires reseed.
    @Test
    void ambiguousCommitOfferRequiresReseed() throws Exception {
        this.assertReseed("AFTER_COMMIT_OFFER", false);
    }

        /// Verifies a recorded commit without a coordinator return requires reseed.
    @Test
    void recordedCommitBeforeCoordinatorReturnRequiresReseed() throws Exception {
        this.assertReseed("AFTER_COMMIT_RECORDED", false);
    }

        /// Verifies the commit-offer boundary fails closed before recording.
    @Test
    void beforeCommitOfferRequiresReseed() throws Exception {
        this.assertReseed("BEFORE_COMMIT_OFFER", false);
    }

        /// Verifies a prepared transaction that never reaches local Store write requires reseed.
    @Test
    void afterPrepareRequiresReseed() throws Exception {
        this.assertReseed("AFTER_PREPARE", false);
    }

        /// Verifies local write ahead fence requires reseed.
    @Test
    void localWriteAheadFenceRequiresReseed() throws Exception {
        this.assertReseed("AFTER_LOCAL_WRITE_BEFORE_COMMIT", false);
    }

        /// Verifies a kill before the next journal slot is written preserves the prior boundary.
    @Test
    void checkpointBeforeJournalSlotRequiresReseed() throws Exception {
        this.assertReseed("BEFORE_JOURNAL_SLOT_WRITE", false);
    }

        /// Verifies a partial inactive journal slot cannot advance the durable boundary.
    @Test
    void checkpointDuringJournalSlotRequiresReseed() throws Exception {
        this.assertReseed("DURING_JOURNAL_SLOT_WRITE", false);
    }

        /// Verifies a forced journal slot remains restartable before in-memory publication.
    @Test
    void checkpointAfterJournalForceContinues() throws Exception {
        this.assertOutcome("AFTER_JOURNAL_SLOT_FORCE",
                false, false, "CONTINUE");
    }

        /// Verifies a durable checkpoint remains restartable before in-memory sequence publication.
    @Test
    void checkpointBeforeSequenceUpdateContinues() throws Exception {
        this.assertOutcome("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
                false, false, "CONTINUE");
    }

        /// Verifies that an orphaned first transaction is never silently reused.
    @Test
    void firstTransactionOrphanRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_DATA_CHUNKS", false, false, "RESEED_REQUIRED", null, 1, 0);
    }

        /// Verifies a first-transaction orphan under publication backpressure
    /// (no live subscriber) still fails closed instead of reusing the orphan.
    @Test
    void writerCrashDuringBackpressureFirstTxRequiresReseed() throws Exception {
        final String previous = System.getProperty("crash.matrix.subscriber");
        System.setProperty("crash.matrix.subscriber", "false");
        try {
            this.assertOutcome("AFTER_DATA_CHUNKS", false, false, "RESEED_REQUIRED", "backpressure-first-tx", 1, 0);
        } finally {
            if (previous == null) System.clearProperty("crash.matrix.subscriber");
            else System.setProperty("crash.matrix.subscriber", previous);
        }
    }

        /// Verifies a minimum-size payload still fails closed on a recorded
    /// commit that never reached the coordinator.
    @Test
    void minPayloadRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", false, false, "RESEED_REQUIRED", "payload=1", 2, 1, 1, "digest", 0);
    }

        /// Verifies a chunk-size-minus-one payload fails closed like any other tail.
    @Test
    void chunkMinusOneRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", false, false, "RESEED_REQUIRED", "payload=16383", 2, 1, 16383, "digest", 0);
    }

        /// Verifies a chunk-size-plus-one payload (two chunks) fails closed.
    @Test
    void chunkPlusOneRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", false, false, "RESEED_REQUIRED", "payload=16385", 2, 1, 16385, "digest", 0);
    }

        /// Verifies a multi-chunk payload fails closed on the recorded tail.
    @Test
    void multiChunkRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", false, false, "RESEED_REQUIRED", "payload=65536", 2, 1, 65536, "digest", 0);
    }

        /// Verifies incompressible bytes fail closed exactly like digests.
    @Test
    void randomBytesRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", false, false, "RESEED_REQUIRED", "payload=random", 2, 1, 4096, "random", 0);
    }

        /// Verifies a large multi-chunk transaction recovers end to end: no
    /// barrier fires, both phases continue, and the Store holds every byte.
    @Test
    void largePayloadFullRecoveryContinues() throws Exception {
        final int size = 200_000;
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            final int livePort = layout.livePort();
            final int controlPort = layout.controlPort();
            CrashEventLog.append(base.resolve("control"), "selection",
                    "large-recovery payload=%d".formatted(size));
            Process child = null;
            try {
                child = this.launch(base, "phase1", "NONE", false, false, livePort, controlPort, 2, 1, size, "digest", 0);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                this.await(base.resolve("control/outcome"), child, budget("crash.budget.startup", 120_000L));
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit");
                final CrashOutcome first = CrashOutcome.parse(
                        Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8));
                assertEquals(RecoveryPolicy.CONTINUE, first.policy(), "large payload phase1 must continue");
                StoreFixture.assertRecords(base.resolve("store.records"),
                        List.of(payload(0, size, "digest"), payload(1, size, "digest")));
                String outcome;
                final long restartDeadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(budget("crash.budget.archiveStop", 30_000L));
                int restartAttempts = 0;
                do {
                    restartAttempts++;
                    child = this.launch(base, "phase2", "NONE", false, false, livePort, controlPort, 2, 1, size, "digest", 0);
                    this.await(base.resolve("control/outcome"), child, budget("crash.budget.startup", 120_000L));
                    assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase2 child did not exit");
                    outcome = Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8);
                    if (!isActiveDriverRetry(outcome)) break;
                    Thread.sleep(1_000L);
                }
                while (System.nanoTime() < restartDeadline);
                assertFalse(isActiveDriverRetry(outcome),
                        "recording never became stopped; attempts=%s outcome=%s".formatted(restartAttempts, outcome));
                final CrashOutcome result = CrashOutcome.parse(outcome);
                assertEquals(RecoveryPolicy.CONTINUE, result.policy(), outcome);
                assertEquals("LIVE", result.health(), outcome);
                StoreFixture.assertRecords(base.resolve("store.records"),
                        List.of(payload(0, size, "digest"), payload(1, size, "digest"), payload(2, size, "digest")));
                CrashEventLog.append(base.resolve("control"), "outcome", "large-recovery CONTINUE");
            } catch (final Throwable failure) {
                if (child != null && child.isAlive()) child.destroyForcibly();
                try {
                    final Path evidence = DiagnosticCollector.collect(base, failure.toString());
                    throw new AssertionError("crash cell evidence: %s".formatted(evidence), failure);
                } catch (final Throwable evidenceFailure) {
                    failure.addSuppressed(evidenceFailure);
                    throw failure;
                }
            }
        }
    }

    /// Verifies a recorded commit crossing a deliberately tiny term
    /// boundary still fails closed after a writer crash.
    @Test
    void recordedCommitAcrossTinyTermBoundaryRequiresReseed() throws Exception {
        final String previous = System.getProperty("crash.matrix.termLength");
        System.setProperty("crash.matrix.termLength", "65536");
        try {
            this.assertOutcome("AFTER_COMMIT_RECORDED", false, false, "RESEED_REQUIRED", "tiny-term-boundary", 2, 1,
                    65536, "digest", 0);
        } finally {
            if (previous == null) System.clearProperty("crash.matrix.termLength");
            else System.setProperty("crash.matrix.termLength", previous);
        }
    }

        /// Verifies a corrupted durable checkpoint fails closed (reseed or
    /// fail-closed) instead of continuing on torn recovery state. The
    /// checkpoint used here survived a CONTINUE cell unmodified; only the
    /// injected byte flip may change the outcome.
    @Test
    void checkpointByteFlipFailsClosed() throws Exception {
        this.assertSafeOutcomeAfterMutation("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
                base -> {
                    final Path checkpoint = base.resolve("checkpoint/writer.checkpoint");
                    assertTrue(Files.exists(checkpoint), "expected a durable checkpoint to corrupt");
                    final byte[] bytes = Files.readAllBytes(checkpoint);
                    assertTrue(bytes.length > 0, "checkpoint file is empty");
                    /* Both journal slots must be damaged: one corrupt slot is
                     * intentionally recovered from the other valid slot. */
                    assertEquals(254, bytes.length);
                    bytes[126] ^= 0x01;
                    bytes[253] ^= 0x01;
                    Files.write(checkpoint, bytes);
                });
    }

        /// Verifies a corrupted Archive tail fails closed instead of replaying
    /// torn frames as committed history.
    @Test
    void archiveTailCorruptionFailsClosed() throws Exception {
        this.assertSafeOutcomeAfterMutation("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT",
                base -> {
                    final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                            base.resolve("checkpoint/writer.checkpoint"));
                    final List<Path> segments = ArchiveArtifactMutator.segments(
                            base.resolve("archive"), checkpoint.recordingId());
                    ArchiveArtifactMutator.corruptFirstEnvelopePayload(segments.getLast());
                });
    }

        /// Envelope-header corruption fuzz: mutates one header field per cell
    /// and, except for the stored CRC fields themselves, recomputes the header
    /// CRC afterwards — so the integrity check passes and only the decoder's
    /// semantic validation (magic, version, kind, identity, nonce, fencing
    /// token, chunk framing) can reject the frame. A mutation that lets the
    /// child continue is silent acceptance of corrupted history and fails the
    /// cell. The default run samples a seeded subset; `-Dcrash.header.fuzz.full=true`
    /// sweeps every field × mutation combination.
    @Test
    void envelopeHeaderCorruptionNeverContinuesSilently() throws Exception {
        final HeaderField[] fields = HeaderField.values();
        final Mutation[] mutations = Mutation.values();
        final boolean full = Boolean.getBoolean("crash.header.fuzz.full");
        final int combosPerRun = Integer.getInteger("crash.header.fuzz.combos", 8);
        final long seed = Long.getLong("crash.matrix.seed", 1L) ^ 0x4C0BABL;
        final Random random = new Random(seed);
        final List<int[]> selected = new ArrayList<>();
        if (full) {
            for (int field = 0; field < fields.length; field++) {
                for (int mutation = 0; mutation < mutations.length; mutation++) {
                    selected.add(new int[]{field, mutation});
                }
            }
        } else {
            for (int i = 0; i < Math.min(combosPerRun, fields.length * mutations.length); i++) {
                selected.add(new int[]{random.nextInt(fields.length), random.nextInt(mutations.length)});
            }
        }
        for (final int[] combo : selected) {
            final HeaderField field = fields[combo[0]];
            final Mutation mutation = mutations[combo[1]];
            /* The stored-CRC fields are mutated without the fix-up: their
             * whole purpose is the integrity check, and a recomputed CRC over
             * a mutated CRC field is meaningless. Every other field is mutated
             * with the CRC fixed up so semantics alone must reject it. */
            final boolean isStoredCrc = field == HeaderField.HEADER_CRC ||
                                        field == HeaderField.PAYLOAD_CRC ||
                                        field == HeaderField.COMMIT_CRC;
            this.assertSafeOutcomeAfterMutation("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT",
                    base -> {
                        final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                                base.resolve("checkpoint/writer.checkpoint"));
                        final List<Path> segments = ArchiveArtifactMutator.segments(
                                base.resolve("archive"), checkpoint.recordingId());
                        ArchiveArtifactMutator.corruptFirstEnvelopeHeader(
                                segments.getLast(), field, mutation, !isStoredCrc);
                    });
        }
    }

        /// Seeded chunk-budget kills: the child dies after a randomized number
    /// of published data chunks rather than at an enumerated milestone,
    /// covering intra-transaction interleavings the milestone list misses.
    /// Every budget kill lands mid-transaction, so the safe outcome is always
    /// a demanded reseed — never silent continuation.
    @Test
    void seededBudgetKillPreservesTheSafeOutcomeInvariant() throws Exception {
        final int iterations = Integer.getInteger("crash.matrix.budget.iterations", 3);
        final long baseSeed = Long.getLong("crash.matrix.seed", 1L);
        final int[] sizes = {64, 4096, 20000};
        final Random random = new Random(baseSeed ^ 0xB17C4L);
        for (int iteration = 0; iteration < iterations; iteration++) {
            final int size = sizes[random.nextInt(sizes.length)];
            /* The child uses 16 KiB chunks: bound the budget by the chunks
             * two transactions actually publish, or the barrier never fires. */
            final int totalChunks = 2 * Math.max(1, (size + 16383) / 16384);
            final int chunksBudget = 1 + random.nextInt(totalChunks);
            this.assertOutcome("NONE", false, false,
                    "RESEED_REQUIRED", "budget=%d,size=%d,iter=%d".formatted(chunksBudget, size, iteration),
                    2, 1, size, "digest", chunksBudget);
        }
    }

        /// Runs one crash cell to a safe (non-continue) outcome after mutating
    /// durable state between the kill and the recovery. The Store fixture must
    /// be byte-identical to the pre-mutation evidence: recovery may refuse to
    /// continue, but it must never silently extend history.
    private void assertSafeOutcomeAfterMutation(final String point, final ThrowingConsumer<Path> mutator) throws Exception {
        this.assertSafeOutcomeAfterMutation(point, mutator,
                java.util.Set.of(RecoveryPolicy.RESEED_REQUIRED, RecoveryPolicy.FAIL_CLOSED));
    }

    private void assertSafeOutcomeAfterMutation(final String point, final ThrowingConsumer<Path> mutator,
                                                final java.util.Set<RecoveryPolicy> allowed) throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            final int livePort = layout.livePort();
            final int controlPort = layout.controlPort();
            CrashEventLog.append(base.resolve("control"), "selection", "mutation point=%s".formatted(point));
            Process child = null;
            try {
                child = this.launch(base, "phase1", point, livePort, controlPort);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                final Path milestone = base.resolve("control/milestone.reached");
                this.await(milestone, child, budget("crash.budget.milestone", 60_000L));
                final ChildMilestone marker = ChildMilestone.read(milestone);
                assertEquals(point, marker.point(), "unexpected milestone point");
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit after kill");
                final StoreFixture.Evidence phase1Store = StoreFixture.inspect(base.resolve("store.records"));
                assertTrue(phase1Store.valid(), "phase1 Store fixture is not a complete record");
                assertPhase1Prefix(point, 1, 0, CrashPayloads.DEFAULT_SIZE, false, phase1Store);
                mutator.accept(base);
                CrashEventLog.append(base.resolve("control"), "mutation", "applied");
                String outcome;
                final long restartDeadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(budget("crash.budget.archiveStop", 30_000L));
                int restartAttempts = 0;
                do {
                    restartAttempts++;
                    child = this.launch(base, "phase2", "NONE", livePort, controlPort);
                    this.await(base.resolve("control/outcome"), child, budget("crash.budget.startup", 120_000L));
                    assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase2 child did not exit");
                    outcome = Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8);
                    if (!isActiveDriverRetry(outcome)) break;
                    Thread.sleep(1_000L);
                }
                while (System.nanoTime() < restartDeadline);
                assertFalse(isActiveDriverRetry(outcome),
                        "recording never became stopped; attempts=%s outcome=%s".formatted(restartAttempts, outcome));
                final CrashOutcome result = CrashOutcome.parse(outcome);
                assertNotHarnessError(result, outcome);
                assertTrue(allowed.contains(result.policy()),
                        "corrupted durable state must end in %s, got %s\n%s".formatted(allowed, result.policy(), outcome));
                assertTrue(result.error() != null && !result.error().isBlank(), outcome);
                assertTrue(result.storeValid(), outcome);
                CrashEventLog.append(base.resolve("control"), "outcome",
                        "mutation policy=%s".formatted(result.policy()));
                StoreFixture.assertRecords(base.resolve("store.records"), phase1Store.records());
            } catch (final Throwable failure) {
                if (child != null && child.isAlive()) child.destroyForcibly();
                try {
                    final Path evidence = DiagnosticCollector.collect(base, failure.toString());
                    throw new AssertionError("crash cell evidence: %s".formatted(evidence), failure);
                } catch (final Throwable evidenceFailure) {
                    failure.addSuppressed(evidenceFailure);
                    throw failure;
                }
            }
        }
    }

    /// Combined failure: a kill right after the commit is recorded under a
    /// deliberately tiny term, then the final Archive frame is truncated by one
    /// byte on the stopped recording. Recovery cannot prove the tail boundary,
    /// so the only safe report is a demanded reseed — never fail-open replay and
    /// never a generic fail-closed that hides the classification.
    @Test
    void truncatedFinalFrameAfterRecordedCommitRequiresReseed() throws Exception {
        final String previousTerm = System.getProperty("crash.matrix.termLength");
        System.setProperty("crash.matrix.termLength", "65536");
        try {
            this.assertSafeOutcomeAfterMutation("AFTER_COMMIT_RECORDED", base -> {
                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                        base.resolve("checkpoint/writer.checkpoint"));
                final List<Path> segments = ArchiveArtifactMutator.segments(
                        base.resolve("archive"), checkpoint.recordingId());
                final Path tail = segments.getLast();
                ArchiveArtifactMutator.truncateFinalFrame(tail, segmentBase(tail),
                        checkpoint.recordingPosition());
            }, java.util.Set.of(RecoveryPolicy.RESEED_REQUIRED));
        } finally {
            if (previousTerm == null) System.clearProperty("crash.matrix.termLength");
            else System.setProperty("crash.matrix.termLength", previousTerm);
        }
    }

    /// Segment base position encoded into the Archive segment file name.
    private static long segmentBase(final Path segment) {
        final String name = segment.getFileName().toString();
        final int dash = name.indexOf('-');
        final int dot = name.lastIndexOf('.');
        return Long.parseLong(name.substring(dash + 1, dot));
    }

    /// Asserts the exact committed Store prefix a crash point must leave behind.
    ///
    /// Content equality alone cannot catch a cell that crashed before writing
    /// anything: an empty-but-valid fixture would pass. Transactions before the
    /// target are fully committed, so their records must exist; the target
    /// transaction itself is the ambiguous tail. Points at or after the local
    /// Store write additionally guarantee the target record — the pipeline
    /// order is Archive prepare chunks, then the local Store enqueue, then the
    /// Archive commit, so anything commit-side or later has a durable target.
    private static void assertPhase1Prefix(final String point, final int targetSequence,
                                           final int budgetChunks, final int payloadSize,
                                           final boolean targetRejected,
                                           final StoreFixture.Evidence phase1Store) {
        final int minimum = expectedMinimumRecords(point, targetSequence, budgetChunks, payloadSize, targetRejected);
        assertTrue(phase1Store.records().size() >= minimum,
                "phase1 Store is missing committed records for %s: have=%d need>=%d".formatted(
                        point, phase1Store.records().size(), minimum));
    }

    private static int expectedMinimumRecords(final String point, final int targetSequence,
                                              final int budgetChunks, final int payloadSize,
                                              final boolean targetRejected) {
        if (budgetChunks > 0) {
            /* The child publishes with 16 KiB chunks: transactions before the
             * budget chunk's own are complete. */
            final int chunksPerTx = Math.max(1, (payloadSize + 16383) / 16384);
            return Math.max(0, (budgetChunks - 1) / chunksPerTx);
        }
        if ("BEFORE_PUBLICATION_CONNECTED".equals(point) || targetSequence <= 0) return 0;
        /* A locally rejected target never appends, but every transaction
         * before it is still fully committed. */
        if (targetRejected) return targetSequence;
        return switch (point) {
            /* The target transaction never reached the local Store: it is
             * still chunking or preparing when the child dies. */
            case "BEFORE_PREPARE",
                 "AFTER_DICTIONARY_CHUNKS",
                 "AFTER_DATA_CHUNKS",
                 "AFTER_PREPARE",
                 "AFTER_PREPARE_BEFORE_LOCAL_WRITE",
                 "AFTER_PREPARE_FAILURE_ABORT_OFFERED" -> targetSequence;
            default -> targetSequence + 1;
        };
    }

    @FunctionalInterface
    private interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }

        /// Verifies the writer remains deterministic when no live subscriber is connected.
    @Test
    void writerWithoutSubscriberStillFailsClosedSafely() throws Exception {
        final String previous = System.getProperty("crash.matrix.subscriber");
        System.setProperty("crash.matrix.subscriber", "false");
        try {
            this.assertReseed("AFTER_COMMIT_OFFER", false);
        } finally {
            if (previous == null) System.clearProperty("crash.matrix.subscriber");
            else System.setProperty("crash.matrix.subscriber", previous);
        }
    }

        /// Verifies failed prepare abort boundary requires reseed.
    @Test
    void failedPrepareAbortBoundaryRequiresReseed() throws Exception {
        this.assertReseed("AFTER_PREPARE_FAILURE_ABORT_OFFERED", true);
    }

        /// Seeded process-kill soak.  It is enabled by the crashmatrix profile and
    /// remains configurable so a nightly run can increase the sample count
    /// without changing the deterministic cells above.
    @Test
    void seededCrashSoakPreservesTheSafeOutcomeInvariant() throws Exception {
        final int iterations = Integer.getInteger("crash.matrix.random.iterations", 0);
        assumeTrue(iterations > 0,
                "crash soak is disabled; run with -Dcrash.matrix.random.iterations=N");
        final int seeds = Math.max(1, Integer.getInteger("crash.matrix.random.seeds", 1));
        /* Keep this rotation aligned with the deterministic barrier cells below.
         * The no-subscriber variant is intentionally a separate deterministic test
         * because it temporarily changes a process-wide system property. */
        final CrashScenario[] scenarios =
                {
                        new CrashScenario("BEFORE_PUBLICATION_CONNECTED", false, false,
                                "CONTINUE"),
                        new CrashScenario("BEFORE_PREPARE", false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_DICTIONARY_CHUNKS", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_DATA_CHUNKS", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_PREPARE_BEFORE_LOCAL_WRITE", false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_LOCAL_WRITE_BEFORE_COMMIT", false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_COMMIT_OFFER", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_COMMIT_RECORDED", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("BEFORE_COMMIT_OFFER", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_PREPARE", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_ABORT_OFFERED", false, true,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_PREPARE_FAILURE_ABORT_OFFERED", true, false, "RESEED_REQUIRED"),
                        new CrashScenario("BEFORE_JOURNAL_SLOT_WRITE", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("DURING_JOURNAL_SLOT_WRITE", false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_JOURNAL_SLOT_FORCE", false, false, "CONTINUE"),
                        new CrashScenario("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
                                false, false, "CONTINUE"),
                        new CrashScenario("AFTER_DATA_CHUNKS", false, false,
                                "RESEED_REQUIRED", 1, 0)
                };
        final long baseSeed = Long.getLong("crash.matrix.seed", 1L);
        for (int seedIndex = 0; seedIndex < seeds; seedIndex++) {
            final Random random = new Random(baseSeed + seedIndex);
            for (int iteration = 0; iteration < iterations; iteration++) {
                final CrashScenario scenario = scenarios[random.nextInt(scenarios.length)];
                this.assertOutcome(scenario.point(), scenario.injectPrepareFailure(),
                        scenario.rejectLocal(), scenario.expectedOutcome(),
                        "seed=%s,iteration=%s".formatted((baseSeed + seedIndex), iteration),
                        scenario.writes(), scenario.targetSequence());
            }
        }
    }

        /// Verifies a second crash after a valid checkpoint is read remains restartable.
    @Test
    void doubleCrashDuringRecoveryPreservesCheckpointContinuation() throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            final int livePort = layout.livePort();
            final int controlPort = layout.controlPort();
            Process child = null;
            try {
                child = this.launch(base, "phase1", "AFTER_JOURNAL_SLOT_FORCE",
                        livePort, controlPort);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                this.await(base.resolve("control/milestone.reached"), child, budget("crash.budget.milestone", 60_000L));
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit");

                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                        base.resolve("checkpoint/writer.checkpoint"));
                assertEquals(AeronReplicationCheckpoint.State.COMMITTED, checkpoint.state(),
                        "phase 1 must leave a valid terminal checkpoint before the recovery crash");
                child = this.launchRecoveryMilestone(base, livePort, controlPort,
                        (int) checkpoint.transactionSequence());
                final ChildMilestone recoveryMarker = ChildMilestone.read(base.resolve("control/milestone.reached"));
                assertEquals("AFTER_RECOVERY_CHECKPOINT_READ", recoveryMarker.point());
                assertEquals(checkpoint.transactionSequence(), recoveryMarker.sequence(),
                        "recovery barrier must report the checkpoint sequence it loaded");
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase2 child did not exit");

                String outcome;
                final long restartDeadline = System.nanoTime() +
                                             TimeUnit.MILLISECONDS.toNanos(budget("crash.budget.archiveStop", 30_000L));
                int restartAttempts = 0;
                do {
                    final long storeSizeBeforeRetry = fileSize(base.resolve("store.records"));
                    restartAttempts++;
                    child = this.launch(base, "phase2", "NONE", livePort, controlPort);
                    this.await(base.resolve("control/outcome"), child, budget("crash.budget.startup", 120_000L));
                    if (!child.waitFor(10, TimeUnit.SECONDS)) {
                        child.destroyForcibly();
                        assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase3 child did not exit");
                    }
                    outcome = Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8);
                    if (!isActiveDriverRetry(outcome)) break;
                    assertEquals(storeSizeBeforeRetry, fileSize(base.resolve("store.records")),
                            "recovery retry changed the Store fixture before startup %s".formatted(restartAttempts));
                    Thread.sleep(1_000L);
                }
                while (System.nanoTime() < restartDeadline);
                assertFalse(isActiveDriverRetry(outcome),
                        "recording never became stopped before recovery retry deadline; attempts=%s outcome=%s\n%s".formatted(restartAttempts, outcome, diagnostics(base.resolve("control"))));
                final CrashOutcome result = CrashOutcome.parse(outcome);
                assertNotHarnessError(result, outcome);
                assertEquals(RecoveryPolicy.CONTINUE, result.policy(), outcome);
                assertEquals("LIVE", result.health(), outcome);
                StoreFixture.assertRecords(base.resolve("store.records"),
                        List.of(payload(0), payload(1), payload(2)));
            } catch (final Throwable failure) {
                if (child != null && child.isAlive()) child.destroyForcibly();
                try {
                    final Path evidence = DiagnosticCollector.collect(base, failure.toString());
                    throw new AssertionError("double-crash evidence: %s".formatted(evidence), failure);
                } catch (final Throwable evidenceFailure) {
                    /* Evidence collection must never mask the crash-cell
                     * failure it was meant to document: a collector outage
                     * (missing class, IO) suppresses into the original. */
                    failure.addSuppressed(evidenceFailure);
                    throw failure;
                }
            }
        }
    }

        /// Verifies two consecutive crashes during recovery still leave a
    /// restartable checkpoint: crash, crash again while reading the recovery
    /// checkpoint, crash a third time the same way, then recover cleanly.
    /// Recovery-of-recovery must converge, not compound.
    @Test
    void tripleCrashDuringRecoveryPreservesCheckpointContinuation() throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            final int livePort = layout.livePort();
            final int controlPort = layout.controlPort();
            CrashEventLog.append(base.resolve("control"), "selection", "triple-recovery-chain");
            Process child = null;
            try {
                child = this.launch(base, "phase1", "AFTER_JOURNAL_SLOT_FORCE",
                        livePort, controlPort);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                this.await(base.resolve("control/milestone.reached"), child, budget("crash.budget.milestone", 60_000L));
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit");

                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                        base.resolve("checkpoint/writer.checkpoint"));
                assertEquals(AeronReplicationCheckpoint.State.COMMITTED, checkpoint.state(),
                        "phase 1 must leave a valid terminal checkpoint before the recovery crashes");
                for (int crash = 1; crash <= 2; crash++) {
                    child = this.launchRecoveryMilestone(base, livePort, controlPort,
                            (int) checkpoint.transactionSequence());
                    final ChildMilestone recoveryMarker = ChildMilestone.read(base.resolve("control/milestone.reached"));
                    assertEquals("AFTER_RECOVERY_CHECKPOINT_READ", recoveryMarker.point());
                    assertEquals(checkpoint.transactionSequence(), recoveryMarker.sequence(),
                            "recovery barrier must report the checkpoint sequence it loaded");
                    child.destroyForcibly();
                    assertTrue(child.waitFor(10, TimeUnit.SECONDS),
                            "recovery crash %d child did not exit".formatted(crash));
                    CrashEventLog.append(base.resolve("control"), "recovery-crash", "crash=%d".formatted(crash));
                }

                String outcome;
                final long restartDeadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(budget("crash.budget.archiveStop", 30_000L));
                int restartAttempts = 0;
                do {
                    final long storeSizeBeforeRetry = fileSize(base.resolve("store.records"));
                    restartAttempts++;
                    child = this.launch(base, "phase2", "NONE", livePort, controlPort);
                    this.await(base.resolve("control/outcome"), child, budget("crash.budget.startup", 120_000L));
                    if (!child.waitFor(10, TimeUnit.SECONDS)) {
                        child.destroyForcibly();
                        assertTrue(child.waitFor(10, TimeUnit.SECONDS), "final recovery child did not exit");
                    }
                    outcome = Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8);
                    if (!isActiveDriverRetry(outcome)) break;
                    assertEquals(storeSizeBeforeRetry, fileSize(base.resolve("store.records")),
                            "recovery retry changed the Store fixture before startup %s".formatted(restartAttempts));
                    Thread.sleep(1_000L);
                }
                while (System.nanoTime() < restartDeadline);
                assertFalse(isActiveDriverRetry(outcome),
                        "recording never became stopped before recovery retry deadline; attempts=%s outcome=%s\n%s".formatted(restartAttempts, outcome, diagnostics(base.resolve("control"))));
                final CrashOutcome result = CrashOutcome.parse(outcome);
                assertNotHarnessError(result, outcome);
                assertEquals(RecoveryPolicy.CONTINUE, result.policy(), outcome);
                assertEquals("LIVE", result.health(), outcome);
                CrashEventLog.append(base.resolve("control"), "outcome", "triple-chain CONTINUE");
                StoreFixture.assertRecords(base.resolve("store.records"),
                        List.of(payload(0), payload(1), payload(2)));
            } catch (final Throwable failure) {
                if (child != null && child.isAlive()) child.destroyForcibly();
                try {
                    final Path evidence = DiagnosticCollector.collect(base, failure.toString());
                    throw new AssertionError("triple-crash evidence: %s".formatted(evidence), failure);
                } catch (final Throwable evidenceFailure) {
                    failure.addSuppressed(evidenceFailure);
                    throw failure;
                }
            }
        }
    }

    private void assertReseed(final String point, final boolean injectPrepareFailure) throws Exception {
        this.assertOutcome(point, injectPrepareFailure, false, "RESEED_REQUIRED");
    }

    private void assertOutcome(final String point, final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome) throws Exception {
        this.assertOutcome(point, injectPrepareFailure, rejectLocal, expectedOutcome, null, 2, 1);
    }

    private void assertOutcome(final String point, final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome,
                               final String runLabel, final int writes, final int targetSequence) throws Exception {
        this.assertOutcome(point, injectPrepareFailure, rejectLocal, expectedOutcome,
                runLabel, writes, targetSequence,
                CrashPayloads.DEFAULT_SIZE, CrashPayloads.DEFAULT_KIND, 0);
    }

    private void assertOutcome(final String point, final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome,
                               final String runLabel, final int writes, final int targetSequence,
                               final int payloadSize, final String payloadKind, final int budgetChunks) throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            if (runLabel != null) {
                Files.createDirectories(base.resolve("control"));
                Files.writeString(base.resolve("control/selection"), "%s,point=%s%s".formatted(runLabel, point, '\n'),
                        StandardCharsets.UTF_8);
            }
            CrashEventLog.append(base.resolve("control"), "selection",
                    "point=%s writes=%d target=%d payload=%d/%s budget=%d label=%s".formatted(
                            point, writes, targetSequence, payloadSize, payloadKind, budgetChunks, runLabel));
            final int livePort = layout.livePort();
            final int controlPort = layout.controlPort();
            Process child = null;
            try {
                child = this.launch(base, "phase1", point, injectPrepareFailure, rejectLocal,
                        livePort, controlPort, writes, targetSequence, payloadSize, payloadKind, budgetChunks);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                final Path milestone = base.resolve("control/milestone.reached");
                this.await(milestone, child, budget("crash.budget.milestone", 60_000L));
                final ChildMilestone marker = ChildMilestone.read(milestone);
                if (budgetChunks > 0) {
                    /* A chunk budget lands after data chunks by construction,
                     * just not at the enumerated milestone: the fuzz point is
                     * the randomized chunk count, recorded in the event log. */
                    assertEquals("AFTER_DATA_CHUNKS", marker.point(), "budget kill landed off the data path");
                } else {
                    assertEquals(point, marker.point(), "unexpected milestone point");
                    assertEquals("BEFORE_PUBLICATION_CONNECTED".equals(point) ? -1L : targetSequence,
                            marker.sequence(), "unexpected milestone sequence");
                }
                CrashEventLog.append(base.resolve("control"), "milestone",
                        "point=%s sequence=%d".formatted(marker.point(), marker.sequence()));
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit after kill");
                final StoreFixture.Evidence phase1Store = StoreFixture.inspect(base.resolve("store.records"));
                assertTrue(phase1Store.valid(), "phase1 Store fixture is not a complete record");
                assertPhase1Prefix(point, targetSequence, budgetChunks, payloadSize, rejectLocal, phase1Store);
                for (int i = 0; i < phase1Store.records().size(); i++) {
                    assertArrayEquals(payload(i, payloadSize, payloadKind), phase1Store.records().get(i),
                            "unexpected phase1 Store payload at record %s".formatted(i));
                }
                String outcome;
                final long restartDeadline = System.nanoTime() +
                                             TimeUnit.MILLISECONDS.toNanos(budget("crash.budget.archiveStop", 30_000L));
                int restartAttempts = 0;
                do {
                    final long storeSizeBeforeRetry = fileSize(base.resolve("store.records"));
                    restartAttempts++;
                    child = this.launch(base, "phase2", "NONE", false, false,
                            livePort, controlPort, 2, 1, payloadSize, payloadKind, 0);
                    this.await(base.resolve("control/outcome"), child, budget("crash.budget.startup", 120_000L));
                    assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase2 child did not exit");
                    outcome = Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8);
                    if (!isActiveDriverRetry(outcome)) break;
                    assertEquals(storeSizeBeforeRetry, fileSize(base.resolve("store.records")),
                            "phase2 changed the Store fixture before recovery on retry %s".formatted(restartAttempts));
                    Thread.sleep(1_000L);
                }
                while (System.nanoTime() < restartDeadline);
                assertFalse(isActiveDriverRetry(outcome),
                        "recording never became stopped before phase2 restart deadline; attempts=%s outcome=%s\n%s".formatted(restartAttempts, outcome, diagnostics(base.resolve("control"))));
                final CrashOutcome result;
                try {
                    result = CrashOutcome.parse(outcome);
                } catch (final RuntimeException parseFailure) {
                    throw new AssertionError("invalid child outcome: %s".formatted(outcome), parseFailure);
                }
                assertNotHarnessError(result, outcome);
                assertEquals(RecoveryPolicy.valueOf(expectedOutcome), result.policy(), outcome);
                assertEquals("CONTINUE".equals(expectedOutcome) ? "LIVE" : "RESEED_REQUIRED",
                        result.health(), outcome);
                assertTrue(result.storeValid(), outcome);
                CrashEventLog.append(base.resolve("control"), "outcome",
                        "policy=%s health=%s attempts=%d".formatted(result.policy(), result.health(), restartAttempts));
                if (result.crc32c() != null &&
                    ("COMMITTED".equals(result.checkpointState()) || "COMMITTING_UNCERTAIN".equals(result.checkpointState()))) {
                    final long checkpointCrc = Integer.toUnsignedLong(result.crc32c());
                    assertTrue(checkpointCrc == Integer.toUnsignedLong(crc(payload(0, payloadSize, payloadKind))) ||
                               checkpointCrc == Integer.toUnsignedLong(crc(payload(1, payloadSize, payloadKind))) ||
                               checkpointCrc == Integer.toUnsignedLong(crc(payload(2, payloadSize, payloadKind))),
                            "checkpoint CRC is not one of the test transaction payloads\n%s".formatted(outcome));
                }
                if (result.policy() == RecoveryPolicy.CONTINUE && result.recordingId() != null && result.recordingId() >= 0) {
                    assertTrue(result.recordingPosition() != null && result.recordingPosition() >= 0,
                            "checkpoint has recording identity without a replayable position\n%s".formatted(outcome));
                }
                if (!"CONTINUE".equals(expectedOutcome)) {
                    assertTrue(result.error() != null && !result.error().isBlank(), outcome);
                }
                final List<byte[]> expectedStore = new java.util.ArrayList<>(phase1Store.records());
                if ("CONTINUE".equals(expectedOutcome)) expectedStore.add(payload(2, payloadSize, payloadKind));
                StoreFixture.assertRecords(base.resolve("store.records"), expectedStore);
            } catch (final Throwable failure) {
                if (child != null && child.isAlive()) child.destroyForcibly();
                try {
                    final Path evidence = DiagnosticCollector.collect(base, failure.toString());
                    throw new AssertionError("crash cell evidence: %s".formatted(evidence), failure);
                } catch (final Throwable evidenceFailure) {
                    /* Evidence collection must never mask the crash-cell
                     * failure it was meant to document: a collector outage
                     * (missing class, IO) suppresses into the original. */
                    failure.addSuppressed(evidenceFailure);
                    throw failure;
                }
            }
        }
    }

    private Process launchRecoveryMilestone(final Path base, final int livePort, final int controlPort,
                                            final int sequence) throws Exception {
        final long deadline = System.nanoTime() +
                TimeUnit.MILLISECONDS.toNanos(budget("crash.budget.archiveStop", 30_000L));
        AssertionError lastStartupFailure = null;
        do {
            final long storeSize = fileSize(base.resolve("store.records"));
            final Process child = this.launch(base, "phase2", "AFTER_RECOVERY_CHECKPOINT_READ",
                    false, false, livePort, controlPort, 2, sequence);
            try {
                this.await(base.resolve("control/milestone.reached"), child,
                        budget("crash.budget.milestone", 60_000L));
                return child;
            } catch (final AssertionError startupFailure) {
                final Path outcomePath = base.resolve("control/outcome");
                if (!Files.exists(outcomePath) ||
                    !isActiveDriverRetry(Files.readString(outcomePath, StandardCharsets.UTF_8))) {
                    if (child.isAlive()) child.destroyForcibly();
                    throw startupFailure;
                }
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "mark-file retry child did not exit");
                assertEquals(storeSize, fileSize(base.resolve("store.records")),
                        "Archive mark-file retry changed the Store fixture");
                lastStartupFailure = startupFailure;
                Thread.sleep(1_000L);
            }
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Archive mark remained active before recovery milestone", lastStartupFailure);
    }

    private Process launch(final Path base, final String mode, final String point,
                           final int livePort, final int controlPort) throws IOException {
        return this.launch(base, mode, point, false, false, livePort, controlPort, 2, 1,
                CrashPayloads.DEFAULT_SIZE, CrashPayloads.DEFAULT_KIND, 0);
    }

    private Process launch(final Path base, final String mode, final String point,
                           final boolean injectPrepareFailure,
                           final boolean rejectLocal, final int livePort, final int controlPort,
                           final int writes, final int targetSequence) throws IOException {
        return this.launch(base, mode, point, injectPrepareFailure, rejectLocal,
                livePort, controlPort, writes, targetSequence,
                CrashPayloads.DEFAULT_SIZE, CrashPayloads.DEFAULT_KIND, 0);
    }

    private Process launch(final Path base, final String mode, final String point,
                           final boolean injectPrepareFailure,
                           final boolean rejectLocal, final int livePort, final int controlPort,
                           final int writes, final int targetSequence,
                           final int payloadSize, final String payloadKind, final int budgetChunks) throws IOException {
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        Files.deleteIfExists(control.resolve("ready"));
        Files.deleteIfExists(control.resolve("ready-phase2"));
        Files.deleteIfExists(control.resolve("milestone.reached"));
        Files.deleteIfExists(control.resolve("outcome"));
        Files.deleteIfExists(control.resolve("release"));
        final Path stdout = control.resolve("%s-stdout.log".formatted(mode));
        final Path stderr = control.resolve("%s-stderr.log".formatted(mode));
        final String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final int crashTermLength = Integer.getInteger("crash.matrix.termLength", 1048576);
        final int maxChunkSize = Math.max(1, crashTermLength / 8 - AeronReplicationEnvelope.HEADER_LENGTH);
        final int crashChunkSize = Integer.getInteger(
                "crash.matrix.chunkSize", Math.min(16384, maxChunkSize));
        final ProcessBuilder builder = new ProcessBuilder(javaExecutable,
                "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.crash.base=%s".formatted(base),
                "-Ddg.crash.mode=%s".formatted(mode),
                "-Ddg.crash.barrier=%s".formatted(point),
                "-Ddg.crash.sequence=%s".formatted(targetSequence),
                "-Ddg.crash.writes=%s".formatted(writes),
                                "-Ddg.crash.injectPrepareFailure=%s".formatted(injectPrepareFailure),
                "-Ddg.crash.rejectLocal=%s".formatted(rejectLocal),
                "-Ddg.crash.rejectSequence=%s".formatted((rejectLocal ? 1 : -1)),
                "-Ddg.crash.payloadSize=%s".formatted(payloadSize),
                "-Ddg.crash.payloadKind=%s".formatted(payloadKind),
                "-Ddg.crash.budgetChunks=%s".formatted(budgetChunks),
                "-Ddg.crash.termLength=%s".formatted(crashTermLength),
                "-Ddg.crash.chunkSize=%s".formatted(crashChunkSize),
                "-Ddg.crash.subscriber=%s".formatted(System.getProperty("crash.matrix.subscriber", "true")),
                "-Ddg.crash.livePort=%s".formatted(livePort),
                "-Ddg.crash.controlPort=%s".formatted(controlPort),
                ProviderCrashChildMain.class.getName());
        builder.redirectOutput(stdout.toFile());
        builder.redirectError(stderr.toFile());
        return builder.start();
    }

    private void await(final Path path, final Process child, final long timeoutMillis)
            throws InterruptedException {
        final long timeout = Math.min(timeoutMillis, budget("crash.budget.cell", timeoutMillis));
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            if (!child.isAlive()) {
                throw new AssertionError("child exited before %s\n%s".formatted(path, diagnostics(path.getParent())));
            }
            Thread.sleep(10L);
        }
        assertTrue(Files.exists(path), "timed out waiting for %s\n%s".formatted(path, diagnostics(path.getParent())));
    }

    private record CrashScenario(
            String point,
            boolean injectPrepareFailure,
            boolean rejectLocal,
            String expectedOutcome,
            int writes,
            int targetSequence
    ) {
        private CrashScenario(final String point, final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome) {
            this(point, injectPrepareFailure, rejectLocal, expectedOutcome, 2, 1);
        }
    }

}
