package peruncs.datagrid.cluster.node.aeron.crashtest;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.datagrid.cluster.storage.aeron.crashtest.ArchiveArtifactMutator;
import peruncs.datagrid.cluster.storage.aeron.crashtest.CrashPayloads;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;
import peruncs.datagrid.cluster.test.ChildJava;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// Child-process tests for the provider's writer and embedded Archive restart
/// boundary. The parent kills the child only after the requested milestone is
/// written to the control file.
class ProviderCrashMatrixIT {
    private static boolean isActiveDriverRetry(final String outcome) {
        final String normalized = outcome.toLowerCase(java.util.Locale.ROOT);
        return normalized.contains("active media driver") ||
               normalized.contains("active mark file") ||
               normalized.contains("driver directory remained active");
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
        this.assertReseed("AFTER_DATA_CHUNKS", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies dictionary data tail requires reseed.
    @Test
    void dictionaryDataTailRequiresReseed() throws Exception {
        this.assertReseed("AFTER_DICTIONARY_CHUNKS", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies prepared before local write requires reseed.
    @Test
    void preparedBeforeLocalWriteRequiresReseed() throws Exception {
        this.assertReseed("AFTER_PREPARE_BEFORE_LOCAL_WRITE", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies the pre-publication fence refuses an ambiguous restart.
    @Test
    void crashBeforePrepareRequiresReseed() throws Exception {
        this.assertOutcome("BEFORE_PREPARE", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false, "RESEED_REQUIRED");
    }

        /// Verifies a crash before the first publication connection leaves no writer state.
    @Test
    void beforePublicationConnectionLeavesNoWriterState() throws Exception {
        this.assertOutcome("BEFORE_PUBLICATION_CONNECTED", ReplicationDurabilityMode.ARCHIVE_FIRST,
                false, false, "CONTINUE");
    }

        /// Verifies local rejection after abort offer requires reseed.
    @Test
    void localRejectionAfterAbortOfferRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_ABORT_OFFERED", ReplicationDurabilityMode.ARCHIVE_FIRST, false, true,
                "RESEED_REQUIRED");
    }

        /// Verifies archive recorded before checkpoint requires reseed.
    @Test
    void archiveRecordedBeforeCheckpointRequiresReseed() throws Exception {
        this.assertReseed("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies ambiguous commit offer requires reseed.
    @Test
    void ambiguousCommitOfferRequiresReseed() throws Exception {
        this.assertReseed("AFTER_COMMIT_OFFER", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies a recorded commit without a coordinator return requires reseed.
    @Test
    void recordedCommitBeforeCoordinatorReturnRequiresReseed() throws Exception {
        this.assertReseed("AFTER_COMMIT_RECORDED", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies the commit-offer boundary fails closed before recording.
    @Test
    void beforeCommitOfferRequiresReseed() throws Exception {
        this.assertReseed("BEFORE_COMMIT_OFFER", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies a prepared transaction that never reaches local Store write requires reseed.
    @Test
    void afterPrepareRequiresReseed() throws Exception {
        this.assertReseed("AFTER_PREPARE", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies local write ahead fence requires reseed.
    @Test
    void localWriteAheadFenceRequiresReseed() throws Exception {
        this.assertReseed("AFTER_LOCAL_WRITE_BEFORE_COMMIT", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies enqueue before prepare fence requires reseed.
    @Test
    void enqueueBeforePrepareFenceRequiresReseed() throws Exception {
        this.assertReseed("AFTER_ENQUEUE_BEFORE_PREPARE", ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE, false);
    }

        /// Verifies uncertain checkpoint requires reseed.
    @Test
    void uncertainCheckpointRequiresReseed() throws Exception {
        this.assertReseed("DURING_COMMITTING_UNCERTAIN_WRITE", ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE, true);
    }

        /// Verifies a checkpoint write interrupted before rename preserves the prior boundary.
    @Test
    void checkpointTempWriteBeforeRenameRequiresReseed() throws Exception {
        this.assertReseed("BEFORE_CHECKPOINT_TEMP_WRITE", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies a checkpoint write interrupted during file output preserves the prior boundary.
    @Test
    void checkpointFileWriteRequiresReseed() throws Exception {
        this.assertReseed("DURING_CHECKPOINT_FILE_WRITE", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies a forced temporary checkpoint that was not renamed leaves the prior boundary.
    @Test
    void checkpointAfterTempWriteBeforeRenameRequiresReseed() throws Exception {
        this.assertReseed("AFTER_CHECKPOINT_TEMP_WRITE_BEFORE_RENAME", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
    }

        /// Verifies a renamed checkpoint remains restartable before directory force completes.
    @Test
    void checkpointRenameBeforeDirectorySyncContinues() throws Exception {
        this.assertOutcome("AFTER_CHECKPOINT_RENAME_BEFORE_DIRECTORY_SYNC",
                ReplicationDurabilityMode.ARCHIVE_FIRST, false, false, "CONTINUE");
    }

        /// Verifies a durable checkpoint remains restartable before in-memory sequence publication.
    @Test
    void checkpointBeforeSequenceUpdateContinues() throws Exception {
        this.assertOutcome("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
                ReplicationDurabilityMode.ARCHIVE_FIRST, false, false, "CONTINUE");
    }

        /// Verifies that an orphaned first transaction is never silently reused.
    @Test
    void firstTransactionOrphanRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_DATA_CHUNKS", ReplicationDurabilityMode.ARCHIVE_FIRST,
                false, false, "RESEED_REQUIRED", null, 1, 0);
    }

        /// Verifies a first-transaction orphan under publication backpressure
    /// (no live subscriber) still fails closed instead of reusing the orphan.
    @Test
    void writerCrashDuringBackpressureFirstTxRequiresReseed() throws Exception {
        final String previous = System.getProperty("crash.matrix.subscriber");
        System.setProperty("crash.matrix.subscriber", "false");
        try {
            this.assertOutcome("AFTER_DATA_CHUNKS", ReplicationDurabilityMode.ARCHIVE_FIRST,
                    false, false, "RESEED_REQUIRED", "backpressure-first-tx", 1, 0);
        } finally {
            if (previous == null) System.clearProperty("crash.matrix.subscriber");
            else System.setProperty("crash.matrix.subscriber", previous);
        }
    }

        /// Verifies a minimum-size payload still fails closed on a recorded
    /// commit that never reached the coordinator.
    @Test
    void minPayloadRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", ReplicationDurabilityMode.ARCHIVE_FIRST,
                false, false, "RESEED_REQUIRED", "payload=1", 2, 1, 1, "digest", 0);
    }

        /// Verifies a chunk-size-minus-one payload fails closed like any other tail.
    @Test
    void chunkMinusOneRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", ReplicationDurabilityMode.ARCHIVE_FIRST,
                false, false, "RESEED_REQUIRED", "payload=16383", 2, 1, 16383, "digest", 0);
    }

        /// Verifies a chunk-size-plus-one payload (two chunks) fails closed.
    @Test
    void chunkPlusOneRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", ReplicationDurabilityMode.ARCHIVE_FIRST,
                false, false, "RESEED_REQUIRED", "payload=16385", 2, 1, 16385, "digest", 0);
    }

        /// Verifies a multi-chunk payload fails closed on the recorded tail.
    @Test
    void multiChunkRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", ReplicationDurabilityMode.ARCHIVE_FIRST,
                false, false, "RESEED_REQUIRED", "payload=65536", 2, 1, 65536, "digest", 0);
    }

        /// Verifies incompressible bytes fail closed exactly like digests.
    @Test
    void randomBytesRecordedCommitRequiresReseed() throws Exception {
        this.assertOutcome("AFTER_COMMIT_RECORDED", ReplicationDurabilityMode.ARCHIVE_FIRST,
                false, false, "RESEED_REQUIRED", "payload=random", 2, 1, 4096, "random", 0);
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
                child = this.launch(base, "phase1", "NONE", ReplicationDurabilityMode.ARCHIVE_FIRST,
                        false, false, livePort, controlPort, 2, 1, size, "digest", 0);
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
                    child = this.launch(base, "phase2", "NONE", ReplicationDurabilityMode.ARCHIVE_FIRST,
                            false, false, livePort, controlPort, 2, 1, size, "digest", 0);
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

        /// Verifies a corrupted durable checkpoint fails closed (reseed or
    /// fail-closed) instead of continuing on torn recovery state. The
    /// checkpoint used here survived a CONTINUE cell unmodified; only the
    /// injected byte flip may change the outcome.
    @Test
    void checkpointByteFlipFailsClosed() throws Exception {
        this.assertSafeOutcomeAfterMutation("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
                ReplicationDurabilityMode.ARCHIVE_FIRST, base -> {
                    final Path checkpoint = base.resolve("checkpoint/writer.checkpoint");
                    assertTrue(Files.exists(checkpoint), "expected a durable checkpoint to corrupt");
                    final byte[] bytes = Files.readAllBytes(checkpoint);
                    assertTrue(bytes.length > 0, "checkpoint file is empty");
                    bytes[bytes.length / 2] ^= 0x01;
                    Files.write(checkpoint, bytes);
                });
    }

        /// Verifies a corrupted Archive tail fails closed instead of replaying
    /// torn frames as committed history.
    @Test
    void archiveTailCorruptionFailsClosed() throws Exception {
        this.assertSafeOutcomeAfterMutation("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT",
                ReplicationDurabilityMode.ARCHIVE_FIRST, base -> {
                    final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                            base.resolve("checkpoint/writer.checkpoint"));
                    final List<Path> segments = ArchiveArtifactMutator.segments(
                            base.resolve("archive"), checkpoint.recordingId());
                    ArchiveArtifactMutator.corruptFirstEnvelopePayload(segments.getLast());
                });
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
            this.assertOutcome("NONE", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                    "RESEED_REQUIRED", "budget=%d,size=%d,iter=%d".formatted(chunksBudget, size, iteration),
                    2, 1, size, "digest", chunksBudget);
        }
    }

        /// Runs one crash cell to a safe (non-continue) outcome after mutating
    /// durable state between the kill and the recovery. The Store fixture must
    /// be byte-identical to the pre-mutation evidence: recovery may refuse to
    /// continue, but it must never silently extend history.
    private void assertSafeOutcomeAfterMutation(final String point, final ReplicationDurabilityMode durability,
                                                final ThrowingConsumer<Path> mutator) throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            final int livePort = layout.livePort();
            final int controlPort = layout.controlPort();
            CrashEventLog.append(base.resolve("control"), "selection", "mutation point=%s".formatted(point));
            Process child = null;
            try {
                child = this.launch(base, "phase1", point, durability, livePort, controlPort);
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
                    child = this.launch(base, "phase2", "NONE", durability, livePort, controlPort);
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
                assertTrue(result.policy() == RecoveryPolicy.RESEED_REQUIRED
                                || result.policy() == RecoveryPolicy.FAIL_CLOSED,
                        "corrupted durable state must fail closed, got %s\n%s".formatted(result.policy(), outcome));
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
                 "AFTER_PREPARE_BEFORE_LOCAL_WRITE" -> targetSequence;
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
            this.assertReseed("AFTER_COMMIT_OFFER", ReplicationDurabilityMode.ARCHIVE_FIRST, false);
        } finally {
            if (previous == null) System.clearProperty("crash.matrix.subscriber");
            else System.setProperty("crash.matrix.subscriber", previous);
        }
    }

        /// Verifies failed prepare abort boundary requires reseed.
    @Test
    void failedPrepareAbortBoundaryRequiresReseed() throws Exception {
        this.assertReseed("AFTER_PREPARE_FAILURE_ABORT_OFFERED", ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE, true);
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
                        new CrashScenario("BEFORE_PUBLICATION_CONNECTED", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "CONTINUE"),
                        new CrashScenario("BEFORE_PREPARE", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_DICTIONARY_CHUNKS", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_DATA_CHUNKS", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_PREPARE_BEFORE_LOCAL_WRITE", ReplicationDurabilityMode.ARCHIVE_FIRST,
                                false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_LOCAL_WRITE_BEFORE_COMMIT", ReplicationDurabilityMode.ARCHIVE_FIRST,
                                false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_COMMIT_OFFER", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", ReplicationDurabilityMode.ARCHIVE_FIRST,
                                false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_COMMIT_RECORDED", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("BEFORE_COMMIT_OFFER", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_PREPARE", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_ABORT_OFFERED", ReplicationDurabilityMode.ARCHIVE_FIRST, false, true,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_ENQUEUE_BEFORE_PREPARE", ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE,
                                false, false, "RESEED_REQUIRED"),
                        new CrashScenario("DURING_COMMITTING_UNCERTAIN_WRITE", ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE,
                                true, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_PREPARE_FAILURE_ABORT_OFFERED", ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE,
                                true, false, "RESEED_REQUIRED"),
                        new CrashScenario("BEFORE_CHECKPOINT_TEMP_WRITE", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("DURING_CHECKPOINT_FILE_WRITE", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_CHECKPOINT_TEMP_WRITE_BEFORE_RENAME", ReplicationDurabilityMode.ARCHIVE_FIRST,
                                false, false, "RESEED_REQUIRED"),
                        new CrashScenario("AFTER_CHECKPOINT_RENAME_BEFORE_DIRECTORY_SYNC", ReplicationDurabilityMode.ARCHIVE_FIRST,
                                false, false, "CONTINUE"),
                        new CrashScenario("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
                                ReplicationDurabilityMode.ARCHIVE_FIRST, false, false, "CONTINUE"),
                        new CrashScenario("AFTER_DATA_CHUNKS", ReplicationDurabilityMode.ARCHIVE_FIRST, false, false,
                                "RESEED_REQUIRED", 1, 0)
                };
        final long baseSeed = Long.getLong("crash.matrix.seed", 1L);
        for (int seedIndex = 0; seedIndex < seeds; seedIndex++) {
            final Random random = new Random(baseSeed + seedIndex);
            for (int iteration = 0; iteration < iterations; iteration++) {
                final CrashScenario scenario = scenarios[random.nextInt(scenarios.length)];
                this.assertOutcome(scenario.point(), scenario.durability(), scenario.injectPrepareFailure(),
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
                child = this.launch(base, "phase1", "AFTER_CHECKPOINT_RENAME_BEFORE_DIRECTORY_SYNC",
                        ReplicationDurabilityMode.ARCHIVE_FIRST, livePort, controlPort);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                this.await(base.resolve("control/milestone.reached"), child, budget("crash.budget.milestone", 60_000L));
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit");

                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                        base.resolve("checkpoint/writer.checkpoint"));
                assertEquals(AeronReplicationCheckpoint.State.COMMITTED, checkpoint.state(),
                        "phase 1 must leave a valid terminal checkpoint before the recovery crash");
                child = this.launch(base, "phase2", "AFTER_RECOVERY_CHECKPOINT_READ",
                        ReplicationDurabilityMode.ARCHIVE_FIRST, false, false, livePort, controlPort, 2,
                        (int) checkpoint.transactionSequence());
                this.await(base.resolve("control/milestone.reached"), child,
                        budget("crash.budget.milestone", 60_000L));
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
                    child = this.launch(base, "phase2", "NONE", ReplicationDurabilityMode.ARCHIVE_FIRST,
                            livePort, controlPort);
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
                child = this.launch(base, "phase1", "AFTER_CHECKPOINT_RENAME_BEFORE_DIRECTORY_SYNC",
                        ReplicationDurabilityMode.ARCHIVE_FIRST, livePort, controlPort);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                this.await(base.resolve("control/milestone.reached"), child, budget("crash.budget.milestone", 60_000L));
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit");

                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(
                        base.resolve("checkpoint/writer.checkpoint"));
                assertEquals(AeronReplicationCheckpoint.State.COMMITTED, checkpoint.state(),
                        "phase 1 must leave a valid terminal checkpoint before the recovery crashes");
                for (int crash = 1; crash <= 2; crash++) {
                    child = this.launch(base, "phase2", "AFTER_RECOVERY_CHECKPOINT_READ",
                            ReplicationDurabilityMode.ARCHIVE_FIRST, false, false, livePort, controlPort, 2,
                            (int) checkpoint.transactionSequence());
                    this.await(base.resolve("control/milestone.reached"), child,
                            budget("crash.budget.milestone", 60_000L));
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
                    child = this.launch(base, "phase2", "NONE", ReplicationDurabilityMode.ARCHIVE_FIRST,
                            livePort, controlPort);
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

    private void assertReseed(final String point, final ReplicationDurabilityMode durability,
                              final boolean injectPrepareFailure) throws Exception {
        this.assertOutcome(point, durability, injectPrepareFailure, false, "RESEED_REQUIRED");
    }

    private void assertOutcome(final String point, final ReplicationDurabilityMode durability,
                               final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome) throws Exception {
        this.assertOutcome(point, durability, injectPrepareFailure, rejectLocal, expectedOutcome, null, 2, 1);
    }

    private void assertOutcome(final String point, final ReplicationDurabilityMode durability,
                               final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome,
                               final String runLabel, final int writes, final int targetSequence) throws Exception {
        this.assertOutcome(point, durability, injectPrepareFailure, rejectLocal, expectedOutcome,
                runLabel, writes, targetSequence,
                CrashPayloads.DEFAULT_SIZE, CrashPayloads.DEFAULT_KIND, 0);
    }

    private void assertOutcome(final String point, final ReplicationDurabilityMode durability,
                               final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome,
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
                    "point=%s durability=%s writes=%d target=%d payload=%d/%s budget=%d label=%s".formatted(
                            point, durability, writes, targetSequence, payloadSize, payloadKind, budgetChunks, runLabel));
            final int livePort = layout.livePort();
            final int controlPort = layout.controlPort();
            Process child = null;
            try {
                child = this.launch(base, "phase1", point, durability, injectPrepareFailure, rejectLocal,
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
                    child = this.launch(base, "phase2", "NONE", durability, false, false,
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

    private Process launch(final Path base, final String mode, final String point,
                           final ReplicationDurabilityMode durability, final int livePort, final int controlPort) throws IOException {
        return this.launch(base, mode, point, durability, false, false, livePort, controlPort, 2, 1,
                CrashPayloads.DEFAULT_SIZE, CrashPayloads.DEFAULT_KIND, 0);
    }

    private Process launch(final Path base, final String mode, final String point,
                           final ReplicationDurabilityMode durability, final boolean injectPrepareFailure,
                           final boolean rejectLocal, final int livePort, final int controlPort,
                           final int writes, final int targetSequence) throws IOException {
        return this.launch(base, mode, point, durability, injectPrepareFailure, rejectLocal,
                livePort, controlPort, writes, targetSequence,
                CrashPayloads.DEFAULT_SIZE, CrashPayloads.DEFAULT_KIND, 0);
    }

    private Process launch(final Path base, final String mode, final String point,
                           final ReplicationDurabilityMode durability, final boolean injectPrepareFailure,
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
        final ProcessBuilder builder = new ProcessBuilder(javaExecutable,
                "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.crash.base=%s".formatted(base),
                "-Ddg.crash.mode=%s".formatted(mode),
                "-Ddg.crash.barrier=%s".formatted(point),
                "-Ddg.crash.sequence=%s".formatted(targetSequence),
                "-Ddg.crash.writes=%s".formatted(writes),
                "-Ddg.crash.durability=%s".formatted(durability),
                "-Ddg.crash.injectPrepareFailure=%s".formatted(injectPrepareFailure),
                "-Ddg.crash.rejectLocal=%s".formatted(rejectLocal),
                "-Ddg.crash.rejectSequence=%s".formatted((rejectLocal ? 1 : -1)),
                "-Ddg.crash.payloadSize=%s".formatted(payloadSize),
                "-Ddg.crash.payloadKind=%s".formatted(payloadKind),
                "-Ddg.crash.budgetChunks=%s".formatted(budgetChunks),
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
            ReplicationDurabilityMode durability,
            boolean injectPrepareFailure,
            boolean rejectLocal,
            String expectedOutcome,
            int writes,
            int targetSequence
    ) {
        private CrashScenario(final String point, final ReplicationDurabilityMode durability,
                              final boolean injectPrepareFailure, final boolean rejectLocal, final String expectedOutcome) {
            this(point, durability, injectPrepareFailure, rejectLocal, expectedOutcome, 2, 1);
        }
    }

}
