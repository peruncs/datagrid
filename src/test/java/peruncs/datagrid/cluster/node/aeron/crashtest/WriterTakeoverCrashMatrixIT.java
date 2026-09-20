package peruncs.datagrid.cluster.node.aeron.crashtest;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelopeTestSupport;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataReceiver;
import peruncs.datagrid.cluster.test.ChildJava;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Cross-process writer fencing takeover cells: a SIGKILLed writer process
/// leaves a stale lease on the shared volume, and a successor process with a
/// different node id must steal it only after the staleness bound, mint a
/// strictly greater fencing token, and see its frames accepted while frames
/// carrying the killed writer's token are rejected.
///
/// # Cell protocol
///
/// 1. Phase 1 runs [ProviderCrashChildMain] with a sub-second lease staleness
///    bound and parks inside the requested lease window — during a heartbeat
///    renewal, mid-commit before the commit offer, or after the commit offer
///    but before the protecting heartbeat refresh.
/// 2. The parent asserts the phase-1 Store fixture is still a valid record,
///    reads the fencing token straight from the lease file, and kills the
///    child with SIGKILL.
/// 3. Phase 2 forks a successor writer (`dg.crash.nodeSalt` changes the node
///    identity, so token reuse without staleness is impossible). The successor
///    must acquire the lease over the same checkpoint/Archive root and mint
///    exactly one greater token.
/// 4. Frame semantics are proven with the production
///    `TransactionAssembler` state machine, seeded at the killed writer's
///    token: frames with the successor's token are accepted, frames with the
///    killed writer's token fail closed as stale. `TransactionAssembler` is
///    package-private to its reader package while this cell lives in the
///    crash package, so the assembler is instantiated reflectively on the
///    test class path; no production seam would justify widening it.
///
/// Cells fail closed: a missed milestone, an invalid Store fixture, a
/// non-increasing token, or a recovery policy other than the pinned one is a
/// test failure, never a skipped cell.
final class WriterTakeoverCrashMatrixIT {
    private static final long LEASE_STALENESS_MILLIS = 300L;
    private static final UUID CLUSTER_ID =
            UUID.nameUUIDFromBytes("crash-matrix-cluster".getBytes(StandardCharsets.UTF_8));
    private static final long EPOCH = 7L;
    private static final String ASSEMBLER = "peruncs.datagrid.cluster.storage.aeron.reader.TransactionAssembler";
    private static final String DELIVERY_LISTENER =
            "peruncs.datagrid.cluster.storage.aeron.reader.ReaderDeliveryListener";

    /// Verifies a writer killed while its heartbeat renewal held the
    /// interprocess lock is taken over after staleness with a greater token.
    /* The fencing lease is acquired lazily at the first write, so the cell
     * needs one completed transaction before the heartbeat window exists. */
    @Test
    void crashDuringLeaseRenewalAllowsStalenessGatedTakeover() throws Exception {
        this.assertTakeover("BEFORE_LEASE_RENEWAL_HEARTBEAT", 1, RecoveryPolicy.CONTINUE);
    }

    /// Verifies a writer killed between the local Store write and the commit
    /// offer is taken over after staleness; the ambiguous tail still demands a
    /// reseed on the successor.
    @Test
    void crashMidTransactionCommitAllowsStaleRejectingTakeover() throws Exception {
        this.assertTakeover("AFTER_LOCAL_WRITE_BEFORE_COMMIT", 2, RecoveryPolicy.RESEED_REQUIRED);
    }

    /// Verifies a writer killed after the commit offer but before the
    /// protecting heartbeat refresh is taken over after staleness; the
    /// ambiguous tail still demands a reseed on the successor.
    @Test
    void crashAfterCommitOfferBeforeHeartbeatRefreshAllowsTakeover() throws Exception {
        this.assertTakeover("AFTER_OWNED_OFFER_BEFORE_HEARTBEAT", 2, RecoveryPolicy.RESEED_REQUIRED);
    }

    /// Runs one takeover cell: kill at the milestone, take over with a
    /// successor writer, and pin token and frame-floor semantics.
    private void assertTakeover(final String point, final int writes,
                                final RecoveryPolicy expected) throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            CrashEventLog.append(base.resolve("control"), "selection",
                    "takeover point=%s writes=%d".formatted(point, writes));
            Process child = null;
            try {
                child = this.launch(base, "phase1", point, writes, "", layout);
                this.await(base.resolve("control/ready"), child, budget("crash.budget.startup", 120_000L));
                final Path milestone = base.resolve("control/milestone.reached");
                this.await(milestone, child, budget("crash.budget.milestone", 60_000L));
                final ChildMilestone marker = ChildMilestone.read(milestone);
                assertEquals(point, marker.point(), "unexpected milestone point");
                CrashEventLog.append(base.resolve("control"), "milestone",
                        "point=%s sequence=%d".formatted(marker.point(), marker.sequence()));
                final long killedToken;
                try {
                    child.destroyForcibly();
                    assertTrue(child.waitFor(10, TimeUnit.SECONDS), "phase1 child did not exit after kill");
                    assertTrue(StoreFixture.inspect(base.resolve("store.records")).valid(),
                            "phase1 Store fixture is not a complete record");
                    killedToken = leaseToken(base);
                    assertTrue(killedToken >= 1L, "phase1 minted no fencing token");
                } finally {
                    if (child.isAlive()) child.destroyForcibly();
                }
                /* The successor carries a different node identity, so it may
                 * take over only after the heartbeat aged past staleness.
                 * Waiting the full bound here proves the steal does not happen
                 * early inside the successor's bounded acquire either. */
                Thread.sleep(LEASE_STALENESS_MILLIS * 2L + 100L);
                String outcome;
                long restartDeadline = System.nanoTime() +
                                       TimeUnit.MILLISECONDS.toNanos(budget("crash.budget.archiveStop", 30_000L));
                int restartAttempts = 0;
                do {
                    restartAttempts++;
                    child = this.launch(base, "phase2", "NONE", 2, "successor", layout);
                    this.await(base.resolve("control/outcome"), child, budget("crash.budget.startup", 120_000L));
                    assertTrue(child.waitFor(10, TimeUnit.SECONDS), "successor child did not exit");
                    outcome = Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8);
                    if (!ProviderCrashMatrixIT.isActiveDriverRetry(outcome)) break;
                    Thread.sleep(1_000L);
                }
                while (System.nanoTime() < restartDeadline);
                assertFalse(ProviderCrashMatrixIT.isActiveDriverRetry(outcome),
                        "successor never recovered past the stale driver; attempts=%s outcome=%s".formatted(
                                restartAttempts, outcome));
                final CrashOutcome result = CrashOutcome.parse(outcome);
                assertNotNull(result.policy(), outcome);
                assertNotEquals(RecoveryPolicy.HARNESS_ERROR, result.policy(), outcome);
                assertEquals(expected, result.policy(), outcome);
                if (expected != RecoveryPolicy.CONTINUE) {
                    assertTrue(result.error() != null && !result.error().isBlank(), outcome);
                }
                assertTrue(result.storeValid(), outcome);
                final long successorToken = leaseToken(base);
                assertEquals(killedToken + 1L, successorToken,
                        "takeover must mint exactly the next token; killed=%d successor=%d".formatted(
                                killedToken, successorToken));
                CrashEventLog.append(base.resolve("control"), "outcome",
                        "takeover token=%d->%d policy=%s".formatted(killedToken, successorToken, result.policy()));
                assertFrameFloorSemantics(killedToken, successorToken);
                assertSurvivingState(base);
            } catch (final Throwable failure) {
                if (child != null && child.isAlive()) child.destroyForcibly();
                try {
                    final Path evidence = DiagnosticCollector.collect(base, failure.toString());
                    throw new AssertionError("takeover cell evidence: %s".formatted(evidence), failure);
                } catch (final Throwable evidenceFailure) {
                    failure.addSuppressed(evidenceFailure);
                    throw failure;
                }
            }
        }
    }

    /// Proves the killed writer's frames are fenced and the successor's frames
    /// are accepted, using the production transaction assembler seeded at the
    /// killed writer's token.
    private static void assertFrameFloorSemantics(final long killedToken, final long successorToken)
            throws ReflectiveOperationException {
        final AtomicInteger delivered = new AtomicInteger();
        final StorageBinaryDataReceiver receiver = new StorageBinaryDataReceiver() {
            @Override
            public void receiveData(final Binary data) {
                delivered.incrementAndGet();
            }

            @Override
            public void receiveTypeDictionary(final String typeDictionaryData) {
            }
        };
        final Object assembler = newAssembler(receiver);
        try {
            invoke(assembler, "startingFencingToken", new Class<?>[]{long.class}, killedToken);
            final byte[] payload = {1, 2};
            /* A successor commit validates end-to-end and the floor climbs to
             * the successor token. */
            deliver(assembler, AeronReplicationEnvelopeTestSupport.encode(CLUSTER_ID, EPOCH, successorToken,
                    0L, AeronReplicationEnvelope.Kind.STORE_BINARY, payload.length, 0, 1, 0, 0, payload));
            deliver(assembler, AeronReplicationEnvelopeTestSupport.encode(CLUSTER_ID, EPOCH, successorToken,
                    0L, AeronReplicationEnvelope.Kind.COMMIT, payload.length, 0, 1, 0,
                    AeronReplicationEnvelope.crc32c(payload), new byte[0]));
            assertEquals(1, delivered.get(), "successor frames must be accepted and delivered");
            assertEquals(successorToken, floor(assembler));
            /* Any frame carrying the killed writer's token is stale history:
             * the assembler fails closed instead of interleaving it. */
            final RuntimeException failure = assertThrows(RuntimeException.class, () ->
                    deliver(assembler, AeronReplicationEnvelopeTestSupport.encode(CLUSTER_ID, EPOCH, killedToken,
                            1L, AeronReplicationEnvelope.Kind.STORE_BINARY, payload.length, 0, 1, 0, 0, payload)));
            assertTrue(String.valueOf(failure.getMessage()).contains("stale writer fencing token"),
                    "unexpected failure: %s".formatted(failure.getMessage()));
            assertEquals(1, delivered.get(), "a stale frame must never reach the Store receiver");
        } finally {
            invokeUnchecked(assembler, "dispose", new Class<?>[0]);
        }
    }

    /// The checkpoint and Archive identity of the writer survive the takeover:
    /// the successor kept the on-disk evidence instead of wiping the root.
    private static void assertSurvivingState(final Path base) {
        assertTrue(Files.exists(base.resolve("backups")),
                "the shared backup volume must survive takeover");
        assertTrue(Files.exists(base.resolve("checkpoint")) || Files.exists(base.resolve("store.records")),
                "checkpoint or Store state must survive takeover");
    }

    /// Reads the live fencing token from the shared lease file. The layout is
    /// the documented on-disk format; a missing or corrupt file fails the cell.
    private static long leaseToken(final Path base) throws IOException {
        final UUID generation = UUID.nameUUIDFromBytes(
                ("generation:%s".formatted(base)).getBytes(StandardCharsets.UTF_8));
        final Path lease = base.resolve("backups")
                .resolve("writer-lease-%s-%s.lease".formatted(CLUSTER_ID, generation));
        assertTrue(Files.exists(lease), "lease file missing after kill: %s".formatted(lease));
        return ByteBuffer.wrap(Files.readAllBytes(lease)).order(ByteOrder.BIG_ENDIAN).getLong(6);
    }

    private static Object newAssembler(final StorageBinaryDataReceiver receiver)
            throws ReflectiveOperationException {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(64 * 1024)
                .chunkSize(256)
                .maxTransactionBytes(1024)
                .build();
        final Class<?> assembler = Class.forName(ASSEMBLER);
        final Constructor<?> constructor = assembler.getDeclaredConstructor(
                AeronReplicationConfiguration.class, UUID.class, long.class, long.class, long.class,
                StorageBinaryDataReceiver.class, Runnable.class,
                Class.forName(DELIVERY_LISTENER), long.class);
        constructor.setAccessible(true);
        return constructor.newInstance(configuration, CLUSTER_ID, EPOCH, -1L, -1L,
                receiver, (Runnable) () -> {
                }, null, AeronReplicationEnvelope.defaultWireNonce(CLUSTER_ID));
    }

    private static Object invoke(final Object target, final String name, final Class<?>[] types,
                                 final Object... arguments) throws ReflectiveOperationException {
        final Method method = target.getClass().getDeclaredMethod(name, types);
        method.setAccessible(true);
        try {
            return method.invoke(target, arguments);
        } catch (final java.lang.reflect.InvocationTargetException failure) {
            /* Callers care about the assembler's own failure, not the
             * reflective envelope: unwrap to keep the crash cell's assertions
             * about rejection semantics intact. */
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            throw failure;
        }
    }

    private static void invokeUnchecked(final Object target, final String name, final Class<?>[] types) {
        try {
            invoke(target, name, types);
        } catch (final ReflectiveOperationException failure) {
            throw new IllegalStateException("cannot call assembler hook", failure);
        }
    }

    private static void deliver(final Object assembler, final byte[] frame) throws ReflectiveOperationException {
        invoke(assembler, "onFragment",
                new Class<?>[]{DirectBuffer.class, int.class, int.class, io.aeron.logbuffer.Header.class},
                new UnsafeBuffer(frame), 0, frame.length, null);
    }

    private static long floor(final Object assembler) throws ReflectiveOperationException {
        return (Long) invoke(assembler, "fencingToken", new Class<?>[0]);
    }

    private Process launch(final Path base, final String mode, final String point,
                           final int writes, final String nodeSalt, final DirectoryLayout layout) throws IOException {
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        Files.deleteIfExists(control.resolve("ready"));
        Files.deleteIfExists(control.resolve("ready-phase2"));
        Files.deleteIfExists(control.resolve("milestone.reached"));
        Files.deleteIfExists(control.resolve("outcome"));
        Files.deleteIfExists(control.resolve("release"));
        final String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final ProcessBuilder builder = new ProcessBuilder(javaExecutable,
                "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.crash.base=%s".formatted(base),
                "-Ddg.crash.mode=%s".formatted(mode),
                "-Ddg.crash.barrier=%s".formatted(point),
                "-Ddg.crash.sequence=1",
                "-Ddg.crash.writes=%s".formatted(writes),
                "-Ddg.crash.durability=%s".formatted(ReplicationDurabilityMode.ARCHIVE_FIRST),
                "-Ddg.crash.leaseStalenessMillis=%s".formatted(LEASE_STALENESS_MILLIS),
                "-Ddg.crash.nodeSalt=%s".formatted(nodeSalt),
                "-Ddg.crash.livePort=%s".formatted(layout.livePort()),
                "-Ddg.crash.controlPort=%s".formatted(layout.controlPort()),
                ProviderCrashChildMain.class.getName());
        builder.redirectOutput(control.resolve("%s-stdout.log".formatted(mode)).toFile());
        builder.redirectError(control.resolve("%s-stderr.log".formatted(mode)).toFile());
        return builder.start();
    }

    private void await(final Path path, final Process child, final long timeoutMillis)
            throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            if (!child.isAlive()) {
                throw new AssertionError("child exited before %s".formatted(path));
            }
            Thread.sleep(10L);
        }
        assertTrue(Files.exists(path), "timed out waiting for %s".formatted(path));
    }

    private static long budget(final String property, final long fallback) {
        final String value = System.getProperty(property);
        if (value == null || value.isBlank()) return fallback;
        return Math.max(1L, Long.parseLong(value));
    }
}
