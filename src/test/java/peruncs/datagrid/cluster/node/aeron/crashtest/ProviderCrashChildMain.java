package peruncs.datagrid.cluster.node.aeron.crashtest;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.datagrid.cluster.node.aeron.AeronCrashHooks;
import peruncs.datagrid.cluster.node.aeron.AeronTransport;
import peruncs.datagrid.cluster.node.aeron.TestNodeProperties;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.CheckpointJournalCrashHooks;
import peruncs.datagrid.cluster.storage.aeron.crashtest.CrashPayloads;
import peruncs.datagrid.cluster.storage.binary.ReplicationPublisher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.zip.CRC32C;

/// Forked provider process for deterministic writer crash cells. The parent
/// kills this process after the exact milestone; this class never self-kills.
public final class ProviderCrashChildMain {
    private static final long SUBSCRIBER_DRIVER_TIMEOUT_MILLIS = 1_000L;
    private static final long SUBSCRIBER_STARTUP_TIMEOUT_MILLIS = 60_000L;
    private static final java.util.Set<String> SUPPORTED_POINTS = java.util.Set.of(
            "BEFORE_PUBLICATION_CONNECTED", "BEFORE_PREPARE", "AFTER_DICTIONARY_CHUNKS", "AFTER_DATA_CHUNKS", "AFTER_PREPARE",
            "AFTER_PREPARE_BEFORE_LOCAL_WRITE", "AFTER_LOCAL_WRITE_BEFORE_COMMIT",
            "AFTER_PREPARE_FAILURE_ABORT_OFFERED",
            "BEFORE_COMMIT_OFFER", "AFTER_COMMIT_OFFER", "AFTER_COMMIT_RECORDED",
            "AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", "AFTER_ABORT_OFFERED",
            "BEFORE_JOURNAL_SLOT_WRITE", "DURING_JOURNAL_SLOT_WRITE", "AFTER_JOURNAL_SLOT_FORCE",
            "AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
            "AFTER_RECOVERY_CHECKPOINT_READ", "NONE");
    /// Fencing-lease windows whose `sequence` carries the lease token, not a
    /// transaction sequence; they match by name only and may fire before the
    /// first transaction or on the heartbeat thread.
    private static final java.util.Set<String> LEASE_POINTS = java.util.Set.of(
            "BEFORE_LEASE_RENEWAL_HEARTBEAT", "AFTER_OWNED_OFFER_BEFORE_HEARTBEAT");
    private static final String STREAM = "crash-matrix";
    private static final String WRITER_ROLE = "writer";
    private static final int STREAM_ID = 1001;

    private ProviderCrashChildMain() {
    }

    static void main(final String[] arguments) throws Exception {
        final String baseProperty = System.getProperty("dg.crash.base");
        if (baseProperty == null || baseProperty.isBlank()) throw new IllegalArgumentException("missing -Ddg.crash.base");
        final Path base = Path.of(baseProperty).toAbsolutePath().normalize();
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        final String mode = System.getProperty("dg.crash.mode", "phase1");
        final String point = System.getProperty("dg.crash.barrier", "NONE");
        if (!SUPPORTED_POINTS.contains(point) && !LEASE_POINTS.contains(point) ||
            (!"NONE".equals(point) && !ChildMilestone.supports(point))) {
            throw new IllegalArgumentException("unsupported or unencodable provider crash point: %s".formatted(point));
        }
        if ("phase1".equals(mode)) {
            runPhase1(base, control, point);
            return;
        }
        if ("phase2".equals(mode)) {
            runPhase2(base, control, point);
            return;
        }
        throw new IllegalArgumentException("unknown dg.crash.mode: %s".formatted(mode));
    }

    private static void runPhase1(final Path base, final Path control, final String point) {
        try (final ChildRuntime runtime = new ChildRuntime(base, control, point)) {
            runtime.start();
            mark(control.resolve("ready"), "ready");
            final int writes = Integer.getInteger("dg.crash.writes", 2);
            if (writes < 0 || writes > 2) throw new IllegalArgumentException("dg.crash.writes must be 0, 1, or 2");
            for (int sequence = 0; sequence < writes; sequence++) {
                runtime.write(transactionPayload(sequence));
            }
            /* Lease renewal parks the heartbeat thread, not the write thread.
             * Wait for that milestone here and then park the whole process, so
             * the parent's kill provably lands while the renewal is held
             * instead of a racing clean close. */
            if (LEASE_POINTS.contains(point)) {
                final long leaseDeadline = System.nanoTime() +
                        java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(60_000L);
                while (!Files.exists(control.resolve("milestone.reached")) &&
                       System.nanoTime() < leaseDeadline) {
                    sleep();
                }
                awaitParent(control.resolve("release"));
                return;
            }
            if (!"NONE".equals(point) && !Files.exists(control.resolve("milestone.reached"))) {
                writeOutcome(control, "HARNESS_ERROR", runtime.checkpoint(), null,
                        "crash barrier was armed but never reached: %s".formatted(point));
                return;
            }
            writeOutcome(control, "CONTINUE", runtime.checkpoint(), null, null);
        }
    }

    private static void runPhase2(final Path base, final Path control, final String point) {
        final ChildRuntime runtime = new ChildRuntime(base, control, point);
        try (runtime) {
            runtime.start();
            mark(control.resolve("ready-phase2"), "ready-phase2");
            /* A startup retry must be idempotent even if a future provider reports
             * its active-driver refusal after the follow-up write. */
            if (!containsStorePayload(base.resolve("store.records"), transactionPayload(2))) {
                runtime.write(transactionPayload(2));
            }
            writeOutcome(control, "CONTINUE", runtime.checkpoint(), null, null);
        } catch (final RuntimeException | Error failure) {
            final String message = failure.getMessage() == null ? failure.toString() : failure.getMessage();
            final String outcome = message.startsWith("RESEED_REQUIRED:")
                    ? "RESEED_REQUIRED" : "FAIL_CLOSED";
            writeOutcome(control, outcome, runtime.checkpoint(), failure, message);
        }
    }

    private static boolean containsStorePayload(final Path path, final byte[] payload) {
        try {
            return StoreFixture.contains(path, payload);
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot inspect phase-2 Store fixture", failure);
        }
    }

    private static byte[] transactionPayload(final int sequence) {
        return CrashPayloads.sized(sequence,
                Integer.getInteger("dg.crash.payloadSize", CrashPayloads.DEFAULT_SIZE),
                System.getProperty("dg.crash.payloadKind", CrashPayloads.DEFAULT_KIND));
    }

    private static void writeOutcome(final Path control, final String outcome,
                                     final AeronReplicationCheckpoint checkpoint,
                                     final Throwable failure, final String error) {
        final StringBuilder value = new StringBuilder()
                .append("ROLE=writer\n")
                .append("PID=").append(ProcessHandle.current().pid()).append('\n')
                .append("HEALTH=").append(health(outcome)).append('\n');
        if (checkpoint != null) {
            value.append("CHECKPOINT_STATE=").append(checkpoint.state()).append('\n')
                    .append("SEQUENCE=").append(checkpoint.transactionSequence()).append('\n')
                    .append("RECORDING_ID=").append(checkpoint.recordingId()).append('\n')
                    .append("RECORDING_POSITION=").append(checkpoint.recordingPosition()).append('\n')
                    .append("CRC32C=").append(Integer.toUnsignedString(checkpoint.resolutionCrc32c())).append('\n');
        }
        if (error != null) value.append("ERROR=").append(error.replace('\n', ' ')).append('\n');
        /* The parent classifies retryable startup failures by type, never by
         * message text: an upstream wording change cannot silently turn a real
         * defect into retried noise. The full cause chain goes out unchanged. */
        for (Throwable current = failure; current != null; current = current.getCause()) {
            value.append("ERROR_TYPE=").append(current.getClass().getName()).append('\n');
        }
        value.append("PROOF_STORE_VALID=").append(storeFixtureValid(control.getParent().resolve("store.records"))).append(System.lineSeparator());
        value.append("PROOF_CRC_TXN0=").append(Integer.toUnsignedString(crc(transactionPayload(0)))).append(System.lineSeparator())
                .append("PROOF_CRC_TXN1=").append(Integer.toUnsignedString(crc(transactionPayload(1)))).append(System.lineSeparator())
                .append("PROOF_CRC_TXN2=").append(Integer.toUnsignedString(crc(transactionPayload(2)))).append(System.lineSeparator());
        if (checkpoint != null) value.append("PROOF_COMMITTED=").append(checkpoint.transactionSequence()).append(System.lineSeparator());
        value.append("OUTCOME=").append(outcome).append(System.lineSeparator());
        atomicWrite(control.resolve("outcome"), value.toString());
    }

    private static String health(final String outcome) {
        return switch (outcome) {
            case "CONTINUE" -> "LIVE";
            case "RESEED_REQUIRED" -> "RESEED_REQUIRED";
            case "HARNESS_ERROR" -> "HARNESS_ERROR";
            default -> "FAILED";
        };
    }

    private static boolean storeFixtureValid(final Path path) {
        if (!Files.exists(path)) return true;
        try {
            final byte[] bytes = Files.readAllBytes(path);
            int offset = 0;
            while (offset < bytes.length) {
                if (bytes.length - offset < Integer.BYTES * 2) return false;
                final int length = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
                final int expected = ByteBuffer.wrap(bytes, offset + Integer.BYTES, Integer.BYTES).getInt();
                if (length < 0 || bytes.length - offset - Integer.BYTES * 2 < length) return false;
                if (crc(bytes, offset + Integer.BYTES * 2, length) != expected) return false;
                offset += Integer.BYTES * 2 + length;
            }
            return offset == bytes.length;
        } catch (final IOException | RuntimeException failure) {
            return false;
        }
    }

    private static int crc(final byte[] bytes) {
        return crc(bytes, 0, bytes.length);
    }

    private static int crc(final byte[] bytes, final int offset, final int length) {
        final CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    private static void mark(final Path path, final String value) {
        atomicWrite(path, "%s\n".formatted(value));
    }

    private static void atomicWrite(final Path destination, final String value) {
        try {
            Files.createDirectories(destination.toAbsolutePath().getParent());
            final Path temporary = Files.createTempFile(destination.getParent(), "%s.tmp-".formatted(destination.getFileName()), null);
            try {
                try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    final ByteBuffer bytes = StandardCharsets.UTF_8.encode(value);
                    writeFully(channel, bytes);
                    channel.force(true);
                }
                Files.move(temporary, destination, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                try (FileChannel directory = FileChannel.open(destination.getParent(), StandardOpenOption.READ)) {
                    directory.force(true);
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot write crash-matrix control file %s".formatted(destination), failure);
        }
    }

    private static void writeFully(final FileChannel channel, final ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) == 0) Thread.onSpinWait();
        }
    }

    private static BiConsumer<String, Long> crashHook(final Path control, final String point) {
        final long targetSequence = Long.getLong("dg.crash.sequence", 1L);
        /* Chunk-budget fuzz: kill the child after the Nth published data
         * chunk instead of at an exact milestone, covering interleavings the
         * enumeration misses (including mid-transaction, between two chunks
         * of one large payload). The milestone reuses AFTER_DATA_CHUNKS: the
         * kill really lands after data chunks, just not at the enumerated one. */
        final int budgetChunks = Integer.getInteger("dg.crash.budgetChunks", 0);
        final java.util.concurrent.atomic.AtomicInteger chunkCount = new java.util.concurrent.atomic.AtomicInteger();
        return (name, sequence) ->
        {
            /* Lease windows carry the fencing token as their `sequence` and
             * can fire on the heartbeat thread or before any transaction; the
             * target-transaction filter does not apply to them. */
            if (LEASE_POINTS.contains(point)) {
                if (point.equals(name)) {
                    writeMilestone(control.resolve("milestone.reached"), name, sequence);
                    awaitParent(control.resolve("release"));
                }
                return;
            }
            if ("DATA_CHUNK".equals(name)) {
                if (budgetChunks > 0 && chunkCount.incrementAndGet() == budgetChunks) {
                    writeMilestone(control.resolve("milestone.reached"), "AFTER_DATA_CHUNKS", sequence);
                    awaitParent(control.resolve("release"));
                }
                return;
            }
            if (sequence != targetSequence &&
                !(sequence == -1L && "BEFORE_PUBLICATION_CONNECTED".equals(point))) return;
            if ("AFTER_DATA_CHUNKS".equals(name) && Boolean.getBoolean("dg.crash.injectPrepareFailure")) {
                throw new IllegalStateException("injected prepare failure after data chunks");
            }
            if (!point.equals(name)) return;
            writeMilestone(control.resolve("milestone.reached"), name, sequence);
            awaitParent(control.resolve("release"));
        };
    }

    private static BiConsumer<String, Path> journalHook(final Path control, final String point) {
        final long targetSequence = Long.getLong("dg.crash.sequence", 1L);
        return (phase, path) ->
        {
            /* The in-flight fence is not the terminal checkpoint targeted by
             * these cells. */
            if (path.getFileName().toString().endsWith(".inflight")) return;
            if (!phase.equals(point)) return;
            final long sequence = checkpointSequence();
            if (sequence != targetSequence) return;
            writeMilestone(control.resolve("milestone.reached"), phase, sequence);
            awaitParent(control.resolve("release"));
        };
    }

    private static long checkpointSequence() {
        try {
            return AeronCrashHooks.currentCheckpointSequence();
        } catch (final RuntimeException failure) {
            throw new IllegalStateException("cannot read provider checkpoint crash context", failure);
        }
    }

    private static void writeMilestone(final Path path, final String point, final long sequence) {
        try {
            ChildMilestone.write(path, point, sequence);
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot write crash milestone %s".formatted(path), failure);
        }
    }

    private static void awaitParent(final Path release) {
        while (!Files.exists(release)) sleep();
    }

    private static void sleep() {
        try {
            Thread.sleep(10L);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("crash child interrupted", interrupted);
        }
    }

    private static String writerLiveChannel() {
        final int port = Integer.getInteger("dg.crash.livePort", 40123);
        return Boolean.getBoolean("dg.crash.externalArchive")
                ? "aeron:udp?endpoint=localhost:%s".formatted(port)
                : "aeron:udp?control=localhost:%s|control-mode=dynamic|fc=max".formatted(port);
    }

    private static String subscriberLiveChannel() {
        return "aeron:udp?endpoint=localhost:0|control=localhost:%s|control-mode=dynamic".formatted(Integer.getInteger("dg.crash.livePort", 40123));
    }

    private static final class ChildRuntime implements AutoCloseable {
        private final Path base;
        private final String barrierPoint;
        private final BiConsumer<String, Long> crashHook;
        private final BiConsumer<String, Path> journalHook;
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean subscriberReady = new AtomicBoolean();
        private final AtomicBoolean runtimeReady = new AtomicBoolean();
        private ClusterReplicationTransport transport;
        private Thread subscriberThread;
        private volatile Throwable subscriberFailure;
        private int writes;

        private ChildRuntime(final Path base, final Path control, final String barrierPoint) {
            this.base = base;
            this.barrierPoint = barrierPoint;
            this.crashHook = crashHook(control, barrierPoint);
            this.journalHook = journalHook(control, barrierPoint);
        }

        private void start() {
            AeronCrashHooks.runWithHook(this.crashHook, () ->
                    CheckpointJournalCrashHooks.runWithHook(this.journalHook, this::startInternal));
        }

        private void startInternal() {
            this.transport = new AeronTransport(new ChildProperties(this.base));
            /* The subscriber must be allowed to connect before the writer factory
             * waits for the Archive recording to become active. Waiting for the
             * persistence target first creates a circular startup dependency. */
            this.runtimeReady.set(true);
            // With a separate Archive the archive's remote subscription is the
            // publication's reader. A second local subscription would bind the same
            // UDP endpoint and make the external recording impossible to start.
            if (Boolean.parseBoolean(System.getProperty("dg.crash.subscriber", "true")) &&
                !Boolean.getBoolean("dg.crash.externalArchive")) {
                this.startSubscriber();
            }
        }

        private void startSubscriber() {
            this.running.set(true);
            this.subscriberThread = Thread.ofVirtual().name("crash-matrix-subscriber").unstarted(
                    AeronCrashHooks.inheritCurrent(() ->
            {
                while (this.running.get() && !this.runtimeReady.get()) sleep();
                if (!this.running.get()) return;
                final Path aeronDir = this.base.resolve(
                        Boolean.getBoolean("dg.crash.externalArchive") ? "writer-aeron" : "aeron");
                final long deadline = System.nanoTime() +
                                      java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(SUBSCRIBER_STARTUP_TIMEOUT_MILLIS);
                Throwable lastFailure = null;
                while (this.running.get() && !this.subscriberReady.get() && System.nanoTime() < deadline) {
                    while (this.running.get() && !Files.exists(aeronDir.resolve("cnc.dat")) &&
                           System.nanoTime() < deadline) sleep();
                    if (!this.running.get() || System.nanoTime() >= deadline) return;
                    this.subscriberFailure = null;
                    try (io.aeron.Aeron aeron = io.aeron.Aeron.connect(
                            new io.aeron.Aeron.Context()
                                    .aeronDirectoryName(aeronDir.toString())
                                    .driverTimeoutMs(SUBSCRIBER_DRIVER_TIMEOUT_MILLIS)
                                    .errorHandler(failure -> this.subscriberFailure = failure));
                         io.aeron.Subscription subscription = aeron.addSubscription(subscriberLiveChannel(), STREAM_ID)) {
                        /* Aeron.connect can map a stale cnc.dat before its conductor
                         * discovers that the old driver was killed. Give that
                         * conductor one timeout window before publishing readiness. */
                        final long handshakeDeadline = System.nanoTime() +
                                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                                        SUBSCRIBER_DRIVER_TIMEOUT_MILLIS + 100L);
                        while (this.running.get() && this.subscriberFailure == null &&
                               System.nanoTime() < handshakeDeadline) sleep();
                        if (!this.running.get()) return;
                        if (isDriverTimeout(this.subscriberFailure)) {
                            lastFailure = this.subscriberFailure;
                            continue;
                        }
                        if (this.subscriberFailure != null) {
                            throw new IllegalStateException("subscriber conductor failed", this.subscriberFailure);
                        }
                        this.subscriberReady.set(true);
                        while (this.running.get() && this.subscriberFailure == null) {
                            subscription.poll((buffer, offset, length, header) -> {
                            }, 50);
                        }
                        if (isDriverTimeout(this.subscriberFailure)) {
                            lastFailure = this.subscriberFailure;
                            this.subscriberReady.set(false);
                            continue;
                        }
                        if (this.subscriberFailure != null) {
                            throw new IllegalStateException("subscriber conductor failed", this.subscriberFailure);
                        }
                        return;
                    } catch (final io.aeron.exceptions.DriverTimeoutException timeout) {
                        lastFailure = timeout;
                        this.subscriberReady.set(false);
                        if (this.running.get()) sleep();
                    } catch (final Throwable failure) {
                        this.subscriberFailure = failure;
                        return;
                    }
                }
                if (this.running.get() && !this.subscriberReady.get()) {
                    this.subscriberFailure = new IllegalStateException(
                            "subscriber did not connect before restart timeout", lastFailure);
                }
            }));
            this.subscriberThread.start();
        }

        private static boolean isDriverTimeout(final Throwable failure) {
            for (Throwable current = failure; current != null; current = current.getCause()) {
                if (current instanceof io.aeron.exceptions.DriverTimeoutException) return true;
            }
            return false;
        }

        private void write(final byte[] payload) {
            AeronCrashHooks.runWithHook(this.crashHook, () ->
                    CheckpointJournalCrashHooks.runWithHook(this.journalHook, () -> this.writeInternal(payload)));
        }

        private void writeInternal(final byte[] payload) {
            if (this.subscriberFailure != null && !isDriverTimeout(this.subscriberFailure))
                throw new IllegalStateException("subscriber failed: %s".formatted(this.subscriberFailure), this.subscriberFailure);
            final ReplicationPublisher distributor = this.transport.distributor(STREAM);
            if (this.writes > 0 && "AFTER_DICTIONARY_CHUNKS".equals(this.barrierPoint)) {
                distributor.distributeTypeDictionary("crash.Type");
            }
            final PersistenceTarget<Binary> target = this.transport.persistenceTargetFactory(STREAM, distributor)
                    .apply(new FileStoreTarget(this.base.resolve("store.records"), this.writes));
            this.awaitSubscriber();
            final Binary binary = ChunksWrapper.New(XMemory.toDirectByteBuffer(payload));
            target.write(binary);
            this.writes++;
        }

        private void awaitSubscriber() {
            if (this.subscriberThread == null) return;
            final long deadline = System.nanoTime() +
                                  java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(SUBSCRIBER_STARTUP_TIMEOUT_MILLIS);
            while (!this.subscriberReady.get() && System.nanoTime() < deadline) {
                if (this.subscriberFailure != null && !isDriverTimeout(this.subscriberFailure)) {
                    throw new IllegalStateException("subscriber failed: %s".formatted(this.subscriberFailure),
                            this.subscriberFailure);
                }
                sleep();
            }
            if (this.subscriberFailure != null && !isDriverTimeout(this.subscriberFailure))
                throw new IllegalStateException("subscriber failed: %s".formatted(this.subscriberFailure), this.subscriberFailure);
            if (!this.subscriberReady.get()) {
                throw new IllegalStateException("subscriber did not start", this.subscriberFailure);
            }
        }

        private AeronReplicationCheckpoint checkpoint() {
            final Path path = this.base.resolve("checkpoint/writer.checkpoint");
            try {
                return Files.exists(path) ? AeronReplicationCheckpointStore.read(path) : null;
            } catch (final IOException failure) {
                return null;
            }
        }

        @Override
        public void close() {
            this.running.set(false);
            if (this.subscriberThread != null) {
                try {
                    this.subscriberThread.join(2_000L);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (this.transport != null) this.transport.close();
        }
    }

    private record FileStoreTarget(Path path, int sequence) implements PersistenceTarget<Binary> {
        @Override
        public void write(final Binary data) {
            try {
                final int rejectedSequence = Integer.getInteger("dg.crash.rejectSequence", -1);
                if (Boolean.getBoolean("dg.crash.rejectLocal") && this.sequence == rejectedSequence) {
                    throw new IllegalStateException("injected local Store rejection at sequence %s".formatted(this.sequence));
                }
                Files.createDirectories(this.path.toAbsolutePath().getParent());
                try (FileChannel channel = FileChannel.open(this.path, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                    final ByteBuffer[] buffers = data.buffers();
                    final CRC32C crc = new CRC32C();
                    int length = 0;
                    for (final ByteBuffer source : buffers) {
                        final ByteBuffer duplicate = source.duplicate();
                        length += duplicate.remaining();
                        crc.update(duplicate);
                    }
                    final ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 2)
                            .putInt(length).putInt((int) crc.getValue()).flip();
                    writeFully(channel, header);
                    for (final ByteBuffer source : buffers) {
                        final ByteBuffer duplicate = source.duplicate();
                        writeFully(channel, duplicate);
                    }
                    channel.force(true);
                }
            } catch (final IOException failure) {
                throw new IllegalStateException("cannot append crash-matrix Store fixture", failure);
            }
        }

        @Override
        public boolean isWritable() {
            return true;
        }
    }

    private static final class ChildProperties extends TestNodeProperties {
        private final Path base;
        private final boolean externalArchive;
        private final UUID clusterId = UUID.nameUUIDFromBytes("crash-matrix-cluster".getBytes(StandardCharsets.UTF_8));
        private final UUID nodeId;
        private final UUID generation;

        private ChildProperties(final Path base) {
            this.base = base;
            this.externalArchive = Boolean.getBoolean("dg.crash.externalArchive");
            /* A takeover successor deliberately carries a different node id:
             * while node identity is the fencing principal, a crashed writer
             * may reclaim its own lease without waiting for staleness. The
             * successor process must instead steal it after the staleness
             * bound, proving the cross-process takeover path. */
            final String nodeSalt = System.getProperty("dg.crash.nodeSalt", "");
            this.nodeId = UUID.nameUUIDFromBytes(
                    ("node:%s%s".formatted(base, nodeSalt)).getBytes(StandardCharsets.UTF_8));
            this.generation = UUID.nameUUIDFromBytes(("generation:%s".formatted(base)).getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public Long writerLeaseStalenessMillis() {
            /* Takeover cells run the lease heartbeat on a sub-second
             * staleness bound so a successor can take over within the cell
             * budget; other cells leave the production default standing. */
            return Long.getLong("dg.crash.leaseStalenessMillis");
        }

        @Override
        public String replicationStreamName() {
            return STREAM;
        }

        @Override
        public String replicationTransport() {
            return "aeron";
        }

        @Override
        public String replicationRole() {
            return WRITER_ROLE;
        }

        @Override
        public Integer storageLimitCheckerIntervalMinutes() {
            return 1;
        }

        @Override
        public Integer storageLimitGB() {
            return 1;
        }

        @Override
        public Long dataMergerTimeoutMs() {
            return 5_000L;
        }

        @Override
        public Long dataMergerCachedDataLimit() {
            return 1024L * 1024L;
        }

        @Override
        public String replicationProperty(final String name) {
            return switch (name) {
                case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> this.clusterId.toString();
                case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> this.nodeId.toString();
                case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> this.generation.toString();
                case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> this.base.resolve(
                        this.externalArchive ? "writer-aeron" : "aeron").toString();
                case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> this.base.resolve("archive").toString();
                case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> this.base.resolve("checkpoint/writer.checkpoint").toString();
                case "ECLIPSE_DATAGRID_BACKUP_PATH" -> this.base.resolve("backups").toString();
                case "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL" -> writerLiveChannel();
                case "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL" -> "aeron:udp?endpoint=localhost:%s".formatted(Integer.getInteger("dg.crash.controlPort", 40124));
                case "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                     "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL" -> "aeron:udp?endpoint=localhost:0";
                case "ECLIPSE_DATAGRID_AERON_TERM_LENGTH" -> Integer.toString(
                        Integer.getInteger("dg.crash.termLength", 1048576));
                case "ECLIPSE_DATAGRID_AERON_MTU_LENGTH" -> "1024";
                case "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE" -> Integer.toString(
                        Integer.getInteger("dg.crash.chunkSize", 16384));
                case "ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES" -> "262144";
                case "ECLIPSE_DATAGRID_AERON_OFFER_TIMEOUT_NANOS" -> "5000000000";
                case "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE" -> Boolean.toString(this.externalArchive);
                default -> null;
            };
        }
    }
}
