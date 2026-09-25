package peruncs.cluster.storage.aeron.crashtest;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.reader.AeronArchiveReader;
import peruncs.cluster.storage.aeron.reader.ReaderDeliveryListener;
import peruncs.cluster.storage.aeron.reader.TransactionCrashHooks;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;

/// Forked reader used by the reader crash matrix. It never self-terminates.
public final class ReaderCrashChildMain {
    static final int INFLIGHT_JOURNAL_BYTES = 254;
    private static final UUID CLUSTER_ID = UUID.nameUUIDFromBytes("reader-crash-cluster".getBytes(StandardCharsets.UTF_8));
    private static final long EPOCH = 2L;
    private static final int LIVE_STREAM_ID = 1001;
    private static final int REPLAY_STREAM_ID = 1002;

    private ReaderCrashChildMain() {
    }

    static void main(final String[] args) throws Exception {
        final Path base = Path.of(required("dg.reader.base")).toAbsolutePath().normalize();
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        final String mode = System.getProperty("dg.reader.mode", "phase1");
        final String point = System.getProperty("dg.reader.barrier", "NONE");
        if (!"NONE".equals(point) && !ReaderMilestone.supports(point)) {
            throw new IllegalArgumentException("unsupported or unencodable reader crash point: %s".formatted(point));
        }
        final Path uncertainty = base.resolve("reader.reader-inflight");
        if ("phase2".equals(mode) && Files.exists(uncertainty)) {
            try {
                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(uncertainty);
                if ("DURING_RECOVERY_CURSOR_PARSE".equals(point)) {
                    /* A second crash lands in the middle of the recovery's own
                     * parse window: the read succeeded but the verdict was
                     * never published. Simulate the SIGKILL with an immediate
                     * halt — no outcome file may be left behind. */
                    ReaderMilestone.write(control.resolve("milestone.reached"), point,
                            checkpoint.transactionSequence(), checkpoint.recordingPosition());
                    Runtime.getRuntime().halt(137);
                }
                if ("AFTER_RECOVERY_CURSOR_VALIDATED".equals(point)) {
                    /* The recovery validated the durable state (marker parsed,
                     * uncertainty recognized) but parked before publishing its
                     * verdict: the parent kills it here. */
                    barrier(control, point, checkpoint.transactionSequence(), checkpoint.recordingPosition());
                }
                writeOutcome(control, "RESEED_REQUIRED", "reader Store import is uncertain at sequence %s: %s".formatted(checkpoint.transactionSequence(), uncertainty));
            } catch (final IOException failure) {
                if ("DURING_RECOVERY_CURSOR_PARSE".equals(point)) {
                    /* Same second-crash window, but the marker itself is torn:
                     * the parse throws and the recovery dies without any
                     * verdict. */
                    ReaderMilestone.write(control.resolve("milestone.reached"), point, -1L, -1L);
                    Runtime.getRuntime().halt(137);
                }
                writeOutcome(control, "RESEED_REQUIRED", "cannot read reader uncertainty marker: %s".formatted(failure));
            }
            return;
        }

        final Path aeronDirectory = Path.of(required("dg.reader.aeronDirectory"));
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024)
                .mtuLength(1408)
                .chunkSize(16 * 1024)
                .maxTransactionBytes(256 * 1024)
                .offerTimeoutNanos(10_000_000_000L)
                /* Crash cells anchor on exact per-transaction marker/cursor
                 * interleavings, so the child replays with the strict
                 * per-transaction barrier instead of the batched default. */
                .readerBarrierMaxTransactions(1)
                .build();
        final MediaDriver.Context mediaContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectory.toString())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        try (MediaDriver driver = Boolean.getBoolean("dg.reader.sharedDriver") ? null : MediaDriver.launch(mediaContext);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory.toString()));
             AeronArchive positionProbe = AeronArchive.connect(new AeronArchive.Context()
                     .aeron(aeron).ownsAeronClient(false)
                     .aeronDirectoryName(aeronDirectory.toString())
                     .controlRequestChannel(required("dg.reader.controlChannel"))
                     .controlResponseChannel(required("dg.reader.controlResponseChannel"))
                     .messageTimeoutNs(configuration.offerTimeoutNanos()))) {
            final AeronArchive.Context archiveContext = new AeronArchive.Context()
                    .aeron(aeron)
                    .aeronDirectoryName(aeronDirectory.toString())
                    .controlRequestChannel(required("dg.reader.controlChannel"))
                    .controlResponseChannel(required("dg.reader.controlResponseChannel"))
                    .messageTimeoutNs(configuration.offerTimeoutNanos());
            final long recordingId = Long.parseLong(required("dg.reader.recordingId"));
            final Cursor cursor = readCursor(base.resolve("reader.cursor"));
            /* Whether this phase must replay recorded history from behind the
             * durable cursor: the recovery outcome distinguishes a genuine
             * archive replay from joining a live tail. */
            final boolean replayedFromArchive = cursor == null || cursor.sequence < 1L;
            final AtomicReference<AeronArchiveReader> readerRef = new AtomicReference<>();
            final ReaderFixture fixture = new ReaderFixture(base, point, new AtomicReference<>());
            final java.util.concurrent.Callable<Void> runReader = () -> {
                final AeronArchiveReader reader = AeronArchiveReader.create(
                        AeronArchiveReader.Configuration.builder()
                        .aeron(aeron).archiveContext(archiveContext).recordingId(recordingId)
                        .startPosition(cursor == null
                                ? io.aeron.archive.client.PersistentSubscription.FROM_START : cursor.position)
                        .liveChannel(required("dg.reader.liveChannel")).liveStreamId(LIVE_STREAM_ID)
                        .replayChannel(required("dg.reader.replayChannel")).replayStreamId(REPLAY_STREAM_ID)
                        .replicationConfiguration(configuration).clusterId(CLUSTER_ID).epoch(EPOCH)
                        .wireNonce(AeronReplicationEnvelope.defaultWireNonce(CLUSTER_ID))
                        .initialSequence(cursor == null ? -1 : cursor.sequence)
                        .initialPosition(cursor == null ? -1 : cursor.position)
                        .receiver(fixture)
                        .recordedPosition(() -> positionProbe.getMaxRecordedPosition(recordingId))
                        .transactionResolved(ignored ->
                        {
                            if (ignored.sequence() < 0) return;
                            final AeronArchiveReader current = readerRef.get();
                            if (current != null) {
                                writeCursor(base.resolve("reader.cursor"),
                                        ignored.sequence(), ignored.position(), point, control);
                            }
                        }).deliveryListener(new Listener(fixture, uncertainty, point, recordingId)).build());
                readerRef.set(reader);
                reader.start();
                atomicText(control.resolve("ready"), "ready");
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
                while (reader.failure() == null && reader.lastResolvedSequence() < 1L && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                if (reader.failure() != null) throw reader.failure();
                if (System.nanoTime() >= deadline) throw new IllegalStateException("reader did not resolve target sequence");
                return null;
            };
            if ("AFTER_RECOVERY_CURSOR_VALIDATED".equals(point)) {
                /* Parks with no import started, no marker written, and the
                 * durable cursor (if any) read and validated: a kill here must
                 * be recoverable by pure archive replay. */
                barrier(control, point, -1L, cursor == null ? -1L : cursor.position);
            }
            if ("DURING_CHUNK_ASSEMBLY".equals(point)) {
                /* Park after the first chunk of a multi-chunk transaction is
                 * buffered: the kill leaves partially assembled natural-memory
                 * state behind, which recovery must never apply. The reader is
                 * CONSTRUCTED inside this scope — scoped values are not
                 * inherited by the reader's virtual-thread poller, so the
                 * TransactionAssembler captures the observer at construction. */
                TransactionCrashHooks.runWithChunkObserver((sequence, chunkIndex, chunkCount) -> {
                    if (chunkIndex == 0) {
                        barrier(control, point, sequence, -1L);
                    }
                }, runReader);
            } else {
                runReader.call();
            }
            writeOutcome(control, replayedFromArchive ? "REPLAY_FROM_ARCHIVE" : "CONTINUE", null);
            readerRef.get().dispose();
        } catch (final RuntimeException | Error failure) {
            writeOutcome(control, failure.getMessage() != null && failure.getMessage().startsWith("RESEED_REQUIRED:")
                    ? "RESEED_REQUIRED" : "FAIL_CLOSED", failure.toString());
            throw failure;
        }
    }

    private static Cursor readCursor(final Path path) throws IOException {
        if (!Files.exists(path)) return null;
        final byte[] bytes = Files.readAllBytes(path);
        if (bytes.length != Long.BYTES * 2 + Integer.BYTES) throw new IOException("invalid reader cursor length");
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        final long sequence = buffer.getLong();
        final long position = buffer.getLong();
        final int expected = buffer.getInt();
        final CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        if ((int) crc.getValue() != expected) throw new IOException("reader cursor CRC mismatch");
        return new Cursor(sequence, position);
    }

    private static void writeCursor(final Path path, final long sequence, final long position,
                                    final String point, final Path control) {
        try {
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES).order(ByteOrder.BIG_ENDIAN)
                    .putLong(sequence).putLong(position);
            final byte[] bytes = buffer.array();
            final CRC32C crc = new CRC32C();
            crc.update(bytes, 0, bytes.length - Integer.BYTES);
            buffer.putInt((int) crc.getValue()).flip();
            final Path parent = path.toAbsolutePath().getParent();
            Files.createDirectories(parent);
            final Path temporary = Files.createTempFile(parent, "%s.tmp-".formatted(path.getFileName()), null);
            try {
                if ("DURING_CURSOR_FILE_WRITE".equals(point)) {
                    barrier(control, point, sequence, position);
                }
                try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    while (buffer.hasRemaining()) {
                        if (channel.write(buffer) == 0) throw new IOException("Cursor write made no progress");
                    }
                    channel.force(true);
                }
                if ("AFTER_CURSOR_TEMP_WRITE_BEFORE_RENAME".equals(point)) {
                    barrier(control, point, sequence, position);
                }
                Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                if ("AFTER_CURSOR_RENAME_BEFORE_DIRECTORY_SYNC".equals(point)) {
                    barrier(control, point, sequence, position);
                }
                try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
                    directory.force(true);
                }
                if ("AFTER_CURSOR_DIRECTORY_SYNC_BEFORE_RETURN".equals(point)) {
                    /* The cursor rename is fully durable but the resolver has
                     * not returned yet, so the uncertainty marker still exists:
                     * a kill here must recover fail-closed via the marker, and
                     * must never double-apply the already-imported record. */
                    barrier(control, point, sequence, position);
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot write reader cursor", failure);
        }
    }

    private static void append(final Path path, final byte[] bytes) {
        try {
            Files.createDirectories(path.toAbsolutePath().getParent());
            final CRC32C crc = new CRC32C();
            crc.update(bytes);
            final ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
                    .putInt(bytes.length).putInt((int) crc.getValue()).flip();
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                while (header.hasRemaining()) {
                    if (channel.write(header) == 0) throw new IOException("Reader fixture header write made no progress");
                }
                final ByteBuffer payload = ByteBuffer.wrap(bytes);
                while (payload.hasRemaining()) {
                    if (channel.write(payload) == 0) throw new IOException("Reader fixture payload write made no progress");
                }
                channel.force(true);
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot append reader Store fixture", failure);
        }
    }

    private static void barrier(final Path control, final String point, final long sequence, final long position) {
        try {
            ReaderMilestone.write(control.resolve("milestone.reached"), point, sequence, position);
            while (!Files.exists(control.resolve("release"))) Thread.sleep(10L);
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot write reader milestone", failure);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("reader barrier interrupted", interrupted);
        }
    }

    private static void atomicText(final Path path, final String value) {
        try {
            final Path parent = path.toAbsolutePath().getParent();
            Files.createDirectories(parent);
            final Path temporary = Files.createTempFile(parent, "%s.tmp-".formatted(path.getFileName()), null);
            try {
                try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    final ByteBuffer bytes = StandardCharsets.UTF_8.encode(value);
                    while (bytes.hasRemaining()) {
                        if (channel.write(bytes) == 0) throw new IOException("Reader milestone write made no progress");
                    }
                    channel.force(true);
                }
                Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
                    directory.force(true);
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot write reader state", failure);
        }
    }

    private static void writeOutcome(final Path control, final String outcome, final String error) {
        final String health = switch (outcome) {
            case "CONTINUE", "REPLAY_FROM_ARCHIVE" -> "LIVE";
            case "RESEED_REQUIRED" -> "RESEED_REQUIRED";
            default -> "FAILED";
        };
        final String value = "HEALTH=%s%s%sOUTCOME=%s%s".formatted(health, System.lineSeparator(), (error == null ? "" : "ERROR=%s%s".formatted(error.replace('\n', ' '), System.lineSeparator())), outcome, System.lineSeparator());
        atomicText(control.resolve("outcome"), value);
    }

    private static String required(final String name) {
        final String value = System.getProperty(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("missing -D%s".formatted(name));
        return value;
    }

    private record Listener(ReaderFixture fixture, Path uncertainty, String point, long recordingId)
            implements ReaderDeliveryListener {
        /// Writes the uncertainty marker, routing checked failures through an
        /// unchecked wrapper when invoked from the hook-bound lambda.
        private void writeMarker(final AeronReplicationCheckpoint marker) throws IOException {
            AeronReplicationCheckpointStore.write(this.uncertainty, marker);
        }

        /// Persists the uncertainty marker before every import, then parks on
        /// the armed barrier for pre-import crash points so the parent can kill.
        @Override
        public void beforeStoreImport(final long sequence, final long position, final int dataLength,
                                      final int dataChunkCount, final int crc32c) {
            this.fixture.importBoundary().set(new ImportBoundary(sequence, position));
            try {
                final AeronReplicationCheckpoint marker = new AeronReplicationCheckpoint(
                        AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                        AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                        CLUSTER_ID, java.util.UUID.nameUUIDFromBytes("reader-crash-node".getBytes(StandardCharsets.UTF_8)),
                        java.util.UUID.nameUUIDFromBytes("reader-crash-generation".getBytes(StandardCharsets.UTF_8)),
                        this.recordingId, EPOCH, 1L, sequence, position, dataLength, dataChunkCount, crc32c);
                if ("DURING_INFLIGHT_MARKER_WRITE".equals(this.point)) {
                    /* Model the first fixed-journal write after preallocation,
                     * before either slot contains a complete checkpoint. */
                    try (FileChannel channel = FileChannel.open(this.uncertainty,
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                        channel.position(INFLIGHT_JOURNAL_BYTES - 1L);
                        if (channel.write(ByteBuffer.wrap(new byte[1])) != 1) {
                            throw new IOException("journal byte was not written");
                        }
                        channel.force(true);
                    }
                    this.fixture.barrier(this.point, sequence, position);
                    writeMarker(marker);
                } else {
                    writeMarker(marker);
                }
            } catch (final IOException failure) {
                throw new IllegalStateException("cannot persist reader uncertainty marker", failure);
            } catch (final java.io.UncheckedIOException failure) {
                throw new IllegalStateException("cannot persist reader uncertainty marker", failure.getCause());
            }
            if ("REPLAY_BEFORE_FIRST_IMPORT".equals(this.point) ||
                "DURING_STORE_IMPORT".equals(this.point) ||
                "DURING_STORE_IMPORT_FAILURE".equals(this.point)) {
                this.fixture.barrier(this.point, sequence, position);
            }
        }

        /// Clears the uncertainty marker once the import is durable; a surviving
        /// marker is what forces the recovery child to demand a reseed.
        @Override
        public void afterStoreImport() {
            try {
                AtomicFileWriter.delete(this.uncertainty);
            } catch (final IOException failure) {
                throw new IllegalStateException("cannot clear reader uncertainty", failure);
            }
        }
    }

    private record ReaderFixture(Path base, String point, AtomicReference<ImportBoundary> importBoundary)
            implements StorageBinaryDataReceiver {
        /// Appends the assembled transaction to the fixture Store, injects the
        /// import failure for its crash point, and parks on the post-import
        /// barrier so the parent can kill mid-boundary.
        @Override
        public void receiveData(final Binary value) {
            if ("DURING_STORE_IMPORT_FAILURE".equals(this.point)) {
                throw new IllegalStateException("injected Store import failure");
            }
            final ByteBuffer[] buffers = value.buffers();
            int length = 0;
            for (final ByteBuffer source : buffers) length += source.position();
            final byte[] bytes = new byte[length];
            int offset = 0;
            for (final ByteBuffer source : buffers) {
                final ByteBuffer duplicate = source.duplicate();
                duplicate.flip();
                final int amount = duplicate.remaining();
                duplicate.get(bytes, offset, amount);
                offset += amount;
            }
            append(this.base.resolve("reader.store"), bytes);
            if ("AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE".equals(this.point)) {
                final ImportBoundary boundary = this.importBoundary.get();
                if (boundary == null) throw new IllegalStateException("missing import boundary metadata");
                ReaderCrashChildMain.barrier(this.base.resolve("control"), this.point,
                        boundary.sequence(), boundary.position());
            }
        }

        @Override
        public void receiveTypeDictionary(final String value) {
        }

        private void barrier(final String point, final long sequence, final long position) {
            ReaderCrashChildMain.barrier(this.base.resolve("control"), point, sequence, position);
        }
    }

    private record ImportBoundary(long sequence, long position) {
    }

    private record Cursor(long sequence, long position) {
    }
}
