package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import peruncs.datagrid.cluster.errors.CorruptReplicationDataException;
import peruncs.datagrid.cluster.storage.Crc32C;
import peruncs.datagrid.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

import static peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronCheckpointCodec.*;

/// Persists restart records without coupling them to the wire format.
///
/// Two fixed journal slots retain the previous checkpoint while the next is
/// forced. Startup selects the newest complete, checksummed slot.
public final class AeronReplicationCheckpointStore {
    private static final int SLOT_BYTES = Long.BYTES + AeronReplicationCheckpoint.ENCODED_BYTES + Integer.BYTES;
    private static final int JOURNAL_BYTES = SLOT_BYTES * 2;
    /* Fixed monitor stripes instead of one monitor per distinct path: the
     * journal is written at commit cadence, and a bounded stripe array
     * cannot retain one monitor per path forever. Writers for different
     * paths rarely collide, and when they do they only serialize briefly. */
    private static final int LOCK_STRIPES = 16;
    private static final Object[] LOCKS = new Object[LOCK_STRIPES];

    static {
        for (int index = 0; index < LOCK_STRIPES; index++) LOCKS[index] = new Object();
    }

    private static Object lockFor(final Path canonical) {
        return LOCKS[Math.floorMod(canonical.hashCode(), LOCK_STRIPES)];
    }
    private static final ScopedValue<BiConsumer<String, Path>> TEST_HOOK = ScopedValue.newInstance();

    private AeronReplicationCheckpointStore() {
    }

    static void runWithTestHook(final BiConsumer<String, Path> hook, final Runnable action) {
        ScopedValue.where(TEST_HOOK, Objects.requireNonNull(hook, "hook")).run(action);
    }

    private static void testPoint(final String phase, final Path path) {
        if (TEST_HOOK.isBound()) TEST_HOOK.get().accept(phase, path);
    }

    /// Forces the next journal slot without renaming the checkpoint file.
    ///
    /// @param path       checkpoint file
    /// @param checkpoint record to persist
    /// @throws IOException if the record cannot be written or forced to disk
    public static void write(final Path path, final AeronReplicationCheckpoint checkpoint) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(checkpoint, "checkpoint");
        final Path canonical = path.toAbsolutePath().normalize();
        synchronized (lockFor(canonical)) {
            final Path parent = canonical.getParent();
            if (parent != null) Files.createDirectories(parent);
            AtomicFileWriter.ensureNoSymbolicLinks(canonical);
            try (FileChannel channel = FileChannel.open(canonical, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                if (channel.size() != 0L && channel.size() != JOURNAL_BYTES) {
                    throw new IOException("invalid Aeron checkpoint journal length=" + channel.size());
                }
                if (channel.size() == 0L) {
                    channel.position(JOURNAL_BYTES - 1L);
                    channel.write(ByteBuffer.wrap(new byte[1]));
                    channel.force(true);
                }
                final Slot first = readSlot(channel, 0);
                final Slot second = readSlot(channel, 1);
                final long generation = Math.max(first == null ? 0L : first.generation(),
                        second == null ? 0L : second.generation()) + 1L;
                if (generation <= 0L) throw new IOException("checkpoint journal generation exhausted");
                final byte[] record = encode(checkpoint);
                final ByteBuffer slot = ByteBuffer.allocate(SLOT_BYTES);
                slot.putLong(generation).put(record);
                slot.putInt(Crc32C.compute(slot.array(), 0, SLOT_BYTES - Integer.BYTES)).flip();
                channel.position((generation & 1L) * SLOT_BYTES);
                testPoint("BEFORE_JOURNAL_SLOT_WRITE", canonical);
                if (TEST_HOOK.isBound()) {
                    final ByteBuffer firstHalf = slot.duplicate();
                    firstHalf.limit(SLOT_BYTES / 2);
                    while (firstHalf.hasRemaining()) {
                        if (channel.write(firstHalf) == 0) throw new IOException("Aeron checkpoint write made no progress");
                    }
                    slot.position(SLOT_BYTES / 2);
                    testPoint("DURING_JOURNAL_SLOT_WRITE", canonical);
                }
                while (slot.hasRemaining()) {
                    if (channel.write(slot) == 0) throw new IOException("Aeron checkpoint write made no progress");
                }
                channel.force(true);
                testPoint("AFTER_JOURNAL_SLOT_FORCE", canonical);
                if (generation == 1L) AtomicFileWriter.forceDirectory(parent);
            }
        }
    }

    private static byte[] encode(final AeronReplicationCheckpoint checkpoint) {
        final byte[] encoded = new byte[AeronReplicationCheckpoint.ENCODED_BYTES];
        int offset = putHeader(encoded, 0, AeronReplicationCheckpoint.MAGIC, AeronReplicationCheckpoint.VERSION);
        offset = putByte(encoded, offset, (byte) checkpoint.recordTypeCode());
        offset = putByte(encoded, offset, (byte) AeronReplicationCheckpoint.DURABILITY_ARCHIVE_FIRST);
        offset = putByte(encoded, offset, (byte) checkpoint.stateCode());
        offset = putUuid(encoded, offset, checkpoint.clusterId());
        offset = putUuid(encoded, offset, checkpoint.nodeId());
        offset = putUuid(encoded, offset, checkpoint.storeGeneration());
        offset = putLong(encoded, offset, checkpoint.recordingId());
        offset = putLong(encoded, offset, checkpoint.writerEpoch());
        offset = putLong(encoded, offset, checkpoint.fencingToken());
        offset = putLong(encoded, offset, checkpoint.transactionSequence());
        offset = putLong(encoded, offset, checkpoint.recordingPosition());
        offset = putInt(encoded, offset, checkpoint.dataLength());
        offset = putInt(encoded, offset, checkpoint.dataChunkCount());
        offset = putInt(encoded, offset, checkpoint.resolutionCrc32c());
        putInt(encoded, offset, Crc32C.compute(encoded, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES));
        return encoded;
    }

    /// Reads a record and rejects a torn, corrupt, or incompatible file.
    ///
    /// @param path checkpoint file
    /// @return validated checkpoint
    /// @throws IOException if the file is missing, truncated, or invalid
    public static AeronReplicationCheckpoint read(final Path path) throws IOException {
        final Path canonical = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        AtomicFileWriter.ensureNoSymbolicLinks(canonical);
        final byte[] bytes;
        synchronized (lockFor(canonical)) {
            try (FileChannel channel = FileChannel.open(canonical, StandardOpenOption.READ,
                    LinkOption.NOFOLLOW_LINKS)) {
                if (channel.size() != JOURNAL_BYTES) {
                    throw new IOException("invalid Aeron checkpoint journal length=" + channel.size());
                }
                final Slot first = readSlot(channel, 0);
                final Slot second = readSlot(channel, 1);
                final Slot newest = first == null ? second : second == null || first.generation() > second.generation()
                        ? first : second;
                if (newest == null) throw new IOException("Aeron checkpoint journal has no valid slot");
                bytes = newest.record();
            }
        }
        final int expected = getInt(bytes, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES);
        if (expected != Crc32C.compute(bytes, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES)) {
            throw new IOException("Aeron checkpoint CRC32C mismatch");
        }
        try {
            if (getInt(bytes, 0) != AeronReplicationCheckpoint.MAGIC ||
                getShort(bytes, VERSION_OFFSET) != AeronReplicationCheckpoint.VERSION) {
                throw new IOException("unknown Aeron checkpoint format");
            }
            if (headerFlags(bytes) != 0) {
                throw new IOException("unsupported Aeron checkpoint flags");
            }
            final var reader = new FrameReader(bytes, AeronCheckpointCodec.HEADER_LENGTH);
            final var recordType = AeronReplicationCheckpoint.RecordType.from(Byte.toUnsignedInt(reader.readByte()));
            final int durabilityCode = Byte.toUnsignedInt(reader.readByte());
            if (durabilityCode != AeronReplicationCheckpoint.DURABILITY_ARCHIVE_FIRST) {
                throw new CorruptReplicationDataException(
                        "unknown replication durability mode: %s".formatted(durabilityCode));
            }
            final var state = AeronReplicationCheckpoint.State.from(Byte.toUnsignedInt(reader.readByte()));
            final UUID clusterId = reader.readUuid();
            final UUID nodeId = reader.readUuid();
            final UUID storeGeneration = reader.readUuid();
            final long recordingId = reader.readLong();
            final long writerEpoch = reader.readLong();
            final long fencingToken = reader.readLong();
            final long transactionSequence = reader.readLong();
            final long recordingPosition = reader.readLong();
            final int dataLength = reader.readInt();
            final int dataChunkCount = reader.readInt();
            final int resolutionCrc32c = reader.readInt();
            return new AeronReplicationCheckpoint(recordType, state,
                    clusterId, nodeId, storeGeneration,
                    recordingId, writerEpoch, fencingToken, transactionSequence, recordingPosition,
                    dataLength, dataChunkCount, resolutionCrc32c);
        } catch (final RuntimeException e) {
            throw new IOException("invalid Aeron checkpoint fields", e);
        }
    }

    private static Slot readSlot(final FileChannel channel, final int index) throws IOException {
        final byte[] bytes = new byte[SLOT_BYTES];
        final ByteBuffer target = ByteBuffer.wrap(bytes);
        channel.position((long) index * SLOT_BYTES);
        while (target.hasRemaining()) {
            final int read = channel.read(target);
            if (read < 0) return null;
            if (read == 0) throw new IOException("Aeron checkpoint read made no progress");
        }
        final long generation = ByteBuffer.wrap(bytes).getLong();
        if (generation <= 0L) return null;
        final int expected = ByteBuffer.wrap(bytes, SLOT_BYTES - Integer.BYTES, Integer.BYTES).getInt();
        if (expected != Crc32C.compute(bytes, 0, SLOT_BYTES - Integer.BYTES)) return null;
        return new Slot(generation, java.util.Arrays.copyOfRange(
                bytes, Long.BYTES, Long.BYTES + AeronReplicationCheckpoint.ENCODED_BYTES));
    }

    private record Slot(long generation, byte[] record) {}

}
