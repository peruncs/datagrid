package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import peruncs.datagrid.cluster.storage.types.AtomicFileStore;
import peruncs.datagrid.cluster.storage.types.Crc32c;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.UUID;

/// Persists restart records without coupling them to the wire format.
///
/// A new record is forced to a temporary file before it replaces the old
/// one. Reads validate the complete record and its checksum. A failed write
/// therefore leaves the previous restart boundary available.
public final class AeronReplicationCheckpointStore {
    private AeronReplicationCheckpointStore() {
    }

    /// Replaces `path` only after the complete record is on disk.
    ///
    /// @param path       checkpoint file
    /// @param checkpoint record to persist
    /// @throws IOException if the record cannot be written or forced to disk
    public static void write(final Path path, final AeronReplicationCheckpoint checkpoint) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(checkpoint, "checkpoint");
        /* The encoded bytes are intentionally owned by this invocation. A callback
         * (including a crash hook) can therefore not overwrite a buffer owned by
         * this write while AtomicFileStore is still consuming it. Checkpoint writes are
         * infrequent and the fixed-size allocation is preferable to an escaping,
         * re-entrancy-sensitive mutable buffer. */
        final byte[] bytes = encode(checkpoint);
        final AtomicFileStore.Phase phase = checkpoint.recordType() == AeronReplicationCheckpoint.RecordType.READER_CURSOR
                ? AtomicFileStore.PHASE_CURSOR
                : AtomicFileStore.PHASE_CHECKPOINT;
        AtomicFileStore.write(path, channel ->
        {
            final ByteBuffer encoded = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            while (encoded.hasRemaining()) {
                if (channel.write(encoded) == 0) throw new IOException("Aeron checkpoint write made no progress");
            }
        }, phase);
    }

    private static byte[] encode(final AeronReplicationCheckpoint checkpoint) {
        final ByteBuffer encoded = ByteBuffer.allocate(AeronReplicationCheckpoint.ENCODED_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(AeronReplicationCheckpoint.MAGIC)
                .putShort(AeronReplicationCheckpoint.VERSION)
                .put((byte) checkpoint.recordTypeCode())
                .put((byte) checkpoint.durabilityModeCode())
                .put((byte) checkpoint.stateCode())
                .putShort((short) 0).put((byte) 0);
        putUuid(encoded, checkpoint.clusterId());
        putUuid(encoded, checkpoint.nodeId());
        putUuid(encoded, checkpoint.storeGeneration());
        encoded.putLong(checkpoint.recordingId()).putLong(checkpoint.writerEpoch())
                .putLong(checkpoint.fencingToken())
                .putLong(checkpoint.transactionSequence()).putLong(checkpoint.recordingPosition())
                .putInt(checkpoint.dataLength()).putInt(checkpoint.dataChunkCount())
                .putInt(checkpoint.resolutionCrc32c());
        final byte[] bytes = encoded.array();
        encoded.putInt(Crc32c.compute(bytes, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES));
        return bytes;
    }

    /// Reads a record and rejects a torn, corrupt, or incompatible file.
    ///
    /// @param path checkpoint file
    /// @return validated checkpoint
    /// @throws IOException if the file is missing, truncated, or invalid
    public static AeronReplicationCheckpoint read(final Path path) throws IOException {
        final byte[] bytes = readFixedRecord(path);
        final int expected = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                .getInt(AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES);
        if (expected != Crc32c.compute(bytes, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES)) {
            throw new IOException("Aeron checkpoint CRC32C mismatch");
        }
        try {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            if (buffer.getInt() != AeronReplicationCheckpoint.MAGIC ||
                buffer.getShort() != AeronReplicationCheckpoint.VERSION) {
                throw new IOException("unknown Aeron checkpoint format");
            }
            final var recordType = AeronReplicationCheckpoint.RecordType.from(Byte.toUnsignedInt(buffer.get()));
            final var mode = ReplicationDurabilityMode.fromCode(Byte.toUnsignedInt(buffer.get()));
            final var state = AeronReplicationCheckpoint.State.from(Byte.toUnsignedInt(buffer.get()));
            if (buffer.getShort() != 0 || buffer.get() != 0) {
                throw new IOException("unsupported Aeron checkpoint reserved fields");
            }
            return new AeronReplicationCheckpoint(recordType, mode, state,
                    readUuid(buffer), readUuid(buffer), readUuid(buffer),
                    buffer.getLong(), buffer.getLong(), buffer.getLong(), buffer.getLong(), buffer.getLong(),
                    buffer.getInt(), buffer.getInt(), buffer.getInt());
        } catch (final RuntimeException e) {
            throw new IOException("invalid Aeron checkpoint fields", e);
        }
    }

    private static byte[] readFixedRecord(final Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        /* Keep the file descriptor open while reading and request NOFOLLOW_LINKS.
         * The old size/readAllBytes sequence allowed a symlink swap between the
         * validation and read, which could make recovery consume attacker-controlled
         * metadata from outside the configured checkpoint directory. */
        try (SeekableByteChannel channel = Files.newByteChannel(
                path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            final long size = channel.size();
            if (size != AeronReplicationCheckpoint.ENCODED_BYTES)
                throw new IOException("invalid Aeron checkpoint length=%s".formatted(size));
            final byte[] bytes = new byte[AeronReplicationCheckpoint.ENCODED_BYTES];
            final ByteBuffer target = ByteBuffer.wrap(bytes);
            while (target.hasRemaining()) {
                final int read = channel.read(target);
                if (read <= 0) throw new IOException("Aeron checkpoint read made no progress");
            }
            return bytes;
        }
    }

    private static UUID readUuid(final ByteBuffer buffer) {
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static void putUuid(final ByteBuffer buffer, final UUID value) {
        buffer.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits());
    }

}
