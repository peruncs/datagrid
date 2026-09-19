package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import peruncs.datagrid.cluster.storage.types.AtomicFileWriter;
import peruncs.datagrid.cluster.storage.types.Crc32c;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.UUID;

import static peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronCheckpointCodec.*;

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
         * this write while AtomicFileWriter is still consuming it. Checkpoint writes are
         * infrequent and the fixed-size allocation is preferable to an escaping,
         * re-entrancy-sensitive mutable buffer. */
        final byte[] bytes = encode(checkpoint);
        final AtomicFileWriter.Phase phase = checkpoint.recordType() == AeronReplicationCheckpoint.RecordType.READER_CURSOR
                ? AtomicFileWriter.Phase.CURSOR
                : AtomicFileWriter.Phase.CHECKPOINT;
        AtomicFileWriter.write(path, channel ->
        {
            final ByteBuffer encoded = ByteBuffer.wrap(bytes);
            while (encoded.hasRemaining()) {
                if (channel.write(encoded) == 0) throw new IOException("Aeron checkpoint write made no progress");
            }
        }, phase);
    }

    private static byte[] encode(final AeronReplicationCheckpoint checkpoint) {
        final byte[] encoded = new byte[AeronReplicationCheckpoint.ENCODED_BYTES];
        int offset = putHeader(encoded, 0, AeronReplicationCheckpoint.MAGIC, AeronReplicationCheckpoint.VERSION);
        offset = putByte(encoded, offset, (byte) checkpoint.recordTypeCode());
        offset = putByte(encoded, offset, (byte) checkpoint.durabilityModeCode());
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
        putInt(encoded, offset, Crc32c.compute(encoded, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES));
        return encoded;
    }

    /// Reads a record and rejects a torn, corrupt, or incompatible file.
    ///
    /// @param path checkpoint file
    /// @return validated checkpoint
    /// @throws IOException if the file is missing, truncated, or invalid
    public static AeronReplicationCheckpoint read(final Path path) throws IOException {
        final byte[] bytes = readFixedRecord(path);
        final int expected = getInt(bytes, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES);
        if (expected != Crc32c.compute(bytes, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES)) {
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
            final var mode = ReplicationDurabilityMode.fromCode(Byte.toUnsignedInt(reader.readByte()));
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
            return new AeronReplicationCheckpoint(recordType, mode, state,
                    clusterId, nodeId, storeGeneration,
                    recordingId, writerEpoch, fencingToken, transactionSequence, recordingPosition,
                    dataLength, dataChunkCount, resolutionCrc32c);
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

}
