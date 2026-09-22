package peruncs.datagrid.cluster.storage.aeron.crashtest;

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.types.Crc32c;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.regex.Pattern;

/// Version-checked mutations used only by Archive corruption tests.
public final class ArchiveArtifactMutator {
    private static final long MAX_MUTATION_BYTES = 128L * 1024L * 1024L;
    private static final Pattern SEGMENT = Pattern.compile("(\\d+)-(\\d+)\\.rec");

    private ArchiveArtifactMutator() {
    }

        /// Returns all recording segments in physical order.
    ///
    /// @param archiveDirectory Archive directory holding the segment files
    /// @param recordingId Archive recording to list segments for
    /// @return recording segments ordered by base position
    /// @throws IOException when the directory cannot be listed or holds no matching segments
    public static java.util.List<Path> segments(final Path archiveDirectory, final long recordingId)
            throws IOException {
        try (var files = Files.list(archiveDirectory)) {
            final var matches = files.filter(path ->
            {
                final var matcher = SEGMENT.matcher(path.getFileName().toString());
                return matcher.matches() && Long.parseLong(matcher.group(1)) == recordingId;
            }).sorted(Comparator.comparingLong(ArchiveArtifactMutator::segmentBasePosition)).toList();
            if (matches.isEmpty()) {
                throw new UnsupportedArtifactLayoutException("no recording segments for %s".formatted(recordingId));
            }
            return matches;
        }
    }

    private static long segmentBasePosition(final Path path) {
        final var matcher = SEGMENT.matcher(path.getFileName().toString());
        if (!matcher.matches()) throw new IllegalArgumentException("not an Archive segment: %s".formatted(path));
        return Long.parseLong(matcher.group(2));
    }

        /// Flips one byte in the first envelope payload and forces the segment.
    ///
    /// @param segment recording segment file to corrupt
    /// @throws IOException when the segment cannot be read, mutated, or forced
    public static void corruptFirstEnvelopePayload(final Path segment) throws IOException {
        if (Files.size(segment) > MAX_MUTATION_BYTES) {
            throw new UnsupportedArtifactLayoutException("segment exceeds mutation bound: %s".formatted(segment));
        }
        final byte[] bytes = Files.readAllBytes(segment);
        final int magic = AeronReplicationEnvelope.MAGIC;
        final byte[] magicBytes = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putInt(magic).array();
        for (int i = 0; i <= bytes.length - magicBytes.length; i++) {
            boolean matches = true;
            for (int j = 0; j < magicBytes.length; j++) {
                if (bytes[i + j] != magicBytes[j]) {
                    matches = false;
                    break;
                }
            }
            if (matches && i + AeronReplicationEnvelope.HEADER_LENGTH < bytes.length &&
                lacksMagic(bytes, i + AeronReplicationEnvelope.HEADER_LENGTH) &&
                ByteBuffer.wrap(bytes, i + Integer.BYTES, Short.BYTES).getShort() == AeronReplicationEnvelope.VERSION &&
                AeronReplicationEnvelope.isPayloadKindCode(
                        Byte.toUnsignedInt(bytes[i + Integer.BYTES + Short.BYTES]))) {
                bytes[i + AeronReplicationEnvelope.HEADER_LENGTH] ^= 0x01;
                try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
                    final ByteBuffer source = ByteBuffer.wrap(bytes);
                    writeFully(channel, source, 0L);
                    channel.force(true);
                }
                return;
            }
        }
        throw new UnsupportedArtifactLayoutException("no replication envelope found in %s".formatted(segment));
    }

    private static boolean lacksMagic(final byte[] bytes, final int offset) {
        if (offset < 0 || offset + Integer.BYTES > bytes.length) return true;
        return ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt() !=
               AeronReplicationEnvelope.MAGIC;
    }

        /// Envelope header fields the corruption fuzz can target.
    public enum HeaderField {
        /// The envelope magic marker.
        MAGIC(0, 4),
        /// The envelope format version.
        VERSION(4, 2),
        /// The commit/abort/data kind code.
        KIND(6, 1),
        /// The writer epoch.
        EPOCH(8, 8),
        /// The transaction sequence.
        SEQUENCE(16, 8),
        /// The declared payload length.
        PAYLOAD_LENGTH(24, 4),
        /// The chunk index within its transaction.
        CHUNK_INDEX(28, 4),
        /// The transaction's chunk count.
        CHUNK_COUNT(32, 4),
        /// The chunk's offset in the transaction payload.
        CHUNK_OFFSET(36, 4),
        /// The stored payload CRC32C.
        PAYLOAD_CRC(40, 4),
        /// The stored commit CRC32C.
        COMMIT_CRC(44, 4),
        /// The expected cluster identity.
        CLUSTER_ID(48, 16),
        /// The writer fencing token.
        FENCING_TOKEN(64, 8),
        /// The deployment wire nonce.
        WIRE_NONCE(72, 8),
        /// The stored header CRC32C.
        HEADER_CRC(80, 4);

        final int offset;
        final int length;

        HeaderField(final int offset, final int length) {
            this.offset = offset;
            this.length = length;
        }
    }

        /// Byte patterns the corruption fuzz applies to a header field.
    public enum Mutation {
        /// Flips one bit of the field's first byte.
        XOR {
            void apply(final byte[] bytes, final HeaderField field) {
                bytes[field.offset] ^= 0x01;
            }
        },
        /// Zero-fills the whole field.
        ZERO {
            void apply(final byte[] bytes, final HeaderField field) {
                java.util.Arrays.fill(bytes, field.offset, field.offset + field.length, (byte) 0);
            }
        },
        /// Sticky-bits: fills the whole field with ones.
        STICKY {
            void apply(final byte[] bytes, final HeaderField field) {
                java.util.Arrays.fill(bytes, field.offset, field.offset + field.length, (byte) 0xFF);
            }
        },
        /// Increments the field's first byte.
        INCREMENT {
            void apply(final byte[] bytes, final HeaderField field) {
                bytes[field.offset] = (byte) (bytes[field.offset] + 1);
            }
        };

        abstract void apply(byte[] bytes, HeaderField field);
    }

        /// Corrupts one header field of the first envelope in the segment.
    ///
    /// With `recomputeHeaderCrc`, the stored header CRC is fixed up after the
    /// mutation, so the CRC check passes and only the decoder's semantic
    /// validation can reject the frame — this is the check for silent
    /// acceptance. Without it, the mutation also breaks the stored CRC and the
    /// integrity check itself must fire.
    ///
    /// @param segment           recording segment file to corrupt
    /// @param field             header field to mutate
    /// @param mutation          byte pattern to apply
    /// @param recomputeHeaderCrc whether to fix up the header CRC after mutating
    /// @throws IOException when the segment cannot be read, mutated, or forced
    public static void corruptFirstEnvelopeHeader(
            final Path segment,
            final HeaderField field,
            final Mutation mutation,
            final boolean recomputeHeaderCrc
    ) throws IOException {
        if (Files.size(segment) > MAX_MUTATION_BYTES) {
            throw new UnsupportedArtifactLayoutException("segment exceeds mutation bound: %s".formatted(segment));
        }
        final byte[] bytes = Files.readAllBytes(segment);
        final int envelope = locateFirstEnvelope(bytes);
        if (envelope + AeronReplicationEnvelope.HEADER_LENGTH > bytes.length) {
            throw new UnsupportedArtifactLayoutException("no replication envelope found in %s".formatted(segment));
        }
        mutation.apply(bytes, field);
        if (recomputeHeaderCrc) {
            /* The stored CRC covers the first 80 header bytes; recompute it so
             * only semantic validation can reject the mutated frame. */
            final int crc = Crc32c.compute(bytes, envelope, 80);
            bytes[envelope + 80] = (byte) (crc >>> 24);
            bytes[envelope + 81] = (byte) (crc >>> 16);
            bytes[envelope + 82] = (byte) (crc >>> 8);
            bytes[envelope + 83] = (byte) crc;
        }
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
            writeFully(channel, ByteBuffer.wrap(bytes), 0L);
            channel.force(true);
        }
    }

    private static int locateFirstEnvelope(final byte[] bytes) throws IOException {
        final byte[] magicBytes = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putInt(AeronReplicationEnvelope.MAGIC).array();
        for (int i = 0; i <= bytes.length - magicBytes.length; i++) {
            boolean matches = true;
            for (int j = 0; j < magicBytes.length; j++) {
                if (bytes[i + j] != magicBytes[j]) {
                    matches = false;
                    break;
                }
            }
            if (matches && i + AeronReplicationEnvelope.HEADER_LENGTH < bytes.length &&
                lacksMagic(bytes, i + AeronReplicationEnvelope.HEADER_LENGTH) &&
                ByteBuffer.wrap(bytes, i + Integer.BYTES, Short.BYTES).getShort() ==
                AeronReplicationEnvelope.VERSION &&
                AeronReplicationEnvelope.isPayloadKindCode(
                        Byte.toUnsignedInt(bytes[i + Integer.BYTES + Short.BYTES]))) {
                return i;
            }
        }
        throw new UnsupportedArtifactLayoutException("no replication envelope found");
    }

    private static void writeFully(final FileChannel channel, final ByteBuffer source, long position)
            throws IOException {
        while (source.hasRemaining()) {
            final int written = channel.write(source, position);
            if (written == 0) throw new IOException("FileChannel made no progress");
            position += written;
        }
    }

        /// Shortens the final Aeron frame by one byte, leaving its replication envelope
    /// incomplete while preserving the segment's preallocated physical length.
    ///
    /// @param segment recording segment file holding the recording tail
    /// @param recordingStartPosition Archive position where the recording starts
    /// @param recordingStopPosition Archive position where the recording stops
    /// @throws IOException when the boundary is unreadable or outside the segment
    public static void truncateFinalFrame(
            final Path segment,
            final long recordingStartPosition,
            final long recordingStopPosition
    ) throws IOException {
        final long recordedLength = recordingStopPosition - recordingStartPosition;
        if (recordedLength <= AeronReplicationEnvelope.HEADER_LENGTH) {
            throw new UnsupportedArtifactLayoutException("recording is too small to truncate: %s".formatted(segment));
        }
        final long segmentBase = segmentBasePosition(segment);
        final long physicalEnd = recordingStartPosition - segmentBase + recordedLength;
        if (physicalEnd <= 0) {
            throw new UnsupportedArtifactLayoutException("recording start is outside segment: %s".formatted(segment));
        }
        if (physicalEnd > MAX_MUTATION_BYTES) {
            throw new UnsupportedArtifactLayoutException("segment exceeds mutation bound: %s".formatted(segment));
        }
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            if (physicalEnd > channel.size()) {
                throw new UnsupportedArtifactLayoutException(
                        "recording positions exceed physical segment size: %s".formatted(segment));
            }
            final ByteBuffer bytes = ByteBuffer.allocate(Math.toIntExact(physicalEnd))
                    .order(ByteOrder.LITTLE_ENDIAN);
            for (long position = 0; bytes.hasRemaining(); ) {
                final int read = channel.read(bytes, position);
                if (read < 0) throw new UnsupportedArtifactLayoutException("segment ended before recording boundary: %s".formatted(segment));
                if (read == 0) throw new UnsupportedArtifactLayoutException("cannot read recording boundary: %s".formatted(segment));
                position += read;
            }
            bytes.flip();
            final int startOffset = Math.toIntExact(recordingStartPosition - segmentBase);
            final int endOffset = Math.toIntExact(physicalEnd);
            int frameOffset = startOffset;
            int lastFrameOffset = -1;
            int lastFrameLength = -1;
            while (frameOffset < endOffset) {
                if (frameOffset + Integer.BYTES > endOffset) {
                    throw new UnsupportedArtifactLayoutException("recording ends inside an Aeron frame header: %s".formatted(segment));
                }
                final int frameLength = bytes.getInt(frameOffset);
                if (frameLength < DataHeaderFlyweight.HEADER_LENGTH ||
                    frameLength > endOffset - frameOffset) {
                    throw new UnsupportedArtifactLayoutException("invalid Aeron frame length %s at %s in %s".formatted(frameLength, frameOffset, segment));
                }
                if (frameLength > DataHeaderFlyweight.HEADER_LENGTH + AeronReplicationEnvelope.HEADER_LENGTH) {
                    lastFrameOffset = frameOffset;
                    lastFrameLength = frameLength;
                }
                final int alignedLength = (frameLength + (FrameDescriptor.FRAME_ALIGNMENT - 1)) &
                                          -(FrameDescriptor.FRAME_ALIGNMENT);
                if (alignedLength <= 0 || alignedLength > endOffset - frameOffset) {
                    throw new UnsupportedArtifactLayoutException("invalid Aeron frame alignment at %s in %s".formatted(frameOffset, segment));
                }
                frameOffset += alignedLength;
            }
            if (lastFrameOffset < 0) {
                throw new UnsupportedArtifactLayoutException("final Aeron frame is too small to truncate: %s".formatted(segment));
            }
            final ByteBuffer length = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(lastFrameLength - 1);
            length.flip();
            writeFully(channel, length, lastFrameOffset);
            channel.force(true);
        }
    }

        /// Truncates the catalog only when it has a recognizable preallocation.
    ///
    /// @param archiveDirectory Archive directory holding `archive.catalog`
    /// @throws IOException when the catalog is missing, too small, or cannot be forced
    public static void truncateCatalog(final Path archiveDirectory) throws IOException {
        final Path catalog = archiveDirectory.resolve("archive.catalog");
        final long size = Files.size(catalog);
        if (size < 64L) throw new UnsupportedArtifactLayoutException("catalog is too small: %s".formatted(catalog));
        try (FileChannel channel = FileChannel.open(catalog, StandardOpenOption.WRITE)) {
            channel.truncate(64L);
            channel.force(true);
        }
    }

    /// Thrown when an Archive layout does not match what this mutator understands.
    public static final class UnsupportedArtifactLayoutException extends IOException {
        /// Reports an Archive layout this mutator does not understand.
        ///
        /// @param message what was unexpected
        public UnsupportedArtifactLayoutException(final String message) {
            super(message);
        }
    }
}
