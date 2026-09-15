package peruncs.datagrid.cluster.storage.aeron.crashtest;

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;

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
                !hasMagic(bytes, i + AeronReplicationEnvelope.HEADER_LENGTH) &&
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

    private static boolean hasMagic(final byte[] bytes, final int offset) {
        if (offset < 0 || offset + Integer.BYTES > bytes.length) return false;
        return ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt() ==
               AeronReplicationEnvelope.MAGIC;
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
    public static void truncateCatalog(final Path archiveDirectory) throws IOException {
        final Path catalog = archiveDirectory.resolve("archive.catalog");
        final long size = Files.size(catalog);
        if (size < 64L) throw new UnsupportedArtifactLayoutException("catalog is too small: %s".formatted(catalog));
        try (FileChannel channel = FileChannel.open(catalog, StandardOpenOption.WRITE)) {
            channel.truncate(64L);
            channel.force(true);
        }
    }

    public static final class UnsupportedArtifactLayoutException extends IOException {
        public UnsupportedArtifactLayoutException(final String message) {
            super(message);
        }
    }
}
