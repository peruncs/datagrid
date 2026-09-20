package peruncs.datagrid.cluster.node.aeron.crashtest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/// Fixed, CRC-protected milestone exchanged by the provider crash child.
///
/// The point table is intentionally the provider-process subset. Reader-only
/// points use `ReaderMilestone` in the Aeron module; the two tables are
/// not ordinal-compatible and must never be decoded interchangeably.
record ChildMilestone(String point, long sequence, long timestampNanos) {
    private static final int MAGIC = 0x4447434D;
    private static final short VERSION = 1;
    private static final int BYTES = Integer.BYTES + Short.BYTES + Short.BYTES + Long.BYTES * 2 + Integer.BYTES;
    private static final String[] POINTS = {
            "BEFORE_PUBLICATION_CONNECTED", "BEFORE_PREPARE", "AFTER_DICTIONARY_CHUNKS",
            "AFTER_DATA_CHUNKS", "AFTER_PREPARE",
            "AFTER_PREPARE_BEFORE_LOCAL_WRITE", "AFTER_LOCAL_WRITE_BEFORE_COMMIT",
            "AFTER_ENQUEUE_BEFORE_PREPARE", "AFTER_PREPARE_FAILURE_ABORT_OFFERED",
            "BEFORE_COMMIT_OFFER", "AFTER_COMMIT_OFFER", "AFTER_COMMIT_RECORDED",
            "AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", "AFTER_ABORT_OFFERED",
            "DURING_COMMITTING_UNCERTAIN_WRITE", "DURING_CHECKPOINT_FILE_WRITE",
            "BEFORE_CHECKPOINT_TEMP_WRITE", "AFTER_CHECKPOINT_TEMP_WRITE_BEFORE_RENAME",
            "AFTER_CHECKPOINT_RENAME_BEFORE_DIRECTORY_SYNC",
            "AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE",
            "AFTER_RECOVERY_CHECKPOINT_READ",
            "BEFORE_LEASE_RENEWAL_HEARTBEAT", "AFTER_OWNED_OFFER_BEFORE_HEARTBEAT"
    };

    static void write(final Path path, final String point, final long sequence) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(MAGIC).putShort(VERSION).putShort((short) code(point))
                .putLong(sequence).putLong(System.nanoTime());
        final byte[] bytes = buffer.array();
        final CRC32C crc = new CRC32C();
        crc.update(bytes, 0, BYTES - Integer.BYTES);
        buffer.putInt((int) crc.getValue()).flip();
        Files.createDirectories(path.toAbsolutePath().getParent());
        final Path temporary = Files.createTempFile(path.getParent(), "%s.tmp-".formatted(path.getFileName()), null);
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                while (buffer.hasRemaining()) {
                    if (channel.write(buffer) == 0) throw new IOException("Milestone write made no progress");
                }
                channel.force(true);
            }
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel directory = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

        /// Returns whether this provider-process schema can encode the named point.
    static boolean supports(final String point) {
        for (final String supported : POINTS) if (supported.equals(point)) return true;
        return false;
    }

    static ChildMilestone read(final Path path) throws IOException {
        final byte[] bytes = Files.readAllBytes(path);
        if (bytes.length != BYTES) throw new IOException("invalid milestone length=%s".formatted(bytes.length));
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        final int magic = buffer.getInt();
        final short version = buffer.getShort();
        final int point = Short.toUnsignedInt(buffer.getShort());
        final long sequence = buffer.getLong();
        final long timestampNanos = buffer.getLong();
        final int actual = buffer.getInt();
        final CRC32C crc = new CRC32C();
        crc.update(bytes, 0, BYTES - Integer.BYTES);
        if (magic != MAGIC || version != VERSION || actual != (int) crc.getValue())
            throw new IOException("invalid milestone header or CRC");
        if (point >= POINTS.length) throw new IOException("unknown milestone point=%s".formatted(point));
        return new ChildMilestone(POINTS[point], sequence, timestampNanos);
    }

    private static int code(final String point) {
        for (int i = 0; i < POINTS.length; i++) if (POINTS[i].equals(point)) return i;
        throw new IllegalArgumentException("unknown crash point %s".formatted(point));
    }
}
