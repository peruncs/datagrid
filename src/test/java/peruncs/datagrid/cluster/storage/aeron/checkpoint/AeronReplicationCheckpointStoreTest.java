package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.Crc32C;
import peruncs.datagrid.cluster.storage.ReplicationDurabilityMode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies that restart records survive only as complete, checksummed files.
class AeronReplicationCheckpointStoreTest {
    private static final int SLOT_BYTES = Long.BYTES + AeronReplicationCheckpoint.ENCODED_BYTES + Integer.BYTES;
    private static final int JOURNAL_BYTES = SLOT_BYTES * 2;
    private static AeronReplicationCheckpoint checkpoint() {
        return new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 7, 5, 13, 4096, 12, 1, 99
        );
    }

        /// Verifies round-tripping of the fixed recovery record.
    @Test
    void roundTripsFixedRecoveryRecord() throws Exception {
        final Path path = Files.createTempFile("datagrid-checkpoint", ".bin");
        final AeronReplicationCheckpoint expected = checkpoint();
        AeronReplicationCheckpointStore.write(path, expected);
        assertEquals(JOURNAL_BYTES, Files.size(path));
        assertEquals(expected, AeronReplicationCheckpointStore.read(path));
        Files.deleteIfExists(path);
    }

        /// Verifies rejection of torn and corrupt records.
    @Test
    void rejectsTornAndCorruptRecords() throws Exception {
        final Path path = Files.createTempFile("datagrid-checkpoint", ".bin");
        AeronReplicationCheckpointStore.write(path, checkpoint());
        byte[] bytes = Files.readAllBytes(path);
        Files.write(path, java.util.Arrays.copyOf(bytes, bytes.length - 1));
        assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
        Files.delete(path);
        AeronReplicationCheckpointStore.write(path, checkpoint());
        bytes = Files.readAllBytes(path);
        bytes[SLOT_BYTES - 1] ^= 1;
        bytes[JOURNAL_BYTES - 1] ^= 1;
        Files.write(path, bytes);
        assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
        Files.deleteIfExists(path);
    }

        /// Verifies reading rejects unsupported header flags even when the checksum is valid.
    @Test
    void rejectsUnsupportedFlagsEvenWithValidChecksum() throws Exception {
        final Path path = Files.createTempFile("datagrid-checkpoint-reserved", ".bin");
        try {
            AeronReplicationCheckpointStore.write(path, checkpoint());
            final byte[] bytes = Files.readAllBytes(path);
            final ByteBuffer encoded = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            final int record = SLOT_BYTES + Long.BYTES;
            encoded.putShort(record + 6, (short) 1);
            encoded.putInt(record + AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES,
                    Crc32C.compute(bytes, record, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES));
            encoded.putInt(JOURNAL_BYTES - Integer.BYTES,
                    Crc32C.compute(bytes, SLOT_BYTES, SLOT_BYTES - Integer.BYTES));
            Files.write(path, bytes);

            assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
        } finally {
            Files.deleteIfExists(path);
        }
    }

    /// Pins the shared checkpoint header shape: magic int, version short, zero flags short.
    @Test
    void usesSharedCheckpointHeaderShape() throws Exception {
        final Path path = Files.createTempFile("datagrid-checkpoint-header", ".bin");
        try {
            AeronReplicationCheckpointStore.write(path, checkpoint());
            final ByteBuffer encoded = ByteBuffer.wrap(Files.readAllBytes(path)).order(ByteOrder.BIG_ENDIAN);
            final int record = SLOT_BYTES + Long.BYTES;
            assertEquals(AeronReplicationCheckpoint.MAGIC, encoded.getInt(record));
            assertEquals((short) 2, encoded.getShort(record + 4));
            assertEquals((short) 0, encoded.getShort(record + 6));
            assertEquals(115, AeronReplicationCheckpoint.ENCODED_BYTES);
        } finally {
            Files.deleteIfExists(path);
        }
    }

        /// Verifies atomically replaces existing checkpoint and creates parent.
    @Test
    void atomicallyReplacesExistingCheckpointAndCreatesParent() throws Exception {
        final Path directory = Files.createTempDirectory("datagrid-checkpoint-parent");
        final Path path = directory.resolve("nested/checkpoint.bin");
        final AeronReplicationCheckpoint first = checkpoint();
        AeronReplicationCheckpointStore.write(path, first);
        final AeronReplicationCheckpoint second = new AeronReplicationCheckpoint(
                first.recordType(), first.durabilityMode(), AeronReplicationCheckpoint.State.COMMITTED,
                first.clusterId(), first.nodeId(), first.storeGeneration(), first.recordingId(),
                first.writerEpoch(), first.fencingToken(), first.transactionSequence() + 1, first.recordingPosition() + 10,
                first.dataLength(), first.dataChunkCount(), 7);
        AeronReplicationCheckpointStore.write(path, second);
        assertEquals(second, AeronReplicationCheckpointStore.read(path));
        try (var paths = Files.walk(directory)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(p ->
            {
                try {
                    Files.deleteIfExists(p);
                } catch (final Exception ignored) {
                }
            });
        }
    }

        /// Verifies rejection of invalid checkpoint fields at construction.
    @Test
    void rejectsInvalidCheckpointFieldsAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.PREPARING,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), -2, 0, 1, -1, -1, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTED,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 0, 3, 7, 1024, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTED,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), -1, 0, 2, 7, -1, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTED,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 0, 4, Long.MAX_VALUE, 1024, 1, 1, 0));
    }

        /// Verifies a resolved checkpoint never carries the token-0 new-reader sentinel.
    @Test
    void rejectsTokenZeroOnResolvedCheckpoints() {
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTED,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 0, 0, 7, 1024, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 11, 3, 0, 7, 4096, 0, 0, 0));
    }

    /// Verifies a symbolic-link checkpoint is rejected instead of being followed.
    @Test
    void rejectsSymbolicLinkCheckpoint() throws Exception {
        final Path directory = Files.createTempDirectory("datagrid-checkpoint-symlink");
        final Path target = directory.resolve("target");
        final Path link = directory.resolve("checkpoint");
        try {
            AeronReplicationCheckpointStore.write(target, checkpoint());
            try {
                Files.createSymbolicLink(link, target.getFileName());
            } catch (final UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
                return;
            }
            assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(link));
        } finally {
            Files.deleteIfExists(link);
            Files.deleteIfExists(target);
            Files.deleteIfExists(directory);
        }
    }

        /// Repeated cursor writes reuse the same fixed journal file.
    @Test
    void readerCursorUsesFixedJournal() throws Exception {
        final Path path = Files.createTempFile("datagrid-reader-cursor", ".bin");
        final AeronReplicationCheckpoint readerCursor = new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                ReplicationDurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 11, 3, 6, 7, 4096, 0, 0, 0);
        try {
            AeronReplicationCheckpointStore.write(path, readerCursor);
            AeronReplicationCheckpointStore.write(path, readerCursor);
            assertEquals(JOURNAL_BYTES, Files.size(path));
            assertEquals(readerCursor, AeronReplicationCheckpointStore.read(path));
        } finally {
            Files.deleteIfExists(path);
        }
    }
}
