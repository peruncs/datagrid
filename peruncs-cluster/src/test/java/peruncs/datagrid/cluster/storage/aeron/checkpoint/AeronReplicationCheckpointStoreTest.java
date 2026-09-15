package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.types.AtomicFileStoreCrashHook;
import peruncs.datagrid.cluster.storage.types.Crc32c;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies that restart records survive only as complete, checksummed files.
class AeronReplicationCheckpointStoreTest {
    private static AeronReplicationCheckpoint checkpoint() {
        return new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 7, 13, 4096, 12, 1, 99
        );
    }

        /// Verifies round-tripping of the fixed recovery record.
    @Test
    void roundTripsFixedRecoveryRecord() throws Exception {
        final Path path = Files.createTempFile("datagrid-checkpoint", ".bin");
        final AeronReplicationCheckpoint expected = checkpoint();
        AeronReplicationCheckpointStore.write(path, expected);
        assertEquals(AeronReplicationCheckpoint.ENCODED_BYTES, Files.size(path));
        assertEquals(expected, AeronReplicationCheckpointStore.read(path));
        Files.deleteIfExists(path);
    }

        /// Verifies rejection of torn and corrupt records.
    @Test
    void rejectsTornAndCorruptRecords() throws Exception {
        final Path path = Files.createTempFile("datagrid-checkpoint", ".bin");
        AeronReplicationCheckpointStore.write(path, checkpoint());
        final byte[] bytes = Files.readAllBytes(path);
        Files.write(path, java.util.Arrays.copyOf(bytes, bytes.length - 1));
        assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
        Files.write(path, bytes);
        bytes[20] ^= 1;
        Files.write(path, bytes);
        assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
        Files.deleteIfExists(path);
    }

    @Test
    void rejectsUnsupportedReservedFieldsEvenWithValidChecksum() throws Exception {
        final Path path = Files.createTempFile("datagrid-checkpoint-reserved", ".bin");
        try {
            AeronReplicationCheckpointStore.write(path, checkpoint());
            final byte[] bytes = Files.readAllBytes(path);
            final ByteBuffer encoded = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            encoded.putShort(9, (short) 1);
            encoded.putInt(AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES,
                    Crc32c.compute(bytes, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES));
            Files.write(path, bytes);

            assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
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
                first.writerEpoch(), first.transactionSequence() + 1, first.recordingPosition() + 10,
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
                AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.PREPARING,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), -2, 0, -1, -1, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTED,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 0, 7, 1024, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTED,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), -1, 0, 7, -1, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTED,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 0, Long.MAX_VALUE, 1024, 1, 1, 0));
    }

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

        /// Reader uncertainty markers use cursor crash phases, never writer checkpoint phases.
    @Test
    void readerCursorUsesCursorCrashPhases() throws Exception {
        final Path path = Files.createTempFile("datagrid-reader-cursor", ".bin");
        final ArrayList<String> phases = new ArrayList<>();
        final AeronReplicationCheckpoint readerCursor = new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 11, 3, 7, 4096, 0, 0, 0);
        try {
            AtomicFileStoreCrashHook.callWithHook((phase, ignored) -> phases.add(phase), () -> {
                AeronReplicationCheckpointStore.write(path, readerCursor);
                return null;
            });
        } finally {
            Files.deleteIfExists(path);
        }
        assertEquals(java.util.List.of(
                "BEFORE_CURSOR_TEMP_WRITE",
                "DURING_CURSOR_FILE_WRITE",
                "AFTER_CURSOR_TEMP_WRITE_BEFORE_RENAME",
                "AFTER_CURSOR_RENAME_BEFORE_DIRECTORY_SYNC"), phases);
    }
}
