package peruncs.datagrid.cluster.node.replication;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.types.AtomicFileStoreCrashHook;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Tests the durable replication cursor manager.
class StoredReplicationCursorManagerTest {
        /// Verifies atomic cursor path round trips and replaces complete records.
    @Test
    void atomicPathRoundTripsAndReplacesCompleteRecords() throws Exception {
        final Path directory = Files.createTempDirectory("replication-cursor-");
        final Path path = directory.resolve("offset");
        final UUID generation = UUID.randomUUID();
        final ReplicationCursor expected = new ReplicationCursor("aeron", generation, 17L, "010203");
        try {
            try (StoredReplicationCursorManager manager = StoredReplicationCursorManager.NewAtomic(path)) {
                manager.set(expected);
                assertEquals(expected.logicalSequence(), manager.get().logicalSequence());
            }
            try (StoredReplicationCursorManager manager = StoredReplicationCursorManager.NewAtomic(path)) {
                final ReplicationCursor restored = manager.get();
                assertEquals(expected.logicalSequence(), restored.logicalSequence());
                assertEquals(expected.transport(), restored.transport());
                assertEquals(expected.storeGeneration(), restored.storeGeneration());
                assertEquals(expected.providerPosition(), restored.providerPosition());
            }
            try (var files = Files.list(directory)) {
                assertEquals(1L, files.count(), "atomic replacement must not leave a temp file");
            }
        } finally {
            try (var files = Files.walk(directory)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(file ->
                {
                    try {
                        Files.deleteIfExists(file);
                    } catch (final java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                });
            }
        }
    }

        /// Verifies production cursor writes use the shared atomic-file boundary.
    @Test
    void atomicCursorWriteUsesAtomicFileStore() throws Exception {
        final Path directory = Files.createTempDirectory("replication-cursor-hook-");
        final Path path = directory.resolve("offset");
        final AtomicReference<String> phase = new AtomicReference<>();
        try {
            AtomicFileStoreCrashHook.runWithHook((name, ignored) -> phase.compareAndSet(null, name), () -> {
                try (StoredReplicationCursorManager manager = StoredReplicationCursorManager.NewAtomic(path)) {
                    manager.set(new ReplicationCursor("aeron", UUID.randomUUID(), 1L, "04"));
                }
            });
            assertEquals("BEFORE_CURSOR_TEMP_WRITE", phase.get());
        } finally {
            try (var files = Files.walk(directory)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(file ->
                {
                    try {
                        Files.deleteIfExists(file);
                    } catch (final java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                });
            }
        }
    }
}
