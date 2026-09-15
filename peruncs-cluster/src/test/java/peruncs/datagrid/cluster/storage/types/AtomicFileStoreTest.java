package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Tests atomic file store behavior.
class AtomicFileStoreTest {
    private static void write(final java.nio.channels.FileChannel channel, final String value)
            throws java.io.IOException {
        final ByteBuffer buffer = StandardCharsets.UTF_8.encode(value);
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) == 0) throw new java.io.IOException("Test file write made no progress");
        }
    }

    private static void delete(final Path directory) throws Exception {
        try (var paths = Files.walk(directory)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
            {
                try {
                    Files.deleteIfExists(path);
                } catch (final java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
    }

        /// Verifies failed replacement leaves previous file intact.
    @Test
    void failedReplacementLeavesPreviousFileIntact() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-store-");
        final Path file = directory.resolve("checkpoint");
        try {
            AtomicFileStore.write(file, channel -> write(channel, "old"));
            assertThrows(java.io.IOException.class, () -> AtomicFileStore.write(file, channel ->
            {
                write(channel, "new");
                throw new java.io.IOException("injected crash before rename");
            }));
            assertEquals("old", Files.readString(file, StandardCharsets.UTF_8));
            try (var paths = Files.list(directory)) {
                assertEquals(1L, paths.count(), "failed write must not leave temp files");
            }
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
                {
                    try {
                        Files.deleteIfExists(path);
                    } catch (final java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                });
            }
        }
    }

        /// Verifies crash during temporary write leaves previous file intact.
    @Test
    void crashDuringTemporaryWriteLeavesPreviousFileIntact() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-store-");
        final Path file = directory.resolve("checkpoint");
        try {
            AtomicFileStore.write(file, channel -> write(channel, "old"));
            AtomicFileStore.runWithTestHook((phase, ignored) ->
            {
                if ("DURING_FILE_WRITE".equals(phase)) throw new IllegalStateException("simulated crash");
            }, () -> assertThrows(IllegalStateException.class,
                    () -> AtomicFileStore.write(file, channel -> write(channel, "new"))));
            assertEquals("old", Files.readString(file, StandardCharsets.UTF_8));
        } finally {
            delete(directory);
        }
    }

        /// Verifies crash after temporary force before rename leaves previous file intact.
    @Test
    void crashAfterTemporaryForceBeforeRenameLeavesPreviousFileIntact() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-store-");
        final Path file = directory.resolve("checkpoint");
        try {
            AtomicFileStore.write(file, channel -> write(channel, "old"));
            AtomicFileStore.runWithTestHook((phase, ignored) ->
            {
                if ("AFTER_TEMP_WRITE_BEFORE_RENAME".equals(phase)) throw new IllegalStateException("simulated crash");
            }, () -> assertThrows(IllegalStateException.class,
                    () -> AtomicFileStore.write(file, channel -> write(channel, "new"))));
            assertEquals("old", Files.readString(file, StandardCharsets.UTF_8));
        } finally {
            delete(directory);
        }
    }

        /// Verifies crash after rename leaves the new complete file visible.
    @Test
    void crashAfterRenameLeavesTheNewCompleteFileVisible() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-store-");
        final Path file = directory.resolve("checkpoint");
        try {
            AtomicFileStore.write(file, channel -> write(channel, "old"));
            AtomicFileStore.runWithTestHook((phase, ignored) ->
            {
                if ("AFTER_RENAME_BEFORE_DIRECTORY_SYNC".equals(phase)) throw new IllegalStateException("simulated crash");
            }, () -> assertThrows(IllegalStateException.class,
                    () -> AtomicFileStore.write(file, channel -> write(channel, "new"))));
            assertEquals("new", Files.readString(file, StandardCharsets.UTF_8));
        } finally {
            delete(directory);
        }
    }

        /// Verifies metadata writes cannot be redirected through a nested symlink.
    @Test
    void rejectsNestedSymbolicLink() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-store-link-");
        final Path target = Files.createTempDirectory("atomic-file-store-target-");
        final Path link = directory.resolve("link");
        try {
            Files.createSymbolicLink(link, target);
            assertThrows(java.io.IOException.class,
                    () -> AtomicFileStore.write(link.resolve("checkpoint"), channel -> write(channel, "data")));
        } finally {
            delete(directory);
            delete(target);
        }
    }
}
