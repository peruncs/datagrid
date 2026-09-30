package peruncs.cluster.storage.io;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;


/// Tests atomic file store behavior.
class AtomicFileWriterTest {
    private static void write(final java.nio.channels.FileChannel channel, final String value)
            throws IOException {
        final ByteBuffer buffer = StandardCharsets.UTF_8.encode(value);
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) == 0) throw new IOException("Test file write made no progress");
        }
    }

    private static void delete(final Path directory) throws Exception {
        try (var paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path ->
            {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException failure) {
                    throw new UncheckedIOException(failure);
                }
            });
        }
    }

        /// Verifies failed replacement leaves previous file intact.
    @Test
    void failedReplacementLeavesPreviousFileIntact() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-metadata-");
        final Path file = directory.resolve("metadata");
        try {
            AtomicFileWriter.write(file, channel -> write(channel, "old"));
            assertThrows(IOException.class, () -> AtomicFileWriter.write(file, channel ->
            {
                write(channel, "new");
                throw new IOException("injected crash before rename");
            }));
            assertEquals("old", Files.readString(file, StandardCharsets.UTF_8));
            try (var paths = Files.list(directory)) {
                assertEquals(1L, paths.count(), "failed write must not leave temp files");
            }
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path ->
                {
                    try {
                        Files.deleteIfExists(path);
                    } catch (final IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                });
            }
        }
    }

        /// Verifies crash during temporary write leaves previous file intact.
    @Test
    void crashDuringTemporaryWriteLeavesPreviousFileIntact() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-metadata-");
        final Path file = directory.resolve("metadata");
        try {
            AtomicFileWriter.write(file, channel -> write(channel, "old"));
            FaultInjection.runWithHook((phase, sequence, path) ->
            {
                if ("DURING_FILE_WRITE".equals(phase)) throw new IllegalStateException("simulated crash");
            }, () -> assertThrows(IllegalStateException.class,
                    () -> AtomicFileWriter.write(file, channel -> write(channel, "new"))));
            assertEquals("old", Files.readString(file, StandardCharsets.UTF_8));
        } finally {
            delete(directory);
        }
    }

        /// Verifies crash after temporary force before rename leaves previous file intact.
    @Test
    void crashAfterTemporaryForceBeforeRenameLeavesPreviousFileIntact() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-metadata-");
        final Path file = directory.resolve("metadata");
        try {
            AtomicFileWriter.write(file, channel -> write(channel, "old"));
            FaultInjection.runWithHook((phase, sequence, path) ->
            {
                if ("AFTER_TEMP_WRITE_BEFORE_RENAME".equals(phase)) throw new IllegalStateException("simulated crash");
            }, () -> assertThrows(IllegalStateException.class,
                    () -> AtomicFileWriter.write(file, channel -> write(channel, "new"))));
            assertEquals("old", Files.readString(file, StandardCharsets.UTF_8));
        } finally {
            delete(directory);
        }
    }

        /// Verifies crash after rename leaves the new complete file visible.
    @Test
    void crashAfterRenameLeavesTheNewCompleteFileVisible() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-store-");
        final Path file = directory.resolve("metadata");
        try {
            AtomicFileWriter.write(file, channel -> write(channel, "old"));
            FaultInjection.runWithHook((phase, sequence, path) ->
            {
                if ("AFTER_RENAME_BEFORE_DIRECTORY_SYNC".equals(phase)) throw new IllegalStateException("simulated crash");
            }, () -> assertThrows(IllegalStateException.class,
                    () -> AtomicFileWriter.write(file, channel -> write(channel, "new"))));
            assertEquals("new", Files.readString(file, StandardCharsets.UTF_8));
        } finally {
            delete(directory);
        }
    }

    /// Verifies a failed Store directory install restores the previous image after the new rename.
    @Test
    void failedStorageReplacementRestoresPreviousImage() throws Exception {
        final Path parent = Files.createTempDirectory("atomic-storage-replace-");
        final Path source = parent.resolve("staged");
        final Path destination = parent.resolve("storage");
        Files.createDirectories(source);
        Files.createDirectories(destination);
        Files.writeString(source.resolve("data"), "new");
        Files.writeString(destination.resolve("data"), "old");
        try {
            FaultInjection.runWithHook((phase, sequence, path) -> {
                if ("AFTER_STORAGE_RENAME_BEFORE_DIRECTORY_SYNC".equals(phase)) {
                    throw new IllegalStateException("simulated install failure");
                }
            }, () -> assertThrows(IllegalStateException.class,
                    () -> AtomicFileWriter.replaceStorage(source, destination)));

            assertEquals("old", Files.readString(destination.resolve("data")));
            assertTrue(Files.exists(source.resolve("data")), "the staged replacement stays available for cleanup");
            try (var paths = Files.list(parent)) {
                assertEquals(2L, paths.count(), "rollback must not leave a hidden previous Store directory");
            }
        } finally {
            delete(parent);
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
            assertThrows(IOException.class,
                    () -> AtomicFileWriter.write(link.resolve("metadata"), channel -> write(channel, "data")));
        } finally {
            delete(directory);
            delete(target);
        }
    }

        /// Verifies deletes cannot be redirected through a nested symlink: the
        /// last-moment re-check rejects the link instead of removing a file in
    /// the link target.
    @Test
    void deleteRejectsNestedSymbolicLink() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-store-delete-link-");
        final Path target = Files.createTempDirectory("atomic-file-store-delete-target-");
        final Path link = directory.resolve("link");
        try {
            Files.createSymbolicLink(link, target);
            final Path secret = target.resolve("metadata");
            Files.writeString(secret, "keep");
            assertThrows(IOException.class,
                    () -> AtomicFileWriter.delete(link.resolve("metadata")));
            assertTrue(Files.exists(secret), "a rejected delete must leave the link target untouched");
        } finally {
            delete(directory);
            delete(target);
        }
    }

    @Test
    void deleteAllowsAReplacementWithANewIdentity() throws Exception {
        final Path directory = Files.createTempDirectory("atomic-file-delete-race-");
        final Path file = directory.resolve("metadata");
        final Path replacement = directory.resolve("replacement");
        try {
            Files.writeString(file, "old");
            Files.writeString(replacement, "new");
            final boolean deleted = FaultInjection.callWithHook((phase, sequence, path) -> {
                if ("AFTER_REGULAR_DELETE".equals(phase)) {
                    try {
                        Files.move(replacement, file);
                    } catch (final IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                }
            }, () -> AtomicFileWriter.deleteRegularFile(file));
            assertTrue(deleted);
            assertEquals("new", Files.readString(file));
        } finally {
            delete(directory);
        }
    }
}
