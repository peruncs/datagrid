package peruncs.datagrid.cluster.storage.types;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

/// Writes small replication metadata files with forced temporary replacement.
/// The operation fails when the filesystem cannot provide atomic rename or
/// directory synchronization; callers must choose a filesystem with those
/// durability primitives for replication metadata.
public final class AtomicFileStore {
        /// Selects checkpoint-specific crash-test phases.
    public static final String PHASE_CHECKPOINT = "CHECKPOINT";
        /// Selects cursor-specific crash-test phases.
    public static final String PHASE_CURSOR = "CURSOR";
    private static final System.Logger LOGGER = System.getLogger(AtomicFileStore.class.getName());
    private static final ScopedValue<BiConsumer<String, Path>> TEST_HOOK = ScopedValue.newInstance();
    private static final FileAttribute<Set<PosixFilePermission>> OWNER_ONLY =
            PosixFilePermissions.asFileAttribute(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));

    private AtomicFileStore() {
    }

        /// Runs file operations with a crash-test hook bound to their dynamic scope.
    static void runWithTestHook(final BiConsumer<String, Path> hook, final Runnable action) {
        if (hook == null) throw new NullPointerException("hook");
        if (action == null) throw new NullPointerException("action");
        ScopedValue.where(TEST_HOOK, hook).run(action);
    }

        /// Calls a file operation with a crash-test hook bound to its scope.
    static <T, X extends Throwable> T callWithTestHook(
            final BiConsumer<String, Path> hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        if (hook == null) throw new NullPointerException("hook");
        if (operation == null) throw new NullPointerException("operation");
        return ScopedValue.where(TEST_HOOK, hook).call(operation);
    }

    static Runnable inheritCurrentTestHook(final Runnable action) {
        if (action == null) throw new NullPointerException("action");
        final BiConsumer<String, Path> hook = TEST_HOOK.isBound() ? TEST_HOOK.get() : null;
        return hook == null ? action : () -> ScopedValue.where(TEST_HOOK, hook).run(action);
    }

    private static void testPoint(final String phase, final Path path) {
        final BiConsumer<String, Path> hook = TEST_HOOK.isBound() ? TEST_HOOK.get() : null;
        if (hook != null) hook.accept(phase, path);
    }

        /// Writes a file through a forced sibling temporary file and replacement.
    ///
    /// The `phase` parameter selects the crash-test hook names.
    /// When `null` the generic names `BEFORE_TEMP_WRITE`,
    /// `DURING_FILE_WRITE`, `AFTER_TEMP_WRITE_BEFORE_RENAME`,
    /// and `AFTER_RENAME_BEFORE_DIRECTORY_SYNC` are used. Checkpoint
    /// and cursor stores pass [#PHASE_CHECKPOINT] or [#PHASE_CURSOR].
    ///
    /// @param path    destination path
    /// @param encoder callback that writes the complete encoded contents
    /// @param phase   crash-test hook phase name, or `null` for generic names
    /// @throws IOException if writing or replacement fails
    public static void write(final Path path, final Encoder encoder, final String phase) throws IOException {
        if (phase != null && !PHASE_CHECKPOINT.equals(phase) && !PHASE_CURSOR.equals(phase)) {
            throw new IllegalArgumentException("unsupported AtomicFileStore phase: %s".formatted(phase));
        }
        final String beforePhase = phase != null ? "BEFORE_%s_TEMP_WRITE".formatted(phase) : "BEFORE_TEMP_WRITE";
        final String duringPhase = phase != null ? "DURING_%s_FILE_WRITE".formatted(phase) : "DURING_FILE_WRITE";
        final String afterTempPhase = phase != null
                ? "AFTER_%s_TEMP_WRITE_BEFORE_RENAME".formatted(phase)
                : "AFTER_TEMP_WRITE_BEFORE_RENAME";
        final String afterRenamePhase = phase != null
                ? "AFTER_%s_RENAME_BEFORE_DIRECTORY_SYNC".formatted(phase)
                : "AFTER_RENAME_BEFORE_DIRECTORY_SYNC";
        write(path, encoder, beforePhase, duringPhase, afterTempPhase, afterRenamePhase);
    }

    private static void write(final Path path, final Encoder encoder,
                              final String beforePhase, final String duringPhase,
                              final String afterTempPhase, final String afterRenamePhase) throws IOException {
        final Path absolute = path.toAbsolutePath();
        final Path parent = absolute.getParent();
        if (parent == null) {
            throw new IOException("Metadata path has no parent directory: %s".formatted(path));
        }
        rejectSymbolicLinks(absolute);
        Files.createDirectories(parent);
        Path temporary;
        try {
            temporary = Files.createTempFile(parent, "%s.tmp-".formatted(absolute.getFileName()), null, OWNER_ONLY);
        } catch (final UnsupportedOperationException failure) {
            throw new IOException("Owner-only permissions are unavailable for replication metadata " + absolute, failure);
        }
        try {
            testPoint(beforePhase, absolute);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                testPoint(duringPhase, absolute);
                encoder.write(channel);
                channel.force(true);
            }
            testPoint(afterTempPhase, absolute);
            try {
                Files.move(temporary, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (final AtomicMoveNotSupportedException failure) {
                throw new IOException("Atomic replacement is unavailable for replication metadata %s".formatted(absolute), failure);
            }
            testPoint(afterRenamePhase, absolute);
            forceDirectory(parent);
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (final IOException | RuntimeException cleanupFailure) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Unable to remove temporary replication metadata file %s".formatted(temporary), cleanupFailure);
            }
        }
    }

        /// Writes a file through a forced sibling temporary file and replacement.
    /// Uses generic phase names for the crash-test hook.
    ///
    /// @param path    destination path
    /// @param encoder callback that writes the complete encoded contents
    /// @throws IOException if writing or replacement fails
    public static void write(final Path path, final Encoder encoder) throws IOException {
        write(path, encoder, null);
    }

        /// Writes raw bytes through the atomic replacement protocol.
    ///
    /// @param path  destination path
    /// @param bytes complete file contents
    /// @throws IOException if writing or replacement fails
    public static void writeBytes(final Path path, final byte[] bytes) throws IOException {
        if (bytes == null) throw new NullPointerException("bytes");
        write(path, channel -> writeFully(channel, java.nio.ByteBuffer.wrap(bytes)));
    }

        /// Verifies that the directory containing `path` supports the complete
    /// atomic metadata protocol without changing the target file.
    ///
    /// @param path representative metadata path
    /// @throws IOException if temporary replacement or directory synchronization is unavailable
    public static void verify(final Path path) throws IOException {
        final Path absolute = path.toAbsolutePath();
        final Path parent = absolute.getParent();
        if (parent == null) {
            throw new IOException("Metadata path has no parent directory: %s".formatted(path));
        }
        rejectSymbolicLinks(absolute);
        Files.createDirectories(parent);
        final Path probe = parent.resolve("%s.probe-%s".formatted(absolute.getFileName(), UUID.randomUUID()));
        try {
            write(probe, channel -> writeFully(channel, java.nio.ByteBuffer.wrap(new byte[]{1})));
        } catch (final IOException | RuntimeException | Error failure) {
            /* Preserve the capability failure itself. Cleanup is best effort and must
             * not replace an informative atomic-move/fsync exception with a secondary
             * delete error. */
            try {
                Files.deleteIfExists(probe);
                forceDirectory(parent);
            } catch (final IOException | RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        Files.deleteIfExists(probe);
        forceDirectory(parent);
    }

        /// Deletes a metadata file and forces the parent directory when the file was
    /// present. This is used for short-lived in-flight recovery records: removing
    /// the record must be durable just like replacing the terminal checkpoint.
    ///
    /// @param path file to remove
    /// @throws IOException if the file or its parent directory cannot be synced
    public static void delete(final Path path) throws IOException {
        delete(path, true);
    }

        /// Deletes a metadata file and optionally forces its parent directory.
    ///
    /// Callers may omit the directory force only for fail-closed markers whose
    /// stale presence is safe after a crash. A stale marker causes reseeding; it
    /// must never make an uncheckpointed Store import appear durable.
    ///
    /// @param path                 file to remove
    /// @param forceParentDirectory whether to force the parent directory after removal
    /// @throws IOException if the file or, when requested, its parent directory cannot be synced
    public static void delete(final Path path, final boolean forceParentDirectory) throws IOException {
        final Path absolute = path.toAbsolutePath();
        rejectSymbolicLinks(absolute);
        if (Files.deleteIfExists(absolute)) {
            if (forceParentDirectory) forceDirectory(absolute.getParent());
        }
    }

    private static void forceDirectory(final Path parent) throws IOException {
        if (parent == null) {
            return;
        }
        try (FileChannel channel = FileChannel.open(parent, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (final UnsupportedOperationException failure) {
            throw new IOException("Directory fsync is unavailable for replication metadata %s".formatted(parent), failure);
        }
    }

    private static void rejectSymbolicLinks(final Path path) throws IOException {
        for (Path current = path.toAbsolutePath(); current != null; current = current.getParent()) {
            if (Files.isSymbolicLink(current) && !isSystemPrivateAlias(current)) {
                throw new IOException("Replication metadata path must not contain a symbolic link: " + current);
            }
        }
    }

        /// Reports the macOS system symlinks (`/tmp`, `/var`, `/etc`) that point
    /// into `/private`. Only a root-level link whose target is the matching
    /// self-named `/private` entry qualifies; creating such a link requires
    /// privileges outside the threat model, and everything else still fails
    /// closed. The shape mirrors the OS convention instead of a fixed name
    /// list so future system aliases keep working.
    private static boolean isSystemPrivateAlias(final Path path) {
        final Path root = path.getRoot();
        if (root == null || !root.equals(path.getParent())) return false;
        final Path name = path.getFileName();
        if (name == null) return false;
        try {
            final Path target = Files.readSymbolicLink(path);
            return target.equals(Path.of("private").resolve(name)) ||
                   target.equals(Path.of("/private").resolve(name));
        } catch (final IOException ignored) {
            return false;
        }
    }

    private static void writeFully(final FileChannel channel, final java.nio.ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) == 0) throw new IOException("Atomic metadata write made no progress");
        }
    }

        /// Writes one complete metadata file to an open channel.
    @FunctionalInterface
    public interface Encoder {
                /// Writes the encoded bytes.
        ///
        /// @param channel open destination channel
        /// @throws IOException if writing fails
        void write(FileChannel channel) throws IOException;
    }
}
