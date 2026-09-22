package peruncs.datagrid.cluster.storage.types;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

/// Writes small replication metadata files with forced temporary replacement.
///
/// The operation fails when the filesystem cannot provide atomic rename or
/// directory synchronization; callers must choose a filesystem with those
/// durability primitives for replication metadata.
public final class AtomicFileWriter {
        /// Selects the metadata family whose crash-test hook names are emitted.
    public enum Phase {
        CHECKPOINT,
        CURSOR
    }

    private static final Logger LOGGER = System.getLogger(AtomicFileWriter.class.getName());
    private static final boolean WINDOWS = System.getProperty("os.name", "")
            .toLowerCase(Locale.ROOT).startsWith("windows");
    private static final ScopedValue<BiConsumer<String, Path>> TEST_HOOK = ScopedValue.newInstance();
    private static final FileAttribute<Set<PosixFilePermission>> OWNER_ONLY = PosixFilePermissions.asFileAttribute(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));

    private AtomicFileWriter() {
    }

        /// Runs file operations with a crash-test hook bound to their dynamic scope.
    static void runWithTestHook(final BiConsumer<String, Path> hook, final Runnable action) {
        Objects.requireNonNull(hook, "hook");
        Objects.requireNonNull(action, "action");
        ScopedValue.where(TEST_HOOK, hook).run(action);
    }

        /// Calls a file operation with a crash-test hook bound to its scope.
    static <T, X extends Throwable> T callWithTestHook(
            final BiConsumer<String, Path> hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        Objects.requireNonNull(hook, "hook");
        Objects.requireNonNull(operation, "operation");
        return ScopedValue.where(TEST_HOOK, hook).call(operation);
    }

    static Runnable inheritCurrentTestHook(final Runnable action) {
        Objects.requireNonNull(action, "action");
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
    /// and cursor stores pass [Phase#CHECKPOINT] or [Phase#CURSOR].
    ///
    /// @param path    destination path
    /// @param encoder callback that writes the complete encoded contents
    /// @param phase   metadata family, or `null` for generic names
    /// @throws IOException if writing or replacement fails
    public static void write(final Path path, final Encoder encoder, final Phase phase) throws IOException {
        /* The crash-test hook names are formatted only when a hook is bound:
         * every production write runs with no hook, and the formatted phase
         * names cost four string allocations per metadata write otherwise. */
        if (!TEST_HOOK.isBound()) {
            write(path, encoder, null, null, null, null);
            return;
        }
        final String phaseName = phase == null ? null : phase.name();
        final String beforePhase = phaseName != null ? "BEFORE_%s_TEMP_WRITE".formatted(phaseName) : "BEFORE_TEMP_WRITE";
        final String duringPhase = phaseName != null ? "DURING_%s_FILE_WRITE".formatted(phaseName) : "DURING_FILE_WRITE";
        final String afterTempPhase = phaseName != null
                ? "AFTER_%s_TEMP_WRITE_BEFORE_RENAME".formatted(phaseName)
                : "AFTER_TEMP_WRITE_BEFORE_RENAME";
        final String afterRenamePhase = phaseName != null
                ? "AFTER_%s_RENAME_BEFORE_DIRECTORY_SYNC".formatted(phaseName)
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
        /* Re-verify the parent immediately before creating the temp file: the
         * earlier rejection cannot prevent a component swap between the check
         * and this use. */
        if (!Files.readAttributes(parent, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).isDirectory()) {
            throw new IOException("Metadata parent is not a real directory: %s".formatted(parent));
        }
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
                LOGGER.log(Level.WARNING,
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
        Objects.requireNonNull(bytes, "bytes");
        write(path, channel -> writeFully(channel, ByteBuffer.wrap(bytes)));
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
            write(probe, channel -> writeFully(channel, ByteBuffer.wrap(new byte[]{1})));
        } catch (final IOException | RuntimeException failure) {
            /* Preserve the capability failure itself. Cleanup is best effort and must
             * not replace an informative atomic-move/fsync exception with a secondary
             * delete error. */
            try {
                Files.deleteIfExists(probe);
                forceDirectory(parent);
            } catch (final IOException | RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        /* The probe succeeded, so a cleanup failure must not turn a supported
         * filesystem into a reported capability failure. */
        try {
            Files.deleteIfExists(probe);
            forceDirectory(parent);
        } catch (final IOException | RuntimeException cleanupFailure) {
            LOGGER.log(Level.WARNING,
                    "Unable to remove atomic-metadata probe %s".formatted(probe), cleanupFailure);
        }
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
    /// # Threat model
    ///
    /// The path is re-checked for symlinks immediately before deletion, but a
    /// same-user process that can swap the parent directory between the check
    /// and the delete can still redirect any path-based removal. Full
    /// protection would require a `SecureDirectoryStream` handle chain, which
    /// this utility deliberately does not build. Callers must therefore keep
    /// the metadata directory writable only by the node account.
    ///
    /// @param path                 file to remove
    /// @param forceParentDirectory whether to force the parent directory after removal
    /// @throws IOException if the file or, when requested, its parent directory cannot be synced
    public static void delete(final Path path, final boolean forceParentDirectory) throws IOException {
        final Path absolute = path.toAbsolutePath();
        rejectSymbolicLinks(absolute);
        final Path parent = absolute.getParent();
        /* Re-verify the parent and the target immediately before deleting:
         * the earlier rejection cannot prevent a component swap between that
         * check and this use. */
        if (parent != null
                && !Files.readAttributes(parent, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).isDirectory()) {
            throw new IOException("Metadata parent is not a real directory: %s".formatted(parent));
        }
        if (Files.isSymbolicLink(absolute)) {
            throw new IOException("Path contains a symbolic link: %s".formatted(absolute));
        }
        if (Files.deleteIfExists(absolute)) {
            if (forceParentDirectory) forceDirectory(parent);
        }
    }

    /// Forces a metadata directory after a new file name becomes visible.
    ///
    /// @param parent directory containing the new file
    /// @throws IOException if the filesystem cannot sync the directory
    public static void forceDirectory(final Path parent) throws IOException {
        if (parent == null) {
            return;
        }
        if (WINDOWS) {
            /* Windows cannot open a directory as a channel; NTFS does not
             * expose an equivalent directory-flush operation. */
            return;
        }
        try (FileChannel channel = FileChannel.open(parent, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (final UnsupportedOperationException failure) {
            throw new IOException("Directory fsync is unavailable for replication metadata %s".formatted(parent), failure);
        }
    }

    private static void rejectSymbolicLinks(final Path path) throws IOException {
        PathSecurity.ensureNoSymbolicLinks(path);
    }

    private static void writeFully(final FileChannel channel, final ByteBuffer buffer) throws IOException {
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
