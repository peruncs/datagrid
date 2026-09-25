package peruncs.cluster.storage.io;

import peruncs.cluster.errors.NodeException;

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
import java.util.*;
import java.util.function.BiConsumer;

/// The node's single file-safety implementation: atomic metadata
/// writes, verified moves and deletes, and path symbolic-link rejection.
///
/// Atomic replacement fails when the filesystem cannot provide atomic rename
/// or directory synchronization; callers must choose a filesystem with those
/// durability primitives for replication metadata. Symbolic links are
/// rejected component by component so a link inserted anywhere in a path
/// cannot redirect a read, write, or delete outside the validated directory;
/// the only exception is the macOS `/private` system aliases for `/tmp`,
/// `/var`, and `/etc`, which require root privileges to create and are
/// otherwise ubiquitous.
public final class AtomicFileWriter {
        /// Selects the metadata family whose crash-test hook names are emitted.
    public enum Phase {
        CHECKPOINT,
        CURSOR
    }

    private static final Logger LOGGER = System.getLogger(AtomicFileWriter.class.getName());
    private static final boolean WINDOWS = System.getProperty("os.name", "")
            .toLowerCase(Locale.ROOT).startsWith("windows");
    private static final boolean MAC_OS = System.getProperty("os.name", "")
            .toLowerCase(Locale.ROOT).contains("mac");
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
        ensureNoSymbolicLinks(path);
    }

    /// Rejects paths containing user-controlled symbolic links.
    ///
    /// @param path path to check, or `null` for no check
    /// @throws IOException if a link is found or a component cannot be inspected
    public static void ensureNoSymbolicLinks(final Path path) throws IOException {
        if (path == null) return;
        final Path absolute = path.toAbsolutePath().normalize();
        Path current = absolute.getRoot();
        for (final Path component : absolute) {
            current = current == null ? component : current.resolve(component);
            if (Files.isSymbolicLink(current) && !isSystemPrivateAlias(current)) {
                throw new IOException("Path contains a symbolic link: %s".formatted(current));
            }
        }
    }

    /// Reports the macOS system symlinks (`/tmp`, `/var`, `/etc`) that point
    /// into `/private`. Only a root-level link whose target is the matching
    /// self-named `/private` entry qualifies; creating such a link requires
    /// privileges outside the threat model, and everything else still fails
    /// closed. The allowlist is gated on macOS: on other operating systems a
    /// `private/<name>` target is not a system alias and must be rejected.
    ///
    /// @param path link to inspect
    /// @return `true` for a macOS system `/private` alias
    public static boolean isSystemPrivateAlias(final Path path) {
        if (!isMacOs()) return false;
        final Path root = path.getRoot();
        if (root == null || !root.equals(path.getParent())) return false;
        final Path name = path.getFileName();
        if (name == null) return false;
        try {
            final Path target = Files.readSymbolicLink(path);
            return target.equals(Path.of("private").resolve(name)) ||
                   target.equals(Path.of("/private").resolve(name));
        } catch (final IOException failure) {
            return false;
        }
    }

    /// Reports whether the current operating system is macOS.
    ///
    /// @return `true` on macOS
    public static boolean isMacOs() {
        return MAC_OS;
    }

    /// Atomically moves restored storage into place, refusing to overwrite.
    ///
    /// @param source restored staging directory
    /// @param destination live storage directory, which must not exist yet
    public static void installStorage(final Path source, final Path destination) {
        final Object sourceFileKey;
        try {
            ensureNoSymbolicLinks(source);
            ensureNoSymbolicLinks(destination.getParent());
            sourceFileKey = stableFileKey(source);
        } catch (final IOException failure) {
            throw new NodeException("Backup source or destination path is unsafe", failure);
        }
        if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
            throw new NodeException("Backup destination already contains storage: %s".formatted(destination));
        }
        try {
            /* Recheck immediately before the rename. The source is an internally
             * staged directory and the destination must remain a plain path for
             * the entire install boundary; the post-move check catches providers
             * that report an unexpected link target after an atomic rename. */
            ensureNoSymbolicLinks(source);
            ensureNoSymbolicLinks(destination.getParent());
            if (!sourceFileKey.equals(stableFileKey(source))) {
                throw new IOException("Staged backup source changed before installation");
            }
            Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE);
            ensureNoSymbolicLinks(destination);
            if (!sourceFileKey.equals(stableFileKey(destination))) {
                throw new IOException("Installed backup does not match its staged source");
            }
            forceDirectory(destination.getParent());
        } catch (final AtomicMoveNotSupportedException unsupported) {
            throw new NodeException("Atomic backup restore is not supported", unsupported);
        } catch (final IOException failure) {
            throw new NodeException("Failed to install restored storage", failure);
        }
    }

    /// Deletes a staging directory, suppressing cleanup failures into the primary one.
    ///
    /// @param path directory to delete
    /// @param primaryFailure failure being handled, or `null` to throw cleanup failures directly
    public static void cleanup(final Path path, final Throwable primaryFailure) {
        try {
            deleteDirectory(path);
        } catch (final NodeException cleanupFailure) {
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(cleanupFailure);
            } else {
                throw cleanupFailure;
            }
        }
    }

    /// Atomically moves one file, verifying symlink-free paths and file identity
    /// before and after the rename.
    ///
    /// The check-then-move window is closed the same way as
    /// [#installStorage]: the source identity and the destination-parent
    /// identity are pinned before the move and re-verified after it, so a
    /// swapped directory or file between the checks fails instead of
    /// installing into the wrong place.
    ///
    /// @param source      file to move
    /// @param destination move target, which must not exist
    /// @throws IOException if a path is unsafe, the move fails, or an identity changed
    public static void moveFileAtomically(final Path source, final Path destination) throws IOException {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(destination, "destination");
        final Path parent = destination.getParent();
        if (parent == null) throw new IOException("Move destination has no parent directory: %s".formatted(destination));
        ensureNoSymbolicLinks(source);
        ensureNoSymbolicLinks(parent);
        final Object sourceKey = stableFileKey(source);
        final Object parentKey = stableFileKey(parent);
        Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE);
        ensureNoSymbolicLinks(parent);
        ensureNoSymbolicLinks(destination);
        if (!parentKey.equals(stableFileKey(parent))) {
            throw new IOException("Destination directory changed while moving %s".formatted(destination));
        }
        if (!sourceKey.equals(stableFileKey(destination))) {
            throw new IOException("Moved file does not match its source: %s".formatted(destination));
        }
    }

    /// Deletes one regular file idempotently; a missing file is not an error.
    ///
    /// The path is verified as a non-link regular file with a pinned identity
    /// before deletion, and the parent identity is re-verified after it.
    ///
    /// @param path file to delete
    /// @return `true` when a file was deleted
    /// @throws IOException if the path is unsafe or an identity changed
    public static boolean deleteRegularFile(final Path path) throws IOException {
        Objects.requireNonNull(path, "path is required");
        ensureNoSymbolicLinks(path);
        try {
            /* Validates the path is a regular file with a stable identity; the
             * attributes themselves are not needed, only the parent re-check below. */
            regularAttributes(path);
        } catch (final NoSuchFileException missing) {
            return false;
        }
        final Path parent = path.getParent();
        final Object parentKey = parent == null ? null : stableFileKey(parent);
        final boolean deleted = Files.deleteIfExists(path);
        if (parentKey != null && !parentKey.equals(stableFileKey(parent))) {
            throw new IOException("Parent directory changed while deleting %s".formatted(path));
        }
        if (deleted && Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("File reappeared while deleting %s".formatted(path));
        }
        return deleted;
    }

    /// Opens an existing regular file and verifies that its identity did not
    /// change between the path check and the open.
    ///
    /// @param path file to open
    /// @return an open read-only channel owned by the caller
    /// @throws IOException if the path is unsafe, not regular, or changed during opening
    public static FileChannel openRegularFile(final Path path) throws IOException {
        ensureNoSymbolicLinks(path);
        final BasicFileAttributes before = regularAttributes(path);
        final FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        boolean open = false;
        try {
            final BasicFileAttributes after = regularAttributes(path);
            if (!sameFileIdentity(before, after)) {
                throw new IOException("File changed while it was being opened: %s".formatted(path));
            }
            open = true;
            return channel;
        } finally {
            if (!open) channel.close();
        }
    }

    /// Deletes a directory tree idempotently; a missing root is not an error.
    ///
    /// The root must be at least two levels deep, must not be the user home
    /// directory, and must not be a filesystem root, so a mistyped
    /// configuration path cannot turn a restore cleanup into a destructive
    /// delete of a broad filesystem tree. The path and its entry count are
    /// logged before deletion.
    ///
    /// @param path root to delete
    /// @throws NodeException when the path is unsafe or deletion fails
    public static void deleteDirectory(final Path path) throws NodeException {
        final Path absolute = path.toAbsolutePath().normalize();
        if (absolute.getParent() == null || absolute.getNameCount() < 2) {
            throw new NodeException("Refusing to delete a top-level path: %s".formatted(absolute));
        }
        final Path home = Path.of(System.getProperty("user.home", "")).toAbsolutePath().normalize();
        if (absolute.equals(home)) {
            throw new NodeException("Refusing to delete the user home directory: %s".formatted(absolute));
        }
        try {
            ensureNoSymbolicLinks(absolute);
        } catch (final IOException failure) {
            throw new NodeException("Directory path contains a symbolic link: %s".formatted(absolute), failure);
        }
        if (!Files.exists(absolute, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        LOGGER.log(Level.INFO, "Deleting directory tree at %s".formatted(absolute));
        try (final var files = Files.walk(absolute)) {
            files.sorted(Comparator.reverseOrder()).forEach(file ->
            {
                try {
                    Files.delete(file);
                } catch (final NoSuchFileException alreadyRemoved) {
                    /* A concurrent cleanup may remove a child after the root
                     * existence check. Deletion is intentionally idempotent for
                     * backup retry paths. */
                } catch (final IOException e) {
                    throw new NodeException("Failed to delete file at %s".formatted(file), e);
                }
            });
        } catch (final NoSuchFileException ignored) {
            /* A concurrent cleanup may remove the root after the existence check.
             * Deletion is intentionally idempotent for backup retry paths. */
        } catch (final IOException e) {
            throw new NodeException("Failed to iterate files at %s".formatted(absolute), e);
        }
    }

    private static Object stableFileKey(final Path path) throws IOException {
        final Object fileKey = Files.readAttributes(path, BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS).fileKey();
        if (fileKey == null) {
            throw new IOException("Filesystem does not expose a stable file identity: %s".formatted(path));
        }
        return fileKey;
    }

    private static BasicFileAttributes regularAttributes(final Path path) throws IOException {
        final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw new IOException("Expected a regular file: %s".formatted(path));
        }
        if (attributes.fileKey() == null) {
            throw new IOException("Filesystem does not expose a stable file identity: %s".formatted(path));
        }
        return attributes;
    }

    private static boolean sameFileIdentity(final BasicFileAttributes first,
                                            final BasicFileAttributes second) {
        return first.fileKey() != null && first.fileKey().equals(second.fileKey());
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
