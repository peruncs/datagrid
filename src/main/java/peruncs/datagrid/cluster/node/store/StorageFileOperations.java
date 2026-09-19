package peruncs.datagrid.cluster.node.store;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.storage.types.PathSecurity;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;

/// Shared filesystem operations used by backup backends and node setup.
///
/// This class is public only because backup and node packages share it;
/// it is not application API.
public final class StorageFileOperations {
    private static final System.Logger LOGGER = System.getLogger(StorageFileOperations.class.getName());

    private StorageFileOperations() {
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
            throw new NodeLibraryException("Backup source or destination path is unsafe", failure);
        }
        if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
            throw new NodeLibraryException("Backup destination already contains storage: %s".formatted(destination));
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
            throw new NodeLibraryException("Atomic backup restore is not supported", unsupported);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to install restored storage", failure);
        }
    }

    /// Deletes a staging directory, suppressing cleanup failures into the primary one.
    ///
    /// @param path directory to delete
    /// @param primaryFailure failure being handled, or `null` to throw cleanup failures directly
    public static void cleanup(final Path path, final Throwable primaryFailure) {
        try {
            deleteDirectory(path);
        } catch (final NodeLibraryException cleanupFailure) {
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(cleanupFailure);
            } else {
                throw cleanupFailure;
            }
        }
    }

    /// Rejects paths containing user-controlled symbolic links.
    ///
    /// @param path path to check, or `null` for no check
    /// @throws IOException if a link is found
    public static void ensureNoSymbolicLinks(final Path path) throws IOException {
        PathSecurity.ensureNoSymbolicLinks(path);
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
        return PathSecurity.isSystemPrivateAlias(path);
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

    /// Forces directory metadata to stable storage.
    ///
    /// Windows cannot open a directory as a channel; the call is a documented
    /// no-op there because NTFS does not expose an equivalent directory-flush
    /// operation. Every other failure is reported with the directory path.
    ///
    /// @param directory directory to force
    /// @throws IOException if the filesystem refuses
    public static void forceDirectory(final Path directory) throws IOException {
        if (System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")) {
            return;
        }
        try (final FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (final UnsupportedOperationException | IOException failure) {
            throw new IOException("Filesystem does not support forcing directory metadata: %s".formatted(directory), failure);
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
    /// @throws NodeLibraryException when the path is unsafe or deletion fails
    public static void deleteDirectory(final Path path) throws NodeLibraryException {
        final Path absolute = path.toAbsolutePath().normalize();
        if (absolute.getParent() == null || absolute.getNameCount() < 2) {
            throw new NodeLibraryException("Refusing to delete a top-level path: %s".formatted(absolute));
        }
        final Path home = Path.of(System.getProperty("user.home", "")).toAbsolutePath().normalize();
        if (absolute.equals(home)) {
            throw new NodeLibraryException("Refusing to delete the user home directory: %s".formatted(absolute));
        }
        try {
            ensureNoSymbolicLinks(absolute);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Directory path contains a symbolic link: %s".formatted(absolute), failure);
        }
        if (!Files.exists(absolute, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        LOGGER.log(System.Logger.Level.INFO, "Deleting directory tree at %s".formatted(absolute));
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
                    throw new NodeLibraryException("Failed to delete file at %s".formatted(file), e);
                }
            });
        } catch (final NoSuchFileException ignored) {
            /* A concurrent cleanup may remove the root after the existence check.
             * Deletion is intentionally idempotent for backup retry paths. */
        } catch (final IOException e) {
            throw new NodeLibraryException("Failed to iterate files at %s".formatted(absolute), e);
        }
    }
}
