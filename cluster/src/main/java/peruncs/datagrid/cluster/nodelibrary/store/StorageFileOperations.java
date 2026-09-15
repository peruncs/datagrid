package peruncs.datagrid.cluster.nodelibrary.store;

import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.Set;

/**
 * Shared filesystem operations used by backup backends and node setup.
 *
 * <p>This class is public only because backup and node packages share it;
 * it is not application API.</p>
 */
public final class StorageFileOperations {
    private StorageFileOperations() {
    }

    public static void installStorage(final Path source, final Path destination) {
        try {
            ensureNoSymbolicLinks(destination.getParent());
        } catch (final IOException failure) {
            throw new NodelibraryException("Backup destination contains a symbolic link", failure);
        }
        if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
            throw new NodelibraryException("Backup destination already contains storage: " + destination);
        }
        try {
            Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE);
            forceDirectory(destination.getParent());
        } catch (final AtomicMoveNotSupportedException unsupported) {
            throw new NodelibraryException("Atomic backup restore is not supported", unsupported);
        } catch (final IOException failure) {
            throw new NodelibraryException("Failed to install restored storage", failure);
        }
    }

    public static void cleanup(final Path path, final Throwable primaryFailure) {
        try {
            deleteDirectory(path);
        } catch (final NodelibraryException cleanupFailure) {
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(cleanupFailure);
            } else {
                throw cleanupFailure;
            }
        }
    }

    public static void ensureNoSymbolicLinks(final Path path) throws IOException {
        if (path == null) return;
        final Path absolute = path.toAbsolutePath().normalize();
        Path current = absolute.getRoot();
        for (final Path component : absolute) {
            current = current == null ? component : current.resolve(component);
            if (Files.isSymbolicLink(current) && !isSystemPrivateAlias(current)) {
                throw new IOException("Path contains a symbolic link: " + current);
            }
        }
    }

    /* macOS exposes /var, /tmp and a few other system directories through
     * root-level links into /private. Those aliases are outside an application's
     * configured backup root and are not user-controlled descendants. Keep the
     * check strict for every other symbolic link. */
    private static boolean isSystemPrivateAlias(final Path path) {
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

    public static FileAttribute<Set<PosixFilePermission>> ownerOnlyDirectoryAttributes() {
        return PosixFilePermissions.asFileAttribute(Set.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE));
    }

    public static void forceDirectory(final Path directory) throws IOException {
        try (final FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (final UnsupportedOperationException unsupported) {
            throw new IOException("Filesystem does not support forcing directory metadata", unsupported);
        }
    }

    public static void deleteDirectory(final Path path) throws NodelibraryException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (final var files = Files.walk(path)) {
            files.sorted(Comparator.reverseOrder()).forEach(file ->
            {
                try {
                    Files.delete(file);
                } catch (final IOException e) {
                    throw new NodelibraryException("Failed to delete file at " + file, e);
                }
            });
        } catch (final NoSuchFileException ignored) {
            /* A concurrent cleanup may remove the root after the existence check.
             * Deletion is intentionally idempotent for backup retry paths. */
        } catch (final IOException e) {
            throw new NodelibraryException("Failed to iterate files at " + path, e);
        }
    }
}
