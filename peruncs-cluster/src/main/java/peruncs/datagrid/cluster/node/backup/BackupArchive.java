package peruncs.datagrid.cluster.node.backup;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.store.StorageFileOperations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/// Encodes and validates the ZIP format used by filesystem backups.
final class BackupArchive {
    static final String USER_UPLOADED_STORAGE_ARCHIVE = StorageBackupBackend.USER_UPLOADED_STORAGE_ARCHIVE;
    private static final LazyConstant<Pattern> BACKUP_NAME =
            LazyConstant.of(() -> Pattern.compile("^(\\d+)(\\.manual)?\\.zip$"));
    private static final int MAX_ARCHIVE_ENTRIES = 1_000_000;
    private static final long MAX_EXTRACTED_BYTES = 1L << 30;
    private static final int MAX_MANIFEST_BYTES = 1 << 20;

    private BackupArchive() {
    }

    static String toArchiveFileName(final BackupMetadata backup) {
        return "%s.zip".formatted(backup.timestamp() + (backup.manualSlot() ? ".manual" : ""));
    }

    static boolean isBackupFileName(final String name) {
        return name != null && BACKUP_NAME.get().matcher(name).matches();
    }

    static BackupMetadata parseMetadata(final String name, final Path volume) throws NodeLibraryException {
        final Matcher matcher = BACKUP_NAME.get().matcher(name);
        if (!matcher.matches()) {
            throw new NodeLibraryException("Invalid backup filename: %s".formatted(volume.resolve(name)));
        }
        try {
            return new BackupMetadata(Long.parseLong(matcher.group(1)), matcher.group(2) != null);
        } catch (final NumberFormatException failure) {
            throw new NodeLibraryException("Invalid backup timestamp: %s".formatted(volume.resolve(name)), failure);
        }
    }

    static void compressStorage(final Path workingDir, final Path archiveFilePath) throws NodeLibraryException {
        try (FileChannel file = FileChannel.open(archiveFilePath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
             ZipOutputStream zip = new ZipOutputStream(Channels.newOutputStream(file))) {
            final byte[] transferBuffer = new byte[8192];
            for (final String rootName : List.of(StorageBackupBackend.STORAGE_ENTRY,
                    StorageBackupBackend.MANIFEST_ENTRY, StorageBackupBackend.READY_ENTRY)) {
                final Path root = workingDir.resolve(rootName).normalize();
                if (!root.startsWith(workingDir.normalize()) || !Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Backup source is missing: %s".formatted(rootName));
                }
                try (var paths = Files.walk(root)) {
                    for (final var iterator = paths.iterator(); iterator.hasNext(); ) {
                        writeArchiveEntry(zip, workingDir, iterator.next(), transferBuffer);
                    }
                }
            }
            zip.finish();
            file.force(true);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to compress storage", failure);
        }
    }

    /// Extracts only archives with safe relative, non-link entries.
    static void extractArchive(
            final Path destination,
            final Path archive,
            final boolean requireBackupMetadata
    ) throws NodeLibraryException {
        final Path root = destination.toAbsolutePath().normalize();
        try {
            StorageFileOperations.ensureNoSymbolicLinks(root);
            Files.createDirectories(root);
            final Set<String> names = new HashSet<>();
            final byte[] transferBuffer = new byte[8192];
            int entryCount = 0;
            long extractedBytes = 0L;
            try (var archiveChannel = StorageFileOperations.openRegularFile(archive);
                 InputStream file = Channels.newInputStream(archiveChannel);
                 ZipInputStream zip = new ZipInputStream(file)) {
                ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    final String name = entry.getName();
                    if (++entryCount > MAX_ARCHIVE_ENTRIES || !names.add(name)) {
                        throw new IOException("Backup archive contains too many or duplicate entries");
                    }
                    if (!safeArchiveName(name)) {
                        throw new NodeLibraryException("Backup archive contains an unsafe entry: %s".formatted(name));
                    }
                    final Path target = root.resolve(name).normalize();
                    if (!target.startsWith(root)) {
                        throw new NodeLibraryException("Backup archive entry escapes extraction root");
                    }
                    ensureNoSymlinkParent(root, target.getParent());
                    StorageFileOperations.ensureNoSymbolicLinks(target);
                    if (entry.isDirectory()) {
                        Files.createDirectories(target);
                        StorageFileOperations.ensureNoSymbolicLinks(target);
                    } else {
                        final Path parent = target.getParent();
                        if (parent != null) {
                            Files.createDirectories(parent);
                            StorageFileOperations.ensureNoSymbolicLinks(parent);
                        }
                        try (OutputStream output = Files.newOutputStream(target,
                                StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.WRITE,
                                LinkOption.NOFOLLOW_LINKS)) {
                            extractedBytes = transferBounded(
                                    zip, output, extractedBytes, MAX_EXTRACTED_BYTES, transferBuffer);
                        }
                    }
                    zip.closeEntry();
                }
            }
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to extract storage", failure);
        }
        validateExtractedArchive(root, requireBackupMetadata);
    }

    static byte[] readManifest(final Path archive) throws IOException {
        final Set<String> names = new HashSet<>();
        int entryCount = 0;
        long declaredBytes = 0L;
        byte[] manifest = null;
        final byte[] transferBuffer = new byte[8192];
        try (var archiveChannel = StorageFileOperations.openRegularFile(archive);
             InputStream file = Channels.newInputStream(archiveChannel);
             ZipInputStream zip = new ZipInputStream(file)) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                final String name = entry.getName();
                if (++entryCount > MAX_ARCHIVE_ENTRIES || !names.add(name) || !safeArchiveName(name)) {
                    throw new IOException("Backup archive contains an unsafe or duplicate entry");
                }
                if (!entry.isDirectory() && entry.getSize() >= 0L) {
                    if (entry.getSize() > MAX_EXTRACTED_BYTES - declaredBytes) {
                        throw new IOException("Backup archive is too large");
                    }
                    declaredBytes += entry.getSize();
                }
                if (StorageBackupBackend.MANIFEST_ENTRY.equals(name)) {
                    if (entry.isDirectory()) {
                        throw new IOException("Backup manifest is missing or too large");
                    }
                    final int initialCapacity = (int) Math.min(
                            MAX_MANIFEST_BYTES, Math.max(0L, entry.getSize()));
                    final ByteArrayOutputStream output = new ByteArrayOutputStream(initialCapacity);
                    transferBounded(zip, output, 0L, MAX_MANIFEST_BYTES, transferBuffer);
                    if (output.size() > MAX_MANIFEST_BYTES) {
                        throw new IOException("Backup manifest is missing or too large");
                    }
                    manifest = output.toByteArray();
                }
                zip.closeEntry();
            }
        }
        if (manifest == null) throw new IOException("Backup archive is missing manifest");
        return manifest;
    }

    private static long transferBounded(
            final InputStream input,
            final OutputStream output,
            long copied,
            final long maximum,
            final byte[] buffer
    ) throws IOException {
        int read;
        while ((read = input.read(buffer)) != -1) {
            if (read > maximum - copied) throw new IOException("Backup archive exceeds extraction limit");
            copied += read;
            output.write(buffer, 0, read);
        }
        return copied;
    }

    private static boolean safeArchiveName(final String name) {
        if (name == null || name.isEmpty() || name.startsWith("/") || name.startsWith("../") || name.equals("..") ||
            name.contains("/../") || name.contains("\\") || name.contains("\0") || name.indexOf(':') >= 0) {
            return false;
        }
        try {
            final Path path = Path.of(name);
            for (final Path part : path) {
                if ("..".equals(part.toString())) return false;
            }
            return !path.isAbsolute() && !path.startsWith("..");
        } catch (final InvalidPathException failure) {
            return false;
        }
    }

    private static void writeArchiveEntry(
            final ZipOutputStream zip,
            final Path workingDir,
            final Path path,
            final byte[] transferBuffer
    ) throws IOException {
        if (Files.isSymbolicLink(path) || (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
                                           !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("Backup source contains an unsupported entry: %s".formatted(path));
        }
        final String name = workingDir.relativize(path).toString().replace(java.io.File.separatorChar, '/');
        if (name.isEmpty()) return;
        final ZipEntry entry;
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            entry = new ZipEntry(name.endsWith("/") ? name : name + "/");
        } else {
            entry = new ZipEntry(name);
        }
        zip.putNextEntry(entry);
        if (!entry.isDirectory()) {
            try (var inputChannel = StorageFileOperations.openRegularFile(path);
                 InputStream input = Channels.newInputStream(inputChannel)) {
                int read;
                while ((read = input.read(transferBuffer)) != -1) {
                    zip.write(transferBuffer, 0, read);
                }
            }
        }
        zip.closeEntry();
    }

    private static void ensureNoSymlinkParent(final Path root, final Path parent) throws IOException {
        for (Path current = parent; current != null && current.startsWith(root) && !current.equals(root);
             current = current.getParent()) {
            if (Files.isSymbolicLink(current)) throw new IOException("Archive path has a symbolic-link parent");
        }
    }

    private static void validateExtractedArchive(final Path root, final boolean requireBackupMetadata)
            throws NodeLibraryException {
        try (final var paths = Files.walk(root)) {
            for (final Path path : paths.toList()) {
                if (Files.isSymbolicLink(path) || (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
                                                   !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))) {
                    throw new NodeLibraryException("Backup archive contains an unsupported extracted entry: %s".formatted(path));
                }
            }
            if (!Files.isDirectory(root.resolve(StorageBackupBackend.STORAGE_ENTRY), LinkOption.NOFOLLOW_LINKS)) {
                throw new NodeLibraryException("Backup archive is missing storage");
            }
            if (requireBackupMetadata &&
                (!Files.isRegularFile(root.resolve(StorageBackupBackend.MANIFEST_ENTRY), LinkOption.NOFOLLOW_LINKS) ||
                 !Files.isRegularFile(root.resolve(StorageBackupBackend.READY_ENTRY), LinkOption.NOFOLLOW_LINKS))) {
                throw new NodeLibraryException("Backup archive is missing manifest or ready marker");
            }
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to validate extracted backup archive", failure);
        }
    }
}
