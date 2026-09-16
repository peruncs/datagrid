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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/// Encodes and validates the ZIP format used by filesystem backups.
final class BackupArchive {
    private static final LazyConstant<Pattern> BACKUP_NAME = LazyConstant.of(
            () -> Pattern.compile("^(\\d+)(\\.manual)?\\.zip$", Pattern.CASE_INSENSITIVE));
    private static final int MAX_ARCHIVE_ENTRIES = 1_000_000;
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
    ///
    /// The extraction budget is derived from the archive's declared entry
    /// sizes when all are known and falls back to the configured absolute
    /// ceiling otherwise, so legitimate large stores restore while
    /// decompression bombs still fail fast.
    static void extractArchive(
            final Path destination,
            final Path archive,
            final boolean requireBackupMetadata,
            final BackupArchiveLimits limits
    ) throws NodeLibraryException {
        Objects.requireNonNull(limits, "limits");
        final Path root = destination.toAbsolutePath().normalize();
        try {
            StorageFileOperations.ensureNoSymbolicLinks(root);
            Files.createDirectories(root);
            try (ZipFile zip = openArchive(archive)) {
                final List<ZipEntry> entries = listEntries(zip);
                final long budget = extractionBudget(entries, limits.maxExtractedBytes());
                final byte[] transferBuffer = new byte[8192];
                long extractedBytes = 0L;
                for (final ZipEntry entry : entries) {
                    extractedBytes = extractEntry(zip, root, entry, extractedBytes, budget, transferBuffer);
                }
            }
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to extract storage", failure);
        }
        validateExtractedArchive(root, requireBackupMetadata);
    }

    static byte[] readManifest(final Path archive, final long maxDeclaredBytes) throws NodeLibraryException {
        if (maxDeclaredBytes <= 0L) throw new IllegalArgumentException("maxDeclaredBytes must be positive");
        try (ZipFile zip = openArchive(archive)) {
            final List<ZipEntry> entries = listEntries(zip);
            /* A bomb that honestly declares petabytes dies here without
             * inflating a single byte. */
            if (declaredTotalBytes(entries) > maxDeclaredBytes) {
                throw new NodeLibraryException("Backup archive is too large");
            }
            ZipEntry manifest = null;
            for (final ZipEntry entry : entries) {
                if (StorageBackupBackend.MANIFEST_ENTRY.equals(entry.getName())) {
                    if (entry.isDirectory()) {
                        throw new NodeLibraryException("Backup manifest is missing or too large");
                    }
                    manifest = entry;
                    break;
                }
            }
            if (manifest == null) throw new NodeLibraryException("Backup archive is missing manifest");
            final long declared = manifest.getSize();
            final byte[] transferBuffer = new byte[8192];
            final int initialCapacity = (int) Math.clamp(declared, 0L, MAX_MANIFEST_BYTES);
            final ByteArrayOutputStream output = new ByteArrayOutputStream(initialCapacity);
            try (InputStream data = zip.getInputStream(manifest)) {
                try {
                    transferBounded(data, output, 0L, MAX_MANIFEST_BYTES, transferBuffer);
                } catch (final IOException truncated) {
                    /* transferBounded fails on oversize input and on corrupt
                     * streams alike. Only a full buffer means oversize;
                     * anything else is a corrupt archive. */
                    if (output.size() >= MAX_MANIFEST_BYTES) {
                        throw new NodeLibraryException("Backup manifest is missing or too large", truncated);
                    }
                    throw truncated;
                }
            }
            if (output.size() > MAX_MANIFEST_BYTES) {
                throw new NodeLibraryException("Backup manifest is missing or too large");
            }
            if (declared >= 0L && output.size() > declared) {
                throw new NodeLibraryException("Backup manifest exceeds its declared size");
            }
            return output.toByteArray();
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to read backup manifest from %s".formatted(archive), failure);
        }
    }

    /// Reports whether the archive carries the storage payload, not just a manifest.
    ///
    /// A truncated archive with an intact manifest is not a complete backup.
    static boolean containsStoragePayload(final Path archive) throws NodeLibraryException {
        try (ZipFile zip = openArchive(archive)) {
            for (final ZipEntry entry : listEntries(zip)) {
                final String name = entry.getName();
                if (StorageBackupBackend.STORAGE_ENTRY.equals(name) ||
                    name.startsWith(StorageBackupBackend.STORAGE_ENTRY + "/")) {
                    return true;
                }
            }
            return false;
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to inspect backup archive %s".formatted(archive), failure);
        }
    }

    private static ZipFile openArchive(final Path archive) throws NodeLibraryException {
        try {
            StorageFileOperations.ensureNoSymbolicLinks(archive);
            return new ZipFile(archive.toFile());
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to open backup archive %s".formatted(archive), failure);
        }
    }

    /// Lists central-directory entries after count, duplicate, and name checks.
    ///
    /// Reading the central directory inflates nothing, so hostile declared
    /// sizes are visible before any entry data is decompressed.
    private static List<ZipEntry> listEntries(final ZipFile zip) throws NodeLibraryException {
        final List<ZipEntry> entries = new ArrayList<>();
        final Set<String> names = new HashSet<>();
        for (final var iterator = zip.entries().asIterator(); iterator.hasNext(); ) {
            final ZipEntry entry = iterator.next();
            if (entries.size() >= MAX_ARCHIVE_ENTRIES || !names.add(entry.getName())) {
                throw new NodeLibraryException("Backup archive contains too many or duplicate entries");
            }
            if (!safeArchiveName(entry.getName())) {
                throw new NodeLibraryException("Backup archive contains an unsafe entry: %s".formatted(entry.getName()));
            }
            entries.add(entry);
        }
        return entries;
    }

    /// Sums declared entry sizes, or `-1` when any size is unknown.
    private static long declaredTotalBytes(final List<ZipEntry> entries) throws NodeLibraryException {
        long total = 0L;
        for (final ZipEntry entry : entries) {
            if (entry.isDirectory()) continue;
            final long size = entry.getSize();
            if (size < 0L) return -1L;
            if (size > Long.MAX_VALUE - total) {
                throw new NodeLibraryException("Backup archive declares more data than the extraction budget");
            }
            total += size;
        }
        return total;
    }

    private static long extractionBudget(final List<ZipEntry> entries, final long maximum)
            throws NodeLibraryException {
        final long declared = declaredTotalBytes(entries);
        if (declared > maximum) {
            throw new NodeLibraryException("Backup archive declares more data than the extraction budget");
        }
        return declared >= 0L ? declared : maximum;
    }

    private static long extractEntry(
            final ZipFile zip,
            final Path root,
            final ZipEntry entry,
            long extractedBytes,
            final long budget,
            final byte[] transferBuffer
    ) throws IOException {
        final Path target = root.resolve(entry.getName()).normalize();
        if (!target.startsWith(root)) {
            throw new NodeLibraryException("Backup archive entry escapes extraction root");
        }
        ensureNoSymlinkParent(root, target.getParent());
        StorageFileOperations.ensureNoSymbolicLinks(target);
        if (entry.isDirectory()) {
            Files.createDirectories(target);
            StorageFileOperations.ensureNoSymbolicLinks(target);
            return extractedBytes;
        }
        final Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
            StorageFileOperations.ensureNoSymbolicLinks(parent);
        }
        /* A lying entry must not out-produce its declared size; unknown
         * sizes fall back to the remaining overall budget. */
        final long declared = entry.getSize();
        final long entryBudget = declared >= 0L ? declared : budget - extractedBytes;
        try (OutputStream output = Files.newOutputStream(target,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS);
             InputStream data = zip.getInputStream(entry)) {
            extractedBytes += transferBounded(data, output, 0L, entryBudget, transferBuffer);
            if (extractedBytes > budget) throw new IOException("Backup archive exceeds extraction limit");
            return extractedBytes;
        }
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
            final var iterator = paths.iterator();
            while (iterator.hasNext()) {
                final Path path = iterator.next();
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
