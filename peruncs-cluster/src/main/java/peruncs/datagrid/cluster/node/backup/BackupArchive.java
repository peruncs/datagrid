package peruncs.datagrid.cluster.node.backup;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.store.StorageFileOperations;
import peruncs.datagrid.cluster.storage.types.Crc32c;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/// Encodes and validates the ZIP format used by filesystem backups.
///
/// Archive names carry the backup generation — timestamp, slot, cluster,
/// store image, epoch, recording, and a random publication id — so backups
/// from different nodes sharing one volume never collide, and unrelated
/// generations stay distinguishable without opening any archive. The node
/// provenance and the content digest travel inside the archive as the
/// `backup-identity` entry; archives without one predate generations and
/// list with unknown provenance.
final class BackupArchive {
        /// Sidecar entry carrying the full backup identity and content digest.
    static final String BACKUP_IDENTITY_ENTRY = "backup-identity";

    private static final LazyConstant<Pattern> BACKUP_NAME = LazyConstant.of(
            () -> Pattern.compile(
                    "^(\\d+)(\\.manual)?\\.([0-9a-f]{32})\\.([0-9a-f]{32})\\.(-?\\d+)\\.(-?\\d+)\\.([0-9a-f]{32})\\.zip$",
                    Pattern.CASE_INSENSITIVE));
    private static final int MAX_ARCHIVE_ENTRIES = 1_000_000;
    private static final int MAX_MANIFEST_BYTES = 1 << 20;
    private static final int MAX_IDENTITY_BYTES = 4096;
    private static final int IDENTITY_MAGIC = 0x44474249; // DGBI
    private static final short IDENTITY_VERSION = 1;
    private static final UUID NIL_UUID = new UUID(0L, 0L);

    private BackupArchive() {
    }

    static String toArchiveFileName(final BackupMetadata backup) {
        return "%d%s.%s.%s.%d.%d.%s.zip".formatted(
                backup.timestamp(),
                backup.manualSlot() ? ".manual" : "",
                hexOrZero(backup.clusterId()),
                hexOrZero(backup.storeGeneration()),
                backup.epoch(),
                backup.recordingId(),
                backup.backupId() == null ? hexOrZero(null) : hex(backup.backupId()));
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
            return new BackupMetadata(
                    Long.parseLong(matcher.group(1)),
                    matcher.group(2) != null,
                    orNullUuid(matcher.group(3)),
                    orNullUuid(matcher.group(4)),
                    unknownIfNegative(Long.parseLong(matcher.group(5))),
                    unknownIfNegative(Long.parseLong(matcher.group(6))),
                    null,
                    uuid(matcher.group(7)),
                    BackupMetadata.UNKNOWN);
        } catch (final IllegalArgumentException failure) {
            throw new NodeLibraryException("Invalid backup filename: %s".formatted(volume.resolve(name)), failure);
        }
    }

    private static long unknownIfNegative(final long value) {
        return value < 0L ? BackupMetadata.UNKNOWN : value;
    }

    private static String hex(final UUID id) {
        return "%016x%016x".formatted(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    private static String hexOrZero(final UUID id) {
        return id == null ? "0".repeat(32) : hex(id);
    }

    private static UUID uuid(final String text) {
        return new UUID(
                Long.parseUnsignedLong(text.substring(0, 16), 16),
                Long.parseUnsignedLong(text.substring(16, 32), 16));
    }

    private static UUID orNullUuid(final String text) {
        final UUID parsed = uuid(text);
        return NIL_UUID.equals(parsed) ? null : parsed;
    }

        /// Writes the full backup identity as an archive sidecar entry.
    ///
    /// @param file   destination file for the encoded identity
    /// @param backup backup identity to encode
    /// @throws NodeLibraryException if the identity cannot be written
    static void writeIdentity(final Path file, final BackupMetadata backup) throws NodeLibraryException {
        if (backup.backupId() == null) {
            throw new NodeLibraryException("Backup identity is missing its backup id");
        }
        if (NIL_UUID.equals(backup.backupId())) {
            throw new NodeLibraryException("Backup identity carries a nil backup id");
        }
        final ByteBuffer data = ByteBuffer.allocate(128);
        data.putInt(IDENTITY_MAGIC);
        data.putShort(IDENTITY_VERSION);
        data.putLong(backup.timestamp());
        data.put(backup.manualSlot() ? (byte) 1 : (byte) 0);
        putUuid(data, backup.clusterId());
        putUuid(data, backup.storeGeneration());
        data.putLong(backup.epoch());
        data.putLong(backup.recordingId());
        putUuid(data, backup.nodeId());
        data.putLong(backup.backupId().getMostSignificantBits());
        data.putLong(backup.backupId().getLeastSignificantBits());
        data.putLong(backup.digest());
        final byte[] payload = Arrays.copyOf(data.array(), data.position());
        final ByteBuffer framed = ByteBuffer.allocate(payload.length + Integer.BYTES);
        framed.put(payload);
        framed.putInt(Crc32c.compute(payload));
        try {
            Files.write(file, framed.array());
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to write backup identity to %s".formatted(file), failure);
        }
    }

    private static void putUuid(final ByteBuffer data, final UUID id) {
        if (id == null) {
            data.put((byte) 0);
            data.putLong(0L);
            data.putLong(0L);
        } else {
            data.put((byte) 1);
            data.putLong(id.getMostSignificantBits());
            data.putLong(id.getLeastSignificantBits());
        }
    }

        /// Reads the backup identity sidecar from an archive.
    ///
    /// @param archive archive to inspect
    /// @return stored identity, or `null` when the archive has no identity entry
    /// @throws NodeLibraryException if the entry is present but corrupt
    static BackupMetadata readIdentity(final Path archive) throws NodeLibraryException {
        try (ZipFile zip = openArchive(archive)) {
            final ZipEntry entry = zip.getEntry(BACKUP_IDENTITY_ENTRY);
            if (entry == null || entry.isDirectory()) {
                return null;
            }
            if (entry.getSize() > MAX_IDENTITY_BYTES) {
                throw new NodeLibraryException("Backup identity is too large in %s".formatted(archive));
            }
            final byte[] bytes = readBounded(zip, entry, MAX_IDENTITY_BYTES);
            return decodeIdentity(archive, bytes);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to read backup identity from %s".formatted(archive), failure);
        }
    }

    private static BackupMetadata decodeIdentity(final Path archive, final byte[] bytes)
            throws NodeLibraryException {
        /* Magic, version, timestamp, slot, two flagged UUIDs, two longs, one
         * flagged UUID, one UUID, one digest, plus the trailing CRC. */
        final int expected = Integer.BYTES + Short.BYTES + Long.BYTES + 1 +
                             (1 + Long.BYTES * 2) * 2 + Long.BYTES * 2 +
                             (1 + Long.BYTES * 2) + Long.BYTES * 2 + Long.BYTES + Integer.BYTES;
        if (bytes.length != expected) {
            throw new NodeLibraryException("Backup identity has an unexpected size in %s".formatted(archive));
        }
        final int stored = ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES).getInt();
        if (stored != Crc32c.compute(bytes, 0, bytes.length - Integer.BYTES)) {
            throw new NodeLibraryException("Backup identity is corrupt in %s".formatted(archive));
        }
        final ByteBuffer data = ByteBuffer.wrap(bytes);
        if (data.getInt() != IDENTITY_MAGIC || data.getShort() != IDENTITY_VERSION) {
            throw new NodeLibraryException("Backup identity has an unsupported format in %s".formatted(archive));
        }
        try {
            final BackupMetadata identity = new BackupMetadata(
                    data.getLong(),
                    data.get() != 0,
                    getUuid(data),
                    getUuid(data),
                    data.getLong(),
                    data.getLong(),
                    getUuid(data),
                    new UUID(data.getLong(), data.getLong()),
                    data.getLong());
            if (NIL_UUID.equals(identity.backupId())) {
                throw new NodeLibraryException("Backup identity carries a nil backup id in %s".formatted(archive));
            }
            return identity;
        } catch (final IllegalArgumentException failure) {
            throw new NodeLibraryException("Backup identity carries invalid values in %s".formatted(archive), failure);
        }
    }

    private static UUID getUuid(final ByteBuffer data) {
        final boolean present = data.get() != 0;
        final long most = data.getLong();
        final long least = data.getLong();
        if (!present && most == 0L && least == 0L) {
            return null;
        }
        if (!present) {
            throw new IllegalArgumentException("Backup identity UUID flag contradicts its value");
        }
        final UUID parsed = new UUID(most, least);
        /* A nil UUID carries no identity, just like a zero filename field. */
        return NIL_UUID.equals(parsed) ? null : parsed;
    }

    private static byte[] readBounded(final ZipFile zip, final ZipEntry entry, final int maximum)
            throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(256, maximum));
        final byte[] buffer = new byte[8192];
        try (InputStream data = zip.getInputStream(entry)) {
            int read;
            int total = 0;
            while ((read = data.read(buffer)) != -1) {
                if (read > maximum - total) {
                    throw new IOException("Backup identity exceeds its size limit");
                }
                total += read;
                output.write(buffer, 0, read);
            }
        }
        return output.toByteArray();
    }

        /// Digests the manifest and storage payload of an export directory.
    ///
    /// The digest covers the manifest bytes followed by every regular file
    /// under `storage`, ordered by slash-separated relative path with each
    /// path framing its content. Zip framing, entry times, the ready marker,
    /// and the identity sidecar are excluded, so the same Store image always
    /// digests identically before and after archiving.
    ///
    /// @param root export or extraction root holding `manifest` and `storage`
    /// @return CRC over the backup content
    /// @throws NodeLibraryException if the content cannot be read
    static long contentDigestOfDirectory(final Path root) throws NodeLibraryException {
        try {
            final CRC32 digest = new CRC32();
            digest.update(Files.readAllBytes(root.resolve(StorageBackupBackend.MANIFEST_ENTRY)));
            final Path storage = root.resolve(StorageBackupBackend.STORAGE_ENTRY);
            final List<String> names = new ArrayList<>();
            try (var paths = Files.walk(storage)) {
                for (final var iterator = paths.iterator(); iterator.hasNext(); ) {
                    final Path path = iterator.next();
                    if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                        names.add(storage.relativize(path).toString().replace(java.io.File.separatorChar, '/'));
                    }
                }
            }
            names.sort(String::compareTo);
            final byte[] buffer = new byte[8192];
            for (final String name : names) {
                digest.update(name.getBytes(StandardCharsets.UTF_8));
                try (InputStream data = Files.newInputStream(storage.resolve(name))) {
                    int read;
                    while ((read = data.read(buffer)) != -1) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return digest.getValue();
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to digest backup content at %s".formatted(root), failure);
        }
    }

        /// Digests the manifest and storage payload carried by an archive.
    ///
    /// Entries are visited in the same order as [#contentDigestOfDirectory],
    /// so a digest taken before compression matches the archived bytes.
    ///
    /// @param archive archive to digest
    /// @return CRC over the backup content
    /// @throws NodeLibraryException if the content cannot be read
    static long contentDigestOfArchive(final Path archive) throws NodeLibraryException {
        try (ZipFile zip = openArchive(archive)) {
            final ZipEntry manifest = zip.getEntry(StorageBackupBackend.MANIFEST_ENTRY);
            if (manifest == null || manifest.isDirectory()) {
                throw new NodeLibraryException("Backup archive is missing manifest in %s".formatted(archive));
            }
            final CRC32 digest = new CRC32();
            final byte[] buffer = new byte[8192];
            try (InputStream data = zip.getInputStream(manifest)) {
                int read;
                while ((read = data.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            /* The directory digest covers storage-relative paths, so the
             * `storage/` prefix is stripped here to keep both sides identical. */
            final String prefix = StorageBackupBackend.STORAGE_ENTRY + "/";
            final List<String> names = new ArrayList<>();
            for (final var iterator = zip.entries().asIterator(); iterator.hasNext(); ) {
                final ZipEntry entry = iterator.next();
                if (!entry.isDirectory() && entry.getName().startsWith(prefix)) {
                    names.add(entry.getName().substring(prefix.length()));
                }
            }
            names.sort(String::compareTo);
            for (final String name : names) {
                digest.update(name.getBytes(StandardCharsets.UTF_8));
                try (InputStream data = zip.getInputStream(zip.getEntry(prefix + name))) {
                    int read;
                    while ((read = data.read(buffer)) != -1) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return digest.getValue();
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to digest backup archive %s".formatted(archive), failure);
        }
    }

    static void compressStorage(final Path workingDir, final Path archiveFilePath) throws NodeLibraryException {
        try (FileChannel file = FileChannel.open(archiveFilePath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
             ZipOutputStream zip = new ZipOutputStream(Channels.newOutputStream(file))) {
            final byte[] transferBuffer = new byte[8192];
            final List<String> roots = new ArrayList<>(List.of(StorageBackupBackend.STORAGE_ENTRY,
                    StorageBackupBackend.MANIFEST_ENTRY, StorageBackupBackend.READY_ENTRY));
            /* Generation sidecars are written by the backend; hand-built
             * archives without one stay valid and list with unknown provenance. */
            if (Files.exists(workingDir.resolve(BACKUP_IDENTITY_ENTRY), LinkOption.NOFOLLOW_LINKS)) {
                roots.add(BACKUP_IDENTITY_ENTRY);
            }
            for (final String rootName : roots) {
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
