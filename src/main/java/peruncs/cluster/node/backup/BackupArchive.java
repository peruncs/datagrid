package peruncs.cluster.node.backup;

import peruncs.cluster.errors.NodeException;
import peruncs.cluster.storage.Crc32C;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.*;

/// Encodes and validates the ZIP format used by filesystem backups.
///
/// Archive names carry the backup generation — timestamp, slot, cluster,
/// store image, epoch, recording, replication sequence, and a random
/// publication id — so backups from different nodes sharing one volume never
/// collide, and unrelated generations stay distinguishable without opening
/// any archive. The node provenance and the content digest travel inside the
/// archive as the `backup-identity` entry; archives without one predate
/// generations and list with unknown provenance.
///
/// A failure that conclusively proves an archive incomplete or corrupt is
/// reported as [IncompleteArchiveException], so callers may replace the
/// partial file. Transient I/O failures stay ordinary [NodeException]s
/// and must never be treated as evidence that a durable archive is partial.
final class BackupArchive {
        /// Sidecar entry carrying the full backup identity and content digest.
    static final String BACKUP_IDENTITY_ENTRY = "backup-identity";
        /// Upper bound for the replication manifest entry.
    static final int MAX_MANIFEST_BYTES = 1 << 20;

    private static final LazyConstant<Pattern> BACKUP_NAME = LazyConstant.of(
            () -> Pattern.compile(
                    "^(\\d+)(\\.manual)?\\.([0-9a-f]{32})\\.([0-9a-f]{32})\\.(-?\\d+)\\.(-?\\d+)\\.(-?\\d+)\\.([0-9a-f]{32})\\.zip$",
                    Pattern.CASE_INSENSITIVE));
    private static final int MAX_IDENTITY_BYTES = 4096;
    private static final int IDENTITY_MAGIC = 0x44474249; // DGBI
    private static final short IDENTITY_VERSION = 2;
    private static final UUID NIL_UUID = new UUID(0L, 0L);

    private BackupArchive() {
    }

    /// Validates a user-uploaded storage archive before it may replace local state.
    ///
    /// Generated backups always carry `storage/`, `manifest`, and `ready`; user
    /// uploads skip the metadata checks of the restore path, so this pre-check
    /// requires the same essentials: exactly one readable, non-empty `manifest`
    /// entry within [BackupArchive#MAX_MANIFEST_BYTES] (ambiguity fails) and at
    /// least one non-empty Store payload entry under `storage/`. Declared sizes
    /// are not trusted: every entry is decompressed through a dry run bounded by
    /// the extraction budget, so a bomb that under-declares its content is
    /// refused here — before any caller destroys local storage — instead of at
    /// extraction time.
    ///
    /// @param archive upload archive path
    /// @param limits  operator-configured extraction budgets
    /// @throws NodeException when the upload is missing, ambiguous, partial, or over budget
    static void validateUpload(final Path archive, final BackupArchiveLimits limits) throws NodeException {
        try (ZipFile zip = openArchive(archive)) {
            final List<ZipEntry> entries = listEntries(zip, limits.maxArchiveEntries());
            int manifests = 0;
            boolean manifestReadable = false;
            boolean storagePayload = false;
            for (final ZipEntry entry : entries) {
                if (StorageBackupBackend.MANIFEST_ENTRY.equals(entry.getName())) {
                    manifests++;
                    manifestReadable = manifestReadable || isReadableManifest(zip, entry);
                } else if (!entry.isDirectory() && entry.getSize() != 0L &&
                           entry.getName().startsWith(StorageBackupBackend.STORAGE_ENTRY + "/")) {
                    storagePayload = true;
                }
            }
            if (manifests != 1 || !manifestReadable) {
                throw new NodeException(
                        "User-uploaded storage archive must contain exactly one readable manifest at %s; refusing to install"
                                .formatted(archive));
            }
            if (!storagePayload) {
                throw new NodeException(
                        "User-uploaded storage archive contains no non-empty storage payload at %s; refusing to install a partial upload"
                                .formatted(archive));
            }
            dryRunBudget(zip, entries, limits.maxExtractedBytes());
        } catch (final IOException closeFailure) {
            throw new NodeException(
                    "User-uploaded storage archive cannot be read at %s; refusing to install".formatted(archive),
                    closeFailure);
        }
        /* openArchive already rejects missing or unsafe paths; any other read
         * failure is a refusal, never a fallback to trust. */
    }

    /// Decompresses every entry while discarding the bytes, bounded by the
    /// extraction budget.
    ///
    /// Declared sizes in the central directory are attacker-controlled; the
    /// dry run measures the real inflated stream, so an under-declaring bomb
    /// is refused before the caller relies on it.
    ///
    /// @param zip        open upload archive
    /// @param entries    bounded, de-duplicated entry list
    /// @param maxExtractedBytes total extraction budget
    /// @throws NodeException when the real content exceeds the budget
    private static void dryRunBudget(
            final ZipFile zip,
            final List<ZipEntry> entries,
            final long maxExtractedBytes
    ) throws NodeException {
        long remaining = maxExtractedBytes;
        final byte[] discard = new byte[64 * 1024];
        for (final ZipEntry entry : entries) {
            if (entry.isDirectory()) continue;
            try (InputStream data = zip.getInputStream(entry)) {
                if (data == null) {
                    throw new NodeException("Backup archive entry cannot be read: %s".formatted(entry.getName()));
                }
                while (true) {
                    final int read = data.read(discard);
                    if (read < 0) break;
                    remaining -= read;
                    if (remaining < 0L) {
                        throw new NodeException(
                                "Backup archive inflates beyond the extraction budget of %s bytes: %s"
                                        .formatted(maxExtractedBytes, entry.getName()));
                    }
                }
            } catch (final IOException unreadable) {
                throw new NodeException(
                        "Backup archive cannot be decompressed: %s".formatted(entry.getName()), unreadable);
            }
        }
    }

    /// Reports whether a manifest entry is a readable, non-empty byte
    /// sequence within the manifest budget.
    private static boolean isReadableManifest(final ZipFile zip, final ZipEntry entry) {
        if (entry.isDirectory()) return false;
        try (InputStream data = zip.getInputStream(entry)) {
            final byte[] bytes = data.readNBytes(MAX_MANIFEST_BYTES);
            return bytes.length != 0 && data.read() == -1;
        } catch (final IOException unreadable) {
            return false;
        }
    }

        /// Reports whether a file name is a current-generation backup archive.
    ///
    /// @param name file name to check, or `null`
    /// @return `true` when the name encodes a backup identity
    static boolean isBackupFileName(final String name) {
        return name != null && BACKUP_NAME.get().matcher(name).matches();
    }

        /// Formats the archive file name for a backup identity.
    ///
    /// The name pins every selection field of the backup, including the
    /// replication sequence, so archives can be selected without opening
    /// them.
    ///
    /// @param backup backup identity
    /// @return archive file name
    static String toArchiveFileName(final BackupMetadata backup) {
        return "%d%s.%s.%s.%d.%d.%d.%s.zip".formatted(
                backup.timestamp(),
                backup.manualSlot() ? ".manual" : "",
                hexOrZero(backup.clusterId()),
                hexOrZero(backup.storeGeneration()),
                backup.epoch(),
                backup.recordingId(),
                backup.logicalSequence(),
                backup.backupId() == null ? hexOrZero(null) : hex(backup.backupId()));
    }

        /// Parses the selection fields back out of an archive file name.
    ///
    /// @param name   archive file name
    /// @param volume volume used for error reporting
    /// @return parsed backup metadata without a digest
    /// @throws NodeException when the name is not a valid archive name
    static BackupMetadata parseMetadata(final String name, final Path volume) throws NodeException {
        final Matcher matcher = BACKUP_NAME.get().matcher(name);
        if (!matcher.matches()) {
            throw new NodeException("Invalid backup filename: %s".formatted(volume.resolve(name)));
        }
        try {
            return new BackupMetadata(
                    Long.parseLong(matcher.group(1)),
                    matcher.group(2) != null,
                    orNullUuid(matcher.group(3)),
                    orNullUuid(matcher.group(4)),
                    unknownIfNegative(Long.parseLong(matcher.group(5))),
                    unknownIfNegative(Long.parseLong(matcher.group(6))),
                    unknownIfNegative(Long.parseLong(matcher.group(7))),
                    null,
                    uuid(matcher.group(8)),
                    BackupMetadata.UNKNOWN);
        } catch (final IllegalArgumentException failure) {
            throw new NodeException("Invalid backup filename: %s".formatted(volume.resolve(name)), failure);
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
    /// The identity is framed with a magic value, a version, and a trailing
    /// CRC32C, and is replaced atomically through [AtomicFileWriter], so a
    /// crash or partial write leaves either the old identity or the new one,
    /// never a torn file.
    ///
    /// @param file   destination file for the encoded identity
    /// @param backup backup identity to encode
    /// @throws NodeException if the identity cannot be written
    static void writeIdentity(final Path file, final BackupMetadata backup) throws NodeException {
        if (backup.backupId() == null) {
            throw new NodeException("Backup identity is missing its backup id");
        }
        if (NIL_UUID.equals(backup.backupId())) {
            throw new NodeException("Backup identity carries a nil backup id");
        }
        final ByteBuffer data = ByteBuffer.allocate(136);
        data.putInt(IDENTITY_MAGIC);
        data.putShort(IDENTITY_VERSION);
        data.putLong(backup.timestamp());
        data.put(backup.manualSlot() ? (byte) 1 : (byte) 0);
        putUuid(data, backup.clusterId());
        putUuid(data, backup.storeGeneration());
        data.putLong(backup.epoch());
        data.putLong(backup.recordingId());
        data.putLong(backup.logicalSequence());
        putUuid(data, backup.nodeId());
        data.putLong(backup.backupId().getMostSignificantBits());
        data.putLong(backup.backupId().getLeastSignificantBits());
        data.putLong(backup.digest());
        final byte[] payload = Arrays.copyOf(data.array(), data.position());
        final ByteBuffer framed = ByteBuffer.allocate(payload.length + Integer.BYTES);
        framed.put(payload);
        framed.putInt(Crc32C.compute(payload));
        try {
            AtomicFileWriter.writeBytes(file, framed.array());
        } catch (final IOException failure) {
            throw new NodeException("Failed to write backup identity to %s".formatted(file), failure);
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
    /// @throws NodeException if the entry is present but corrupt
    static BackupMetadata readIdentity(final Path archive) throws NodeException {
        try (ZipFile zip = openArchive(archive)) {
            final ZipEntry entry = zip.getEntry(BACKUP_IDENTITY_ENTRY);
            if (entry == null || entry.isDirectory()) {
                return null;
            }
            if (entry.getSize() > MAX_IDENTITY_BYTES) {
                throw new NodeException("Backup identity is too large in %s".formatted(archive));
            }
            final byte[] bytes = readBoundedIdentity(zip, entry);
            return decodeIdentity(archive, bytes);
        } catch (final IOException failure) {
            throw new NodeException("Failed to read backup identity from %s".formatted(archive), failure);
        }
    }

    private static BackupMetadata decodeIdentity(final Path archive, final byte[] bytes)
            throws NodeException {
        /* Magic, version, timestamp, slot, two flagged UUIDs, three longs,
         * one flagged UUID, one UUID, one digest, plus the trailing CRC. */
        final int expected = Integer.BYTES + Short.BYTES + Long.BYTES + 1 +
                             (1 + Long.BYTES * 2) * 2 + Long.BYTES * 3 +
                             (1 + Long.BYTES * 2) + Long.BYTES * 2 + Long.BYTES + Integer.BYTES;
        if (bytes.length != expected) {
            throw new NodeException("Backup identity has an unexpected size in %s".formatted(archive));
        }
        final int stored = ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES).getInt();
        if (stored != Crc32C.compute(bytes, 0, bytes.length - Integer.BYTES)) {
            throw new NodeException("Backup identity is corrupt in %s".formatted(archive));
        }
        final ByteBuffer data = ByteBuffer.wrap(bytes);
        if (data.getInt() != IDENTITY_MAGIC || data.getShort() != IDENTITY_VERSION) {
            throw new NodeException("Backup identity has an unsupported format in %s".formatted(archive));
        }
        try {
            final BackupMetadata identity = new BackupMetadata(
                    data.getLong(),
                    data.get() != 0,
                    getUuid(data),
                    getUuid(data),
                    data.getLong(),
                    data.getLong(),
                    data.getLong(),
                    getUuid(data),
                    new UUID(data.getLong(), data.getLong()),
                    data.getLong());
            if (NIL_UUID.equals(identity.backupId())) {
                throw new NodeException("Backup identity carries a nil backup id in %s".formatted(archive));
            }
            return identity;
        } catch (final IllegalArgumentException failure) {
            throw new NodeException("Backup identity carries invalid values in %s".formatted(archive), failure);
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

    private static byte[] readBoundedIdentity(final ZipFile zip, final ZipEntry entry)
            throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(256, MAX_IDENTITY_BYTES));
        final byte[] buffer = new byte[8192];
        try (InputStream data = zip.getInputStream(entry)) {
            int read;
            int total = 0;
            while ((read = data.read(buffer)) != -1) {
                if (read > MAX_IDENTITY_BYTES - total) {
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
    /// @throws NodeException if the content cannot be read
    static long contentDigestOfDirectory(final Path root) throws NodeException {
        try {
            final CRC32 digest = new CRC32();
            digest.update(Files.readAllBytes(root.resolve(StorageBackupBackend.MANIFEST_ENTRY)));
            final Path storage = root.resolve(StorageBackupBackend.STORAGE_ENTRY);
            final List<String> names = new ArrayList<>();
            try (var paths = Files.walk(storage)) {
                for (final var iterator = paths.iterator(); iterator.hasNext(); ) {
                    final Path path = iterator.next();
                    if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                        names.add(storage.relativize(path).toString().replace(File.separatorChar, '/'));
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
            throw new NodeException("Failed to digest backup content at %s".formatted(root), failure);
        }
    }

        /// Digests the manifest and storage payload carried by an archive.
    ///
    /// Entries are visited in the same order as [#contentDigestOfDirectory],
    /// so a digest taken before compression matches the archived bytes. The
    /// walk is bounded by the configured entry budget and the manifest by
    /// [BackupArchive#MAX_MANIFEST_BYTES], exactly like [#readManifest].
    ///
    /// @param archive archive to digest
    /// @param limits  entry and byte budgets
    /// @return CRC over the backup content
    /// @throws NodeException if the content cannot be read
    static long contentDigestOfArchive(final Path archive, final BackupArchiveLimits limits)
            throws NodeException {
        Objects.requireNonNull(limits, "limits");
        try (ZipFile zip = openArchive(archive)) {
            final List<ZipEntry> entries = listEntries(zip, limits.maxArchiveEntries());
            final long declared = declaredTotalBytes(entries);
            if (declared > limits.maxExtractedBytes()) {
                throw new NodeException("Backup archive is too large");
            }
            final String prefix = StorageBackupBackend.STORAGE_ENTRY + "/";
            ZipEntry manifest = null;
            final List<String> names = new ArrayList<>();
            for (final ZipEntry entry : entries) {
                if (StorageBackupBackend.MANIFEST_ENTRY.equals(entry.getName())) {
                    if (entry.isDirectory()) {
                        throw new IncompleteArchiveException("Backup manifest is missing in %s".formatted(archive));
                    }
                    manifest = entry;
                } else if (!entry.isDirectory() && entry.getName().startsWith(prefix)) {
                    names.add(entry.getName().substring(prefix.length()));
                }
            }
            if (manifest == null) {
                throw new IncompleteArchiveException("Backup archive is missing manifest in %s".formatted(archive));
            }
            final CRC32 digest = new CRC32();
            digest.update(readBoundedManifest(zip, manifest, archive));
            names.sort(String::compareTo);
            final byte[] buffer = new byte[8192];
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
            throw new NodeException("Failed to digest backup archive %s".formatted(archive), failure);
        }
    }

        /// Compresses the export root into a new archive file.
    ///
    /// @param workingDir      export workspace holding `storage`, `manifest`,
    ///                        `ready`, and optionally `backup-identity`
    /// @param archiveFilePath archive file to create; it must not exist
    /// @throws NodeException if the content cannot be compressed
    static void compressStorage(final Path workingDir, final Path archiveFilePath) throws NodeException {
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
            throw new NodeException("Failed to compress storage", failure);
        }
    }

        /// Extracts only archives with safe relative, non-link entries.
    ///
    /// The extraction budget is derived from the archive's declared entry
    /// sizes when all are known and falls back to the configured absolute
    /// ceiling otherwise, so legitimate large stores restore while
    /// decompression bombs still fail fast. The destination must be a freshly
    /// created private directory: it is validated once and entries rely on
    /// `NOFOLLOW_LINKS` creates inside it.
    ///
    /// @param destination           extraction root to create
    /// @param archive               archive to extract
    /// @param requireBackupMetadata whether the manifest and ready marker are required
    /// @param limits                entry and byte budgets
    /// @throws NodeException if the archive cannot be extracted safely
    static void extractArchive(
            final Path destination,
            final Path archive,
            final boolean requireBackupMetadata,
            final BackupArchiveLimits limits
    ) throws NodeException {
        Objects.requireNonNull(limits, "limits");
        final Path root = destination.toAbsolutePath().normalize();
        try {
            AtomicFileWriter.ensureNoSymbolicLinks(root);
            Files.createDirectories(root);
            AtomicFileWriter.ensureNoSymbolicLinks(root);
            try (ZipFile zip = openArchive(archive)) {
                final List<ZipEntry> entries = listEntries(zip, limits.maxArchiveEntries());
                final long budget = extractionBudget(entries, limits.maxExtractedBytes());
                final byte[] transferBuffer = new byte[8192];
                long extractedBytes = 0L;
                for (final ZipEntry entry : entries) {
                    extractedBytes = extractEntry(zip, root, entry, extractedBytes, budget, transferBuffer);
                }
            }
        } catch (final IOException failure) {
            throw new NodeException("Failed to extract storage", failure);
        }
        validateExtractedArchive(root, requireBackupMetadata);
    }

        /// Reads the manifest from an archive without extracting the payload.
    ///
    /// The manifest is bounded by [BackupArchive#MAX_MANIFEST_BYTES], the
    /// entry walk by `maxArchiveEntries`, and the total declared archive size
    /// by `maxTotalDeclaredBytes`, so an oversized or bomb archive is rejected
    /// before any manifest byte is inflated. The parameter covers the total
    /// declared archive size, not the manifest size.
    ///
    /// @param archive                archive to read
    /// @param maxTotalDeclaredBytes  total declared archive size ceiling
    /// @param maxArchiveEntries      entry-count ceiling
    /// @return manifest bytes
    /// @throws NodeException if the manifest cannot be read
    static byte[] readManifest(
            final Path archive,
            final long maxTotalDeclaredBytes,
            final int maxArchiveEntries
    ) throws NodeException {
        if (maxTotalDeclaredBytes <= 0L) {
            throw new IllegalArgumentException("maxTotalDeclaredBytes must be positive");
        }
        if (maxArchiveEntries <= 0) {
            throw new IllegalArgumentException("maxArchiveEntries must be positive");
        }
        try (ZipFile zip = openArchive(archive)) {
            final List<ZipEntry> entries = listEntries(zip, maxArchiveEntries);
            /* A bomb that honestly declares petabytes dies here without
             * inflating a single byte. */
            if (declaredTotalBytes(entries) > maxTotalDeclaredBytes) {
                throw new NodeException("Backup archive is too large");
            }
            ZipEntry manifest = null;
            for (final ZipEntry entry : entries) {
                if (StorageBackupBackend.MANIFEST_ENTRY.equals(entry.getName())) {
                    if (entry.isDirectory()) {
                        throw new IncompleteArchiveException("Backup manifest is missing in %s".formatted(archive));
                    }
                    manifest = entry;
                    break;
                }
            }
            if (manifest == null) {
                throw new IncompleteArchiveException("Backup archive is missing manifest in %s".formatted(archive));
            }
            return readBoundedManifest(zip, manifest, archive);
        } catch (final IOException failure) {
            throw new NodeException("Failed to read backup manifest from %s".formatted(archive), failure);
        }
    }

    private static byte[] readBoundedManifest(final ZipFile zip, final ZipEntry manifest, final Path archive)
            throws NodeException {
        if (manifest.getSize() > MAX_MANIFEST_BYTES) {
            throw new NodeException("Backup manifest is too large in %s".formatted(archive));
        }
        final long declared = manifest.getSize();
        final byte[] transferBuffer = new byte[8192];
        final int initialCapacity = (int) Math.clamp(declared, 0L, MAX_MANIFEST_BYTES);
        final ByteArrayOutputStream output = new ByteArrayOutputStream(initialCapacity);
        try (InputStream data = zip.getInputStream(manifest)) {
            transferBounded(data, output, 0L, MAX_MANIFEST_BYTES, transferBuffer);
        } catch (final IOException truncated) {
            /* transferBounded fails on oversize input and on corrupt streams
             * alike. Only a full buffer means oversize; anything else is a
             * truncated or corrupt archive. */
            if (output.size() >= MAX_MANIFEST_BYTES) {
                throw new NodeException("Backup manifest is too large in %s".formatted(archive), truncated);
            }
            throw incomplete("Backup manifest is truncated or corrupt in %s".formatted(archive), truncated);
        }
        if (declared >= 0L && output.size() > declared) {
            throw new NodeException("Backup manifest exceeds its declared size in %s".formatted(archive));
        }
        return output.toByteArray();
    }

    /// Reports whether the archive carries the storage payload, not just a manifest.
    ///
    /// A truncated archive with an intact manifest is not a complete backup.
    ///
    /// @param archive           archive to inspect
    /// @param maxArchiveEntries entry-count ceiling
    /// @return `true` when a storage entry is present
    /// @throws NodeException if the archive cannot be inspected
    static boolean containsStoragePayload(final Path archive, final int maxArchiveEntries)
            throws NodeException {
        try (ZipFile zip = openArchive(archive)) {
            for (final ZipEntry entry : listEntries(zip, maxArchiveEntries)) {
                final String name = entry.getName();
                if (StorageBackupBackend.STORAGE_ENTRY.equals(name) ||
                    name.startsWith(StorageBackupBackend.STORAGE_ENTRY + "/")) {
                    return true;
                }
            }
            return false;
        } catch (final IOException failure) {
            throw new NodeException("Failed to inspect backup archive %s".formatted(archive), failure);
        }
    }

    private static ZipFile openArchive(final Path archive) throws NodeException {
        try {
            AtomicFileWriter.ensureNoSymbolicLinks(archive);
            return new ZipFile(archive.toFile());
        } catch (final ZipException corrupt) {
            throw incomplete("Backup archive is corrupt: %s".formatted(archive), corrupt);
        } catch (final FileNotFoundException missing) {
            /* ZipFile reports both a missing file and an unreadable one as
             * FileNotFoundException. Only a path that is actually gone is
             * conclusive evidence of an incomplete archive; a permission or
             * transient open failure must fail closed. */
            if (Files.notExists(archive)) {
                throw incomplete("Backup archive is missing: %s".formatted(archive), missing);
            }
            throw new NodeException("Failed to open backup archive %s".formatted(archive), missing);
        } catch (final IOException failure) {
            throw new NodeException("Failed to open backup archive %s".formatted(archive), failure);
        }
    }

    private static IncompleteArchiveException incomplete(final String message, final Throwable cause) {
        return new IncompleteArchiveException(message, cause);
    }

    /// Lists central-directory entries after count, duplicate, and name checks.
    ///
    /// Reading the central directory inflates nothing, so hostile declared
    /// sizes are visible before any entry data is decompressed.
    private static List<ZipEntry> listEntries(final ZipFile zip, final int maxEntries) throws NodeException {
        final List<ZipEntry> entries = new ArrayList<>();
        final Set<String> names = new HashSet<>();
        for (final var iterator = zip.entries().asIterator(); iterator.hasNext(); ) {
            final ZipEntry entry = iterator.next();
            if (entries.size() >= maxEntries || !names.add(entry.getName())) {
                throw new NodeException("Backup archive contains too many or duplicate entries");
            }
            if (!safeArchiveName(entry.getName())) {
                throw new NodeException("Backup archive contains an unsafe entry: %s".formatted(entry.getName()));
            }
            entries.add(entry);
        }
        return entries;
    }

    /// Sums declared entry sizes, or `-1` when any size is unknown.
    private static long declaredTotalBytes(final List<ZipEntry> entries) throws NodeException {
        long total = 0L;
        for (final ZipEntry entry : entries) {
            if (entry.isDirectory()) continue;
            final long size = entry.getSize();
            if (size < 0L) return -1L;
            if (size > Long.MAX_VALUE - total) {
                throw new NodeException("Backup archive declares more data than the extraction budget");
            }
            total += size;
        }
        return total;
    }

    private static long extractionBudget(final List<ZipEntry> entries, final long maximum)
            throws NodeException {
        final long declared = declaredTotalBytes(entries);
        if (declared > maximum) {
            throw new NodeException("Backup archive declares more data than the extraction budget");
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
            throw new NodeException("Backup archive entry escapes extraction root");
        }
        if (entry.isDirectory()) {
            Files.createDirectories(target);
            return extractedBytes;
        }
        final Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        /* A lying entry must not out-produce its declared size; unknown
         * sizes fall back to the remaining overall budget. The fresh private
         * root and `NOFOLLOW_LINKS` create keep the entry from following a
         * link, without re-walking every path component for each entry. */
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
        final String name = workingDir.relativize(path).toString().replace(File.separatorChar, '/');
        if (name.isEmpty()) return;
        final ZipEntry entry;
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            entry = new ZipEntry(name.endsWith("/") ? name : name + "/");
        } else {
            entry = new ZipEntry(name);
        }
        zip.putNextEntry(entry);
        if (!entry.isDirectory()) {
            try (var inputChannel = AtomicFileWriter.openRegularFile(path);
                 InputStream input = Channels.newInputStream(inputChannel)) {
                int read;
                while ((read = input.read(transferBuffer)) != -1) {
                    zip.write(transferBuffer, 0, read);
                }
            }
        }
        zip.closeEntry();
    }

    private static void validateExtractedArchive(final Path root, final boolean requireBackupMetadata)
            throws NodeException {
        try (final var paths = Files.walk(root)) {
            final var iterator = paths.iterator();
            while (iterator.hasNext()) {
                final Path path = iterator.next();
                if (Files.isSymbolicLink(path) || (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
                                                   !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))) {
                    throw new NodeException("Backup archive contains an unsupported extracted entry: %s".formatted(path));
                }
            }
            if (!Files.isDirectory(root.resolve(StorageBackupBackend.STORAGE_ENTRY), LinkOption.NOFOLLOW_LINKS)) {
                throw new NodeException("Backup archive is missing storage");
            }
            if (requireBackupMetadata &&
                (!Files.isRegularFile(root.resolve(StorageBackupBackend.MANIFEST_ENTRY), LinkOption.NOFOLLOW_LINKS) ||
                 !Files.isRegularFile(root.resolve(StorageBackupBackend.READY_ENTRY), LinkOption.NOFOLLOW_LINKS))) {
                throw new NodeException("Backup archive is missing manifest or ready marker");
            }
        } catch (final IOException failure) {
            throw new NodeException("Failed to validate extracted backup archive", failure);
        }
    }

        /// Signals that an archive is conclusively incomplete or corrupt.
    ///
    /// Only truncation, a corrupt ZIP structure, or a missing manifest may be
    /// reported through this type. Callers may replace such a file, while a
    /// plain [NodeException] from a transient read error must fail
    /// closed and leave any durable archive untouched.
    static final class IncompleteArchiveException extends NodeException {
                /// Creates an incomplete-archive failure.
        ///
        /// @param message failure message
        IncompleteArchiveException(final String message) {
            super(message);
        }

                /// Creates an incomplete-archive failure with a cause.
        ///
        /// @param message failure message
        /// @param cause   evidence of truncation or corruption
        IncompleteArchiveException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
