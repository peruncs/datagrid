package peruncs.cluster.node.backup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.replication.ReplicationCursorStore;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies archive safety limits and the stable backup archive format.
class BackupArchiveTest {
    private static void writeArchive(final Path archive, final Entry... entries) throws Exception {
        try (OutputStream file = Files.newOutputStream(archive);
             ZipOutputStream zip = new ZipOutputStream(file)) {
            for (final Entry source : entries) {
                final ZipEntry entry = new ZipEntry(source.name());
                if (source.data() != null) entry.setSize(source.data().length);
                zip.putNextEntry(entry);
                if (source.data() != null) zip.write(source.data());
                zip.closeEntry();
            }
        }
    }

    /// Verifies an archive with a path-traversal entry is rejected before installing storage and writes nothing outside the destination.
    @Test
    void rejectsTraversalBeforeInstallingStorage(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("unsafe.zip");
        writeArchive(archive, new Entry("../escaped", "bad"));

        assertThrows(NodeException.class,
                () -> BackupArchive.extractArchive(
                        root.resolve("extracted"), archive, true, BackupArchiveLimits.defaults()));
        assertFalse(Files.exists(root.resolve("escaped")));
    }

    /// Verifies a valid backup archive extracts and its stored payload reads back intact.
    @Test
    void extractsValidArchive(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("valid.zip");
        writeArchive(archive,
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/", (String) null),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/data", "payload"),
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, "manifest"),
                new Entry(StorageBackupBackend.READY_ENTRY, ""));

        BackupArchive.extractArchive(
                root.resolve("extracted"), archive, true, BackupArchiveLimits.defaults());

        assertEquals("payload", Files.readString(root.resolve("extracted").resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
    }

    /// Verifies the cursor manifest reads directly from the archive without extracting the Store payload.
    @Test
    void readsCursorManifestWithoutExtractingStorage(@TempDir final Path root) throws Exception {
        final ReplicationCursor expected = new ReplicationCursor("test", null, 9L, "0405");
        final Path archive = root.resolve("cursor.zip");
        writeArchive(archive,
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/", (String) null),
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, ReplicationCursorStore.encode(expected)),
                new Entry(StorageBackupBackend.READY_ENTRY, (String) null));

        assertEquals(expected, ReplicationCursorStore.decode(
                BackupArchive.readManifest(archive,
                        BackupArchiveLimits.defaults().maxExtractedBytes(),
                        BackupArchiveLimits.defaults().maxArchiveEntries())));
        assertFalse(Files.exists(root.resolve("extracted")));
    }

    /// Verifies an archive with duplicate entries is rejected and leaves no Store payload behind.
    @Test
    void rejectsDuplicateArchiveEntries(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("duplicate.zip");
        writeDuplicateArchive(archive);

        final Path extracted = root.resolve("extracted");
        assertThrows(NodeException.class,
                () -> BackupArchive.extractArchive(
                        extracted, archive, true, BackupArchiveLimits.defaults()));
        AtomicFileWriter.cleanup(extracted, null);
        assertFalse(Files.exists(extracted.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
    }

    /// Verifies an archive missing its manifest is rejected when reading the cursor.
    @Test
    void rejectsMissingManifest(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("missing-manifest.zip");
        writeArchive(archive, new Entry(StorageBackupBackend.STORAGE_ENTRY + "/", (String) null));

        assertThrows(NodeException.class, () -> BackupArchive.readManifest(
                archive, BackupArchiveLimits.defaults().maxExtractedBytes(),
                BackupArchiveLimits.defaults().maxArchiveEntries()));
    }

    /// Verifies a manifest larger than the manifest bound is rejected.
    @Test
    void rejectsManifestLargerThanLimit(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("large-manifest.zip");
        writeArchive(archive, new Entry(StorageBackupBackend.MANIFEST_ENTRY, new byte[(1 << 20) + 1]));

        assertThrows(NodeException.class, () -> BackupArchive.readManifest(
                archive, BackupArchiveLimits.defaults().maxExtractedBytes(),
                BackupArchiveLimits.defaults().maxArchiveEntries()));
    }

    /// Verifies a malformed backup file name is not recognized and its metadata parsing fails.
    @Test
    void rejectsMalformedBackupFilename() {
        assertFalse(BackupArchive.isBackupFileName("123.evil.zip"));
        assertThrows(NodeException.class,
                () -> BackupArchive.parseMetadata("123.evil.zip", Path.of("backups")));
    }

    /// Verifies backup file-name recognition accepts uppercase archive suffixes.
    @Test
    void acceptsCaseInsensitiveBackupFilename() {
        assertTrue(BackupArchive.isBackupFileName(
                "123.MANUAL.38F5081FA27C4682AC01943D9DB25170.C5537F6F32824C38BAC12D2BC4D76659.5.42.7.B9A38329F6904FCC9B825E6908C30D9F.ZIP"));
    }

    /// Verifies an archive declaring more bytes than the extraction budget is rejected for both extraction and manifest reads.
    @Test
    void rejectsArchiveDeclaringMoreThanBudget(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("lying.zip");
        writeRawStoredArchive(archive,
                new RawEntry(StorageBackupBackend.STORAGE_ENTRY + "/data", "tiny", 2_000_000_000L),
                new RawEntry(StorageBackupBackend.MANIFEST_ENTRY, "manifest", 8L),
                new RawEntry(StorageBackupBackend.READY_ENTRY, "", 0L));

        final Path extracted = root.resolve("extracted");
        assertThrows(NodeException.class, () -> BackupArchive.extractArchive(
                extracted, archive, true, BackupArchiveLimits.of(1024L)));
        assertThrows(NodeException.class, () -> BackupArchive.readManifest(archive, 1024L, 8));
        assertFalse(Files.exists(extracted.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
    }

    /// Verifies an entry writing more data than its declared size is rejected during extraction.
    @Test
    void rejectsEntryDataBeyondDeclaredSize(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("overrun.zip");
        writeRawStoredArchive(archive,
                new RawEntry(StorageBackupBackend.STORAGE_ENTRY + "/data", "hello", 2L),
                new RawEntry(StorageBackupBackend.MANIFEST_ENTRY, "manifest", 8L),
                new RawEntry(StorageBackupBackend.READY_ENTRY, "", 0L));

        assertThrows(NodeException.class, () -> BackupArchive.extractArchive(
                root.resolve("extracted"), archive, true, BackupArchiveLimits.defaults()));
    }

    /// Verifies extraction enforces the configured byte budget, rejecting a tight limit while the default limit succeeds.
    @Test
    void extractionBudgetComesFromLimits(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("budgeted.zip");
        writeArchive(archive,
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/", (String) null),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/data", "payload"),
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, "manifest"),
                new Entry(StorageBackupBackend.READY_ENTRY, ""));

        final Path tight = root.resolve("tight");
        assertThrows(NodeException.class, () -> BackupArchive.extractArchive(
                tight, archive, true, BackupArchiveLimits.of(8L)));
        BackupArchive.extractArchive(
                root.resolve("roomy"), archive, true, BackupArchiveLimits.defaults());
        assertEquals("payload", Files.readString(
                root.resolve("roomy").resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data")));
    }

    /// Verifies the content digest of a Store directory matches the digest of its compressed archive.
    @Test
    void directoryAndArchiveDigestsAgree(@TempDir final Path root) throws Exception {
        final Path export = root.resolve("export");
        Files.createDirectories(export.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("sub"));
        Files.writeString(export.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("data"), "payload");
        Files.writeString(export.resolve(StorageBackupBackend.STORAGE_ENTRY).resolve("sub").resolve("nested"), "nested");
        Files.writeString(export.resolve(StorageBackupBackend.MANIFEST_ENTRY), "manifest");
        Files.writeString(export.resolve(StorageBackupBackend.READY_ENTRY), "");

        final long before = BackupArchive.contentDigestOfDirectory(export);
        final Path archive = root.resolve("backup.zip");
        BackupArchive.compressStorage(export, archive);

        assertEquals(before, BackupArchive.contentDigestOfArchive(archive, BackupArchiveLimits.defaults()),
                "identical content must digest identically before and after archiving");
    }

    /// Verifies storage-payload detection distinguishes full backups from manifest-only archives.
    @Test
    void reportsStoragePayloadPresence(@TempDir final Path root) throws Exception {
        final Path full = root.resolve("full.zip");
        writeArchive(full,
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/", (String) null),
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, "manifest"),
                new Entry(StorageBackupBackend.READY_ENTRY, ""));
        assertTrue(BackupArchive.containsStoragePayload(full, BackupArchiveLimits.defaults().maxArchiveEntries()));

        final Path manifestOnly = root.resolve("manifest-only.zip");
        writeArchive(manifestOnly, new Entry(StorageBackupBackend.MANIFEST_ENTRY, "manifest"));
        assertFalse(BackupArchive.containsStoragePayload(manifestOnly, BackupArchiveLimits.defaults().maxArchiveEntries()));
    }

    /// Verifies the archive digest honors the configured entry budget.
    @Test
    void digestHonorsTheEntryBudget(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("many-entries.zip");
        writeArchive(archive,
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, "manifest"),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/one", "1"),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/two", "2"));

        assertThrows(NodeException.class, () -> BackupArchive.contentDigestOfArchive(
                archive, BackupArchiveLimits.of(1L << 30, 2)));
        assertTrue(BackupArchive.contentDigestOfArchive(archive, BackupArchiveLimits.defaults()) >= 0L);
    }

    /// Verifies the digest rejects an archive whose manifest exceeds the manifest bound.
    @Test
    void digestRejectsAnOversizedManifest(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("oversized-manifest.zip");
        writeArchive(archive,
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, new byte[BackupArchive.MAX_MANIFEST_BYTES + 1]),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/data", "payload"));

        assertThrows(NodeException.class,
                () -> BackupArchive.contentDigestOfArchive(archive, BackupArchiveLimits.defaults()));
    }

    /// Verifies an explicit entry budget bounds extraction independently of the byte budget.
    @Test
    void entryBudgetBoundsExtraction(@TempDir final Path root) throws Exception {
        final Path archive = root.resolve("entry-budget.zip");
        writeArchive(archive,
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/", (String) null),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/one", "1"),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/two", "2"),
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, "manifest"),
                new Entry(StorageBackupBackend.READY_ENTRY, ""));

        assertThrows(NodeException.class, () -> BackupArchive.extractArchive(
                root.resolve("tight"), archive, true, BackupArchiveLimits.of(1L << 30, 2)));
    }

    /// Verifies default limits derive an entry budget proportional to the byte budget.
    @Test
    void entryBudgetIsProportionalToTheByteBudget() {
        assertTrue(BackupArchiveLimits.of(1L << 20).maxArchiveEntries()
                < BackupArchiveLimits.of(1L << 30).maxArchiveEntries());
        assertTrue(BackupArchiveLimits.of(1L).maxArchiveEntries() >= 1);
        assertEquals(BackupArchiveLimits.MAX_ENTRY_BUDGET,
                BackupArchiveLimits.of(Long.MAX_VALUE).maxArchiveEntries());
    }

    private static void writeDuplicateArchive(final Path archive) throws IOException {
        final Entry[] entries = {
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/", (String) null),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/data", "first"),
                new Entry(StorageBackupBackend.STORAGE_ENTRY + "/data", "second"),
                new Entry(StorageBackupBackend.MANIFEST_ENTRY, "manifest"),
                new Entry(StorageBackupBackend.READY_ENTRY, (String) null)
        };
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final int[] offsets = new int[entries.length];
        for (int index = 0; index < entries.length; index++) {
            offsets[index] = bytes.size();
            writeLocalEntry(bytes, entries[index]);
        }
        final int centralDirectoryOffset = bytes.size();
        for (int index = 0; index < entries.length; index++) {
            writeCentralEntry(bytes, entries[index], offsets[index]);
        }
        final int centralDirectoryLength = bytes.size() - centralDirectoryOffset;
        writeLeInt(bytes, 0x06054b50);
        writeLeShort(bytes, 0);
        writeLeShort(bytes, 0);
        writeLeShort(bytes, entries.length);
        writeLeShort(bytes, entries.length);
        writeLeInt(bytes, centralDirectoryLength);
        writeLeInt(bytes, centralDirectoryOffset);
        writeLeShort(bytes, 0);
        Files.write(archive, bytes.toByteArray());
    }

    /// Writes STORED entries whose central-directory declared sizes may lie about the data.
    private static void writeRawStoredArchive(final Path archive, final RawEntry... entries) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final int[] offsets = new int[entries.length];
        for (int index = 0; index < entries.length; index++) {
            offsets[index] = bytes.size();
            writeRawLocalEntry(bytes, entries[index]);
        }
        final int centralDirectoryOffset = bytes.size();
        for (int index = 0; index < entries.length; index++) {
            writeRawCentralEntry(bytes, entries[index], offsets[index]);
        }
        final int centralDirectoryLength = bytes.size() - centralDirectoryOffset;
        writeLeInt(bytes, 0x06054b50);
        writeLeShort(bytes, 0);
        writeLeShort(bytes, 0);
        writeLeShort(bytes, entries.length);
        writeLeShort(bytes, entries.length);
        writeLeInt(bytes, centralDirectoryLength);
        writeLeInt(bytes, centralDirectoryOffset);
        writeLeShort(bytes, 0);
        Files.write(archive, bytes.toByteArray());
    }

    private static void writeRawLocalEntry(final OutputStream output, final RawEntry entry) throws IOException {
        final byte[] name = entry.name().getBytes(StandardCharsets.UTF_8);
        writeLeInt(output, 0x04034b50);
        writeLeShort(output, 20);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeInt(output, crc32(entry.data()));
        writeLeInt(output, entry.data().length);
        writeLeInt(output, (int) entry.declaredSize());
        writeLeShort(output, name.length);
        writeLeShort(output, 0);
        output.write(name);
        output.write(entry.data());
    }

    private static void writeRawCentralEntry(final OutputStream output, final RawEntry entry, final int offset)
            throws IOException {
        final byte[] name = entry.name().getBytes(StandardCharsets.UTF_8);
        writeLeInt(output, 0x02014b50);
        writeLeShort(output, 20);
        writeLeShort(output, 20);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeInt(output, crc32(entry.data()));
        writeLeInt(output, entry.data().length);
        writeLeInt(output, (int) entry.declaredSize());
        writeLeShort(output, name.length);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeInt(output, 0);
        writeLeInt(output, offset);
        output.write(name);
    }

    private static void writeLocalEntry(final OutputStream output, final Entry entry) throws IOException {
        final byte[] name = entry.name().getBytes(StandardCharsets.UTF_8);
        final byte[] data = entry.data() == null ? new byte[0] : entry.data();
        writeLeInt(output, 0x04034b50);
        writeLeShort(output, 20);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeInt(output, crc32(data));
        writeLeInt(output, data.length);
        writeLeInt(output, data.length);
        writeLeShort(output, name.length);
        writeLeShort(output, 0);
        output.write(name);
        output.write(data);
    }

    private static void writeCentralEntry(final OutputStream output, final Entry entry, final int offset)
            throws IOException {
        final byte[] name = entry.name().getBytes(StandardCharsets.UTF_8);
        final byte[] data = entry.data() == null ? new byte[0] : entry.data();
        writeLeInt(output, 0x02014b50);
        writeLeShort(output, 20);
        writeLeShort(output, 20);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeInt(output, crc32(data));
        writeLeInt(output, data.length);
        writeLeInt(output, data.length);
        writeLeShort(output, name.length);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeShort(output, 0);
        writeLeInt(output, entry.name().endsWith("/") ? 0x10 : 0);
        writeLeInt(output, offset);
        output.write(name);
    }

    private static int crc32(final byte[] data) {
        final CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    private static void writeLeShort(final OutputStream output, final int value) throws IOException {
        output.write(value & 0xff);
        output.write((value >>> 8) & 0xff);
    }

    private static void writeLeInt(final OutputStream output, final int value) throws IOException {
        writeLeShort(output, value);
        writeLeShort(output, value >>> 16);
    }

    private record RawEntry(String name, byte[] data, long declaredSize) {
        private RawEntry(final String name, final String data, final long declaredSize) {
            this(name, data.getBytes(StandardCharsets.UTF_8), declaredSize);
        }
    }

    private record Entry(String name, byte[] data) {
        private Entry(final String name, final String data) {
            this(name, data == null ? null : data.getBytes(StandardCharsets.UTF_8));
        }

        private Entry(final String name, final byte[] data) {
            this.name = name;
            this.data = data == null ? null : data.clone();
        }
    }
}
