package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.backup.StorageBackupBackend;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/// Validates a user-uploaded storage archive before it may replace local state.
///
/// Generated backups always carry `storage/`, `manifest`, and `ready`; user
/// uploads skip the metadata checks of the restore path, so this pre-check
/// requires the same essentials: exactly one readable, non-empty `manifest`
/// entry (ambiguity fails) and at least one non-empty Store payload file
/// under `storage/`. It runs before the local image is deleted, so a partial
/// upload never destroys working storage, and a rejected upload is left in
/// place for inspection instead of being silently consumed.
final class UserUploadValidator {
    /* Upper bound for the manifest probe. Generated backups cap their
     * manifest at the same size; anything larger is not a manifest but a
     * damaged or hostile upload. */
    private static final int MANIFEST_LIMIT_BYTES = 1 << 20;

    private UserUploadValidator() {
    }

        /// Fails closed when the user-uploaded archive is missing, ambiguous, or partial.
    ///
    /// The archive path is validated for symbolic links and its file identity
    /// is pinned across the open, so a swap between the existence check and
    /// the read fails instead of validating one file and installing another.
    ///
    /// @param archive upload archive path
    /// @throws NodeLibraryException when the upload is missing, ambiguous, or partial
    static void requireValidUpload(final Path archive) {
        if (!Files.isRegularFile(archive, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(archive)) {
            throw new NodeLibraryException(
                    "User-uploaded storage archive is missing or ambiguous at %s; refusing to install".formatted(archive));
        }
        final Object beforeKey;
        try {
            beforeKey = stableFileKey(archive);
        } catch (final IOException unsafe) {
            throw new NodeLibraryException(
                    "User-uploaded storage archive cannot be inspected at %s; refusing to install".formatted(archive), unsafe);
        }
        int manifests = 0;
        boolean manifestReadable = false;
        boolean storagePayload = false;
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            if (!beforeKey.equals(stableFileKey(archive))) {
                throw new NodeLibraryException(
                        "User-uploaded storage archive changed while it was being opened at %s; refusing to install".formatted(archive));
            }
            final Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                final ZipEntry entry = entries.nextElement();
                final String name = entry.getName();
                if (StorageBackupBackend.MANIFEST_ENTRY.equals(name)) {
                    manifests++;
                    manifestReadable = manifestReadable || isReadableWithinBudget(zip, entry);
                } else if (!entry.isDirectory() && entry.getSize() != 0L &&
                           name.startsWith(StorageBackupBackend.STORAGE_ENTRY + "/")) {
                    storagePayload = true;
                }
            }
        } catch (final IOException failure) {
            throw new NodeLibraryException(
                    "User-uploaded storage archive cannot be read at %s; refusing to install".formatted(archive),
                    failure);
        }
        if (manifests != 1 || !manifestReadable) {
            throw new NodeLibraryException(
                    "User-uploaded storage archive must contain exactly one readable manifest at %s; refusing to install".formatted(archive));
        }
        if (!storagePayload) {
            throw new NodeLibraryException(
                    "User-uploaded storage archive contains no non-empty storage payload at %s; refusing to install a partial upload".formatted(archive));
        }
    }

        /// Reports whether a manifest entry is a readable, non-empty byte
    /// sequence that fits the manifest budget.
    ///
    /// @param zip   open upload archive
    /// @param entry manifest entry
    /// @return `true` when the entry is a non-empty readable byte sequence
    private static boolean isReadableWithinBudget(final ZipFile zip, final ZipEntry entry) {
        if (entry.isDirectory()) {
            return false;
        }
        try (InputStream data = zip.getInputStream(entry)) {
            final byte[] bytes = data.readNBytes(MANIFEST_LIMIT_BYTES);
            return bytes.length != 0 && data.read() == -1;
        } catch (final IOException unreadable) {
            return false;
        }
    }

    private static Object stableFileKey(final Path path) throws IOException {
        final BasicFileAttributes attributes = Files.readAttributes(
                path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attributes.fileKey() == null) {
            throw new IOException("Filesystem does not expose a stable file identity: %s".formatted(path));
        }
        return attributes.fileKey();
    }
}
