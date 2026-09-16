package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.ReplicationCursorStore;
import peruncs.datagrid.cluster.node.store.StorageFileOperations;
import peruncs.datagrid.cluster.storage.types.AtomicFileStore;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

import static org.eclipse.serializer.util.X.notNull;

/// Stores compressed backup archives on a filesystem volume.
///
/// The volume may be network-mounted. A backup is visible only after its
/// complete archive has been atomically moved into the volume root.
public interface FilesystemVolumeBackupBackend extends StorageBackupBackend {
    /// Creates a filesystem backup backend with default restore limits.
    ///
    /// @param backupVolumePath backup volume path
    /// @return filesystem backup backend
    static FilesystemVolumeBackupBackend New(final Path backupVolumePath) {
        return New(backupVolumePath, BackupArchiveLimits.defaults());
    }

    /// Creates a filesystem backup backend with explicit restore limits.
    ///
    /// @param backupVolumePath backup volume path
    /// @param limits budgets bounding restore and manifest reads
    /// @return filesystem backup backend
    static FilesystemVolumeBackupBackend New(final Path backupVolumePath, final BackupArchiveLimits limits) {
        return new Default(notNull(backupVolumePath).toAbsolutePath().normalize(), notNull(limits));
    }

    /// Implements archive export, restore, and cleanup.
    final class Default implements FilesystemVolumeBackupBackend {
        private static final System.Logger LOGGER = System.getLogger(FilesystemVolumeBackupBackend.class.getName());

        private final Path backupVolumePath;
        private final Path userUploadedStorageArchivePath;
        private final BackupArchiveLimits limits;

        private Default(final Path backupVolumePath, final BackupArchiveLimits limits) {
            this.backupVolumePath = backupVolumePath;
            this.userUploadedStorageArchivePath = backupVolumePath.resolve(StorageBackupBackend.USER_UPLOADED_STORAGE_ARCHIVE);
            this.limits = limits;
        }

        @Override
        public ReplicationCursor getCursorForBackup(final BackupMetadata backup) throws NodeLibraryException {
            Objects.requireNonNull(backup, "backup");
            return this.readBackupCursor(this.toArchivePath(backup));
        }

        private ReplicationCursor readBackupCursor(final Path archive) throws NodeLibraryException {
            try {
                return ReplicationCursorStore.decode(
                        BackupArchive.readManifest(archive, this.limits.maxExtractedBytes()));
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to decode backup manifest from %s".formatted(archive), failure);
            }
        }

        @Override
        public List<BackupMetadata> listBackups() throws NodeLibraryException {
            return this.listBackupVolumeFiles().stream()
                    .filter(BackupArchive::isBackupFileName)
                    .filter(name -> Files.isRegularFile(this.backupVolumePath.resolve(name), LinkOption.NOFOLLOW_LINKS))
                    .map(this::parseListedBackup)
                    .filter(Objects::nonNull)
                    .sorted(Comparator.comparingLong(BackupMetadata::timestamp)
                            .thenComparing(BackupMetadata::backupId))
                    .toList();
        }

                /// Parses one volume file, overlaying the archived identity when present.
        ///
        /// The file name carries the selection fields; the identity sidecar
        /// additionally carries provenance and the content digest. Archives
        /// without a sidecar predate generations and list with unknown
        /// provenance, while a corrupt sidecar means an untrustworthy file
        /// that is skipped instead of selected.
        ///
        /// @param name volume file name
        /// @return listed backup, or `null` when its identity is corrupt
        private BackupMetadata parseListedBackup(final String name) {
            final Path archive = this.backupVolumePath.resolve(name);
            final BackupMetadata parsed = BackupArchive.parseMetadata(name, this.backupVolumePath);
            final BackupMetadata identity;
            try {
                identity = BackupArchive.readIdentity(archive);
            } catch (final NodeLibraryException corrupt) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Skipping backup archive with an unreadable identity at %s".formatted(archive), corrupt);
                return null;
            }
            return identity == null ? parsed : identity;
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodeLibraryException {
            this.deleteArchive(this.toArchivePath(backup));
        }

        @Override
        public void createBackup(final StorageConnection connection, final ReplicationCursor cursor, final BackupMetadata backup)
                throws NodeLibraryException {
            final Path exportDirectory = this.createTemporaryDirectory(".backup-export-");
            Throwable primaryFailure = null;
            try {
                final var fs = Storage.DefaultFileSystem();
                connection.issueFullBackup(fs.ensureDirectory(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY)));
                final byte[] manifestBytes;
                try {
                    Files.createDirectories(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY));
                    StorageFileOperations.forceDirectory(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY));
                    manifestBytes = ReplicationCursorStore.encode(cursor);
                    AtomicFileStore.writeBytes(
                            exportDirectory.resolve(StorageBackupBackend.MANIFEST_ENTRY), manifestBytes);
                    AtomicFileStore.write(exportDirectory.resolve(StorageBackupBackend.READY_ENTRY), channel -> {
                    });
                } catch (final IOException failure) {
                    throw new NodeLibraryException("Failed to write backup replication manifest", failure);
                }

                /* The digest covers the manifest and the storage payload, so a
                 * later publication under the same backup id is idempotent
                 * only for identical content. The random backup id in the file
                 * name keeps concurrent publishers from ever sharing a name. */
                final BackupMetadata stamped =
                        backup.withDigest(BackupArchive.contentDigestOfDirectory(exportDirectory));
                BackupArchive.writeIdentity(
                        exportDirectory.resolve(BackupArchive.BACKUP_IDENTITY_ENTRY), stamped);
                final Path temporaryArchive = exportDirectory.resolve(BackupArchive.toArchiveFileName(stamped));
                BackupArchive.compressStorage(exportDirectory, temporaryArchive);
                this.publishArchive(temporaryArchive, this.toArchivePath(backup), manifestBytes, stamped.digest());
            } catch (final RuntimeException | Error failure) {
                primaryFailure = failure;
                throw failure;
            } finally {
                try {
                    StorageFileOperations.cleanup(exportDirectory, primaryFailure);
                } catch (final NodeLibraryException cleanupFailure) {
                    LOGGER.log(System.Logger.Level.WARNING,
                            "Failed to clean up backup export workspace %s".formatted(exportDirectory), cleanupFailure);
                }
            }
        }

        @Override
        public void restoreBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
                throws NodeLibraryException {
            this.restoreArchive(this.toArchivePath(backup), storageDestinationParentPath, true);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodeLibraryException {
            this.ensureVolumeDirectory();
            return Files.isRegularFile(this.userUploadedStorageArchivePath, LinkOption.NOFOLLOW_LINKS);
        }

        @Override
        public void restoreUserUploadedStorage(final Path storageDestinationParentPath) throws NodeLibraryException {
            this.restoreArchive(this.userUploadedStorageArchivePath, storageDestinationParentPath, false);
        }

        @Override
        public void deleteUserUploadedStorage() throws NodeLibraryException {
            this.deleteArchive(this.userUploadedStorageArchivePath);
        }

        private void restoreArchive(
                final Path archive,
                final Path storageDestinationParentPath,
                final boolean requireBackupMetadata
        ) throws NodeLibraryException {
            this.createDestinationDirectory(storageDestinationParentPath);
            final Path workingDirectory = this.createTemporaryDirectory(storageDestinationParentPath, ".backup-restore-");
            Throwable primaryFailure = null;
            try {
                final Path extracted = workingDirectory.resolve("extracted");
                BackupArchive.extractArchive(extracted, archive, requireBackupMetadata, this.limits);
                this.verifyExtractedDigest(archive, extracted);
                StorageFileOperations.installStorage(
                        extracted.resolve(StorageBackupBackend.STORAGE_ENTRY),
                        storageDestinationParentPath.resolve(StorageBackupBackend.STORAGE_ENTRY));
            } catch (final RuntimeException | Error failure) {
                primaryFailure = failure;
                throw failure;
            } finally {
                StorageFileOperations.cleanup(workingDirectory, primaryFailure);
            }
        }

                /// Rejects an archive whose content no longer matches its identity digest.
        ///
        /// Archives without an identity sidecar predate generations and skip
        /// verification; anything carrying a digest must still match it before
        /// it may replace local storage.
        ///
        /// @param archive   source archive
        /// @param extracted extraction root holding `manifest` and `storage`
        /// @throws NodeLibraryException when the digest contradicts the content
        private void verifyExtractedDigest(final Path archive, final Path extracted) throws NodeLibraryException {
            final BackupMetadata identity = BackupArchive.readIdentity(archive);
            if (identity == null || identity.digest() < 0L) {
                return;
            }
            final long actual = BackupArchive.contentDigestOfDirectory(extracted);
            if (actual != identity.digest()) {
                throw new NodeLibraryException(
                        "Backup archive content digest mismatch at %s; refusing to install".formatted(archive));
            }
        }

        private Path toArchivePath(final BackupMetadata backup) {
            return this.backupVolumePath.resolve(BackupArchive.toArchiveFileName(backup));
        }

        private void publishArchive(
                final Path temporaryArchive,
                final Path destination,
                final byte[] manifestBytes,
                final long digest
        ) {
            try {
                /* An atomic rename replaces an existing destination on Unix
                 * instead of failing, so the collision must be detected with
                 * an existence check first; the move-time catch only covers
                 * the residual race with another publisher. */
                if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                    this.resolveSameNamePublication(temporaryArchive, destination, manifestBytes, digest);
                } else {
                    try {
                        StorageFileOperations.moveFileAtomically(temporaryArchive, destination);
                    } catch (final FileAlreadyExistsException raced) {
                        this.resolveSameNamePublication(temporaryArchive, destination, manifestBytes, digest);
                    }
                }
                StorageFileOperations.forceDirectory(this.backupVolumePath);
            } catch (final AtomicMoveNotSupportedException unsupported) {
                throw new NodeLibraryException("Atomic backup publication is not supported", unsupported);
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to publish backup archive %s".formatted(destination), failure);
            }
        }

                /// Resolves a publication whose archive name is already taken.
        ///
        /// A crash between publication and acknowledgement retries the same
        /// backup identity: the retry is idempotent only when the published
        /// archive is complete and holds the identical manifest and content
        /// digest. A same-name partial file is replaced instead of mistaken
        /// for a durable backup, while the same backup id with different
        /// content is a conflicting publication that fails instead of
        /// overwriting — concurrent nodes must differ by backup id.
        ///
        /// @param temporaryArchive new publication in the export workspace
        /// @param destination      occupied archive path
        /// @param manifestBytes    manifest of the new publication
        /// @param digest           content digest of the new publication
        /// @throws NodeLibraryException on a conflicting publication
        private void resolveSameNamePublication(
                final Path temporaryArchive,
                final Path destination,
                final byte[] manifestBytes,
                final long digest
        ) throws NodeLibraryException {
            try {
                if (!this.isCompleteArchive(destination)) {
                    StorageFileOperations.deleteRegularFile(destination);
                    StorageFileOperations.moveFileAtomically(temporaryArchive, destination);
                } else if (this.isIdenticalPublication(destination, manifestBytes, digest)) {
                    LOGGER.log(System.Logger.Level.DEBUG,
                            "Backup archive is already published at %s".formatted(destination));
                    /* The export workspace cleanup deletes this file; a best
                     * effort delete here must not turn a durable backup into
                     * a publication failure. */
                    try {
                        Files.deleteIfExists(temporaryArchive);
                    } catch (final IOException cleanupFailure) {
                        LOGGER.log(System.Logger.Level.WARNING,
                                "Failed to delete superseded backup workspace file %s".formatted(temporaryArchive),
                                cleanupFailure);
                    }
                } else {
                    throw new NodeLibraryException(
                            "Conflicting backup archive is already published at %s; refusing to overwrite"
                                    .formatted(destination));
                }
            } catch (final AtomicMoveNotSupportedException unsupported) {
                throw new NodeLibraryException("Atomic backup publication is not supported", unsupported);
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to publish backup archive %s".formatted(destination), failure);
            }
        }

                /// Reports whether the published archive holds the same backup.
        ///
        /// Both the immutable manifest and the content digest must match: the
        /// manifest pins the replication position, the digest pins the Store
        /// image. Anything else under the same backup id is a conflict.
        ///
        /// @param destination   published archive
        /// @param manifestBytes manifest of the new publication
        /// @param digest        content digest of the new publication
        /// @return `true` for an idempotent retry of the same backup
        private boolean isIdenticalPublication(
                final Path destination,
                final byte[] manifestBytes,
                final long digest
        ) throws NodeLibraryException {
            final byte[] publishedManifest =
                    BackupArchive.readManifest(destination, this.limits.maxExtractedBytes());
            if (!Arrays.equals(publishedManifest, manifestBytes)) {
                return false;
            }
            /* The archived bytes must digest to the new publication's digest;
             * a stored digest that disagrees with either side means bit-rot. */
            if (BackupArchive.contentDigestOfArchive(destination) != digest) {
                return false;
            }
            final BackupMetadata identity = BackupArchive.readIdentity(destination);
            return identity == null || identity.digest() < 0L || identity.digest() == digest;
        }

        private boolean isCompleteArchive(final Path destination) {
            try {
                final String name = destination.getFileName().toString();
                if (!BackupArchive.isBackupFileName(name)) return false;
                BackupArchive.parseMetadata(name, this.backupVolumePath);
                BackupArchive.readManifest(destination, this.limits.maxExtractedBytes());
                /* A truncated archive with an intact manifest must not count
                 * as a durable backup. */
                return BackupArchive.containsStoragePayload(destination);
            } catch (final RuntimeException incomplete) {
                return false;
            }
        }

        private void deleteArchive(final Path archive) throws NodeLibraryException {
            try {
                if (StorageFileOperations.deleteRegularFile(archive)) {
                    StorageFileOperations.forceDirectory(this.backupVolumePath);
                }
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to delete backup archive %s".formatted(archive), failure);
            }
        }

        private void createDestinationDirectory(final Path destination) throws NodeLibraryException {
            try {
                StorageFileOperations.ensureNoSymbolicLinks(destination);
                Files.createDirectories(destination);
                StorageFileOperations.ensureNoSymbolicLinks(destination);
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to create backup destination %s".formatted(destination), failure);
            }
        }

        private Path createTemporaryDirectory(final String prefix) throws NodeLibraryException {
            this.ensureVolumeDirectory();
            return createTemporaryDirectory(this.backupVolumePath, prefix);
        }

        private Path createTemporaryDirectory(final Path parent, final String prefix) throws NodeLibraryException {
            try {
                StorageFileOperations.ensureNoSymbolicLinks(parent);
                Files.createDirectories(parent);
                final Path temporary =
                        Files.createTempDirectory(parent, prefix, privateDirectoryAttributes(parent));
                StorageFileOperations.ensureNoSymbolicLinks(temporary);
                return temporary;
            } catch (final IOException | UnsupportedOperationException | SecurityException failure) {
                throw new NodeLibraryException("Failed to create backup workspace", failure);
            }
        }

        /// Owner-only permissions where the filesystem supports them, default
        /// permissions otherwise. Passing POSIX attributes to a filesystem
        /// without a POSIX view (Windows, archive filesystems) fails directory
        /// creation outright instead of ignoring them.
        static FileAttribute<?>[] privateDirectoryAttributes(final Path parent) {
            if (parent.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                return new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(Set.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE))};
            }
            return new FileAttribute<?>[0];
        }

        private void ensureVolumeDirectory() throws NodeLibraryException {
            try {
                StorageFileOperations.ensureNoSymbolicLinks(this.backupVolumePath);
                Files.createDirectories(this.backupVolumePath);
                StorageFileOperations.ensureNoSymbolicLinks(this.backupVolumePath);
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to prepare backup volume %s".formatted(this.backupVolumePath), failure);
            }
        }

        private List<String> listBackupVolumeFiles() throws NodeLibraryException {
            this.ensureVolumeDirectory();
            try (final var listStream = Files.list(this.backupVolumePath)) {
                return listStream.map(path -> path.getFileName().toString()).sorted().toList();
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to iterate backup files at %s".formatted(this.backupVolumePath), failure);
            }
        }
    }
}
