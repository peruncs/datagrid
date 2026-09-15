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
import java.util.Comparator;
import java.util.List;
import java.util.Set;

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
        return new Default(
                notNull(backupVolumePath).toAbsolutePath().normalize(), notNull(limits));
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
        public ReplicationCursor getCursorFromPreviousBackup(final int skip) throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.TRACE, "Getting backup metadata info of latest-%s".formatted(skip));
            final var previousBackup = this.getLastBackup(skip);
            if (previousBackup == null) return null;

            final Path archive = this.toArchivePath(previousBackup);
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
                    .map(name -> BackupArchive.parseMetadata(name, this.backupVolumePath))
                    .sorted(Comparator.comparingLong(BackupMetadata::timestamp))
                    .toList();
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodeLibraryException {
            this.deleteArchive(this.toArchivePath(backup));
        }

        @Override
        public void createBackup(
                final StorageConnection connection,
                final ReplicationCursor cursor,
                final BackupMetadata backup
        ) throws NodeLibraryException {
            final Path exportDirectory = this.createTemporaryDirectory(".backup-export-");
            Throwable primaryFailure = null;
            try {
                final var fs = Storage.DefaultFileSystem();
                connection.issueFullBackup(fs.ensureDirectory(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY)));
                try {
                    Files.createDirectories(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY));
                    StorageFileOperations.forceDirectory(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY));
                    AtomicFileStore.writeBytes(
                            exportDirectory.resolve(StorageBackupBackend.MANIFEST_ENTRY), ReplicationCursorStore.encode(cursor));
                    AtomicFileStore.write(exportDirectory.resolve(StorageBackupBackend.READY_ENTRY), channel -> {
                    });
                } catch (final IOException failure) {
                    throw new NodeLibraryException("Failed to write backup replication manifest", failure);
                }

                final Path temporaryArchive = exportDirectory.resolve(BackupArchive.toArchiveFileName(backup));
                BackupArchive.compressStorage(exportDirectory, temporaryArchive);
                this.publishArchive(temporaryArchive, this.toArchivePath(backup));
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
        public void restoreLatestBackup(final Path targetRootPath) throws NodeLibraryException {
            final var backup = this.getLastBackup(0);
            if (backup == null) throw new NodeLibraryException("No backups are available to restore");
            this.restoreBackup(targetRootPath, backup);
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
                BackupArchive.extractArchive(
                        workingDirectory.resolve("extracted"), archive, requireBackupMetadata, this.limits);
                StorageFileOperations.installStorage(
                        workingDirectory.resolve("extracted").resolve(StorageBackupBackend.STORAGE_ENTRY),
                        storageDestinationParentPath.resolve(StorageBackupBackend.STORAGE_ENTRY));
            } catch (final RuntimeException | Error failure) {
                primaryFailure = failure;
                throw failure;
            } finally {
                StorageFileOperations.cleanup(workingDirectory, primaryFailure);
            }
        }

        private Path toArchivePath(final BackupMetadata backup) {
            return this.backupVolumePath.resolve(BackupArchive.toArchiveFileName(backup));
        }

        private void publishArchive(final Path temporaryArchive, final Path destination) {
            try {
                try {
                    StorageFileOperations.moveFileAtomically(temporaryArchive, destination);
                } catch (final FileAlreadyExistsException alreadyPublished) {
                    /* A crash between publication and acknowledgement retries the same
                     * backup identity. Publish is idempotent only when the existing
                     * archive is complete; a same-name partial file must be
                     * replaced instead of mistaken for a durable backup. */
                    if (this.isCompleteArchive(destination)) {
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
                        return;
                    }
                    StorageFileOperations.deleteRegularFile(destination);
                    StorageFileOperations.moveFileAtomically(temporaryArchive, destination);
                }
                StorageFileOperations.forceDirectory(this.backupVolumePath);
            } catch (final AtomicMoveNotSupportedException unsupported) {
                throw new NodeLibraryException("Atomic backup publication is not supported", unsupported);
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to publish backup archive %s".formatted(destination), failure);
            }
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
