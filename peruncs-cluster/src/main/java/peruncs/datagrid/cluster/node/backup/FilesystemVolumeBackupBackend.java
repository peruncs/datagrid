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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.eclipse.serializer.util.X.notNull;

/// Stores compressed backup archives on a filesystem volume.
///
/// The volume may be network-mounted. A backup is visible only after its
/// complete archive has been atomically moved into the volume root.
public interface FilesystemVolumeBackupBackend extends StorageBackupBackend {
    /// Creates a filesystem backup backend.
    ///
    /// @param backupVolumePath backup volume path
    /// @return filesystem backup backend
    static FilesystemVolumeBackupBackend New(final Path backupVolumePath) {
        return new Default(notNull(backupVolumePath).toAbsolutePath().normalize());
    }

    /// Implements archive export, restore, and cleanup.
    final class Default implements FilesystemVolumeBackupBackend {
        private static final System.Logger LOGGER = System.getLogger(FilesystemVolumeBackupBackend.class.getName());

        private final Path backupVolumePath;
        private final Path userUploadedStorageArchivePath;

        private Default(final Path backupVolumePath) {
            this.backupVolumePath = backupVolumePath;
            this.userUploadedStorageArchivePath = backupVolumePath.resolve(BackupArchive.USER_UPLOADED_STORAGE_ARCHIVE);
        }

        @Override
        public Optional<ReplicationCursor> getCursorFromPreviousBackup(final int skip) throws NodeLibraryException {
            LOGGER.log(System.Logger.Level.TRACE, "Getting backup metadata info of latest-%s".formatted(skip));
            final var previousBackup = this.getLastBackup(skip).orElse(null);
            if (previousBackup == null) return Optional.empty();

            final Path archive = this.toArchivePath(previousBackup);
            try {
                return Optional.of(ReplicationCursorStore.decode(BackupArchive.readManifest(archive)));
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to read backup manifest from %s".formatted(archive), failure);
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
        public void createAndUploadBackup(
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
        public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
                throws NodeLibraryException {
            this.restoreArchive(this.toArchivePath(backup), storageDestinationParentPath, true);
        }

        @Override
        public void downloadLatestBackup(final Path targetRootPath) throws NodeLibraryException {
            final var backup = this.latestBackup(false);
            if (backup == null) throw new NodeLibraryException("No backups are available to download");
            this.downloadBackup(targetRootPath, backup);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodeLibraryException {
            this.ensureVolumeDirectory();
            return Files.isRegularFile(this.userUploadedStorageArchivePath, LinkOption.NOFOLLOW_LINKS);
        }

        @Override
        public void downloadUserUploadedStorage(final Path storageDestinationParentPath) throws NodeLibraryException {
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
                        workingDirectory.resolve("extracted"), archive, requireBackupMetadata);
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
                StorageFileOperations.ensureNoSymbolicLinks(destination);
                Files.move(temporaryArchive, destination, StandardCopyOption.ATOMIC_MOVE);
                StorageFileOperations.forceDirectory(this.backupVolumePath);
            } catch (final AtomicMoveNotSupportedException unsupported) {
                throw new NodeLibraryException("Atomic backup publication is not supported", unsupported);
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to publish backup archive %s".formatted(destination), failure);
            }
        }

        private void deleteArchive(final Path archive) throws NodeLibraryException {
            try {
                StorageFileOperations.ensureNoSymbolicLinks(archive);
                if (Files.deleteIfExists(archive)) StorageFileOperations.forceDirectory(this.backupVolumePath);
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
                final Path temporary = Files.createTempDirectory(
                        parent, prefix + UUID.randomUUID() + "-", StorageFileOperations.ownerOnlyDirectoryAttributes());
                StorageFileOperations.ensureNoSymbolicLinks(temporary);
                return temporary;
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to create backup workspace", failure);
            }
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
