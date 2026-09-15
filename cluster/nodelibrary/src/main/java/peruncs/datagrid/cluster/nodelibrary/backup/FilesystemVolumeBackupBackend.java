package peruncs.datagrid.cluster.nodelibrary.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursor;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursorStore;
import peruncs.datagrid.cluster.nodelibrary.store.StorageFileOperations;
import peruncs.datagrid.storage.distributed.types.AtomicFileStore;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.eclipse.serializer.util.X.notNull;

/**
 * This backend stores backups as directories on a local filesystem volume.
 *
 * <p>A backup becomes visible only after its storage, metadata, manifest, and
 * ready marker have been written. The backend keeps user-uploaded storage in
 * a separate directory so it is not mistaken for a generated backup.</p>
 */
public interface FilesystemVolumeBackupBackend extends StorageBackupBackend
{
	/** Creates a filesystem backup backend.
	 *
	 * @param backupVolumePath backup volume path
	 * @return filesystem backup backend
	 */
	static FilesystemVolumeBackupBackend New(
        final Path backupVolumePath
    )
    {
        return new Default(notNull(backupVolumePath));
    }

    /** Implements the filesystem backup marker and copy protocol. */
    class Default implements FilesystemVolumeBackupBackend
    {
        private static final Logger LOG = LoggerFactory.getLogger(FilesystemVolumeBackupBackend.class);

        private final Path backupVolumePath;
        private final Path userUploadedStorageFolderPath;
        private Default(final Path backupVolumePath)
        {
            this.backupVolumePath = backupVolumePath;
            this.userUploadedStorageFolderPath = backupVolumePath.resolve(BackupFileNames.USER_UPLOADED_STORAGE);
        }

        @Override
        public Optional<ReplicationCursor> getCursorFromPreviousBackup(final int skip) throws NodelibraryException
        {
            LOG.trace("Getting backup metadata info of latest-{}", skip);

            final var previousBackupMetadata = this.getLastBackup(skip).orElse(null);
            if (previousBackupMetadata == null)
            {
                return Optional.empty();
            }

            try
            {
				final Path manifest = this.toBackupFolderPath(previousBackupMetadata).resolve(BackupFileNames.MANIFEST);
				return Optional.of(ReplicationCursorStore.decode(Files.readAllBytes(manifest)));
            }
            catch (final IOException e)
            {
				throw new NodelibraryException("Failed to read backup manifest", e);
            }
        }

        @Override
        public List<BackupMetadata> listBackups() throws NodelibraryException
        {
			return this.listBackupVolumeFiles()
				.stream()
				.filter(f -> !f.equals(BackupFileNames.USER_UPLOADED_STORAGE))
                .filter(this::isCompleteBackup)
                .map(this::parseMetadata)
				.sorted(Comparator.comparingLong(BackupMetadata::timestamp))
				.collect(Collectors.toList());
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodelibraryException
        {
            StorageFileOperations.deleteDirectory(this.toBackupFolderPath(backup));
        }

        @Override
        public void createAndUploadBackup(
            final StorageConnection connection,
            final ReplicationCursor cursor,
            final BackupMetadata backup
        ) throws NodelibraryException
        {
            final Path backupRootPath = this.toBackupFolderPath(backup);
            final var fs = Storage.DefaultFileSystem();

			connection.issueFullBackup(fs.ensureDirectory(backupRootPath.resolve(BackupFileNames.STORAGE)));
			try
			{
				StorageFileOperations.forceDirectory(backupRootPath.resolve(BackupFileNames.STORAGE));
				final byte[] manifest = ReplicationCursorStore.encode(cursor);
				AtomicFileStore.writeBytes(backupRootPath.resolve(BackupFileNames.MANIFEST), manifest);
				/* The ready marker is the visibility commit.  It is written last
				 * through the same forced replacement protocol so a crash cannot
				 * publish a marker before the manifest and directory entry are
				 * durable. */
				AtomicFileStore.write(backupRootPath.resolve(BackupFileNames.READY), channel -> { });
			}
			catch (final IOException e)
			{
                throw new NodelibraryException("Failed to publish backup replication manifest", e);
            }
        }

        @Override
        public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
            throws NodelibraryException
        {
            final Path backupRootPath = this.toBackupFolderPath(backup);
			this.restoreDirectory(backupRootPath.resolve(BackupFileNames.STORAGE), storageDestinationParentPath);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodelibraryException
        {
            return this.listBackupVolumeFiles()
                .stream()
                .anyMatch(f -> f.equals(this.userUploadedStorageFolderPath.getFileName().toString()));
        }

        @Override
        public void downloadUserUploadedStorage(final Path storageDestinationParentPath) throws NodelibraryException
        {
			this.restoreDirectory(this.userUploadedStorageFolderPath, storageDestinationParentPath);
        }

        @Override
        public void deleteUserUploadedStorage() throws NodelibraryException
        {
            StorageFileOperations.deleteDirectory(this.userUploadedStorageFolderPath);
        }

        @Override
        public void downloadLatestBackup(final Path targetRootPath) throws NodelibraryException
        {
            final var backup = this.latestBackup(false);
            if (backup == null)
            {
                throw new NodelibraryException("No backups are available to download");
            }
            this.downloadBackup(targetRootPath, backup);
        }

        private Path toBackupFolderPath(final BackupMetadata backup)
        {
            return this.backupVolumePath.resolve(
                backup.timestamp() + (backup.manualSlot() ? ".manual" : "")
            );
        }

        private BackupMetadata parseMetadata(final String folderName) throws NodelibraryException
        {
            final String manualSuffix = ".manual";
            final boolean isManual = folderName.endsWith(manualSuffix);
            final long timestamp;
            final String backupName = isManual ? folderName.substring(0, folderName.length() - manualSuffix.length())
                                               : folderName;
            try
            {
                timestamp = Long.parseLong(backupName);
            }
			catch (final NumberFormatException e)
			{
				throw new NodelibraryException(
					"Failed to parse backup timestamp for backup " + this.backupVolumePath.resolve(folderName), e
				);
            }
            return new BackupMetadata(timestamp, isManual);
        }

        /**
         * A backup directory is visible only after the final ready marker has
         * been forced.  Requiring the storage directory and manifest as well
         * prevents an interrupted copy from being selected as the latest
         * recoverable backup.
         */
		private boolean isCompleteBackup(final String folderName)
		{
			final Path root = this.backupVolumePath.resolve(folderName);
			return Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)
			&& Files.isDirectory(root.resolve(BackupFileNames.STORAGE), LinkOption.NOFOLLOW_LINKS)
			&& Files.isRegularFile(root.resolve(BackupFileNames.MANIFEST), LinkOption.NOFOLLOW_LINKS)
			&& Files.isRegularFile(root.resolve(BackupFileNames.READY), LinkOption.NOFOLLOW_LINKS);
        }

		private void copyDirectory(final Path sourceDir, final Path destinationDir)
		{
			try
			{
				StorageFileOperations.ensureNoSymbolicLinks(destinationDir);
				if (!Files.isDirectory(sourceDir, LinkOption.NOFOLLOW_LINKS))
				{
					throw new NodelibraryException("Backup source is not a directory: " + sourceDir);
				}
				if (Files.exists(destinationDir, LinkOption.NOFOLLOW_LINKS))
				{
					throw new NodelibraryException("Backup destination already contains storage: " + destinationDir);
				}
				Files.createDirectories(destinationDir);
				StorageFileOperations.ensureNoSymbolicLinks(destinationDir);
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to create backup destination " + destinationDir, e);
			}
			try (final var storageFiles = Files.walk(sourceDir))
			{
				storageFiles.forEach(f ->
				{
					if (f.equals(sourceDir)) return;
					try
					{
						final var destinationFile = destinationDir.resolve(sourceDir.relativize(f));
						StorageFileOperations.ensureNoSymbolicLinks(destinationFile);
						if (Files.isSymbolicLink(f))
						{
							throw new IOException("Backup source contains a symbolic link: " + f);
						}
						if (Files.isDirectory(f, LinkOption.NOFOLLOW_LINKS))
						{
							Files.createDirectories(destinationFile);
							StorageFileOperations.ensureNoSymbolicLinks(destinationFile);
						}
						else if (Files.isRegularFile(f, LinkOption.NOFOLLOW_LINKS))
						{
							Files.createDirectories(destinationFile.getParent());
							StorageFileOperations.ensureNoSymbolicLinks(destinationFile.getParent());
							Files.copy(f, destinationFile, LinkOption.NOFOLLOW_LINKS);
						}
						else
						{
							throw new IOException("Unsupported backup entry: " + f);
						}
                    }
                    catch (final IOException e)
                    {
                        throw new NodelibraryException("Failed to copy file", e);
                    }
                });
            }
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to walk files at " + sourceDir, e);
			}
		}

		private void restoreDirectory(final Path sourceDir, final Path destinationParent)
		{
			try
			{
				StorageFileOperations.ensureNoSymbolicLinks(destinationParent);
				Files.createDirectories(destinationParent);
				StorageFileOperations.ensureNoSymbolicLinks(destinationParent);
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to create backup destination " + destinationParent, failure);
			}
			final Path temporary;
			try
			{
				temporary = Files.createTempDirectory(destinationParent, ".backup-restore-",
					StorageFileOperations.ownerOnlyDirectoryAttributes());
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to create temporary backup destination", failure);
			}
			Throwable primaryFailure = null;
			try
			{
				this.copyDirectory(sourceDir, temporary.resolve(BackupFileNames.STORAGE));
				StorageFileOperations.installStorage(temporary.resolve(BackupFileNames.STORAGE),
					destinationParent.resolve(BackupFileNames.STORAGE));
			}
			catch (final RuntimeException | Error failure)
			{
				primaryFailure = failure;
				throw failure;
			}
			finally
			{
				StorageFileOperations.cleanup(temporary, primaryFailure);
			}
		}

		private List<String> listBackupVolumeFiles() throws NodelibraryException
		{
			if (Files.notExists(this.backupVolumePath, LinkOption.NOFOLLOW_LINKS))
			{
				return List.of();
			}
			try (final var listStream = Files.list(this.backupVolumePath))
            {
                return listStream.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
            }
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to iterate backup files at " + this.backupVolumePath, e);
			}
        }
    }
}
