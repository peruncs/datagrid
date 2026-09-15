package peruncs.datagrid.cluster.nodelibrary.backup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
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
 * This backend exports backups locally and stores them through a backup proxy.
 *
 * <p>The scratch directory holds temporary archives. The proxy is the durable
 * boundary, so a backup is usable only after its archive and replication
 * metadata have both been uploaded.</p>
 */
public interface NetworkArchiveBackupBackend extends StorageBackupBackend
{
	/** Creates a network-backed backup backend.
	 *
	 * @param storageExportScratchSpacePath local scratch directory
	 * @param backupProxyHttpClient remote backup client
	 * @return network backup backend
	 */
	static NetworkArchiveBackupBackend New(
        final Path storageExportScratchSpacePath,
        final BackupProxyHttpClient backupProxyHttpClient
    )
    {
        return new Default(notNull(storageExportScratchSpacePath), notNull(backupProxyHttpClient));
    }

    /** Implements backup export, upload, download, and cleanup. */
    final class Default implements NetworkArchiveBackupBackend
    {
        private static final Logger LOG = LoggerFactory.getLogger(NetworkArchiveBackupBackend.class);
        private static final String USER_UPLOADED_STORAGE_S3_KEY = BackupFileNames.USER_UPLOADED_STORAGE + ".tar.xz";
        private static final Pattern BACKUP_NAME = Pattern.compile("^(\\d+)(\\.manual)?\\.tar\\.xz$");
        private static final int MAX_ARCHIVE_ENTRIES = 1_000_000;
        private static final long MAX_EXTRACTED_BYTES = 1L << 30;
        private static final int MAX_MANIFEST_BYTES = 1 << 20;
        private final Path storageExportScratchSpacePath;
        private final BackupProxyHttpClient http;
        private Default(
            final Path storageExportScratchSpacePath,
            final BackupProxyHttpClient backupProxyHttpClient
        )
        {
            this.storageExportScratchSpacePath = storageExportScratchSpacePath;
            this.http = backupProxyHttpClient;
        }

		@Override
		public List<BackupMetadata> listBackups() throws NodelibraryException
		{
			final List<BackupMetadata> result = new java.util.ArrayList<>();
			for (final BackupMetadataDto metadata : this.http.list())
			{
				if (metadata == null)
				{
					throw new NodelibraryException("Remote backup metadata contains a null entry");
				}
				if (USER_UPLOADED_STORAGE_S3_KEY.equals(metadata.name())) continue;
				try
				{
					result.add(this.parseMetadata(metadata));
				}
				catch (final IllegalArgumentException failure)
				{
					throw new NodelibraryException(
						"Failed to parse backup metadata for remote key " + metadata.name(), failure);
				}
			}
			result.sort(Comparator.comparingLong(BackupMetadata::timestamp));
			return result;
		}

		private BackupMetadata parseMetadata(final BackupMetadataDto metadata)
		{
			if (metadata == null || metadata.name() == null)
			{
				throw new IllegalArgumentException("Remote backup metadata has no name");
			}
			final String name = metadata.name();
			final Matcher matcher = BACKUP_NAME.matcher(name);
			if (!matcher.matches()) throw new IllegalArgumentException("Invalid remote backup name: " + name);
			return new BackupMetadata(
				Long.parseLong(matcher.group(1)),
				matcher.group(2) != null
			);
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

            final Path scratchSpacePath = this.createCursorScratchDirectory();
            final String archiveFileName = this.toArchiveFileName(previousBackupMetadata);
            final Path archiveFilePath = scratchSpacePath.resolve(archiveFileName);

			final byte[] manifestContent;

            try
            {
                this.http.download(archiveFileName, archiveFilePath);
				manifestContent = this.readManifest(archiveFilePath);
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to read backup manifest from " + archiveFileName, failure);
			}
			finally
            {
                try
                {
                    StorageFileOperations.deleteDirectory(scratchSpacePath);
                }
                catch (final NodelibraryException e)
                {
                    LOG.warn("Failed to clean up cursor scratch storage at {}.", scratchSpacePath, e);
                }
            }

			try
			{
				return Optional.of(ReplicationCursorStore.decode(manifestContent));
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to decode backup cursor from " + archiveFileName, failure);
			}
        }

        @Override
        public void deleteBackup(final BackupMetadata backup) throws NodelibraryException
        {
            this.http.delete(this.toArchiveFileName(backup));
        }

        @Override
        public void createAndUploadBackup(
            final StorageConnection connection,
            final ReplicationCursor cursor,
            final BackupMetadata backup
        ) throws NodelibraryException
        {
            final String archiveFileName = this.toArchiveFileName(backup);
            // the exported storage is inside a scratch space volume so we can create the compressed storage there as well
            final Path archiveFilePath = this.storageExportScratchSpacePath.resolve(archiveFileName);

            final var fs = Storage.DefaultFileSystem();

            this.clearScratchSpace();

			try
			{
				Files.createDirectories(this.storageExportScratchSpacePath);
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to create backup scratch space", e);
			}

            try
            {
				connection.issueFullBackup(fs.ensureDirectory(this.storageExportScratchSpacePath.resolve(BackupFileNames.STORAGE)));
				try
				{
					StorageFileOperations.forceDirectory(this.storageExportScratchSpacePath.resolve(BackupFileNames.STORAGE));
					final byte[] manifest = ReplicationCursorStore.encode(cursor);
					AtomicFileStore.writeBytes(this.storageExportScratchSpacePath.resolve(BackupFileNames.MANIFEST), manifest);
					AtomicFileStore.write(this.storageExportScratchSpacePath.resolve(BackupFileNames.READY), channel -> { });
                }
                catch (final IOException e)
                {
                    throw new NodelibraryException("Failed to write backup replication manifest", e);
                }

                this.compressStorage(this.storageExportScratchSpacePath, archiveFilePath);
                this.http.upload(archiveFileName, archiveFilePath);
            }
            finally
            {
                try
                {
                    this.deleteFile(archiveFilePath);
                }
                catch (final NodelibraryException e)
                {
                    LOG.warn("Failed to clean up exported storage at {}", archiveFilePath, e);
                }
				try
				{
					/* The archive is the remote visibility boundary.  Remove the exported
					 * Store and manifest as well; leaving them in scratch would retain an
					 * entire backup between requests and could mix files after a crash. */
					this.clearScratchSpace();
				}
				catch (final NodelibraryException e)
				{
					LOG.warn("Failed to clean up exported backup scratch space at {}",
						this.storageExportScratchSpacePath, e);
				}
            }
        }

        @Override
        public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
            throws NodelibraryException
        {
            final String archiveFileName = this.toArchiveFileName(backup);
			this.createDestinationDirectory(storageDestinationParentPath);
			final Path workingDirectory = this.createTemporaryDirectory(storageDestinationParentPath);
			final Path archiveFilePath = workingDirectory.resolve(archiveFileName);
			Throwable primaryFailure = null;
			try
			{
				this.http.download(archiveFileName, archiveFilePath);
				this.extractArchive(workingDirectory.resolve("extracted"), archiveFilePath, true);
				StorageFileOperations.installStorage(workingDirectory.resolve("extracted").resolve(BackupFileNames.STORAGE),
					storageDestinationParentPath.resolve(BackupFileNames.STORAGE));
			}
			catch (final RuntimeException | Error failure)
			{
				primaryFailure = failure;
				throw failure;
			}
			finally
			{
				StorageFileOperations.cleanup(workingDirectory, primaryFailure);
			}
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

        @Override
        public void downloadUserUploadedStorage(final Path storageDestinationParentPath) throws NodelibraryException
        {
            final String archiveFileName = USER_UPLOADED_STORAGE_S3_KEY;
			this.createDestinationDirectory(storageDestinationParentPath);
			final Path workingDirectory = this.createTemporaryDirectory(storageDestinationParentPath);
			final Path archiveFilePath = workingDirectory.resolve(archiveFileName);
			Throwable primaryFailure = null;
			try
			{
				this.http.download(archiveFileName, archiveFilePath);
				this.extractArchive(workingDirectory.resolve("extracted"), archiveFilePath, false);
				StorageFileOperations.installStorage(workingDirectory.resolve("extracted").resolve(BackupFileNames.STORAGE),
					storageDestinationParentPath.resolve(BackupFileNames.STORAGE));
			}
			catch (final RuntimeException | Error failure)
			{
				primaryFailure = failure;
				throw failure;
			}
			finally
			{
				StorageFileOperations.cleanup(workingDirectory, primaryFailure);
			}
        }

        @Override
        public void deleteUserUploadedStorage() throws NodelibraryException
        {
            this.http.delete(USER_UPLOADED_STORAGE_S3_KEY);
        }

        @Override
        public boolean hasUserUploadedStorage() throws NodelibraryException
        {
            return this.http.list().stream().anyMatch(m -> m != null && USER_UPLOADED_STORAGE_S3_KEY.equals(m.name()));
        }

		private void clearScratchSpace()
		{
			final Path storagePath = this.storageExportScratchSpacePath.resolve(BackupFileNames.STORAGE);
			final Path manifestPath = this.storageExportScratchSpacePath.resolve(BackupFileNames.MANIFEST);

			if (Files.exists(storagePath, LinkOption.NOFOLLOW_LINKS))
            {
                StorageFileOperations.deleteDirectory(storagePath);
            }

			if (Files.exists(manifestPath, LinkOption.NOFOLLOW_LINKS))
			{
				this.deleteFile(manifestPath);
			}
			final Path readyPath = this.storageExportScratchSpacePath.resolve(BackupFileNames.READY);
			if (Files.exists(readyPath, LinkOption.NOFOLLOW_LINKS)) this.deleteFile(readyPath);
		}

		private void createDestinationDirectory(final Path destination) throws NodelibraryException
		{
			try
				{
					StorageFileOperations.ensureNoSymbolicLinks(destination);
					Files.createDirectories(destination);
					StorageFileOperations.ensureNoSymbolicLinks(destination);
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to create backup destination %s".formatted(destination), e);
			}
		}

        private void compressStorage(final Path workingDir, final Path archiveFilePath) throws NodelibraryException
        {
            LOG.trace("Compressing storage");
			try (OutputStream file = Files.newOutputStream(archiveFilePath);
				XZCompressorOutputStream xz = new XZCompressorOutputStream(file);
				TarArchiveOutputStream tar = new TarArchiveOutputStream(xz))
			{
				tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
				for (final String rootName : List.of(BackupFileNames.STORAGE, BackupFileNames.MANIFEST, BackupFileNames.READY))
				{
					final Path root = workingDir.resolve(rootName).normalize();
					if (!root.startsWith(workingDir.normalize()) || !Files.exists(root, LinkOption.NOFOLLOW_LINKS))
					{
						throw new IOException("Backup source is missing: %s".formatted(rootName));
					}
					try (var paths = Files.walk(root))
					{
						for (final var iterator = paths.iterator(); iterator.hasNext();)
						{
							this.writeArchiveEntry(tar, workingDir, iterator.next());
						}
					}
				}
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to compress storage", failure);
			}
        }

        private void deleteFile(final Path path) throws NodelibraryException
        {
            LOG.trace("Deleting file {}", path);
            try
            {
				Files.deleteIfExists(path);
            }
            catch (final IOException e)
            {
                throw new NodelibraryException("Failed to delete file", e);
            }
        }

		private Path createTemporaryDirectory(final Path parent) throws NodelibraryException
		{
			try
			{
				StorageFileOperations.ensureNoSymbolicLinks(parent);
				final Path temporary = Files.createTempDirectory(parent, ".backup-restore-",
					StorageFileOperations.ownerOnlyDirectoryAttributes());
				StorageFileOperations.ensureNoSymbolicLinks(temporary);
				return temporary;
			}
			catch (final IOException failure) { throw new NodelibraryException("Failed to create backup restore workspace", failure); }
		}

		private Path createCursorScratchDirectory() throws NodelibraryException
		{
			try
			{
				StorageFileOperations.ensureNoSymbolicLinks(this.storageExportScratchSpacePath);
				Files.createDirectories(this.storageExportScratchSpacePath);
				StorageFileOperations.ensureNoSymbolicLinks(this.storageExportScratchSpacePath);
				return Files.createTempDirectory(this.storageExportScratchSpacePath, ".cursor-",
					StorageFileOperations.ownerOnlyDirectoryAttributes());
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to create backup metadata scratch space", failure);
			}
		}

		/** Extracts only archives with safe relative, non-link entries. */
		private void extractArchive(
			final Path destination,
			final Path archive,
			final boolean requireBackupMetadata
		) throws NodelibraryException
		{
			final Path root = destination.toAbsolutePath().normalize();
			try
			{
				StorageFileOperations.ensureNoSymbolicLinks(root);
				Files.createDirectories(root);
				final Set<String> names = new HashSet<>();
				int entryCount = 0;
				long extractedBytes = 0L;
				try (InputStream file = Files.newInputStream(archive);
					XZCompressorInputStream xz = new XZCompressorInputStream(file);
					TarArchiveInputStream tar = new TarArchiveInputStream(xz))
				{
					TarArchiveEntry entry;
					while ((entry = tar.getNextEntry()) != null)
					{
						if (++entryCount > MAX_ARCHIVE_ENTRIES || !names.add(entry.getName()))
						{
							throw new IOException("Backup archive contains too many or duplicate entries");
						}
						if (!safeArchiveName(entry.getName()) || entry.isSymbolicLink() || entry.isLink() ||
							(!entry.isDirectory() && !entry.isFile()))
						{
							throw new NodelibraryException("Backup archive contains an unsafe entry: " + entry.getName());
						}
						final Path target = root.resolve(entry.getName()).normalize();
						if (!target.startsWith(root))
						{
							throw new NodelibraryException("Backup archive entry escapes extraction root");
						}
						this.ensureNoSymlinkParent(root, target.getParent());
						StorageFileOperations.ensureNoSymbolicLinks(target);
					if (entry.isDirectory())
					{
						Files.createDirectories(target);
						StorageFileOperations.ensureNoSymbolicLinks(target);
					}
					else
					{
						final Path parent = target.getParent();
						if (parent != null)
						{
							Files.createDirectories(parent);
							StorageFileOperations.ensureNoSymbolicLinks(parent);
						}
							try (OutputStream output = Files.newOutputStream(target,
								StandardOpenOption.CREATE_NEW,
								StandardOpenOption.WRITE))
							{
								extractedBytes = transferBounded(tar, output, extractedBytes, MAX_EXTRACTED_BYTES);
							}
						}
					}
				}
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to extract storage", failure);
			}
			this.validateExtractedArchive(root, requireBackupMetadata);
		}

		private byte[] readManifest(final Path archive) throws IOException
		{
			final Set<String> names = new HashSet<>();
			int entryCount = 0;
			long declaredBytes = 0L;
			byte[] manifest = null;
			try (InputStream file = Files.newInputStream(archive);
				XZCompressorInputStream xz = new XZCompressorInputStream(file);
				TarArchiveInputStream tar = new TarArchiveInputStream(xz))
			{
				TarArchiveEntry entry;
				while ((entry = tar.getNextEntry()) != null)
				{
					if (++entryCount > MAX_ARCHIVE_ENTRIES || !names.add(entry.getName()) ||
						!safeArchiveName(entry.getName()) || entry.isSymbolicLink() || entry.isLink() ||
						(!entry.isDirectory() && !entry.isFile()))
					{
						throw new IOException("Backup archive contains an unsafe or duplicate entry");
					}
					if (entry.isFile() && entry.getSize() >= 0L)
					{
						if (entry.getSize() > MAX_EXTRACTED_BYTES - declaredBytes)
						{
							throw new IOException("Backup archive is too large");
						}
						declaredBytes += entry.getSize();
					}
					if (BackupFileNames.MANIFEST.equals(entry.getName()))
					{
						if (!entry.isFile() || entry.getSize() > MAX_MANIFEST_BYTES)
						{
							throw new IOException("Backup manifest is missing or too large");
						}
						final ByteArrayOutputStream output = new ByteArrayOutputStream(
							(int)Math.max(0L, entry.getSize()));
						transferBounded(tar, output, 0L, MAX_MANIFEST_BYTES);
						manifest = output.toByteArray();
					}
				}
			}
			if (manifest == null) throw new IOException("Backup archive is missing manifest");
			return manifest;
		}

		private static long transferBounded(
			final InputStream input,
			final OutputStream output,
			long copied,
			final long maximum
		) throws IOException
		{
			final byte[] buffer = new byte[8192];
			int read;
			while ((read = input.read(buffer)) != -1)
			{
				if (read > maximum - copied) throw new IOException("Backup archive exceeds extraction limit");
				copied += read;
				output.write(buffer, 0, read);
			}
			return copied;
		}

		private void writeArchiveEntry(
			final TarArchiveOutputStream tar,
			final Path workingDir,
			final Path path
		) throws IOException
		{
			if (Files.isSymbolicLink(path) || (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
				!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)))
			{
				throw new IOException("Backup source contains an unsupported entry: %s".formatted(path));
			}
			final String name = workingDir.relativize(path).toString().replace(java.io.File.separatorChar, '/');
			final TarArchiveEntry entry = new TarArchiveEntry(path, name, LinkOption.NOFOLLOW_LINKS);
			tar.putArchiveEntry(entry);
			if (entry.isFile())
			{
					try (InputStream input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) { input.transferTo(tar); }
			}
			tar.closeArchiveEntry();
		}

		private static boolean safeArchiveName(final String name)
		{
			if (name == null || name.isEmpty() || name.startsWith("/") || name.startsWith("../") || name.equals("..") ||
				name.contains("/../") || name.contains("\0")) return false;
			try
			{
				final Path path = Path.of(name);
				for (final Path part : path)
				{
					if ("..".equals(part.toString())) return false;
				}
				return !path.isAbsolute() && !path.startsWith("..");
			}
			catch (final InvalidPathException failure)
			{
				return false;
			}
		}

		private void ensureNoSymlinkParent(final Path root, final Path parent) throws IOException
		{
			for (Path current = parent; current != null && current.startsWith(root) && !current.equals(root);
				current = current.getParent())
			{
				if (Files.isSymbolicLink(current)) throw new IOException("Archive path has a symbolic-link parent");
			}
		}



		private void validateExtractedArchive(final Path root, final boolean requireBackupMetadata)
			throws NodelibraryException
		{
			try (final var paths = Files.walk(root))
			{
				for (final Path path : paths.toList())
				{
					if (Files.isSymbolicLink(path) || (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
						!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)))
					{
						throw new NodelibraryException("Backup archive contains an unsupported extracted entry: " + path);
					}
				}
				if (!Files.isDirectory(root.resolve(BackupFileNames.STORAGE), LinkOption.NOFOLLOW_LINKS))
				{
					throw new NodelibraryException("Backup archive is missing storage");
				}
				if (requireBackupMetadata &&
					(!Files.isRegularFile(root.resolve(BackupFileNames.MANIFEST), LinkOption.NOFOLLOW_LINKS) ||
					!Files.isRegularFile(root.resolve(BackupFileNames.READY), LinkOption.NOFOLLOW_LINKS)))
				{
					throw new NodelibraryException("Backup archive is missing manifest or ready marker");
				}
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to validate extracted backup archive", failure);
			}
		}



        private String toArchiveFileName(final BackupMetadata backup)
        {
            return backup.timestamp() + (backup.manualSlot() ? ".manual" : "") + ".tar.xz";
        }

    }
}
