package peruncs.datagrid.cluster.nodelibrary.backup;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursor;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursorStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NetworkArchiveBackupBackendTest
{
	@Test
	void rejectsTraversalArchiveBeforeInstallingStorage(@TempDir final Path root) throws Exception
	{
		final Path archive = root.resolve("unsafe.tar.xz");
		writeArchive(archive, new Entry("../escaped", "bad"));
		final NetworkArchiveBackupBackend backend = NetworkArchiveBackupBackend.New(
			root.resolve("scratch"), new FixedArchiveClient(archive));
		final Path destination = root.resolve("destination");

		assertThrows(NodelibraryException.class,
			() -> backend.downloadBackup(destination, new BackupMetadata(1L, false)));
		assertFalse(Files.exists(root.resolve("escaped")));
		assertFalse(Files.exists(destination.resolve(BackupFileNames.STORAGE)));
	}

	@Test
	void extractsValidArchiveAndInstallsOnlyStorage(@TempDir final Path root) throws Exception
	{
		final Path archive = root.resolve("valid.tar.xz");
		writeArchive(archive,
			new Entry("storage/", (String)null),
			new Entry("storage/data", "payload"),
			new Entry(BackupFileNames.MANIFEST, "manifest"),
			new Entry(BackupFileNames.READY, ""));
		final NetworkArchiveBackupBackend backend = NetworkArchiveBackupBackend.New(
			root.resolve("scratch"), new FixedArchiveClient(archive));
		final Path destination = root.resolve("destination");

		backend.downloadBackup(destination, new BackupMetadata(1L, false));

		assertEquals("payload", Files.readString(destination.resolve(BackupFileNames.STORAGE).resolve("data")));
		assertFalse(Files.exists(destination.resolve(BackupFileNames.MANIFEST)));
		assertFalse(Files.exists(destination.resolve(BackupFileNames.READY)));
		assertTrue(Files.isDirectory(destination.resolve(BackupFileNames.STORAGE)));
	}

	@Test
	void readsCursorManifestWithoutInstallingTheArchive(@TempDir final Path root) throws Exception
	{
		final Path archive = root.resolve("cursor.tar.xz");
		final ReplicationCursor expected = new ReplicationCursor("test", null, 9L, new byte[] { 4, 5 });
		writeArchive(archive,
			new Entry("storage/", (String)null),
			new Entry("storage/data", "payload"),
			new Entry(BackupFileNames.MANIFEST, ReplicationCursorStore.encode(expected)),
			new Entry(BackupFileNames.READY, (String)null));
		final NetworkArchiveBackupBackend backend = NetworkArchiveBackupBackend.New(
			root.resolve("scratch"), new FixedArchiveClient(archive,
				List.of(new BackupMetadataDto("10.tar.xz", Files.size(archive)))));

		assertEquals(expected, backend.getCursorFromPreviousBackup(0).orElseThrow());
		assertFalse(Files.exists(root.resolve("scratch").resolve(BackupFileNames.STORAGE)));
	}

	@Test
	void rejectsDuplicateArchiveEntries(@TempDir final Path root) throws Exception
	{
		final Path archive = root.resolve("duplicate.tar.xz");
		writeArchive(archive,
			new Entry("storage/", (String)null),
			new Entry("storage/data", "first"),
			new Entry("storage/data", "second"),
			new Entry(BackupFileNames.MANIFEST, "manifest"),
			new Entry(BackupFileNames.READY, (String)null));
		final NetworkArchiveBackupBackend backend = NetworkArchiveBackupBackend.New(
			root.resolve("scratch"), new FixedArchiveClient(archive));

		assertThrows(NodelibraryException.class,
			() -> backend.downloadBackup(root.resolve("destination"), new BackupMetadata(1L, false)));
		assertFalse(Files.exists(root.resolve("destination").resolve(BackupFileNames.STORAGE)));
	}

	@Test
	void downloadsUserStorageWithoutGeneratedBackupMetadata(@TempDir final Path root) throws Exception
	{
		final Path archive = root.resolve("user.tar.xz");
		writeArchive(archive, new Entry("storage/", (String)null), new Entry("storage/data", "user"));
		final NetworkArchiveBackupBackend backend = NetworkArchiveBackupBackend.New(
			root.resolve("scratch"), new FixedArchiveClient(archive));
		final Path destination = root.resolve("destination");

		backend.downloadUserUploadedStorage(destination);

		assertEquals("user", Files.readString(destination.resolve(BackupFileNames.STORAGE).resolve("data")));
	}

	@Test
	void rejectsMalformedRemoteBackupName(@TempDir final Path root)
	{
		final NetworkArchiveBackupBackend backend = NetworkArchiveBackupBackend.New(
			root.resolve("scratch"), new FixedArchiveClient(root.resolve("archive"),
				List.of(new BackupMetadataDto("123.evil.tar.xz", 0L))));

		assertThrows(NodelibraryException.class, backend::listBackups);
	}

	private static void writeArchive(final Path archive, final Entry... entries) throws Exception
	{
		try (OutputStream file = Files.newOutputStream(archive);
			XZCompressorOutputStream xz = new XZCompressorOutputStream(file);
			TarArchiveOutputStream tar = new TarArchiveOutputStream(xz))
		{
			for (final Entry source : entries)
			{
				final TarArchiveEntry entry = new TarArchiveEntry(source.name());
				if (source.data() != null) entry.setSize(source.data().length);
				tar.putArchiveEntry(entry);
				if (source.data() != null) tar.write(source.data());
				tar.closeArchiveEntry();
			}
		}
	}

	private record Entry(String name, byte[] data)
	{
		private Entry(final String name, final String data)
		{
			this(name, data == null ? null : data.getBytes(StandardCharsets.UTF_8));
		}

		private Entry(final String name, final byte[] data)
		{
			this.name = name;
			this.data = data == null ? null : data.clone();
		}
	}

	private static final class FixedArchiveClient implements BackupProxyHttpClient
	{
		private final Path archive;
		private final List<BackupMetadataDto> metadata;

		private FixedArchiveClient(final Path archive)
		{
			this(archive, List.of());
		}

		private FixedArchiveClient(final Path archive, final List<BackupMetadataDto> metadata)
		{
			this.archive = archive;
			this.metadata = List.copyOf(metadata);
		}

		@Override
		public void upload(final String key, final Path path)
		{
		}

		@Override
		public void delete(final String key)
		{
		}

		@Override
		public Path download(final String key, final Path destination) throws NodelibraryException
		{
			try
			{
				return Files.copy(this.archive, destination, StandardCopyOption.REPLACE_EXISTING);
			}
			catch (final Exception failure)
			{
				throw new NodelibraryException("Failed to copy test archive", failure);
			}
		}

		@Override
		public List<BackupMetadataDto> list()
		{
			return this.metadata;
		}
	}
}
