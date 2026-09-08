package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/** Writes small replication metadata files with forced temporary replacement. */
public final class AtomicFileStore
{
	private static final System.Logger LOGGER = System.getLogger(AtomicFileStore.class.getName());
	private static final FileAttribute<Set<PosixFilePermission>> OWNER_ONLY =
            PosixFilePermissions.asFileAttribute(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));

    private AtomicFileStore()
	{
	}

	@FunctionalInterface
	public interface Encoder
	{
		void write(FileChannel channel) throws IOException;
	}

	/**
	 * Writes a file through a forced sibling temporary file and replacement.
	 * Temporary files use owner read/write permissions where the file system
	 * supports POSIX attributes. If atomic rename is unavailable, the fallback
	 * is logged because crash atomicity is reduced.
	 *
	 * @param path destination path
	 * @param encoder callback that writes the complete encoded contents
	 * @throws IOException if writing or replacement fails
	 */
	public static void write(final Path path, final Encoder encoder) throws IOException
	{
		final Path absolute = path.toAbsolutePath();
		final Path parent = absolute.getParent();
		if (parent != null)
		{
			Files.createDirectories(parent);
		}
		Path temporary;
		try
		{
			temporary = Files.createTempFile(parent, absolute.getFileName() + ".tmp-", null, OWNER_ONLY);
		}
		catch (final UnsupportedOperationException ignored)
		{
			temporary = Files.createTempFile(parent, absolute.getFileName() + ".tmp-", null);
		}
		try
		{
			try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
			{
				encoder.write(channel);
				channel.force(true);
			}
			try
			{
				Files.move(temporary, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			}
			catch (final AtomicMoveNotSupportedException ignored)
			{
				LOGGER.log(System.Logger.Level.WARNING, "Atomic move is unavailable for replication metadata {0}; crash atomicity is reduced", absolute);
				Files.move(temporary, absolute, StandardCopyOption.REPLACE_EXISTING);
			}
			forceDirectory(parent);
		}
		finally
		{
			try
			{
				Files.deleteIfExists(temporary);
			}
			catch (final IOException cleanupFailure)
			{
				LOGGER.log(System.Logger.Level.WARNING,
					"Unable to remove temporary replication metadata file " + temporary, cleanupFailure);
			}
		}
	}

	private static void forceDirectory(final Path parent)
	{
		if (parent == null)
		{
			return;
		}
		try (FileChannel channel = FileChannel.open(parent, StandardOpenOption.READ))
		{
			channel.force(true);
		}
		catch (final IOException | UnsupportedOperationException failure)
		{
			LOGGER.log(System.Logger.Level.WARNING,
				"Directory fsync is unavailable for replication metadata " + parent +
					"; crash durability is reduced", failure);
		}
	}
}
