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
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Writes small replication metadata files with forced temporary replacement.
 * The operation fails when the filesystem cannot provide atomic rename or
 * directory synchronization; callers must choose a filesystem with those
 * durability primitives for replication metadata.
 */
public final class AtomicFileStore
{
	/** Selects checkpoint-specific crash-test phases. */
	public static final String PHASE_CHECKPOINT = "CHECKPOINT";
	/** Selects cursor-specific crash-test phases. */
	public static final String PHASE_CURSOR = "CURSOR";
	private static final System.Logger LOGGER = System.getLogger(AtomicFileStore.class.getName());
	private static final ThreadLocal<BiConsumer<String, Path>> TEST_HOOK = new ThreadLocal<>();
	private static final FileAttribute<Set<PosixFilePermission>> OWNER_ONLY =
		PosixFilePermissions.asFileAttribute(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));

	private AtomicFileStore()
	{
	}

	/**
	 * Installs a thread-confined crash-test hook; production callers must leave
	 * it unset. A blocking hook belongs only in a forked child because file
	 * writes may run while a provider monitor is held.
	 */
	static void setTestHook(final BiConsumer<String, Path> hook)
	{
		if (hook == null) TEST_HOOK.remove();
		else TEST_HOOK.set(hook);
	}

	/** Clears the package-local crash-test hook. */
	static void clearTestHook()
	{
		TEST_HOOK.remove();
	}

	private static void testPoint(final String phase, final Path path)
	{
		final BiConsumer<String, Path> hook = TEST_HOOK.get();
		if (hook != null) hook.accept(phase, path);
	}

	@FunctionalInterface
	/** Writes one complete metadata file to an open channel. */
	public interface Encoder
	{
		void write(FileChannel channel) throws IOException;
	}

	/** Writes a file through a forced sibling temporary file and replacement.
	 *
	 * <p>The {@code phase} parameter selects the crash-test hook names.
	 * When {@code null} the generic names {@code BEFORE_TEMP_WRITE},
	 * {@code DURING_FILE_WRITE}, {@code AFTER_TEMP_WRITE_BEFORE_RENAME},
	 * and {@code AFTER_RENAME_BEFORE_DIRECTORY_SYNC} are used. Checkpoint
	 * and cursor stores pass {@link #PHASE_CHECKPOINT} or {@link #PHASE_CURSOR}.</p>
	 *
	 * @param path destination path
	 * @param encoder callback that writes the complete encoded contents
	 * @param phase crash-test hook phase name, or {@code null} for generic names
	 * @throws IOException if writing or replacement fails
	 */
	public static void write(final Path path, final Encoder encoder, final String phase) throws IOException
	{
		if (phase != null && !PHASE_CHECKPOINT.equals(phase) && !PHASE_CURSOR.equals(phase))
		{
			throw new IllegalArgumentException("unsupported AtomicFileStore phase: " + phase);
		}
		final String beforePhase = phase != null ? "BEFORE_" + phase + "_TEMP_WRITE" : "BEFORE_TEMP_WRITE";
		final String duringPhase = phase != null ? "DURING_" + phase + "_FILE_WRITE" : "DURING_FILE_WRITE";
		final String afterTempPhase = phase != null
			? "AFTER_" + phase + "_TEMP_WRITE_BEFORE_RENAME"
			: "AFTER_TEMP_WRITE_BEFORE_RENAME";
		final String afterRenamePhase = phase != null
			? "AFTER_" + phase + "_RENAME_BEFORE_DIRECTORY_SYNC"
			: "AFTER_RENAME_BEFORE_DIRECTORY_SYNC";
		write(path, encoder, beforePhase, duringPhase, afterTempPhase, afterRenamePhase);
	}

	private static void write(final Path path, final Encoder encoder,
		final String beforePhase, final String duringPhase,
		final String afterTempPhase, final String afterRenamePhase) throws IOException
	{
		final Path absolute = path.toAbsolutePath();
		final Path parent = absolute.getParent();
		if (parent == null)
		{
			throw new IOException("Metadata path has no parent directory: " + path);
		}
		Files.createDirectories(parent);
		Path temporary;
		try
		{
			temporary = Files.createTempFile(parent, absolute.getFileName() + ".tmp-", null, OWNER_ONLY);
		}
		catch (final UnsupportedOperationException ignored)
		{
			LOGGER.log(System.Logger.Level.WARNING,
				"POSIX permissions are unavailable for replication metadata temporary file " + absolute);
			temporary = Files.createTempFile(parent, absolute.getFileName() + ".tmp-", null);
		}
		try
		{
			testPoint(beforePhase, absolute);
			try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
			{
				testPoint(duringPhase, absolute);
				encoder.write(channel);
				channel.force(true);
			}
			testPoint(afterTempPhase, absolute);
			try
			{
				Files.move(temporary, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			}
			catch (final AtomicMoveNotSupportedException failure)
			{
				throw new IOException("Atomic replacement is unavailable for replication metadata " + absolute, failure);
			}
			testPoint(afterRenamePhase, absolute);
			forceDirectory(parent);
		}
		finally
		{
			try
			{
				Files.deleteIfExists(temporary);
			}
			catch (final IOException | RuntimeException cleanupFailure)
			{
				LOGGER.log(System.Logger.Level.WARNING,
					"Unable to remove temporary replication metadata file " + temporary, cleanupFailure);
			}
		}
	}

	/** Writes a file through a forced sibling temporary file and replacement.
	 * Uses generic phase names for the crash-test hook. */
	public static void write(final Path path, final Encoder encoder) throws IOException
	{
		write(path, encoder, null);
	}

	/**
	 * Verifies that the directory containing {@code path} supports the complete
	 * atomic metadata protocol without changing the target file.
	 *
	 * @param path representative metadata path
	 * @throws IOException if temporary replacement or directory synchronization is unavailable
	 */
	public static void verify(final Path path) throws IOException
	{
		final Path absolute = path.toAbsolutePath();
		final Path parent = absolute.getParent();
		if (parent == null)
		{
			throw new IOException("Metadata path has no parent directory: " + path);
		}
		Files.createDirectories(parent);
		final Path probe = parent.resolve(absolute.getFileName() + ".probe-" + UUID.randomUUID());
		try
		{
			write(probe, channel -> writeFully(channel, java.nio.ByteBuffer.wrap(new byte[] {1})));
		}
		catch (final IOException | RuntimeException | Error failure)
		{
			/* Preserve the capability failure itself. Cleanup is best effort and must
			 * not replace an informative atomic-move/fsync exception with a secondary
			 * delete error. */
			try
			{
				Files.deleteIfExists(probe);
				forceDirectory(parent);
			}
			catch (final IOException | RuntimeException | Error cleanupFailure)
			{
				failure.addSuppressed(cleanupFailure);
			}
			throw failure;
		}
		Files.deleteIfExists(probe);
		forceDirectory(parent);
	}

	/**
	 * Deletes a metadata file and forces the parent directory when the file was
	 * present. This is used for short-lived in-flight recovery records: removing
	 * the record must be durable just like replacing the terminal checkpoint.
	 *
	 * @param path file to remove
	 * @throws IOException if the file or its parent directory cannot be synced
	 */
	public static void delete(final Path path) throws IOException
	{
		final Path absolute = path.toAbsolutePath();
		if (Files.deleteIfExists(absolute))
		{
			forceDirectory(absolute.getParent());
		}
	}

	private static void forceDirectory(final Path parent) throws IOException
	{
		if (parent == null)
		{
			return;
		}
		try (FileChannel channel = FileChannel.open(parent, StandardOpenOption.READ))
		{
			channel.force(true);
		}
		catch (final UnsupportedOperationException failure)
		{
			throw new IOException("Directory fsync is unavailable for replication metadata " + parent, failure);
		}
	}

	private static void writeFully(final FileChannel channel, final java.nio.ByteBuffer buffer) throws IOException
	{
		while (buffer.hasRemaining())
		{
			if (channel.write(buffer) == 0) throw new IOException("Atomic metadata write made no progress");
		}
	}
}
