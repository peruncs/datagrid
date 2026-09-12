package org.eclipse.datagrid.cluster.nodelibrary.aeron.crashtest;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

/** Separate Archive/MediaDriver process used by the external-topology cell. */
public final class ArchiveProcessMain
{
	private ArchiveProcessMain() { }

	public static void main(final String[] args) throws Exception
	{
		final Path base = Path.of(required("dg.archive.base")).toAbsolutePath().normalize();
		final Path control = base.resolve("control");
		Files.createDirectories(control);
		final Path aeronDirectory = base.resolve("archive-aeron");
		final Path archiveDirectory = base.resolve("archive");
		final String controlChannel = required("dg.archive.controlChannel");
		final String replayChannel = required("dg.archive.replayChannel");
		final MediaDriver.Context mediaContext = new MediaDriver.Context()
			.aeronDirectoryName(aeronDirectory.toString())
			.threadingMode(ThreadingMode.SHARED)
			.dirDeleteOnStart(true)
			.dirDeleteOnShutdown(true);
		final Archive.Context archiveContext = new Archive.Context()
			.aeronDirectoryName(aeronDirectory.toString())
			.archiveDir(archiveDirectory.toFile())
			.deleteArchiveOnStart(Boolean.getBoolean("dg.archive.reseed"))
			.threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED)
			.controlChannel(controlChannel)
			.localControlChannel("aeron:ipc")
			.replicationChannel(replayChannel)
			.fileSyncLevel(1)
			.catalogFileSyncLevel(1);
		try (ArchivingMediaDriver ignored = ArchivingMediaDriver.launch(mediaContext, archiveContext))
		{
			atomicWrite(control.resolve("archive-ready"), "ready\n");
			while (!Files.exists(control.resolve("archive-stop")))
			{
				try
				{
					Thread.sleep(10L);
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Archive control loop interrupted", interrupted);
				}
			}
		}
		catch (final Exception failure)
		{
			final IllegalStateException outcomeFailure = new IllegalStateException(
				"RESEED_REQUIRED: Archive catalog is not safely reusable: " + failure.getMessage(), failure);
			try
			{
				atomicWrite(control.resolve("archive-outcome"),
					"OUTCOME=RESEED_REQUIRED\nERROR=" + outcomeFailure.getMessage() + "\n");
			}
			catch (final RuntimeException writeFailure)
			{
				outcomeFailure.addSuppressed(writeFailure);
			}
			throw outcomeFailure;
		}
		catch (final Error failure)
		{
			try
			{
				atomicWrite(control.resolve("archive-outcome"),
					"OUTCOME=FAIL_CLOSED\nERROR=" + failure.getMessage() + "\n");
			}
			catch (final RuntimeException writeFailure)
			{
				failure.addSuppressed(writeFailure);
			}
			throw failure;
		}
	}

	private static void atomicWrite(final Path destination, final String value)
	{
		try
		{
			Files.createDirectories(destination.toAbsolutePath().getParent());
			final Path temporary = Files.createTempFile(destination.getParent(), destination.getFileName() + ".tmp-", null);
			try
			{
				try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE))
				{
					final var bytes = StandardCharsets.UTF_8.encode(value);
					while (bytes.hasRemaining())
					{
						if (channel.write(bytes) == 0) throw new IOException("Archive control write made no progress");
					}
					channel.force(true);
				}
				Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
				try (FileChannel directory = FileChannel.open(destination.getParent(), StandardOpenOption.READ))
				{
					directory.force(true);
				}
			}
			finally
			{
				Files.deleteIfExists(temporary);
			}
		}
		catch (final Exception failure)
		{
			throw new IllegalStateException("cannot write Archive control file " + destination, failure);
		}
	}

	private static String required(final String name)
	{
		final String value = System.getProperty(name);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("missing -D" + name);
		return value;
	}
}
