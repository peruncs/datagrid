package org.eclipse.datagrid.cluster.nodelibrary.aeron;

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

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.agrona.ErrorHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Owns one node's MediaDriver, Aeron client, and Archive client lifecycle. */
final class AeronRuntime implements AutoCloseable
{
	private static final long STALE_DRIVER_RETRY_DELAY_MILLIS = 100L;

	private final AeronSettings settings;
	private final ErrorHandler errorHandler;
	private AutoCloseable driver;
	private Aeron aeron;
	private AeronArchive archive;

	private AeronRuntime(final AeronSettings settings, final ErrorHandler errorHandler)
	{
		this.settings = settings;
		this.errorHandler = errorHandler;
	}

	static AeronRuntime start(final AeronSettings settings, final ErrorHandler errorHandler,
		final Runnable beforeDriverLaunch)
	{
		final AeronRuntime runtime = new AeronRuntime(settings, errorHandler);
		try
		{
			runtime.start(beforeDriverLaunch);
			return runtime;
		}
		catch (final RuntimeException | Error failure)
		{
			final RuntimeException closeFailure = runtime.closeAllQuietly();
			if (closeFailure != null) failure.addSuppressed(closeFailure);
			throw failure;
		}
	}

	private void start(final Runnable beforeDriverLaunch)
	{
		ensurePrivateDirectory(this.settings.aeronDirectory());
		final Path checkpointParent = this.settings.checkpointPath().toAbsolutePath().getParent();
		if (checkpointParent == null) throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
		ensurePrivateDirectory(checkpointParent);
		final boolean embeddedWriter = "writer".equals(this.settings.role()) && !this.settings.externalArchive();
		if (embeddedWriter) ensurePrivateDirectory(this.settings.archiveDirectory());
		final MediaDriver.Context media = new MediaDriver.Context()
			.aeronDirectoryName(this.settings.aeronDirectory().toString())
			.driverTimeoutMs(this.settings.driverTimeoutMillis())
			.threadingMode(this.settings.threadingMode())
			.mtuLength(this.settings.replication().mtuLength())
			.publicationTermBufferLength(this.settings.replication().termLength())
			.spiesSimulateConnection(embeddedWriter && !explicitlyDisablesSpySimulation(this.settings.liveChannel()))
			.errorHandler(this.errorHandler)
			.dirDeleteOnStart(false)
			.dirDeleteOnShutdown(false);
		beforeDriverLaunch.run();
		if (embeddedWriter)
		{
			final Archive.Context archiveContext = new Archive.Context()
				.aeronDirectoryName(this.settings.aeronDirectory().toString())
				.archiveDir(this.settings.archiveDirectory().toFile())
				.deleteArchiveOnStart(false)
				.threadingMode(this.settings.archiveThreadingMode())
				.controlChannel(this.settings.controlChannel())
				.localControlChannel("aeron:ipc")
				.replicationChannel(this.settings.archiveReplicationChannel())
				.segmentFileLength(this.settings.archiveSegmentFileLength())
				.lowStorageSpaceThreshold(this.settings.archiveLowStorageSpaceThreshold())
				.maxConcurrentReplays(this.settings.maxConcurrentReplays())
				.errorHandler(this.errorHandler)
				.fileSyncLevel(this.settings.archiveFileSyncLevel())
				.catalogFileSyncLevel(this.settings.archiveFileSyncLevel());
			this.driver = launchDriver(media,
				() -> ArchivingMediaDriver.launch(media.clone(), archiveContext.clone()));
		}
		else
		{
			this.driver = launchDriver(media, () -> MediaDriver.launch(media.clone()));
		}
		this.aeron = Aeron.connect(new Aeron.Context()
			.aeronDirectoryName(this.settings.aeronDirectory().toString())
			.driverTimeoutMs(this.settings.driverTimeoutMillis())
			.errorHandler(this.errorHandler)
			.subscriberErrorHandler(this.errorHandler));
		this.archive = AeronArchive.connect(this.archiveContext());
	}

	Aeron aeron() { return this.aeron; }
	AeronArchive archive() { return this.archive; }

	AeronArchive.Context archiveContext()
	{
		return new AeronArchive.Context()
			.aeron(this.aeron)
			.aeronDirectoryName(this.settings.aeronDirectory().toString())
			.controlRequestChannel(this.settings.controlChannel())
			.controlResponseChannel(this.settings.controlResponseChannel())
			.errorHandler(this.errorHandler)
			.messageTimeoutNs(this.settings.replication().offerTimeoutNanos());
	}

	void stopDriver()
	{
		if (this.driver == null) throw new IllegalStateException("Aeron driver is not running");
		try
		{
			this.driver.close();
			this.driver = null;
		}
		catch (final Exception failure)
		{
			throw new IllegalStateException("failed to stop Aeron driver", failure);
		}
	}

	@Override
	public void close()
	{
		final RuntimeException failure = this.closeAllQuietly();
		if (failure != null) throw failure;
	}

	/** Exhaustively releases every owned resource and aggregates close failures. */
	private RuntimeException closeAllQuietly()
	{
		RuntimeException failure = null;
		if (this.archive != null)
		{
			try { this.archive.close(); }
			catch (final RuntimeException closeFailure) { failure = closeFailure; }
			finally { this.archive = null; }
		}
		if (this.aeron != null)
		{
			try { this.aeron.close(); }
			catch (final RuntimeException closeFailure) { failure = append(failure, closeFailure); }
			finally { this.aeron = null; }
		}
		if (this.driver != null)
		{
			try { this.driver.close(); }
			catch (final Exception closeFailure)
			{
				failure = append(failure, new IllegalStateException("failed to close Aeron driver", closeFailure));
			}
			finally { this.driver = null; }
		}
		return failure;
	}

	private static RuntimeException append(final RuntimeException current, final RuntimeException additional)
	{
		if (current == null) return additional;
		if (current != additional) current.addSuppressed(additional);
		return current;
	}

	private static <T extends AutoCloseable> T launchDriver(final MediaDriver.Context context,
		final Supplier<T> launcher)
	{
		RuntimeException lastFailure = null;
		final long timeoutMillis = Math.max(3_000L, context.driverTimeoutMs() + 1_000L);
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (System.nanoTime() < deadline)
		{
			try { return launcher.get(); }
			catch (final ActiveDriverException failure) { lastFailure = failure; }
			try { Thread.sleep(STALE_DRIVER_RETRY_DELAY_MILLIS); }
			catch (final InterruptedException interrupted)
			{
				Thread.currentThread().interrupt();
				throw new IllegalStateException("interrupted while waiting for stale Aeron driver cleanup", interrupted);
			}
		}
		throw new IllegalStateException(
			"Aeron driver directory remained active after " + timeoutMillis + " milliseconds", lastFailure);
	}

	static void ensurePrivateDirectory(final Path path)
	{
		try
		{
			if (Files.isSymbolicLink(path)) throw new IOException("symbolic-link directory is not allowed: " + path);
			try
			{
				Files.createDirectories(path,
					PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
			}
			catch (final UnsupportedOperationException ignored) { Files.createDirectories(path); }
			if (!Files.isDirectory(path)) throw new IOException("path is not a directory");
			try { Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------")); }
			catch (final UnsupportedOperationException ignored) { /* Non-POSIX filesystem. */ }
		}
		catch (final IOException failure)
		{
			throw new IllegalStateException("cannot create or protect Aeron directory " + path, failure);
		}
	}

	private static boolean explicitlyDisablesSpySimulation(final String channel)
	{
		final int query = channel.indexOf('?');
		if (query < 0) return false;
		for (final String option : channel.substring(query + 1).split("\\|"))
		{
			if (option.trim().equalsIgnoreCase("ssc=false")) return true;
		}
		return false;
	}
}
