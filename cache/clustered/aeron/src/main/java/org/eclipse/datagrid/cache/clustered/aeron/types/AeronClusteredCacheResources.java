package org.eclipse.datagrid.cache.clustered.aeron.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Aeron
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
import io.aeron.ConcurrentPublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Objects;

/**
 * Owns the Aeron client, optional embedded driver, publication, and
 * subscription shared by one clustered-cache provider.
 *
 * <p>All access is synchronized. The client and driver connect lazily on first
 * use, so a provider can be constructed from configuration but only pays for
 * Aeron when a cache sender or receiver is actually requested. When an
 * embedded driver has no configured directory it creates its own private,
 * auto-generated directory, and the Aeron client follows
 * {@link MediaDriver#aeronDirectoryName()}; when a directory is configured it
 * must be exclusive to this provider, because sharing a driver directory
 * corrupts the driver.</p>
 *
 * <p>The resources are single-lifecycle: after both the publication and the
 * subscription are closed, the shared client is released and the resources
 * cannot be used again. Create a new provider for a new lifecycle.</p>
 */
final class AeronClusteredCacheResources implements AutoCloseable
{
	private static final Logger logger = LoggerFactory.getLogger(AeronClusteredCacheResources.class);
	private static final ErrorHandler ERROR_HANDLER =
		failure -> logger.error("Aeron clustered-cache transport error", failure);

	private final String aeronDirectory;
	private final String channel;
	private final int streamId;
	private final long driverTimeoutMillis;
	private final boolean embeddedDriver;

	private String resolvedAeronDirectory;
	private Aeron aeron;
	private MediaDriver mediaDriver;
	private ConcurrentPublication publication;
	private Subscription subscription;
	private boolean closed;

	/**
	 * Creates the resource owner with no connection yet.
	 *
	 * @param aeronDirectory Aeron driver directory, or {@code null} to use Aeron's directory
	 * @param channel Aeron channel shared by all participants
	 * @param streamId Aeron stream id shared by all participants
	 * @param driverTimeoutMillis maximum time the Aeron client waits for a driver
	 * @param embeddedDriver whether to launch a private embedded MediaDriver for this provider
	 */
	AeronClusteredCacheResources(
		final String aeronDirectory,
		final String channel,
		final int streamId,
		final long driverTimeoutMillis,
		final boolean embeddedDriver
	)
	{
		this.aeronDirectory = aeronDirectory;
		this.channel = channel;
		this.streamId = streamId;
		this.driverTimeoutMillis = driverTimeoutMillis;
		this.embeddedDriver = embeddedDriver;
	}

	/** Returns whether this owner was created with the given configuration. */
	boolean matches(final String channel, final int streamId, final String directory,
		final long driverTimeoutMillis, final boolean embeddedDriver)
	{
		return this.channel.equals(channel) && this.streamId == streamId
			&& Objects.equals(this.aeronDirectory, directory)
			&& this.driverTimeoutMillis == driverTimeoutMillis && this.embeddedDriver == embeddedDriver;
	}

	/** Returns the bound configuration for diagnostics. */
	String describe()
	{
		return "channel=" + this.channel + ", streamId=" + this.streamId +
			", directory=" + this.aeronDirectory + ", driverTimeoutMillis=" + this.driverTimeoutMillis +
			", embeddedDriver=" + this.embeddedDriver;
	}

	/**
	 * Returns the shared publication, connecting Aeron if needed.
	 *
	 * @return the shared publication
	 */
	synchronized ConcurrentPublication publication()
	{
		this.ensureConnected();
		if (this.publication == null)
		{
			this.publication = this.aeron.addPublication(this.channel, this.streamId);
		}
		return this.publication;
	}

	/**
	 * Returns the shared subscription, connecting Aeron if needed.
	 *
	 * @return the shared subscription
	 */
	synchronized Subscription subscription()
	{
		this.ensureConnected();
		if (this.subscription == null)
		{
			this.subscription = this.aeron.addSubscription(this.channel, this.streamId);
		}
		return this.subscription;
	}

	/** Closes the publication; releases Aeron when no other resource remains open. */
	synchronized void closePublication()
	{
		final ConcurrentPublication current = this.publication;
		this.publication = null;
		this.closeResource(current == null ? null : current::close);
	}

	/** Closes the subscription; releases Aeron when no other resource remains open. */
	synchronized void closeSubscription()
	{
		final Subscription current = this.subscription;
		this.subscription = null;
		this.closeResource(current == null ? null : current::close);
	}

	private void closeResource(final Runnable closeResource)
	{
		Throwable failure = null;
		if (closeResource != null)
		{
			try
			{
				closeResource.run();
			}
			catch (final Throwable closeFailure)
			{
				failure = closeFailure;
			}
		}
		try
		{
			this.closeIfUnused();
		}
		catch (final Throwable closeFailure)
		{
			failure = append(failure, closeFailure);
		}
		if (failure != null)
		{
			throw rethrowAsRuntime(failure);
		}
	}

	private void ensureConnected()
	{
		if (this.closed)
		{
			throw new IllegalStateException("Aeron clustered-cache resources are closed");
		}
		if (this.aeron != null)
		{
			return;
		}
		if (this.embeddedDriver && this.mediaDriver == null)
		{
			this.ensureDirectory();
			final MediaDriver.Context context = new MediaDriver.Context()
				.dirDeleteOnStart(false)
				.dirDeleteOnShutdown(true)
				.threadingMode(ThreadingMode.SHARED)
				.errorHandler(ERROR_HANDLER);
			if (this.aeronDirectory != null)
			{
				context.aeronDirectoryName(this.aeronDirectory);
			}
			this.mediaDriver = MediaDriver.launchEmbedded(context);
			this.resolvedAeronDirectory = this.mediaDriver.aeronDirectoryName();
			this.hardenDirectory(this.resolvedAeronDirectory);
		}
		final Aeron.Context context = new Aeron.Context()
			.driverTimeoutMs(this.driverTimeoutMillis)
			.errorHandler(ERROR_HANDLER)
			.subscriberErrorHandler(ERROR_HANDLER);
		final String directory =
			this.resolvedAeronDirectory != null ? this.resolvedAeronDirectory : this.aeronDirectory;
		if (directory != null)
		{
			context.aeronDirectoryName(directory);
		}
		this.aeron = Aeron.connect(context);
	}

	private void closeIfUnused()
	{
		if (this.publication == null && this.subscription == null)
		{
			this.close();
		}
	}

	/**
	 * Releases the Aeron client and any embedded driver, aggregating close
	 * failures. Idempotent; the resources are terminal after this call.
	 */
	@Override
	public synchronized void close()
	{
		if (this.closed)
		{
			return;
		}
		this.closed = true;
		final Aeron currentAeron = this.aeron;
		this.aeron = null;
		final MediaDriver currentDriver = this.mediaDriver;
		this.mediaDriver = null;
		this.resolvedAeronDirectory = null;

		Throwable failure = null;
		if (currentAeron != null)
		{
			try
			{
				currentAeron.close();
			}
			catch (final Throwable closeFailure)
			{
				failure = closeFailure;
			}
		}
		if (currentDriver != null)
		{
			try
			{
				currentDriver.close();
			}
			catch (final Throwable closeFailure)
			{
				failure = append(failure, closeFailure);
			}
		}
		if (failure != null)
		{
			throw rethrowAsRuntime(failure);
		}
	}

	/** Aggregates a close failure with an earlier one. */
	private static Throwable append(final Throwable current, final Throwable additional)
	{
		if (current == null)
		{
			return additional;
		}
		if (current != additional)
		{
			current.addSuppressed(additional);
		}
		return current;
	}

	/** Converts an aggregated close failure into a throwable runtime failure. */
	private static RuntimeException rethrowAsRuntime(final Throwable failure)
	{
		if (failure instanceof final Error error)
		{
			throw error;
		}
		if (failure instanceof final RuntimeException runtime)
		{
			return runtime;
		}
		return new IllegalStateException("Aeron resource close failed", failure);
	}

	/** Creates a private driver directory with owner-only permissions. */
	private void ensureDirectory()
	{
		if (this.aeronDirectory == null)
		{
			return;
		}
		final Path path = Paths.get(this.aeronDirectory);
		try
		{
			try
			{
				Files.createDirectories(path,
					PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
			}
			catch (final UnsupportedOperationException ignored)
			{
				Files.createDirectories(path);
			}
			try
			{
				Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
			}
			catch (final UnsupportedOperationException ignored)
			{
				/* Non-POSIX filesystem. */
			}
		}
		catch (final IOException failure)
		{
			throw new IllegalStateException("cannot create or protect Aeron driver directory " + path, failure);
		}
	}

	/** Restricts a driver directory to the owner, including a generated one. */
	private void hardenDirectory(final String directory)
	{
		try
		{
			Files.setPosixFilePermissions(Paths.get(directory), PosixFilePermissions.fromString("rwx------"));
		}
		catch (final UnsupportedOperationException ignored)
		{
			/* Non-POSIX filesystem. */
		}
		catch (final IOException failure)
		{
			throw new IllegalStateException("cannot protect Aeron driver directory " + directory, failure);
		}
	}

}
