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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongSupplier;

/** Cached Archive free-space view and writer admission policy. */
final class AeronArchiveCapacity
{
	private static final long CACHE_NANOS = 250_000_000L;

	private final boolean externalArchive;
	private final long minimumFreeBytes;
	private final int segmentFileLength;
	private final LongSupplier usableSpace;
	private volatile long checkedNanos;
	private volatile long cachedUsableSpace = Long.MIN_VALUE;

	AeronArchiveCapacity(final AeronSettings settings)
	{
		this(settings.externalArchive(), settings.minimumArchiveFreeBytes(),
			settings.archiveSegmentFileLength(), () -> queryUsableSpace(settings.archiveDirectory()));
	}

	AeronArchiveCapacity(final boolean externalArchive, final long minimumFreeBytes,
		final int segmentFileLength, final LongSupplier usableSpace)
	{
		if (minimumFreeBytes < 0 || segmentFileLength <= 0)
			throw new IllegalArgumentException("invalid Aeron Archive capacity policy");
		this.externalArchive = externalArchive;
		this.minimumFreeBytes = minimumFreeBytes;
		this.segmentFileLength = segmentFileLength;
		this.usableSpace = usableSpace;
	}

	boolean available()
	{
		return this.minimumFreeBytes == 0 || this.externalArchive || this.usableSpace() >= this.minimumFreeBytes;
	}

	boolean available(final long transactionBytes)
	{
		if (transactionBytes < 0) return false;
		if (this.externalArchive) return true;
		final long reserve = Math.max(transactionBytes, this.segmentFileLength);
		final long required;
		try
		{
			required = Math.addExact(this.minimumFreeBytes, reserve);
		}
		catch (final ArithmeticException ignored)
		{
			return false;
		}
		return this.usableSpace() >= required;
	}

	long usableSpaceBytes()
	{
		return this.externalArchive ? -1L : this.usableSpace();
	}

	void invalidate()
	{
		this.checkedNanos = 0L;
	}

	private long usableSpace()
	{
		final long now = System.nanoTime();
		final long checked = this.checkedNanos;
		if (checked != 0L && now - checked < CACHE_NANOS) return this.cachedUsableSpace;
		final long usable = this.usableSpace.getAsLong();
		this.cachedUsableSpace = usable;
		this.checkedNanos = now;
		return usable;
	}

	private static long queryUsableSpace(final Path archiveDirectory)
	{
		try
		{
			return Files.getFileStore(archiveDirectory).getUsableSpace();
		}
		catch (final IOException failure)
		{
			/* Unknown capacity must not admit a write when a threshold is configured. */
			return -1L;
		}
	}
}
