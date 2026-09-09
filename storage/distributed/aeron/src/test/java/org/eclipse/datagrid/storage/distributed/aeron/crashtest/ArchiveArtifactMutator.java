package org.eclipse.datagrid.storage.distributed.aeron.crashtest;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.regex.Pattern;

/** Version-checked mutations used only by Archive corruption tests. */
public final class ArchiveArtifactMutator
{
	private static final long MAX_MUTATION_BYTES = 128L * 1024L * 1024L;
	private static final Pattern SEGMENT = Pattern.compile("(\\d+)-(\\d+)\\.rec");

	private ArchiveArtifactMutator()
	{
	}

	/** Returns all recording segments in physical order. */
	public static java.util.List<Path> segments(final Path archiveDirectory, final long recordingId)
		throws IOException
	{
		try (var files = Files.list(archiveDirectory))
		{
			final var matches = files.filter(path ->
			{
				final var matcher = SEGMENT.matcher(path.getFileName().toString());
				return matcher.matches() && Long.parseLong(matcher.group(1)) == recordingId;
			}).sorted(Comparator.comparingLong(ArchiveArtifactMutator::segmentBasePosition)).toList();
			if (matches.isEmpty())
			{
				throw new UnsupportedArtifactLayoutException("no recording segments for " + recordingId);
			}
			return matches;
		}
	}

	private static long segmentBasePosition(final Path path)
	{
		final var matcher = SEGMENT.matcher(path.getFileName().toString());
		if (!matcher.matches()) throw new IllegalArgumentException("not an Archive segment: " + path);
		return Long.parseLong(matcher.group(2));
	}

	/** Flips one byte in the first envelope payload and forces the segment. */
	public static void corruptFirstEnvelopePayload(final Path segment) throws IOException
	{
		if (Files.size(segment) > MAX_MUTATION_BYTES)
		{
			throw new UnsupportedArtifactLayoutException("segment exceeds mutation bound: " + segment);
		}
		final byte[] bytes = Files.readAllBytes(segment);
		final int magic = AeronReplicationEnvelope.MAGIC;
		final byte[] magicBytes = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN)
			.putInt(magic).array();
		for (int i = 0; i <= bytes.length - magicBytes.length; i++)
		{
			boolean matches = true;
			for (int j = 0; j < magicBytes.length; j++)
			{
				if (bytes[i + j] != magicBytes[j]) { matches = false; break; }
			}
			if (matches && i + AeronReplicationEnvelope.HEADER_LENGTH < bytes.length &&
				!hasMagic(bytes, i + AeronReplicationEnvelope.HEADER_LENGTH) &&
				ByteBuffer.wrap(bytes, i + Integer.BYTES, Short.BYTES).getShort() == AeronReplicationEnvelope.VERSION &&
				AeronReplicationEnvelope.isPayloadKindCode(
					Byte.toUnsignedInt(bytes[i + Integer.BYTES + Short.BYTES])))
			{
				bytes[i + AeronReplicationEnvelope.HEADER_LENGTH] ^= 0x01;
				try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE))
				{
					final ByteBuffer source = ByteBuffer.wrap(bytes);
					while (source.hasRemaining()) channel.write(source);
					channel.force(true);
				}
				return;
			}
		}
		throw new UnsupportedArtifactLayoutException("no replication envelope found in " + segment);
	}

	private static boolean hasMagic(final byte[] bytes, final int offset)
	{
		if (offset < 0 || offset + Integer.BYTES > bytes.length) return false;
		return ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt() ==
			AeronReplicationEnvelope.MAGIC;
	}

	/**
	 * Truncates the recorded bytes by one byte, leaving the final frame incomplete.
	 * The archive segment is preallocated, therefore truncating the physical file
	 * length alone would only remove unused tail capacity and would not corrupt
	 * the recording.
	 */
	public static void truncateFinalFrame(
		final Path segment,
		final long recordingStartPosition,
		final long recordingStopPosition
	) throws IOException
	{
		final long recordedLength = recordingStopPosition - recordingStartPosition;
		if (recordedLength <= AeronReplicationEnvelope.HEADER_LENGTH)
		{
			throw new UnsupportedArtifactLayoutException("recording is too small to truncate: " + segment);
		}
		final long segmentBase = segmentBasePosition(segment);
		final long physicalEnd = recordingStartPosition - segmentBase + recordedLength;
		if (physicalEnd <= 0)
		{
			throw new UnsupportedArtifactLayoutException("recording start is outside segment: " + segment);
		}
		try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE))
		{
			if (physicalEnd > channel.size())
			{
				throw new UnsupportedArtifactLayoutException(
					"recording positions exceed physical segment size: " + segment);
			}
			final long truncatedSize = physicalEnd - 1L;
			channel.truncate(truncatedSize);
			channel.force(true);
			if (channel.size() != truncatedSize)
			{
				throw new UnsupportedArtifactLayoutException("segment truncation did not change physical length: " + segment);
			}
		}
	}

	/** Truncates the catalog only when it has a recognizable preallocation. */
	public static void truncateCatalog(final Path archiveDirectory) throws IOException
	{
		final Path catalog = archiveDirectory.resolve("archive.catalog");
		final long size = Files.size(catalog);
		if (size < 64L) throw new UnsupportedArtifactLayoutException("catalog is too small: " + catalog);
		try (FileChannel channel = FileChannel.open(catalog, StandardOpenOption.WRITE))
		{
			channel.truncate(64L);
			channel.force(true);
		}
	}

	public static final class UnsupportedArtifactLayoutException extends IOException
	{
		public UnsupportedArtifactLayoutException(final String message)
		{
			super(message);
		}
	}
}
