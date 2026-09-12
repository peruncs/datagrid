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
package org.eclipse.datagrid.cluster.nodelibrary.aeron;

import io.aeron.archive.client.AeronArchive;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationCursor;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationLogRetention;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronAuthenticatedWatermark;
import org.eclipse.datagrid.storage.distributed.types.AtomicFileStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Writer-owned, authenticated Archive retention controller.
 *
 * <p>This component deliberately owns no transport lifecycle. The provider
 * supplies a small writer access view, so retention cannot accidentally close
 * or replace the publication while validating a reader watermark.</p>
 */
final class AeronArchiveRetention implements ReplicationLogRetention
{
	private final byte[] secret;
	private final AeronAuthenticatedWatermark.Quorum quorum;
	private final Runnable ensureWriter;
	private final Supplier<AeronArchive> archive;
	private final LongSupplier recordingId;
	private final Supplier<AeronWriterBoundary> writerBoundary;
	private final UUID clusterId;
	private final UUID storeGeneration;
	private final long writerEpoch;
	private final IntSupplier termLength;
	private final IntSupplier segmentLength;
	private final boolean watermarkDeliveryAvailable;
	private final Path statePath;
	private boolean closed;

	AeronArchiveRetention(
		final byte[] secret,
		final Set<UUID> readers,
		final Runnable ensureWriter,
		final Supplier<AeronArchive> archive,
		final LongSupplier recordingId,
		final Supplier<AeronWriterBoundary> writerBoundary,
		final UUID clusterId,
		final UUID storeGeneration,
		final long writerEpoch,
		final IntSupplier termLength,
		final IntSupplier segmentLength,
		final boolean watermarkDeliveryAvailable,
		final Path statePath
	)
	{
		this.secret = secret.clone();
		this.quorum = new AeronAuthenticatedWatermark.Quorum(readers, this.secret);
		this.ensureWriter = ensureWriter;
		this.archive = archive;
		this.recordingId = recordingId;
		this.writerBoundary = writerBoundary;
		this.clusterId = clusterId;
		this.storeGeneration = storeGeneration;
		this.writerEpoch = writerEpoch;
		this.termLength = termLength;
		this.segmentLength = segmentLength;
		this.watermarkDeliveryAvailable = watermarkDeliveryAvailable;
		this.statePath = statePath;
		this.restoreState();
	}

	@Override
	public synchronized boolean isSupported()
	{
		return !this.closed && this.watermarkDeliveryAvailable && this.quorum.isComplete();
	}

	@Override
	public synchronized void deleteThrough(final ReplicationCursor cursor)
	{
		if (this.closed) throw new IllegalStateException("Aeron retention is closed");
		if (!this.watermarkDeliveryAvailable)
		{
			throw new UnsupportedOperationException(
				"Aeron retention requires a deployed reader-to-writer watermark channel");
		}
		if (cursor == null || !"aeron".equalsIgnoreCase(cursor.transport()))
			throw new IllegalArgumentException("Aeron retention requires an Aeron cursor");
		if (cursor.logicalSequence() < 0)
			throw new IllegalArgumentException("retention cursor must name a resolved sequence");
		try
		{
			final AeronAuthenticatedWatermark quorumWatermark = this.quorum.aggregate();
			final AeronAuthenticatedWatermark requested = this.requestedWatermark(cursor);
			if (quorumWatermark.sequence() < requested.sequence() ||
				quorumWatermark.sequence() == requested.sequence() &&
				quorumWatermark.position() < requested.position())
				throw new IllegalStateException("Aeron reader quorum has not reached the requested sequence");
			final long targetPosition = Math.min(quorumWatermark.position(), requested.position());
			this.ensureWriter.run();
			final AeronWriterBoundary terminal = this.writerBoundary.get();
			if (terminal == null || terminal.sequence() < 0 || terminal.position() < 0 ||
				requested.sequence() > terminal.sequence() || targetPosition > terminal.position())
			{
				throw new IllegalStateException("reader watermark is ahead of the durable writer boundary");
			}
			final long activeRecordingId = this.recordingId.getAsLong();
			if (requested.recordingId() != activeRecordingId || quorumWatermark.recordingId() != activeRecordingId)
				throw new IllegalArgumentException("retention watermark recording does not match the active writer");
			final AeronArchive archive = this.archive.get();
			if (archive == null) throw new IllegalStateException("Aeron Archive is not running");
			final long start = archive.getStartPosition(activeRecordingId);
			/* Aeron explicitly supports purging completed segments from an active
			 * recording; getStopPosition() is NULL_POSITION until recording stops.  The
			 * durable writer boundary is the safety proof for the requested position, so
			 * use the current recorded position for an active recording and never purge
			 * the segment that is still being appended. */
			final long stop = archive.getStopPosition(activeRecordingId);
			final long recorded = stop < 0 ? archive.getRecordingPosition(activeRecordingId) : stop;
			if (recorded < 0) throw new IllegalStateException("Aeron recording has no durable position");
			final long boundary = AeronArchive.segmentFileBasePosition(start, targetPosition,
				this.termLength.getAsInt(), this.segmentLength.getAsInt());
			if (boundary <= start || boundary > targetPosition || boundary > recorded)
				throw new IllegalArgumentException("retention watermark does not cover a complete Archive segment");
			archive.purgeSegments(activeRecordingId, boundary);
		}
		catch (final SecurityException | IllegalArgumentException | IllegalStateException failure)
		{
			throw failure;
		}
		catch (final RuntimeException failure)
		{
			throw new IllegalStateException("Aeron Archive retention failed closed", failure);
		}
	}

	@Override
	public synchronized void recordReaderWatermark(final ReplicationCursor cursor)
	{
		if (this.closed) throw new IllegalStateException("Aeron retention is closed");
		if (!this.watermarkDeliveryAvailable)
		{
			throw new UnsupportedOperationException(
				"Aeron retention watermark delivery is not configured");
		}
		if (cursor == null || !"aeron".equalsIgnoreCase(cursor.transport()))
			throw new IllegalArgumentException("Aeron retention requires an Aeron cursor");
		if (cursor.logicalSequence() < 0)
			throw new IllegalArgumentException("reader watermark must name a resolved sequence");
		final AeronAuthenticatedWatermark watermark;
		try
		{
			watermark = AeronAuthenticatedWatermark.decode(cursor.providerPosition());
		}
		catch (final RuntimeException failure)
		{
			throw new IllegalArgumentException("reader cursor has no authenticated Aeron watermark", failure);
		}
		if (!this.quorum.acceptsReader(watermark.readerId()) || !watermark.verify(this.secret) ||
			watermark.position() < 0 || watermark.sequence() != cursor.logicalSequence() ||
			!this.storeGeneration.equals(cursor.storeGeneration()) ||
			!this.matchesWriter(watermark) ||
			(this.recordingId.getAsLong() >= 0 && watermark.recordingId() != this.recordingId.getAsLong()))
		{
			throw new SecurityException("reader watermark identity or authentication is invalid");
		}
		/* Resolve a lazily created recording only after authenticating the token.
		 * This prevents an unauthenticated caller from forcing writer startup while
		 * still ensuring that a valid token is checked against the actual recording. */
		this.ensureWriter.run();
		if (watermark.recordingId() != this.recordingId.getAsLong())
		{
			throw new SecurityException("reader watermark recording does not match the active writer");
		}
		final AeronWriterBoundary terminal = this.writerBoundary.get();
		if (terminal == null || terminal.sequence() < watermark.sequence() ||
			terminal.sequence() == watermark.sequence() && terminal.position() < watermark.position())
		{
			throw new IllegalStateException("reader watermark is ahead of the durable writer boundary");
		}
		final AeronAuthenticatedWatermark previous = this.quorum.latest(watermark.readerId());
		this.quorum.accept(watermark);
		try
		{
			this.persistState();
		}
		catch (final RuntimeException failure)
		{
			/* The in-memory quorum must never advance beyond the durable quorum file.
			 * Otherwise a failed write could authorize deletion that disappears on
			 * restart. Restore the prior acknowledgement before propagating failure. */
			this.quorum.restore(watermark.readerId(), previous);
			throw failure;
		}
	}

	@Override
	public synchronized void close()
	{
		this.closed = true;
	}

	private AeronAuthenticatedWatermark requestedWatermark(final ReplicationCursor cursor)
	{
		try
		{
			final AeronAuthenticatedWatermark requested = AeronAuthenticatedWatermark.decode(cursor.providerPosition());
			if (!this.quorum.acceptsReader(requested.readerId()) || !requested.verify(this.secret) ||
				requested.sequence() != cursor.logicalSequence() || requested.position() < 0 ||
				!this.storeGeneration.equals(cursor.storeGeneration()) ||
				!this.matchesWriter(requested))
			{
				throw new SecurityException("retention cursor watermark identity or authentication is invalid");
			}
			return requested;
		}
		catch (final SecurityException failure)
		{
			throw failure;
		}
		catch (final RuntimeException failure)
		{
			throw new IllegalArgumentException("retention cursor must carry an authenticated Aeron watermark", failure);
		}
	}

	private void restoreState()
	{
		if (this.statePath == null || !Files.exists(this.statePath)) return;
		try
		{
			final long size = Files.size(this.statePath);
			if (size > 1_048_576) throw new IOException("retention state is too large");
			final byte[] encoded = Files.readAllBytes(this.statePath);
			if (encoded.length != size) throw new IOException("retention state changed while reading");
			final ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
			if (buffer.remaining() < Integer.BYTES * 2 || buffer.getInt() != 1)
				throw new IOException("unsupported retention state version");
			final int count = buffer.getInt();
			if (count < 0 || count > 1024) throw new IOException("invalid retention state count");
			for (int i = 0; i < count; i++)
			{
				if (buffer.remaining() < Integer.BYTES) throw new IOException("truncated retention state");
				final int length = buffer.getInt();
				if (length <= 0 || length > buffer.remaining()) throw new IOException("invalid retention token length");
				final byte[] token = new byte[length];
				buffer.get(token);
				final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.decode(token);
					if (!this.quorum.acceptsReader(watermark.readerId()) || !watermark.verify(this.secret) ||
						!this.matchesWriter(watermark))
				{
					throw new SecurityException("retention state belongs to another Aeron writer");
				}
				this.quorum.accept(watermark);
			}
			if (buffer.hasRemaining()) throw new IOException("trailing retention state bytes");
		}
		catch (final IOException | RuntimeException failure)
		{
			throw new IllegalStateException("cannot load authenticated Aeron retention state " + this.statePath, failure);
		}
	}

	private boolean matchesWriter(final AeronAuthenticatedWatermark watermark)
	{
		return watermark.clusterId().equals(this.clusterId) &&
			watermark.storeGeneration().equals(this.storeGeneration) &&
			watermark.writerEpoch() == this.writerEpoch;
	}

	private void persistState()
	{
		if (this.statePath == null) return;
		try
		{
			final var snapshot = this.quorum.snapshot().entrySet().stream()
				.sorted(java.util.Map.Entry.comparingByKey()).map(java.util.Map.Entry::getValue).toList();
			int length = Integer.BYTES * 2;
			for (final AeronAuthenticatedWatermark watermark : snapshot) length += Integer.BYTES + watermark.encode().length;
			final ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN).putInt(1).putInt(snapshot.size());
			for (final AeronAuthenticatedWatermark watermark : snapshot)
			{
				final byte[] token = watermark.encode();
				buffer.putInt(token.length).put(token);
			}
			buffer.flip();
			AtomicFileStore.write(this.statePath, channel ->
			{
				final ByteBuffer source = buffer.duplicate();
				while (source.hasRemaining())
				{
					if (channel.write(source) == 0) throw new IOException("Archive retention state write made no progress");
				}
			});
		}
		catch (final IOException failure)
		{
			throw new IllegalStateException("cannot persist authenticated Aeron retention state " + this.statePath, failure);
		}
	}
}
