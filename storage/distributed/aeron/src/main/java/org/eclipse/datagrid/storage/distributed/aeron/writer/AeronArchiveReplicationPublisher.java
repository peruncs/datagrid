package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.concurrent.status.CountersReader;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;

import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

/** Creates a recorded exclusive publication without changing the DataGrid API. */
public final class AeronArchiveReplicationPublisher implements AutoCloseable
{
	@FunctionalInterface
	public interface CheckpointWriter
	{
		/** Receives a writer transition and its transaction metadata. */
		void onState(AeronReplicationCheckpoint.State state, long sequence, int dataLength,
			int dataChunkCount, int dataCrc32c, long position);
	}

	private final AeronArchive archive;
	private final ExclusivePublication publication;
	private final AeronReplicationPublisher publisher;
	private final long recordingIdHint;
	private final AeronReplicationConfiguration configuration;
	private volatile int recordingCounterId = -1;
	private boolean closed;

	private AeronArchiveReplicationPublisher(
		final AeronArchive archive,
		final ExclusivePublication publication,
		final AeronReplicationPublisher publisher,
		final long recordingIdHint,
		final AeronReplicationConfiguration configuration
	)
	{
		this.archive = archive;
		this.publication = publication;
		this.publisher = publisher;
		this.recordingIdHint = recordingIdHint;
		this.configuration = configuration;
	}

	public static AeronArchiveReplicationPublisher New(
		final AeronArchive archive,
		final String channel,
		final int streamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence
	)
	{
		final ExclusivePublication publication = archive.addRecordedExclusivePublication(channel, streamId);
		try
		{
			awaitRecordingStarted(archive, publication, Aeron.NULL_VALUE, configuration);
			final long recordingId = findRecordingId(archive, publication);
			return new AeronArchiveReplicationPublisher(
				archive,
				publication,
				new AeronReplicationPublisher(
					publication, configuration, clusterId, epoch, initialSequence, true,
					position -> awaitRecorded(archive, publication, Aeron.NULL_VALUE, configuration, position)
				),
				recordingId,
				configuration
			);
		}
		catch (final RuntimeException | Error failure)
		{
			tryStopRecording(archive, publication, Aeron.NULL_VALUE);
			try
			{
				publication.close();
			}
			catch (final RuntimeException closeFailure)
			{
				failure.addSuppressed(closeFailure);
			}
			throw failure;
		}
	}

	/**
	 * Reopens a stopped recording at its next archive position.
	 *
	 * <p>The recording must belong to {@code streamId} and use the same term and
	 * MTU framing as {@code configuration}. The exact initial-position URI is
	 * passed to both the publication and the Archive extension request so the
	 * recorded stream cannot be rebound with different framing.</p>
	 */
	public static AeronArchiveReplicationPublisher Extend(
		final AeronArchive archive,
		final long recordingId,
		final int streamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence
	)
	{
		final String[] channel = new String[1];
		final long[] position = new long[1];
		final int[] initialTermId = new int[1];
		final int[] termLength = new int[1];
		final int[] mtuLength = new int[1];
		final int[] recordedStream = new int[1];
		if (archive.listRecording(recordingId, (a, b, id, c, d, start, stop, termId, segment, term, mtu, session,
			stream, stripped, original, source) ->
		{
			channel[0] = stripped;
			position[0] = stop;
			initialTermId[0] = termId;
			termLength[0] = term;
			mtuLength[0] = mtu;
			recordedStream[0] = stream;
		}) == 0 || channel[0] == null)
		{
			throw new IllegalArgumentException("Unknown Aeron recording: " + recordingId);
		}
		if (position[0] < 0)
		{
			throw new IllegalStateException("Aeron recording is still active: " + recordingId);
		}
		if (recordedStream[0] != streamId)
		{
			throw new IllegalArgumentException("Aeron recording stream does not match replication configuration");
		}
		if (termLength[0] != configuration.termLength() || mtuLength[0] != configuration.mtuLength())
		{
			throw new IllegalArgumentException("Aeron recording framing does not match replication configuration");
		}
		final String baseChannel = new ChannelUriStringBuilder(channel[0]).sessionId((Integer)null).build();
		final String extendedChannel = new ChannelUriStringBuilder(baseChannel)
			.initialPosition(position[0], initialTermId[0], termLength[0]).build();
		final ExclusivePublication publication = archive.context().aeron().addExclusivePublication(extendedChannel, streamId);
		try
		{
			archive.extendRecording(recordingId, extendedChannel, streamId, SourceLocation.LOCAL);
			awaitRecordingStarted(archive, publication, recordingId, configuration);
			return new AeronArchiveReplicationPublisher(archive, publication,
				new AeronReplicationPublisher(publication, configuration, clusterId, epoch, initialSequence, true,
					positionValue -> awaitRecorded(archive, publication, recordingId, configuration, positionValue)), recordingId,
				configuration);
		}
		catch (final RuntimeException | Error failure)
		{
			tryStopRecording(archive, publication, recordingId);
			try
			{
				publication.close();
			}
			catch (final RuntimeException closeFailure)
			{
				failure.addSuppressed(closeFailure);
			}
			throw failure;
		}
	}

	public synchronized long publishTransaction(final byte[] dictionary, final java.nio.ByteBuffer[] data)
	{
		if (this.closed) throw new IllegalStateException("Aeron archive publisher is closed");
		return this.publisher.publishTransaction(dictionary, data);
	}

	public long recordingId()
	{
		final CountersReader counters = this.archive.context().aeron().countersReader();
		final int counterId = this.recordingCounterId(counters);
		return counterId < 0 ? Aeron.NULL_VALUE : RecordingPos.getRecordingId(counters, counterId);
	}

	private int recordingCounterId(final CountersReader counters)
	{
		final int cached = this.recordingCounterId;
		if (cached >= 0 && cached <= counters.maxCounterId())
		{
			final long cachedRecordingId = RecordingPos.getRecordingId(counters, cached);
			if (cachedRecordingId >= 0 && (this.recordingIdHint < 0 || cachedRecordingId == this.recordingIdHint))
			{
				return cached;
			}
		}
		int counterId = RecordingPos.findCounterIdBySession(counters, this.publication.sessionId(), this.archive.archiveId());
		if (counterId < 0 && this.recordingIdHint >= 0)
		{
			counterId = RecordingPos.findCounterIdByRecording(counters, this.recordingIdHint, this.archive.archiveId());
		}
		this.recordingCounterId = counterId;
		return counterId;
	}

	private static long findRecordingId(final AeronArchive archive, final ExclusivePublication publication)
	{
		final CountersReader counters = archive.context().aeron().countersReader();
		final int counterId = RecordingPos.findCounterIdBySession(
			counters, publication.sessionId(), archive.archiveId());
		return counterId < 0 ? Aeron.NULL_VALUE : RecordingPos.getRecordingId(counters, counterId);
	}

	/** Returns the archive's recorded position, or {@link Aeron#NULL_VALUE} when unavailable. */
	public long recordedPosition()
	{
		final CountersReader counters = this.archive.context().aeron().countersReader();
		final int counterId = this.recordingCounterId(counters);
		return counterId < 0 ? Aeron.NULL_VALUE : counters.getCounterValue(counterId);
	}

	private static long awaitRecorded(
		final AeronArchive archive,
		final ExclusivePublication publication,
		final long recordingIdHint,
		final AeronReplicationConfiguration configuration,
		final long commitPosition
	)
	{
		final CountersReader counters = archive.context().aeron().countersReader();
		final long deadline = System.nanoTime() + configuration.offerTimeoutNanos();
		long lastRecordedPosition = Aeron.NULL_VALUE;
		boolean lastActive = false;
		while (true)
		{
			final String archiveError = archive.pollForErrorResponse();
			if (archiveError != null) throw new IllegalStateException("Aeron archive error: " + archiveError);
			if (recordingIdHint >= 0)
			{
				final long archivePosition = archive.getRecordingPosition(recordingIdHint);
				if (archivePosition >= commitPosition)
				{
					return archivePosition;
				}
			}
			int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
			if (counterId < 0 && recordingIdHint >= 0)
			{
				counterId = RecordingPos.findCounterIdByRecording(counters, recordingIdHint, archive.archiveId());
			}
			if (counterId >= 0)
			{
				final long recordingId = RecordingPos.getRecordingId(counters, counterId);
				final long recordedPosition = counters.getCounterValue(counterId);
				lastRecordedPosition = recordedPosition;
				lastActive = RecordingPos.isActive(counters, counterId, recordingId);
				if (recordedPosition >= commitPosition)
				{
					return recordedPosition;
				}
				if (!lastActive)
				{
					throw new IllegalStateException("Aeron archive recording stopped before commit position " + commitPosition);
				}
			}
			if (System.nanoTime() >= deadline)
			{
				throw new IllegalStateException("Aeron archive did not record commit position " + commitPosition +
					" (publicationPosition=" + publication.position() + ", recordedPosition=" + lastRecordedPosition +
					", active=" + lastActive + ", recordingId=" + recordingIdHint + ")");
			}
			LockSupport.parkNanos(Math.min(1_000_000L, Math.max(1L, deadline - System.nanoTime())));
		}
	}

	private static void awaitRecordingStarted(
		final AeronArchive archive,
		final ExclusivePublication publication,
		final long recordingIdHint,
		final AeronReplicationConfiguration configuration
	)
	{
		final CountersReader counters = archive.context().aeron().countersReader();
		final long deadline = System.nanoTime() + configuration.offerTimeoutNanos();
		while (true)
		{
			final String archiveError = archive.pollForErrorResponse();
			if (archiveError != null) throw new IllegalStateException("Aeron archive error: " + archiveError);
			int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
			if (counterId < 0 && recordingIdHint >= 0)
			{
				counterId = RecordingPos.findCounterIdByRecording(counters, recordingIdHint, archive.archiveId());
			}
			if (counterId >= 0)
			{
				final long recordingId = RecordingPos.getRecordingId(counters, counterId);
				if (RecordingPos.isActive(counters, counterId, recordingId)) return;
			}
			if (System.nanoTime() >= deadline)
			{
				throw new IllegalStateException("Aeron archive recording did not start before timeout (recordingId=" +
					recordingIdHint + ", session=" + publication.sessionId() + ", channel=" + publication.channel() + ")");
			}
			LockSupport.parkNanos(Math.min(1_000_000L, Math.max(1L, deadline - System.nanoTime())));
		}
	}

	synchronized boolean recordingIsActive()
	{
		final CountersReader counters = this.archive.context().aeron().countersReader();
		final int counterId = this.recordingCounterId(counters);
		return counterId >= 0 && RecordingPos.isActive(counters, counterId, RecordingPos.getRecordingId(counters, counterId));
	}

	ExclusivePublication publication()
	{
		return this.publication;
	}

	/** Creates the package-owned write coordinator for this recorded publisher. */
	public AeronReplicationWriteCoordinator newWriteCoordinator(
		final ReplicationDurabilityMode durabilityMode,
		final CheckpointWriter writer)
	{
		if (writer == null) throw new NullPointerException("writer");
		return new AeronReplicationWriteCoordinator(this.publisher, durabilityMode, writer::onState);
	}

	/** Aligns the publisher with a recovered DataGrid sequence before the next write. */
	public void synchronizeNextSequence(final long nextSequence)
	{
		this.publisher.synchronizeNextSequence(nextSequence);
	}

	@Override
	public void close()
	{
		synchronized (this)
		{
			if (this.closed) return;
			this.closed = true;
		}
		RuntimeException failure = null;
		long recordingId = Aeron.NULL_VALUE;
		try
		{
			recordingId = this.recordingId();
			if (recordingId < 0 && this.recordingIdHint >= 0)
			{
				recordingId = this.recordingIdHint;
			}
		}
		catch (final RuntimeException recordingFailure)
		{
			failure = recordingFailure;
		}
		if (recordingId >= 0)
		{
			try
			{
				this.archive.tryStopRecording(recordingId);
			}
			catch (final RuntimeException stopFailure)
			{
				if (failure == null) failure = stopFailure;
				else failure.addSuppressed(stopFailure);
			}
		}
		try
		{
			this.publisher.close();
		}
		catch (final RuntimeException closeFailure)
		{
			if (failure == null) failure = closeFailure;
			else failure.addSuppressed(closeFailure);
		}
		if (recordingId >= 0)
		{
			try { awaitStopped(this.archive, recordingId, this.configuration); }
			catch (final RuntimeException stopFailure)
			{
				if (failure == null) failure = stopFailure;
				else failure.addSuppressed(stopFailure);
			}
		}
		if (failure != null)
		{
			throw new IllegalStateException("failed to close Aeron archive publisher", failure);
		}
	}

	private static void tryStopRecording(
		final AeronArchive archive,
		final ExclusivePublication publication,
		final long recordingIdHint
	)
	{
		try
		{
			long recordingId = recordingIdHint;
			if (recordingId < 0)
			{
				final CountersReader counters = archive.context().aeron().countersReader();
				final int counterId = RecordingPos.findCounterIdBySession(
					counters, publication.sessionId(), archive.archiveId());
				if (counterId >= 0)
				{
					recordingId = RecordingPos.getRecordingId(counters, counterId);
				}
			}
			if (recordingId >= 0)
			{
				try
				{
					archive.tryStopRecording(recordingId);
				}
				catch (final RuntimeException ignored)
				{
					// Preserve the original construction failure. The provider will
					// report an active recording on the next startup if this stop failed.
				}
			}
		}
		catch (final RuntimeException ignored)
		{
			// The archive client may already be unusable; publication cleanup still
			// runs in the caller.
		}
	}

	private static void awaitStopped(
		final AeronArchive archive,
		final long recordingId,
		final AeronReplicationConfiguration configuration
	)
	{
		final long deadline = System.nanoTime() + configuration.offerTimeoutNanos();
		while (archive.getStopPosition(recordingId) < 0)
		{
			final String archiveError = archive.pollForErrorResponse();
			if (archiveError != null) throw new IllegalStateException("Aeron archive error: " + archiveError);
			if (System.nanoTime() >= deadline)
			{
				throw new IllegalStateException("Aeron archive recording did not stop before timeout");
			}
			LockSupport.parkNanos(Math.min(1_000_000L, Math.max(1L, deadline - System.nanoTime())));
		}
	}
}
