package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
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
import java.util.function.BooleanSupplier;

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

/**
 * Publishes the replication stream and waits for the Archive to record it.
 *
 * <p>The commit marker is the visibility boundary for a transaction. A commit
 * is reported only after the Archive's recorded position reaches that marker.
 * The Archive and Aeron client are borrowed from the transport; this class
 * closes only the publication and its recording.</p>
 */
public final class AeronArchiveReplicationPublisher implements AutoCloseable
{
	@FunctionalInterface
	public interface CheckpointWriter
	{
		/**
		 * Receives a writer transition and the metadata needed for restart.
		 * Non-terminal states are deliberately useful for diagnosis only; restart
		 * must not treat them as committed data.
		 *
		 * @param state state reached by the writer
		 * @param sequence transaction sequence, or {@code -1} for a local rejection
		 * @param dataLength Store binary length
		 * @param dataChunkCount Store binary chunk count
		 * @param dataCrc32c Store binary checksum
		 * @param position Archive position of the terminal marker, or {@code -1}
		 */
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

	/**
	 * Creates a local Archive recording and its writer publication.
	 *
	 * @param archive Archive client that owns the recording
	 * @param channel publication channel
	 * @param streamId publication stream
	 * @param configuration shared framing and timeout limits
	 * @param clusterId replication cluster identity
	 * @param epoch writer epoch
	 * @param initialSequence first sequence to publish
	 * @return a publisher that owns the publication and recording
	 */
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
		return New(archive, channel, streamId, configuration, clusterId, epoch, initialSequence,
			SourceLocation.LOCAL);
	}

	/**
	 * Creates a recording for a publication recorded by a separate Archive.
	 * The caller still supplies the connected Archive client used for the
	 * recording commands.
	 */
	public static AeronArchiveReplicationPublisher NewRemote(
		final AeronArchive archive,
		final String channel,
		final int streamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence
	)
	{
		return New(archive, channel, streamId, configuration, clusterId, epoch, initialSequence,
			SourceLocation.REMOTE);
	}

	private static AeronArchiveReplicationPublisher New(
		final AeronArchive archive,
		final String channel,
		final int streamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final SourceLocation sourceLocation
	)
	{
		ExclusivePublication publication = null;
		try
		{
			if (sourceLocation == SourceLocation.LOCAL)
			{
				publication = archive.addRecordedExclusivePublication(channel, streamId);
			}
			else
			{
				publication = archive.context().aeron().addExclusivePublication(channel, streamId);
				archive.startRecording(ChannelUri.addSessionId(channel, publication.sessionId()), streamId, sourceLocation);
			}
			final long recordingId = awaitRecordingStarted(archive, publication, Aeron.NULL_VALUE, configuration);
			final ExclusivePublication ownedPublication = publication;
			return new AeronArchiveReplicationPublisher(
				archive,
				ownedPublication,
				new AeronReplicationPublisher(
					ownedPublication, configuration, clusterId, epoch, initialSequence, true,
					position -> awaitRecorded(archive, ownedPublication, recordingId, configuration, position)
				),
				recordingId,
				configuration
			);
		}
		catch (final RuntimeException | Error failure)
		{
			if (publication != null)
			{
				final RuntimeException stopFailure = tryStopRecording(archive, publication, Aeron.NULL_VALUE);
				if (stopFailure != null) failure.addSuppressed(stopFailure);
				try
				{
					publication.close();
				}
				catch (final RuntimeException | Error closeFailure)
				{
					failure.addSuppressed(closeFailure);
				}
			}
			throw failure;
		}
	}

	/**
	 * Extends a stopped recording without changing its framing.
	 * The recording must belong to {@code streamId}; the same initial-position
	 * URI is used for the new publication and the Archive extension.
	 *
	 * @throws IllegalArgumentException if the recording is unknown, uses another
	 *         stream, or has different framing
	 * @throws IllegalStateException if the recording is still active
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
		return Extend(archive, recordingId, streamId, configuration, clusterId, epoch, initialSequence,
			SourceLocation.LOCAL);
	}

	/** Reopens a stopped recording whose source publication uses another driver. */
	public static AeronArchiveReplicationPublisher ExtendRemote(
		final AeronArchive archive,
		final long recordingId,
		final int streamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence
	)
	{
		return Extend(archive, recordingId, streamId, configuration, clusterId, epoch, initialSequence,
			SourceLocation.REMOTE);
	}

	private static AeronArchiveReplicationPublisher Extend(
		final AeronArchive archive,
		final long recordingId,
		final int streamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final SourceLocation sourceLocation
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
			archive.extendRecording(recordingId, extendedChannel, streamId, sourceLocation);
			awaitRecordingStarted(archive, publication, recordingId, configuration);
			return new AeronArchiveReplicationPublisher(archive, publication,
				new AeronReplicationPublisher(publication, configuration, clusterId, epoch, initialSequence, true,
					positionValue -> awaitRecorded(archive, publication, recordingId, configuration, positionValue)), recordingId,
				configuration);
		}
		catch (final RuntimeException | Error failure)
		{
			final RuntimeException stopFailure = tryStopRecording(archive, publication, recordingId);
			if (stopFailure != null) failure.addSuppressed(stopFailure);
			try
			{
				publication.close();
			}
			catch (final RuntimeException | Error closeFailure)
			{
				failure.addSuppressed(closeFailure);
			}
			throw failure;
		}
	}

	/**
	 * Publishes a raw transaction for package-local integration tests and
	 * low-level Archive fixtures. Production Store writes must go through the
	 * coordinator-backed persistence target so the local acceptance fence is
	 * recorded as well.
	 *
	 * @param dictionary optional type dictionary bytes
	 * @param data Store binary buffers; their positions are read but not changed
	 * @return the Archive position at or after the commit marker
	 */
	synchronized long publishTransaction(final byte[] dictionary, final java.nio.ByteBuffer[] data)
	{
		if (this.closed) throw new IllegalStateException("Aeron archive publisher is closed");
		return this.publisher.publishTransaction(dictionary, data);
	}

	public long recordingId()
	{
		final CountersReader counters = this.archive.context().aeron().countersReader();
		final int counterId = this.recordingCounterId(counters);
		return counterId >= 0 ? RecordingPos.getRecordingId(counters, counterId) :
			this.recordingIdHint >= 0 ? this.recordingIdHint : Aeron.NULL_VALUE;
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
		if (counterId >= 0) return RecordingPos.getRecordingId(counters, counterId);

		final long[] recordingId = { Aeron.NULL_VALUE };
		archive.listRecordings(0, Integer.MAX_VALUE, (controlSessionId, correlationId, id,
			startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId,
			segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
			strippedChannel, originalChannel, sourceIdentity) ->
		{
			if (sessionId == publication.sessionId() && streamId == publication.streamId())
			{
				recordingId[0] = id;
			}
		});
		return recordingId[0];
	}

	/** Returns whether publication or durability failure made this writer fail closed. */
	public boolean isFailed()
	{
		return this.publisher.isFailed();
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
			if (counterId < 0 && recordingIdHint >= 0)
			{
				/* A local RecordingPos counter is authoritative. Only ask the Archive
				 * control session when the counter is not visible (the remote/fallback
				 * path); otherwise every back-pressured offer would issue a synchronous
				 * control round-trip. */
				final long archivePosition = archive.getRecordingPosition(recordingIdHint);
				if (archivePosition >= commitPosition)
				{
					return archivePosition;
				}
			}
			/* Embedded recordings expose a local counter. Check it before polling the
			 * control channel so the normal commit path does not pay a synchronous
			 * Archive request once the local recording has already advanced. Remote
			 * recordings still use this poll to surface control-session failures. */
			final String archiveError = archive.pollForErrorResponse();
			if (archiveError != null) throw new IllegalStateException("Aeron archive error: " + archiveError);
			if (System.nanoTime() >= deadline)
			{
				throw new IllegalStateException("Aeron archive did not record commit position " + commitPosition +
					" (publicationPosition=" + publication.position() + ", recordedPosition=" + lastRecordedPosition +
					", active=" + lastActive + ", recordingId=" + recordingIdHint + ")");
			}
			LockSupport.parkNanos(Math.min(1_000_000L, Math.max(1L, deadline - System.nanoTime())));
		}
	}

	private static long awaitRecordingStarted(
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
				if (RecordingPos.isActive(counters, counterId, recordingId)) return recordingId;
			}
			else
			{
				final long discovered = recordingIdHint >= 0 ? recordingIdHint : findRecordingId(archive, publication);
				if (discovered >= 0) return discovered;
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

	/**
	 * Creates the coordinator that records restart state for this publisher.
	 *
	 * @param durabilityMode ordering between local acceptance and Archive
	 * @param writer receiver for checkpoint transitions
	 * @return a coordinator backed by this publisher
	 */
	public AeronReplicationWriteCoordinator newWriteCoordinator(
		final ReplicationDurabilityMode durabilityMode,
		final CheckpointWriter writer)
	{
		return this.newWriteCoordinator(durabilityMode, writer, () -> true);
	}

	/** Creates a coordinator with a local write-admission predicate. */
	public AeronReplicationWriteCoordinator newWriteCoordinator(
		final ReplicationDurabilityMode durabilityMode,
		final CheckpointWriter writer,
		final BooleanSupplier writeAdmission)
	{
		if (writer == null) throw new NullPointerException("writer");
		return new AeronReplicationWriteCoordinator(this.publisher, durabilityMode, writer,
			writeAdmission);
	}

	/**
	 * Aligns the next transaction with a sequence recovered from the Store.
	 *
	 * @param nextSequence next sequence that may be published
	 */
	public void synchronizeNextSequence(final long nextSequence)
	{
		this.publisher.synchronizeNextSequence(nextSequence);
	}

	/** Stops the recording, aborts any pending transaction, and closes the publication. */
	@Override
	public void close()
	{
		synchronized (this)
		{
			if (this.closed) return;
		}
		RuntimeException failure = null;
		Error fatalFailure = null;
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
		try
		{
			/* Abort pending data before stopping the recording, otherwise the abort
			 * marker can be offered to an already-stopped recording. */
			this.publisher.close();
		}
		catch (final RuntimeException closeFailure)
		{
			if (failure == null) failure = closeFailure;
			else failure.addSuppressed(closeFailure);
		}
		catch (final Error closeFailure)
		{
			fatalFailure = closeFailure;
		}
		/* If the pending abort could not be offered, keep the recording alive so a
		 * retry can terminate the same sequence instead of making recovery harder. */
		if (this.publisher.hasPendingTransaction())
		{
			final IllegalStateException pendingFailure = new IllegalStateException(
				"cannot stop Aeron recording while pending abort is unresolved", failure);
			if (fatalFailure != null)
			{
				fatalFailure.addSuppressed(pendingFailure);
				throw fatalFailure;
			}
			throw pendingFailure;
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
		if (recordingId >= 0)
		{
			try { awaitStopped(this.archive, recordingId, this.configuration); }
			catch (final RuntimeException stopFailure)
			{
				if (failure == null) failure = stopFailure;
				else failure.addSuppressed(stopFailure);
			}
		}
		if (fatalFailure != null)
		{
			if (failure != null) fatalFailure.addSuppressed(failure);
			throw fatalFailure;
		}
		if (failure != null)
		{
			throw new IllegalStateException("failed to close Aeron archive publisher", failure);
		}
		synchronized (this)
		{
			this.closed = true;
		}
	}

	private static RuntimeException tryStopRecording(
		final AeronArchive archive,
		final ExclusivePublication publication,
		final long recordingIdHint
	)
	{
		RuntimeException failure = null;
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
				catch (final RuntimeException stopFailure)
				{
					failure = stopFailure;
				}
			}
		}
		catch (final RuntimeException cleanupFailure)
		{
			failure = cleanupFailure;
		}
		return failure;
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
