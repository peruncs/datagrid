package org.eclipse.datagrid.storage.distributed.aeron.reader;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * Reads committed Store transactions from an Archive and then from the live
 * publication.
 *
 * <p>Replay starts at the supplied durable position. The reader reports live
 * only after replay catches up, so a restart needs no separate snapshot path.
 * The subscription belongs to this reader; the caller remains responsible for
 * the shared Aeron and Archive clients.</p>
 */
public final class StorageBinaryDataClientAeronArchive implements StorageBinaryDataClient
{
	private final PersistentSubscription subscription;
	private final TransactionAssembler assembler;
	private final long stopTimeoutNanos;
	private final AtomicBoolean active = new AtomicBoolean();
	private volatile Thread thread;
	private volatile CountDownLatch stopped = new CountDownLatch(0);
	private volatile boolean disposed;
	private volatile boolean stopAtLatest;
	private volatile long stopDeadlineNanos;



	StorageBinaryDataClientAeronArchive(
		final PersistentSubscription subscription,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver,
		final Runnable transactionResolved,
		final ReaderDeliveryListener deliveryListener
	)
	{
		this.subscription = java.util.Objects.requireNonNull(subscription, "subscription");
		try
		{
			this.stopTimeoutNanos = java.util.Objects.requireNonNull(configuration, "configuration").offerTimeoutNanos();
			this.assembler = new TransactionAssembler(
				configuration, clusterId, epoch, initialSequence, receiver, transactionResolved, deliveryListener
			);
		}
		catch (final RuntimeException | Error failure)
		{
			try
			{
				subscription.close();
			}
			catch (final RuntimeException closeFailure)
			{
				failure.addSuppressed(closeFailure);
			}
			throw failure;
		}
	}

	/**
	 * Creates a reader with no delivery durability callback.
	 *
	 * @param aeron shared Aeron client, not owned by the reader
	 * @param archiveContext Archive connection settings
	 * @param recordingId recording to replay
	 * @param startPosition first Archive position to replay
	 * @param liveChannel live publication channel
	 * @param liveStreamId live publication stream
	 * @param replayChannel replay channel
	 * @param replayStreamId replay stream
	 * @param configuration shared framing and timeout limits
	 * @param clusterId expected cluster identity
	 * @param epoch expected writer epoch
	 * @param initialSequence last sequence already applied by the Store
	 * @param receiver destination for complete Store binaries
	 * @param transactionResolved callback after a transaction is delivered
	 * @return a reader that owns its subscriptions
	 */
	public static StorageBinaryDataClientAeronArchive New(
		final Aeron aeron,
		final AeronArchive.Context archiveContext,
		final long recordingId,
		final long startPosition,
		final String liveChannel,
		final int liveStreamId,
		final String replayChannel,
		final int replayStreamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver,
		final Runnable transactionResolved
	)
	{
		return New(aeron, archiveContext, recordingId, startPosition, liveChannel, liveStreamId,
			replayChannel, replayStreamId, configuration, clusterId, epoch, initialSequence, receiver,
			transactionResolved, null);
	}

	/**
	 * Creates a reader with callbacks around Store materialisation.
	 *
	 * <p>The reader does not close {@code aeron} or the Archive client. Call
	 * {@link #dispose()} when the reader is no longer needed.</p>
	 *
	 * @param aeron shared Aeron client, not owned by the reader
	 * @param archiveContext Archive connection settings
	 * @param recordingId recording to replay
	 * @param startPosition first Archive position to replay
	 * @param liveChannel live publication channel
	 * @param liveStreamId live publication stream
	 * @param replayChannel replay channel
	 * @param replayStreamId replay stream
	 * @param configuration shared framing and timeout limits
	 * @param clusterId expected cluster identity
	 * @param epoch expected writer epoch
	 * @param initialSequence last sequence already applied by the Store
	 * @param receiver destination for complete Store binaries
	 * @param transactionResolved callback after a transaction is delivered
	 * @param deliveryListener callback around Store materialisation
	 * @return a reader that owns its subscriptions
	 */
	public static StorageBinaryDataClientAeronArchive New(
		final Aeron aeron,
		final AeronArchive.Context archiveContext,
		final long recordingId,
		final long startPosition,
		final String liveChannel,
		final int liveStreamId,
		final String replayChannel,
		final int replayStreamId,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver,
		final Runnable transactionResolved,
		final ReaderDeliveryListener deliveryListener
	)
	{
		final AeronArchive.Context subscriptionArchiveContext = archiveContext.clone().aeron(aeron);
		final PersistentSubscription.Context subscriptionContext = new PersistentSubscription.Context()
			.aeron(aeron)
			.ownsAeronClient(false)
			.recordingId(recordingId)
			.startPosition(startPosition)
			.liveChannel(liveChannel)
			.liveStreamId(liveStreamId)
			.replayChannel(replayChannel)
			.replayStreamId(replayStreamId)
			.aeronArchiveContext(subscriptionArchiveContext);
		PersistentSubscription subscription = null;
		try
		{
			subscription = PersistentSubscription.create(subscriptionContext);
			return new StorageBinaryDataClientAeronArchive(
				subscription, configuration, clusterId, epoch, initialSequence, receiver, transactionResolved,
				deliveryListener
			);
		}
		catch (final RuntimeException | Error failure)
		{
			if (subscription != null)
			{
				try
				{
					subscription.close();
				}
				catch (final RuntimeException closeFailure)
				{
					failure.addSuppressed(closeFailure);
				}
			}
			else
			{
				try
				{
					subscriptionContext.close();
				}
				catch (final RuntimeException closeFailure)
				{
					failure.addSuppressed(closeFailure);
				}
			}
			throw failure;
		}
	}

	/** Starts replay and live polling; repeated calls have no effect. */
	@Override
	public synchronized void start()
	{
		if (this.disposed)
		{
			throw new IllegalStateException("Aeron Archive reader is disposed");
		}
		if (this.active.getAndSet(true))
		{
			return;
		}
		this.stopAtLatest = false;
		this.stopDeadlineNanos = 0L;
		this.stopped = new CountDownLatch(1);
		this.thread = new Thread(this::run, "datagrid-aeron-archive-reader");
		this.thread.setDaemon(true);
		this.thread.start();
	}

	private void run()
	{
		try
		{
			AeronReaderLifecycle.runPollingLoop(
				this.active,
				this.subscription::hasFailed,
				() -> this.subscription.poll(this::onMessage, 10),
				() -> this.stopAtLatest && this.subscription.isLive(),
				() -> this.stopAtLatest && System.nanoTime() >= this.stopDeadlineNanos,
				() -> this.assembler.failure(new IllegalStateException(
					"Timed out waiting for Aeron Archive replay to reach the live tail"))
			);
			if (this.subscription.hasFailed())
			{
				this.assembler.failure(new IllegalStateException(
					"PersistentSubscription failed", this.subscription.failureReason()));
			}
		}
		catch (final RuntimeException e)
		{
			this.assembler.failure(e);
		}
		catch (final Error e)
		{
			this.assembler.failure(new IllegalStateException("Aeron Archive reader polling failed", e));
		}
		finally
		{
			this.active.set(false);
			this.stopped.countDown();
		}
	}

	/**
	 * Restarts a reader stopped at the live tail. A replay or validation failure
	 * is terminal; create a new reader from the durable cursor instead of
	 * reusing incomplete transaction state.
	 */
	public synchronized void resume()
	{
		final RuntimeException failure = this.failure();
		if (failure != null)
		{
			throw new IllegalStateException(
				"cannot resume a failed Aeron Archive reader; create a new reader from its durable cursor", failure);
		}
		this.stopAtLatest = false;
		this.start();
	}

	/** Requests a stop after replay reaches the current live tail. */
	public synchronized void stopAtLatestMessage()
	{
		if (!this.disposed)
		{
			this.stopAtLatest = true;
			this.stopDeadlineNanos = System.nanoTime() + this.stopTimeoutNanos;
		}
	}

	private void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
	{
		this.assembler.onFragment(buffer, offset, length, header);
	}

	/** Returns the last sequence delivered after commit and checksum validation. */
	public long lastResolvedSequence()
	{
		return this.assembler.lastResolvedSequence();
	}

	/** Returns whether the polling thread is active and has not failed. */
	public boolean isRunning()
	{
		return this.active.get() && this.failure() == null;
	}

	/** Returns whether replay has transitioned to the live subscription. */
	public boolean isLive()
	{
		return this.subscription.isLive();
	}

	/** Returns the Archive position of the last resolved commit. */
	public long lastResolvedPosition()
	{
		return this.assembler.lastResolvedPosition();
	}

	/** Builds a cursor that can resume this reader from the same recording. */
	public AeronReplicationCursor cursor(
		final UUID nodeId,
		final UUID storeGeneration,
		final long recordingId
	)
	{
		final TransactionAssembler.CursorSnapshot snapshot =
			this.assembler.cursorSnapshot();
		return new AeronReplicationCursor(
			this.assembler.clusterId(),
			nodeId,
			storeGeneration,
			this.assembler.epoch(),
			recordingId,
			snapshot.position(),
			snapshot.sequence()
		);
	}

	/** Returns the terminal polling failure, or {@code null} while healthy. */
	public RuntimeException failure()
	{
		return this.assembler.failure();
	}

	/** Stops polling and releases this reader's subscriptions. */
	@Override
	public synchronized void dispose()
	{
		if (this.disposed) return;
		this.disposed = true;
		final Thread pollingThread = this.thread;
		this.thread = null;
		AeronReaderLifecycle.stopAndClose(this.active, pollingThread, this.stopped, this.subscription::close);
	}
}
