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
 * DataGrid reader using Aeron Archive replay/catch-up followed by live UDP.
 *
 * <p>The reader owns a replay subscription and a live subscription. It starts
 * from the supplied recording position, applies the same commit-gated
 * assembler as the live client, and reports {@link #isLive()} only after the
 * replay has caught up. This lets a restarted node recover without a separate
 * snapshot protocol.</p>
 */
public final class StorageBinaryDataClientAeronArchive implements StorageBinaryDataClient
{
	private final PersistentSubscription subscription;
	private final TransactionAssembler assembler;
	private final long stopTimeoutNanos;
	private final AtomicBoolean active = new AtomicBoolean();
	private volatile Thread thread;
	private volatile boolean disposed;
	private volatile boolean stopAtLatest;
	private volatile long stopDeadlineNanos;



	public StorageBinaryDataClientAeronArchive(
		final PersistentSubscription subscription,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver,
		final Runnable transactionResolved
	)
	{
		this.subscription = java.util.Objects.requireNonNull(subscription, "subscription");
		try
		{
			this.stopTimeoutNanos = java.util.Objects.requireNonNull(configuration, "configuration").offerTimeoutNanos();
			this.assembler = new TransactionAssembler(
				configuration, clusterId, epoch, initialSequence, receiver, transactionResolved
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
				subscription, configuration, clusterId, epoch, initialSequence, receiver, transactionResolved
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

	/** Starts replay and live polling; repeated calls are idempotent. */
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
		}
	}

	/**
	 * Restarts a stopped reader. A replay or validation failure is terminal:
	 * create a new reader from its durable cursor rather than reusing a reader
	 * whose transaction state may be incomplete.
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

	/** Stops after replay has reached the current live tail; the subscription remains resumable. */
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

	/** Returns the last sequence delivered after commit validation. */
	public long lastResolvedSequence()
	{
		return this.assembler.lastResolvedSequence();
	}

	public boolean isRunning()
	{
		return this.active.get() && this.failure() == null;
	}

	/** Returns true after replay has transitioned to the live subscription. */
	public boolean isLive()
	{
		return this.subscription.isLive();
	}

	/** Returns the Archive/Aeron position of the last resolved commit. */
	public long lastResolvedPosition()
	{
		return this.assembler.lastResolvedPosition();
	}

	/** Builds a restart cursor containing recording id, position, and sequence. */
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

	public RuntimeException failure()
	{
		return this.assembler.failure();
	}

	@Override
	public synchronized void dispose()
	{
		if (this.disposed) return;
		this.disposed = true;
		final Thread pollingThread = this.thread;
		this.thread = null;
		AeronReaderLifecycle.stopAndClose(this.active, pollingThread, this.subscription::close);
	}
}
