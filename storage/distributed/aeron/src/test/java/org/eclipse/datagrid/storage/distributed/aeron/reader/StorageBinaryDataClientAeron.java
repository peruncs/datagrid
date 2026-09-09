package org.eclipse.datagrid.storage.distributed.aeron.reader;

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

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-only live reader used by low-level UDP tests. Production clustering
 * uses {@link StorageBinaryDataClientAeronArchive}; both readers share
 * {@link TransactionAssembler} for commit-gated delivery and CRC validation.
 */
public final class StorageBinaryDataClientAeron implements StorageBinaryDataClient
{
	private final Subscription subscription;
	private final TransactionAssembler assembler;
	private final AtomicBoolean active = new AtomicBoolean();
	private final FragmentAssembler fragmentAssembler;
	private volatile Thread thread;
	private volatile CountDownLatch stopped = new CountDownLatch(0);
	private volatile boolean disposed;
	private volatile boolean stopAtLatest;

	public StorageBinaryDataClientAeron(
		final Subscription subscription,
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver
	)
	{
		this.subscription = subscription;
		try
		{
			this.assembler = new TransactionAssembler(configuration, clusterId, epoch, initialSequence, receiver);
			this.fragmentAssembler = new FragmentAssembler(this.assembler::onFragment);
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

	@Override
	public synchronized void start()
	{
		if (this.disposed) throw new IllegalStateException("Aeron reader is disposed");
		if (this.active.getAndSet(true)) return;
		this.stopAtLatest = false;
		this.stopped = new CountDownLatch(1);
		this.thread = new Thread(this::run, "datagrid-aeron-reader");
		this.thread.setDaemon(true);
		this.thread.start();
	}

	private void run()
	{
		try
		{
			AeronReaderLifecycle.runPollingLoop(
				this.active,
				() -> false,
				() -> this.subscription.poll(this.fragmentAssembler, 10),
				() -> this.stopAtLatest,
				() -> false,
				() -> { }
			);
		}
		catch (final RuntimeException e)
		{
			this.assembler.failure(e);
		}
		catch (final Error e)
		{
			this.assembler.failure(new IllegalStateException("Aeron reader polling failed", e));
		}
		finally
		{
			this.active.set(false);
			this.stopped.countDown();
		}
	}

	public synchronized void resume()
	{
		final RuntimeException failure = this.failure();
		if (failure != null)
		{
			throw new IllegalStateException(
				"cannot resume a failed Aeron reader; create a new reader from its durable cursor", failure);
		}
		this.stopAtLatest = false;
		this.start();
	}

	public synchronized void stopAtLatestMessage()
	{
		if (!this.disposed) this.stopAtLatest = true;
	}

	public long lastResolvedSequence() { return this.assembler.lastResolvedSequence(); }
	public boolean isRunning() { return this.active.get() && this.failure() == null; }
	public long lastResolvedPosition() { return this.assembler.lastResolvedPosition(); }

	public AeronReplicationCursor cursor(final UUID nodeId, final UUID storeGeneration, final long recordingId)
	{
		final TransactionAssembler.CursorSnapshot snapshot = this.assembler.cursorSnapshot();
		return new AeronReplicationCursor(
			this.assembler.clusterId(), nodeId, storeGeneration, this.assembler.epoch(), recordingId,
			snapshot.position(), snapshot.sequence());
	}

	public RuntimeException failure() { return this.assembler.failure(); }

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
