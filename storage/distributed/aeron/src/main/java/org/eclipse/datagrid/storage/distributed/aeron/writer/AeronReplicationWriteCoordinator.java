package org.eclipse.datagrid.storage.distributed.aeron.writer;

import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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

/** Serializes writer transactions and records their checkpoint state. */
public final class AeronReplicationWriteCoordinator implements StorageBinaryDataDistributor, Disposable
{
	@FunctionalInterface
	interface CheckpointListener
	{
		void onState(AeronReplicationCheckpoint.State state, long sequence, int dataLength,
			int dataChunkCount, int dataCrc32c, long position);
	}

	private final AeronReplicationPublisher publisher;
	private final ReplicationDurabilityMode durabilityMode;
	private final CheckpointListener listener;
	private byte[] pendingDictionary;

	AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher)
	{
		this(publisher, ReplicationDurabilityMode.ARCHIVE_FIRST,
			(state, sequence, length, chunks, crc, position) -> { });
	}

	AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher, final CheckpointListener listener)
	{
		this(publisher, ReplicationDurabilityMode.ARCHIVE_FIRST, listener);
	}

	AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
		final ReplicationDurabilityMode durabilityMode, final CheckpointListener listener)
	{
		if (publisher == null || durabilityMode == null || listener == null) throw new NullPointerException();
		if (durabilityMode == ReplicationDurabilityMode.LOCAL_DURABLE_FIRST)
		{
			throw new IllegalArgumentException("Store durable completion callback is required for LOCAL_DURABLE_FIRST");
		}
		this.publisher = publisher;
		this.durabilityMode = durabilityMode;
		this.listener = listener;
	}

	ReplicationDurabilityMode durabilityMode()
	{
		return this.durabilityMode;
	}

	@Override
	public synchronized void distributeTypeDictionary(final String typeDictionaryData)
	{
		this.pendingDictionary = typeDictionaryData == null ? null : typeDictionaryData.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public synchronized String consumeTypeDictionary()
	{
		final byte[] dictionary = this.pendingDictionary;
		this.pendingDictionary = null;
		return dictionary == null ? null : new String(dictionary, StandardCharsets.UTF_8);
	}

	@Override
	public synchronized void distributeData(final Binary data)
	{
		try (final AeronReplicationPublisher.PreparedTransaction prepared = this.prepare(data))
		{
			this.commitOrMarkUncertain(prepared);
		}
	}

	/** Commits a token or records the ambiguous terminal state before rethrowing. */
	synchronized long commitOrMarkUncertain(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		try
		{
			return this.commit(prepared);
		}
		catch (final RuntimeException | Error failure)
		{
			try
			{
				this.markCommittingUncertain(prepared);
			}
			catch (final RuntimeException | Error uncertainFailure)
			{
				failure.addSuppressed(uncertainFailure);
			}
			throw failure;
		}
	}

	/**
	 * Publishes a transaction's prepare phase and reports PREPARING to the
	 * checkpoint listener. The returned token must be committed or closed;
	 * listener failure aborts the prepared publication and fails the publisher
	 * closed.
	 */
	synchronized AeronReplicationPublisher.PreparedTransaction prepare(final Binary data)
	{
		if (data == null)
		{
			this.pendingDictionary = null;
			throw new NullPointerException("data");
		}
		final ByteBuffer[] buffers = data.buffers();
		final AeronReplicationPublisher.PreparedTransaction prepared;
		try
		{
			prepared = this.publisher.prepareTransaction(
				this.pendingDictionary, buffers);
			this.pendingDictionary = null;
		}
		catch (final RuntimeException | Error failure)
		{
			this.pendingDictionary = null;
			throw failure;
		}
		try
		{
			this.notifyState(AeronReplicationCheckpoint.State.PREPARING, prepared, -1);
		}
		catch (final RuntimeException | Error failure)
		{
			try { this.publisher.abort(prepared); }
			catch (final RuntimeException | Error abortFailure) { failure.addSuppressed(abortFailure); }
			this.publisher.failClosed();
			this.pendingDictionary = null;
			throw failure;
		}
		return prepared;
	}

	synchronized void markEnqueued(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		try
		{
			this.notifyState(AeronReplicationCheckpoint.State.ENQUEUED, prepared, -1);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
	}

	/**
	 * Records that the local Store accepted an ENQUEUE_THEN_ARCHIVE write but
	 * publication preparation failed. The marker deliberately remains
	 * non-terminal so restart fails closed instead of assuming the Archive has
	 * the local transaction; recovery must reseed or explicitly repair the gap.
	 */
	synchronized void markEnqueueWithoutArchive()
	{
		final AeronReplicationPublisher.FailedPrepare failed = this.publisher.failedPrepare();
		if (failed == null)
		{
			throw new IllegalStateException("no Aeron sequence was reserved by the failed prepare");
		}
		final AeronReplicationPublisher.PreparedTransaction marker =
			AeronReplicationPublisher.PreparedTransaction.checkpointOnly(this.publisher, failed.sequence(),
				failed.dataLength(), failed.dataChunkCount(), failed.crc32c());
		try
		{
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, marker, -1);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
	}

	synchronized long commit(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		final long position = this.publisher.commit(prepared);
		this.pendingDictionary = null;
		try
		{
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTED, prepared, position);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
		return position;
	}

	synchronized long abort(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		try
		{
			final long position = this.publisher.abort(prepared);
			this.pendingDictionary = null;
			try
			{
				this.notifyState(AeronReplicationCheckpoint.State.REJECTED, prepared, position);
			}
			catch (final RuntimeException | Error failure)
			{
				this.publisher.failClosed();
				throw failure;
			}
			return position;
		}
		catch (final RuntimeException failure)
		{
			this.pendingDictionary = null;
			throw failure;
		}
	}

	synchronized void markCommittingUncertain(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		try
		{
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, prepared, -1);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
	}

	private void notifyState(final AeronReplicationCheckpoint.State state,
		final AeronReplicationPublisher.PreparedTransaction transaction, final long position)
	{
		this.listener.onState(state, transaction.sequence(), transaction.dataLength(),
			transaction.dataChunkCount(), transaction.dataCrc32c(), position);
	}

	@Override
	public synchronized void dispose()
	{
		this.publisher.close();
	}
}
