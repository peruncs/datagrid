package org.eclipse.datagrid.storage.distributed.aeron.writer;

import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

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
 * Keeps local Store acceptance and Aeron publication in one ordered state
 * machine.
 *
 * <p>The coordinator owns the pending dictionary and prepared transaction for
 * its write path. It reports state changes to the checkpoint writer so
 * restart can distinguish a committed transaction from an uncertain one.</p>
 */
public final class AeronReplicationWriteCoordinator implements StorageBinaryDataDistributor, Disposable
{
	static void setCrashHook(final BiConsumer<String, Long> hook) { CrashHook.install(hook); }
	static void clearCrashHook() { CrashHook.clear(); }
	private static void crashPoint(final String name, final long sequence)
	{
		CrashHook.invoke(name, sequence);
	}

	/** Receives primitive checkpoint data without exposing the wire record type. */
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
	private LocalEnqueue localAcceptanceFence;

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

	synchronized long nextSequence()
	{
		return this.publisher.nextSequence();
	}

	/** Saves a type dictionary for the next transaction. */
	@Override
	public synchronized void distributeTypeDictionary(final String typeDictionaryData)
	{
		this.pendingDictionary = typeDictionaryData == null ? null : typeDictionaryData.getBytes(StandardCharsets.UTF_8);
	}

	/** Returns and clears the dictionary saved for the next transaction. */
	@Override
	public synchronized String consumeTypeDictionary()
	{
		final byte[] dictionary = this.pendingDictionary;
		this.pendingDictionary = null;
		return dictionary == null ? null : new String(dictionary, StandardCharsets.UTF_8);
	}

	/** Publishes and commits one Store binary. */
	@Override
	public synchronized void distributeData(final Binary data)
	{
		try (final AeronReplicationPublisher.PreparedTransaction prepared = this.prepare(data))
		{
			this.commitOrMarkUncertain(prepared);
		}
	}

	/**
	 * Commits a token. If the result is unclear, records that fact before
	 * rethrowing so restart cannot silently reuse the sequence.
	 */
	synchronized void commitOrMarkUncertain(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		try
		{
			this.commit(prepared);
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
	 * checkpoint listener. The writer first records a small PREPARING fence for
	 * archive-first writes, because a local Store can accept data before the
	 * publication has a terminal marker. Enqueue-first writes reuse their existing
	 * ENQUEUED fence. The returned token must be committed or closed; listener
	 * failure fails the publisher closed.
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
		final boolean archiveFirst = this.localAcceptanceFence == null;
		AeronReplicationPublisher.TransactionMetadata metadata = null;
		long sequence = -1L;
		/* ARCHIVE_FIRST used to publish before it left any durable local
		 * evidence. Reserve the sequence and persist a PREPARING fence first so a
		 * crash after local Store acceptance cannot silently disappear from the
		 * next writer. The enqueue mode already owns an equivalent fence and must
		 * not write it twice. */
		if (archiveFirst)
		{
			metadata = this.publisher.transactionMetadata(buffers);
			sequence = this.publisher.reserveSequence();
			final AeronReplicationPublisher.PreparedTransaction fence = this.fence(sequence, metadata);
			try
			{
				this.notifyState(AeronReplicationCheckpoint.State.PREPARING, fence, -1);
			}
			catch (final RuntimeException | Error failure)
			{
				this.publisher.failClosed();
				this.pendingDictionary = null;
				throw failure;
			}
		}
		try
		{
			prepared = archiveFirst
				? this.publisher.prepareTransaction(this.pendingDictionary, buffers, sequence, metadata)
				: this.publisher.prepareTransaction(this.pendingDictionary, buffers,
					this.localAcceptanceFence.sequence(), this.localAcceptanceFence.metadata());
			this.pendingDictionary = null;
		}
		catch (final RuntimeException | Error failure)
		{
			this.pendingDictionary = null;
			if (archiveFirst) this.publisher.failClosed();
			throw failure;
		}
		prepared.onAbort(() ->
		{
			try
			{
				this.listener.onState(AeronReplicationCheckpoint.State.REJECTED, prepared.sequence(),
					prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), -1);
			}
			catch (final RuntimeException | Error failure)
			{
				this.publisher.failClosed();
				throw failure;
			}
		});
		if (this.localAcceptanceFence != null)
		{
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
		}
		return prepared;
	}

	synchronized void markEnqueued(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		final LocalEnqueue local = this.localAcceptanceFence;
		if (local != null && local.sequence() == prepared.sequence())
		{
			// ENQUEUE_THEN_ARCHIVE fenced the local write before preparation; do
			// not emit a second identical state transition after publication.
			return;
		}
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
	 * Records a local ENQUEUE before Aeron preparation begins. This closes the
	 * dual-write window where the Store can accept data and the process can die
	 * before the reserved sequence is durably recorded. The returned sequence is
	 * the one that preparation must later reuse.
	 */
	synchronized long markLocalEnqueue(final Binary data)
	{
		if (data == null) throw new NullPointerException("data");
		final AeronReplicationPublisher.TransactionMetadata metadata =
			this.publisher.transactionMetadata(data.buffers());
		/* Reserve the sequence before writing the fence.  The ENQUEUE_THEN_ARCHIVE
		 * preparation must reuse this exact reservation; reading nextSequence()
		 * here would leave the fence one sequence behind the published data. */
		final long sequence = this.publisher.reserveSequence();
		this.localAcceptanceFence = new LocalEnqueue(sequence, metadata);
		final AeronReplicationPublisher.PreparedTransaction marker = this.fence(sequence, metadata);
		this.notifyState(AeronReplicationCheckpoint.State.ENQUEUED, marker, -1);
		return sequence;
	}

	/** Clears the pre-enqueue fence when the Store rejected the write. */
	synchronized void clearLocalEnqueue()
	{
		final LocalEnqueue local = this.localAcceptanceFence;
		if (local == null) return;
		try
		{
			/* The Store rejected the write. Do not publish a terminal checkpoint for
			 * a sequence that does not exist in the log; doing so would make restart
			 * skip the next real sequence. The negative sequence is an explicit
			 * "delete fence only" signal consumed by the provider checkpoint writer. */
			this.listener.onState(AeronReplicationCheckpoint.State.REJECTED, -1,
				local.metadata().dataLength(), local.metadata().dataChunkCount(),
				local.metadata().crc32c(), -1);
			this.publisher.releaseReservedSequence(local.sequence());
		}
		finally
		{
			this.localAcceptanceFence = null;
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
		final LocalEnqueue local = this.localAcceptanceFence;
		if (failed == null && local == null)
		{
			throw new IllegalStateException("no Aeron sequence was reserved by the failed prepare");
		}
		final long sequence = failed == null ? local.sequence() : failed.sequence();
		final int dataLength = failed == null ? local.metadata().dataLength() : failed.dataLength();
		final int dataChunkCount = failed == null ? local.metadata().dataChunkCount() : failed.dataChunkCount();
		final int crc32c = failed == null ? local.metadata().crc32c() : failed.crc32c();
		final AeronReplicationPublisher.PreparedTransaction marker =
			AeronReplicationPublisher.PreparedTransaction.checkpointOnly(this.publisher, sequence,
				dataLength, dataChunkCount, crc32c);
		try
		{
			crashPoint("DURING_COMMITTING_UNCERTAIN_WRITE", sequence);
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, marker, -1);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
		this.localAcceptanceFence = null;
	}

	synchronized long commit(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		final long position = this.publisher.commit(prepared);
		this.pendingDictionary = null;
		try
		{
			crashPoint("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", prepared.sequence());
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTED, prepared, position);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
		this.localAcceptanceFence = null;
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
			this.localAcceptanceFence = null;
			return position;
		}
		catch (final RuntimeException failure)
		{
			this.pendingDictionary = null;
			throw failure;
		}
	}

	private record LocalEnqueue(long sequence, AeronReplicationPublisher.TransactionMetadata metadata)
	{
	}

	private AeronReplicationPublisher.PreparedTransaction fence(final long sequence,
		final AeronReplicationPublisher.TransactionMetadata metadata)
	{
		return AeronReplicationPublisher.PreparedTransaction.checkpointOnly(this.publisher, sequence,
			metadata.dataLength(), metadata.dataChunkCount(), metadata.crc32c());
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

	/** Closes the publisher owned by this coordinator. */
	@Override
	public synchronized void dispose()
	{
		this.publisher.close();
	}
}
