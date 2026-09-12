package org.eclipse.datagrid.storage.distributed.aeron.writer;

import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;
import org.eclipse.serializer.persistence.binary.types.Binary;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.BiConsumer;
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
 * Keeps local Store acceptance and Aeron publication in one ordered state
 * machine.
 *
 * <p>The coordinator owns the pending dictionary and prepared transaction for
 * its write path. It reports state changes to the checkpoint writer so
 * restart can distinguish a committed transaction from an uncertain one.</p>
 *
 * <p>This is an Aeron-only write coordinator. Store integration must use
 * {@link AeronStorageBinaryTargetDistributing}; exposing this object as the
 * transport-neutral distributor would allow publication without local Store
 * acceptance and would bypass the durable fence.</p>
 */
public final class AeronReplicationWriteCoordinator implements AutoCloseable
{
	static void setCrashHook(final BiConsumer<String, Long> hook) { CrashHook.install(hook); }
	static void clearCrashHook() { CrashHook.clear(); }
	private static void crashPoint(final String name, final long sequence)
	{
		CrashHook.invoke(name, sequence);
	}

	private final AeronReplicationPublisher publisher;
	private final ReplicationDurabilityMode durabilityMode;
	private final AeronArchiveReplicationPublisher.CheckpointWriter listener;
	private final BooleanSupplier writeAdmission;
	private byte[] pendingDictionary;
	private LocalEnqueue localAcceptanceFence;
	/* The coordinator is single-threaded. Reusing this channel-order scratch
	 * array removes the ArrayList and temporary array from every write. A local
	 * acceptance fence keeps the count beside the array until preparation ends. */
	private ByteBuffer[] bufferScratch = new ByteBuffer[8];
	private int bufferScratchCount;
	/* Set only after publisher.commit() has returned.  A checkpoint cleanup
	 * failure after that point must not overwrite a durable COMMITTED record with
	 * COMMITTING_UNCERTAIN. */
	private boolean commitMarkerPublished;

	AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher)
	{
		this(publisher, ReplicationDurabilityMode.ARCHIVE_FIRST,
			(state, sequence, length, chunks, crc, position) -> { }, () -> true);
	}

	AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
		final AeronArchiveReplicationPublisher.CheckpointWriter listener)
	{
		this(publisher, ReplicationDurabilityMode.ARCHIVE_FIRST, listener);
	}

	AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
		final ReplicationDurabilityMode durabilityMode,
		final AeronArchiveReplicationPublisher.CheckpointWriter listener)
	{
		this(publisher, durabilityMode, listener, () -> true);
	}

	AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
		final ReplicationDurabilityMode durabilityMode,
		final AeronArchiveReplicationPublisher.CheckpointWriter listener,
		final BooleanSupplier writeAdmission)
	{
		if (publisher == null) throw new NullPointerException("publisher");
		if (durabilityMode == null) throw new NullPointerException("durabilityMode");
		if (listener == null) throw new NullPointerException("listener");
		if (writeAdmission == null) throw new NullPointerException("writeAdmission");
		if (durabilityMode == ReplicationDurabilityMode.LOCAL_DURABLE_FIRST)
		{
			throw new IllegalArgumentException("Store durable completion callback is required for LOCAL_DURABLE_FIRST");
		}
		this.publisher = publisher;
		this.durabilityMode = durabilityMode;
		this.listener = listener;
		this.writeAdmission = writeAdmission;
		this.publisher.claimCoordinator(this);
	}

	ReplicationDurabilityMode durabilityMode()
	{
		return this.durabilityMode;
	}

	synchronized long nextSequence()
	{
		return this.publisher.nextSequence();
	}

	/** Saves a type dictionary for the next transaction.
	 *
	 * @param typeDictionaryData dictionary text, or {@code null} to clear it
	 */
	public synchronized void distributeTypeDictionary(final String typeDictionaryData)
	{
		this.pendingDictionary = typeDictionaryData == null ? null : typeDictionaryData.getBytes(StandardCharsets.UTF_8);
	}

	/**
	 * Publishes and commits one Store binary using the archive-first fence.
	 *
	 * <p>This entry point is for the neutral distributor, which has no local
	 * persistence target to fence. Store writes should use
	 * {@link AeronStorageBinaryTargetDistributing} so local acceptance and
	 * publication remain one operation.</p>
	 *
	 * @param data binary to publish
	 */
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
		this.commitMarkerPublished = false;
		try
		{
			this.commit(prepared);
		}
		catch (final RuntimeException | Error failure)
		{
			if (!this.commitMarkerPublished)
			{
				try
				{
					this.markCommittingUncertain(prepared);
				}
				catch (final RuntimeException | Error uncertainFailure)
				{
					failure.addSuppressed(uncertainFailure);
				}
			}
			else
			{
				/* The Archive terminal marker is known durable. Keep the checkpoint
				 * state already written by notifyState(COMMITTED); a failed fence
				 * cleanup is a degraded shutdown, not an uncertain commit. */
				failure.addSuppressed(new IllegalStateException(
					"Aeron commit is durable but its checkpoint cleanup failed"));
			}
			throw failure;
		}
		finally
		{
			this.commitMarkerPublished = false;
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
		this.ensureWriteAdmitted();
		if (data == null)
		{
			throw new NullPointerException("data");
		}
		if (this.publisher.hasPendingTransaction())
		{
			throw new IllegalStateException("an Aeron prepared transaction is already pending");
		}
		final AeronReplicationPublisher.PreparedTransaction prepared;
		final LocalEnqueue local = this.localAcceptanceFence;
		/* ENQUEUE_THEN_ARCHIVE already collected the channel-ordered buffer array while
		 * fencing the local Store write. Reuse that view after the Store
		 * restores the marked positions; collecting again would repeat the channel
		 * walk and allocate another array for the same transaction. */
		final int bufferCount;
		final ByteBuffer[] buffers;
		if (local == null)
		{
			bufferCount = this.collectBuffers(data);
			buffers = this.bufferScratch;
		}
		else
		{
			bufferCount = local.bufferCount();
			buffers = local.buffers();
		}
		final boolean archiveFirst = local == null;
		AeronReplicationPublisher.TransactionMetadata metadata = null;
		long sequence = -1L;
		/* ARCHIVE_FIRST used to publish before it left any durable local
		 * evidence. Reserve the sequence and persist a PREPARING fence first so a
		 * crash after local Store acceptance cannot silently disappear from the
		 * next writer. The enqueue mode already owns an equivalent fence and must
		 * not write it twice. */
		if (archiveFirst)
		{
			try
			{
				metadata = this.publisher.transactionMetadata(buffers, bufferCount);
				sequence = this.publisher.reserveSequence();
				this.notifyState(AeronReplicationCheckpoint.State.PREPARING, sequence,
					metadata.dataLength(), metadata.dataChunkCount(), metadata.crc32c(), -1);
			}
			catch (final RuntimeException | Error failure)
			{
				this.clearBufferScratch();
				this.publisher.failClosed();
				throw failure;
			}
		}
		try
		{
			prepared = archiveFirst
				? this.publisher.prepareTransaction(this.pendingDictionary, buffers, bufferCount, sequence, metadata)
				: this.publisher.prepareTransaction(this.pendingDictionary, buffers,
					bufferCount, local.sequence(), local.metadata());
		}
		catch (final RuntimeException | Error failure)
		{
			this.clearBufferScratch();
			if (archiveFirst) this.publisher.failClosed();
			throw failure;
		}
		this.clearBufferScratch();
		prepared.onAbort(abortPosition ->
		{
			try
			{
				/* A negative position means the ABORT marker was offered but its
				 * durable Archive position could not be established. Treat that path as
				 * uncertain; a restart must reseed rather than accept a rejection whose
				 * terminal evidence may still be in flight. */
				final AeronReplicationCheckpoint.State state = abortPosition < 0
					? AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN
					: AeronReplicationCheckpoint.State.REJECTED;
				this.listener.onState(state, prepared.sequence(),
					prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), abortPosition);
			}
			catch (final RuntimeException | Error failure)
			{
				this.publisher.failClosed();
				throw failure;
			}
		});
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
		if (this.durabilityMode == ReplicationDurabilityMode.ARCHIVE_FIRST)
		{
			/* PREPARING is already a durable refusal fence for this mode.  Replacing
			 * it with ENQUEUED adds another forced file+directory sync without adding
			 * recovery information; COMMITTED/REJECTED is the next meaningful state. */
			return;
		}
		try
		{
			this.notifyState(AeronReplicationCheckpoint.State.ENQUEUED, prepared.sequence(),
				prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), -1);
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
		this.ensureWriteAdmitted();
		if (data == null) throw new NullPointerException("data");
		if (this.localAcceptanceFence != null)
		{
			throw new IllegalStateException("an Aeron local acceptance fence is already pending");
		}
		final int bufferCount = this.collectBuffers(data);
		final ByteBuffer[] buffers = this.bufferScratch;
		final AeronReplicationPublisher.TransactionMetadata metadata;
		final long sequence;
		try
		{
			metadata = this.publisher.transactionMetadata(buffers, bufferCount);
			/* Reserve the sequence before writing the fence.  The ENQUEUE_THEN_ARCHIVE
			 * preparation must reuse this exact reservation; reading nextSequence()
			 * here would leave the fence one sequence behind the published data. */
			sequence = this.publisher.reserveSequence();
			this.localAcceptanceFence = new LocalEnqueue(sequence, metadata, buffers, bufferCount);
			this.notifyState(AeronReplicationCheckpoint.State.ENQUEUED, sequence,
				metadata.dataLength(), metadata.dataChunkCount(), metadata.crc32c(), -1);
		}
		catch (final RuntimeException | Error failure)
		{
			/* A failed fence write must not leave a live reservation that a later
			 * transaction could accidentally skip or reuse. The publisher is failed
			 * closed because the durable boundary itself is no longer trustworthy. */
			this.publisher.failClosed();
			if (this.localAcceptanceFence != null)
			{
				try
				{
					this.publisher.releaseReservedSequence(this.localAcceptanceFence.sequence());
				}
				catch (final RuntimeException | Error releaseFailure)
				{
					failure.addSuppressed(releaseFailure);
				}
			}
			this.localAcceptanceFence = null;
			this.clearBufferScratch();
			throw failure;
		}
		return sequence;
	}

	private void ensureWriteAdmitted()
	{
		if (!this.writeAdmission.getAsBoolean())
		{
			throw new IllegalStateException("Aeron Archive free capacity is below the configured write threshold");
		}
	}

	/** Clears the pre-enqueue fence when the Store rejected the write. */
	synchronized void clearLocalEnqueue()
	{
		final LocalEnqueue local = this.localAcceptanceFence;
		if (local == null) return;
		try
		{
			/* The Store rejected the write. There is no Aeron transaction to
			 * represent, so clear only the local acceptance fence. */
			this.listener.clearEnqueueFence();
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			try
			{
				this.publisher.releaseReservedSequence(local.sequence());
			}
			catch (final RuntimeException | Error releaseFailure)
			{
				failure.addSuppressed(releaseFailure);
			}
			this.localAcceptanceFence = null;
			throw failure;
		}
		try
		{
			this.publisher.releaseReservedSequence(local.sequence());
		}
		catch (final RuntimeException | Error failure)
		{
			/* A reservation that cannot be released must never be reused by a later
			 * write. Fail closed before propagating the cleanup error. */
			this.publisher.failClosed();
			throw failure;
		}
		finally
		{
			this.localAcceptanceFence = null;
			this.clearBufferScratch();
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
		try
		{
			crashPoint("DURING_COMMITTING_UNCERTAIN_WRITE", sequence);
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, sequence,
				dataLength, dataChunkCount, crc32c, -1);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
		this.localAcceptanceFence = null;
		this.clearBufferScratch();
	}

	synchronized void commit(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		this.commitMarkerPublished = false;
		final long position = this.publisher.commit(prepared);
		this.commitMarkerPublished = true;
		this.pendingDictionary = null;
		try
		{
			crashPoint("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", prepared.sequence());
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTED, prepared.sequence(),
				prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), position);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
		this.localAcceptanceFence = null;
		this.clearBufferScratch();
		this.commitMarkerPublished = false;
	}

	synchronized void abort(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		try
		{
			/* prepare() registers the rejection callback on the token. The publisher
			 * invokes it for every successful abort path, including direct publisher
			 * aborts and shutdown, so the checkpoint transition cannot be skipped or
			 * emitted twice. */
			this.publisher.abort(prepared);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
		this.localAcceptanceFence = null;
		this.clearBufferScratch();
	}

	/** Pairs a reserved sequence with the transaction queued for publication. */
	private record LocalEnqueue(long sequence, AeronReplicationPublisher.TransactionMetadata metadata,
		ByteBuffer[] buffers, int bufferCount)
	{
	}

	/** Collects channel buffers into the reusable writer-owned array. */
	private int collectBuffers(final Binary data)
	{
		if (data == null) throw new NullPointerException("data");
		this.bufferScratchCount = 0;
		data.iterateChannelChunks(channel ->
		{
			if (channel == null) throw new IllegalStateException("Serializer returned a null channel");
			for (final ByteBuffer buffer : channel.buffers())
			{
				if (buffer == null) throw new IllegalStateException("Serializer returned a null channel buffer");
				if (this.bufferScratchCount == this.bufferScratch.length)
				{
					this.bufferScratch = Arrays.copyOf(this.bufferScratch, this.bufferScratch.length * 2);
				}
				this.bufferScratch[this.bufferScratchCount++] = buffer;
			}
		});
		return this.bufferScratchCount;
	}

	/** Drops references to the last transaction's source buffers. */
	private void clearBufferScratch()
	{
		Arrays.fill(this.bufferScratch, 0, this.bufferScratchCount, null);
		this.bufferScratchCount = 0;
	}

	synchronized void markCommittingUncertain(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		try
		{
			this.notifyState(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, prepared.sequence(),
				prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), -1);
		}
		catch (final RuntimeException | Error failure)
		{
			this.publisher.failClosed();
			throw failure;
		}
	}

	private void notifyState(final AeronReplicationCheckpoint.State state, final long sequence,
		final int dataLength, final int dataChunkCount, final int dataCrc32c, final long position)
	{
		this.listener.onState(state, sequence, dataLength, dataChunkCount, dataCrc32c, position);
	}

	/** Closes the publisher owned by this coordinator. */
	@Override
	public void close()
	{
		this.dispose();
	}

	/** Releases the publisher owned by this coordinator. */
	public synchronized void dispose()
	{
		/* ENQUEUE_THEN_ARCHIVE may be interrupted after the Store accepted data
		 * but before the Archive terminal marker was published.  Never turn that
		 * local acceptance into an ABORT during shutdown: close the publication
		 * without a terminal marker and keep the durable ENQUEUED fence so the next
		 * process reseeds instead of silently discarding Store data. */
		if (this.localAcceptanceFence != null && this.publisher.hasPendingTransaction())
		{
			this.publisher.closeWithoutAbort();
			this.publisher.releaseCoordinator(this);
			return;
		}
		/* Keep the coordinator claim until publication shutdown has completed.  If an
		 * abort/close offer is transiently unavailable, releasing first would allow a
		 * second coordinator to claim the same publisher while this one still owns a
		 * pending sequence. */
		this.publisher.close();
		this.publisher.releaseCoordinator(this);
	}
}
