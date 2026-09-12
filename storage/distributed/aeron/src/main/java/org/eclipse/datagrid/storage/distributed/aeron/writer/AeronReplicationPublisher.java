package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;
import java.util.zip.CRC32C;

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
 * Publishes one ordered transaction at a time.
 *
 * <p>Data chunks are prepared first. A commit marker makes the complete
 * transaction visible; an abort marker closes a rejected or abandoned one.
 * The publisher stages frames in one direct buffer so a large Store binary is
 * not flattened into a second heap copy.</p>
 */
final class AeronReplicationPublisher implements AutoCloseable
{
	private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer(new byte[0]);
	private final AeronOfferRetryer offerer;
	private final int maxMessageLength;
	private final AeronReplicationConfiguration configuration;
	private final UUID clusterId;
	private final long epoch;
	/* All sequence operations are protected by this publisher's monitor. Keeping
	 * the value primitive avoids an allocation and makes the ownership rule
	 * explicit instead of implying lock-free access. */
	private long nextSequence;
	private final AutoCloseable closeAction;
	private final LongUnaryOperator commitPositionAwaiter;
	private final ByteBuffer envelopeStorage;
	private final UnsafeBuffer envelopeBuffer;
	/* Publisher methods are synchronized, so one reusable CRC instance is enough
	 * and avoids retaining a ThreadLocal value on every caller thread. */
	private final CRC32C dataCrc = new CRC32C();
	private FailedPrepare failedPrepare;
	private PreparedTransaction pendingTransaction;
	private Object coordinatorOwner;
	private volatile boolean failed;
	private boolean closeRequested;
	private boolean closeInProgress;
	private boolean envelopeFreed;
	private boolean closed;

	/** Installs a package-private fault seam used by deterministic crash tests. */
	static void setCrashHook(final BiConsumer<String, Long> hook) { CrashHook.install(hook); }

	static void clearCrashHook()
	{
		CrashHook.clear();
	}

	private static void crashPoint(final String name, final long sequence)
	{
		CrashHook.invoke(name, sequence);
	}

	private static AeronOfferRetryer.Offerer offerer(final ExclusivePublication publication)
	{
		return new AeronOfferRetryer.Offerer()
		{
			@Override
			public long offer(final org.agrona.DirectBuffer buffer, final int offset, final int length)
			{
				return publication.offer(buffer, offset, length);
			}

			@Override
			public boolean isConnected()
			{
				return publication.isConnected();
			}
		};
	}


	AeronReplicationPublisher(final ExclusivePublication publication,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final boolean closePublication)
	{
		this(offerer(publication), configuration.maxMessageLength(), configuration, clusterId, epoch, initialSequence,
			closePublication ? publication : null, LongUnaryOperator.identity());
	}

	AeronReplicationPublisher(final ExclusivePublication publication,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final boolean closePublication, final LongUnaryOperator commitPositionAwaiter)
	{
		this(offerer(publication), configuration.maxMessageLength(), configuration, clusterId, epoch, initialSequence,
			closePublication ? publication : null, commitPositionAwaiter);
	}

	AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence)
	{
		this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, (AutoCloseable)null);
	}

	AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final LongUnaryOperator commitPositionAwaiter)
	{
		this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, null,
			commitPositionAwaiter);
	}

	private AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final AutoCloseable closeAction)
	{
		this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, closeAction,
			LongUnaryOperator.identity());
	}

	private AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final AutoCloseable closeAction, final LongUnaryOperator commitPositionAwaiter)
	{
		if (offerer == null || configuration == null || clusterId == null || commitPositionAwaiter == null ||
			initialSequence < 0 || initialSequence == Long.MAX_VALUE || maxMessageLength <= 0 ||
			maxMessageLength > configuration.maxMessageLength())
		{
			throw new IllegalArgumentException("invalid publisher configuration");
		}
		this.offerer = new AeronOfferRetryer(offerer, configuration);
		this.maxMessageLength = maxMessageLength;
		this.configuration = configuration;
		this.clusterId = clusterId;
		this.epoch = epoch;
		this.nextSequence = initialSequence;
		this.closeAction = closeAction;
		this.commitPositionAwaiter = commitPositionAwaiter;
		this.envelopeStorage = ByteBuffer.allocateDirect(maxMessageLength);
		this.envelopeBuffer = new UnsafeBuffer(this.envelopeStorage);
	}

	/** Publishes one transaction and its terminal commit marker. */
	synchronized long publishTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers)
	{
		return this.commit(this.prepareTransaction(dictionary, dataBuffers));
	}

	/**
	 * Publishes dictionary and Store-data chunks and returns a token whose commit
	 * marker is pending. The caller must commit or close the token. If publishing
	 * fails after a sequence is reserved, the publisher attempts an abort and
	 * then fails closed so a later write cannot skip the damaged sequence.
	 * This low-level overload is reserved for direct publisher users; coordinator
	 * writes must use the explicit-sequence overload so their durable fence and
	 * publication cannot diverge.
	 *
	 * @param dictionary optional type dictionary bytes
	 * @param dataBuffers Store binary buffers; positions are not changed
	 * @return a token that must be committed or closed
	 */
	synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers)
	{
		this.ensureOpen();
		if (this.coordinatorOwner != null)
		{
			throw new IllegalStateException(
				"coordinator-owned publisher requires the explicit reserved-sequence preparation path");
		}
		this.failedPrepare = null;
		this.ensureNoPendingTransaction();
		crashPoint("BEFORE_PREPARE", this.nextSequence);
		final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
		final long totalDataLength = totalRemaining(dataBuffers);
		if (totalDataLength > (long)this.configuration.maxTransactionBytes() - dictionaryLength)
		{
			throw new IllegalArgumentException("Store transaction exceeds maxTransactionBytes");
		}
		final int dataLength = (int)totalDataLength;
		final long sequence = this.nextSequence;
		if (sequence < 0 || sequence == Long.MAX_VALUE)
		{
			throw new IllegalStateException("Aeron replication sequence space is exhausted");
		}
		this.nextSequence = sequence + 1;
		final int dataChunks = chunkCount(dataLength);
		return this.prepareReserved(dictionary, dataBuffers, sequence, dataLength, dataChunks, -1, false);
	}

	/**
	 * Completes preparation for a sequence reserved before a durable fence was
	 * written. The explicit sequence prevents a crash between the fence write
	 * and publication from leaving two different sequence numbers in the log.
	 * The metadata must describe the same buffers; it is checked again before
	 * publication so a caller cannot reuse a fence for different data.
	 * This explicit reservation is part of the durable-fence contract and must
	 * not be replaced with an independent sequence allocation.
	 *
	 * @param dictionary optional type dictionary bytes
	 * @param dataBuffers Store binary buffers whose positions are not changed
	 * @param reservedSequence sequence returned by {@link #reserveSequence()}
	 * @param metadata length, chunk count, and CRC captured for the fence
	 * @return a token that must be committed or closed
	 * @throws IllegalStateException if the reservation is no longer current
	 * @throws IllegalArgumentException if the metadata does not match the buffers
	 */
	synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers,
		final long reservedSequence, final TransactionMetadata metadata)
	{
		this.ensureOpen();
		this.failedPrepare = null;
		this.ensureNoPendingTransaction();
		if (metadata == null || reservedSequence < 0 || reservedSequence == Long.MAX_VALUE ||
			this.nextSequence != reservedSequence + 1)
		{
			throw new IllegalStateException("reserved replication sequence is no longer current");
		}
		final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
		if (metadata.dataLength() != totalRemaining(dataBuffers) ||
			metadata.dataLength() < 0 || metadata.dataChunkCount() != this.chunkCount(metadata.dataLength()) ||
			(long)metadata.dataLength() > (long)this.configuration.maxTransactionBytes() - dictionaryLength)
		{
			throw new IllegalArgumentException("transaction metadata does not match Store data");
		}
		crashPoint("BEFORE_PREPARE", reservedSequence);
		return this.prepareReserved(dictionary, dataBuffers, reservedSequence, metadata.dataLength(),
			metadata.dataChunkCount(), metadata.crc32c(), true);
	}

	private PreparedTransaction prepareReserved(final byte[] dictionary, final ByteBuffer[] dataBuffers,
		final long sequence, final int dataLength, final int dataChunks, final int expectedCrc32c,
		final boolean verifyExpectedCrc)
	{
		try
		{
			if (dictionary != null && dictionary.length != 0)
			{
				this.publishDictionaryChunks(sequence, new UnsafeBuffer(dictionary), dictionary.length);
				crashPoint("AFTER_DICTIONARY_CHUNKS", sequence);
			}
			final int dataCrc32c = this.publishDataChunks(sequence, dataBuffers, dataLength);
			if (verifyExpectedCrc && dataCrc32c != expectedCrc32c)
			{
				throw new IllegalArgumentException("transaction data changed after durable fence");
			}
			crashPoint("AFTER_DATA_CHUNKS", sequence);
			final PreparedTransaction prepared = new PreparedTransaction(this, sequence, dataLength, dataChunks,
				dataCrc32c);
			this.pendingTransaction = prepared;
			crashPoint("AFTER_PREPARE", sequence);
			return prepared;
		}
		catch (final RuntimeException | Error failure)
		{
			/* AFTER_PREPARE can throw after the token has become the pending
			 * transaction. The recovery abort below is the terminal decision for
			 * that token; clear the owner reference even when the abort offer fails
			 * so close() cannot emit a second terminal marker. */
			final PreparedTransaction pending = this.pendingTransaction;
			int failedCrc32c = 0;
			try
			{
				failedCrc32c = this.computeDataCrc(dataBuffers, dataLength);
			}
			catch (final RuntimeException | Error crcFailure)
			{
				failure.addSuppressed(crcFailure);
			}
			this.failedPrepare = new FailedPrepare(sequence, dataLength, dataChunks, failedCrc32c);
			try
			{
				// Clear any transaction prefix that reached the log. If the publication
				// itself is gone this fails as well, and recovery must retain the tail.
				this.offerMarker(sequence, AeronReplicationEnvelope.Kind.ABORT, dataLength, dataChunks, 0);
				this.failed = true;
				crashPoint("AFTER_PREPARE_FAILURE_ABORT_OFFERED", sequence);
			}
			catch (final RuntimeException | Error abortFailure)
			{
				failure.addSuppressed(abortFailure);
			}
			if (pending != null)
			{
				pending.terminal = true;
				this.pendingTransaction = null;
			}
			this.failed = true;
			throw failure;
		}
	}

	/** Returns metadata for the most recent failed prepare, if any. */
	synchronized FailedPrepare failedPrepare()
	{
		return this.failedPrepare;
	}

	/**
	 * Returns the next unreserved sequence for diagnostics and recovery checks.
	 * This method does not reserve the value; use {@link #reserveSequence()} when
	 * a durable fence must carry the same sequence as a later publication.
	 */
	synchronized long nextSequence()
	{
		return this.nextSequence;
	}

	/**
	 * Reserves the next sequence so a durable fence and its later publication
	 * share one sequence number. The reservation must either be used by the
	 * explicit-sequence preparation method or released after a local rejection.
	 * A caller must not publish another transaction while this reservation is
	 * outstanding.
	 *
	 * @return the sequence reserved for the next explicit-sequence preparation
	 */
	synchronized long reserveSequence()
	{
		this.ensureOpen();
		this.ensureNoPendingTransaction();
		final long sequence = this.nextSequence;
		if (sequence < 0 || sequence == Long.MAX_VALUE)
		{
			throw new IllegalStateException("Aeron replication sequence space is exhausted");
		}
		this.nextSequence = sequence + 1;
		return sequence;
	}

	/** Releases a reservation when the local Store rejects the fenced write. */
	synchronized void releaseReservedSequence(final long sequence)
	{
		if (sequence < 0 || sequence == Long.MAX_VALUE || this.nextSequence != sequence + 1)
		{
			throw new IllegalStateException("replication sequence reservation is no longer current");
		}
		this.nextSequence = sequence;
	}

	/** Computes the metadata used by an in-flight local-write record. */
	synchronized TransactionMetadata transactionMetadata(final ByteBuffer[] dataBuffers)
	{
		final long length = totalRemaining(dataBuffers);
		if (length > this.configuration.maxTransactionBytes())
		{
			throw new IllegalArgumentException("Store transaction exceeds maxTransactionBytes");
		}
		final int dataLength = (int)length;
		return new TransactionMetadata(dataLength, this.chunkCount(dataLength),
			this.computeDataCrc(dataBuffers, dataLength));
	}

	/** Metadata retained while a transaction moves through writer states. */
	record TransactionMetadata(int dataLength, int dataChunkCount, int crc32c)
	{
	}

	/** Source metadata retained when preparing a transaction fails. */
	record FailedPrepare(long sequence, int dataLength, int dataChunkCount, int crc32c)
	{
	}

	/** Publishes Store data directly from the caller's buffer sequence. */
	private int publishDataChunks(final long sequence, final ByteBuffer[] sources, final int length)
	{
		final CRC32C crc = this.dataCrc;
		crc.reset();
		if (length == 0)
		{
			this.offerEncoded(sequence, AeronReplicationEnvelope.Kind.STORE_BINARY,
				0, 0, 1, 0, 0, EMPTY_BUFFER, 0, 0);
			return 0;
		}

		final int count = this.chunkCount(length);
		int sourceIndex = 0;
		ByteBuffer source = sources[sourceIndex].duplicate();
		int logicalOffset = 0;
		for (int chunkIndex = 0; chunkIndex < count; chunkIndex++)
		{
			final int chunkLength = Math.min(this.configuration.chunkSize(), length - logicalOffset);
			int copied = 0;
			while (copied < chunkLength)
			{
				while (!source.hasRemaining())
				{
					if (++sourceIndex >= sources.length) throw new IllegalArgumentException("data buffer length changed");
					source = sources[sourceIndex].duplicate();
				}
				final int amount = Math.min(source.remaining(), chunkLength - copied);
				final int sourcePosition = source.position();
				final int sourceLimit = source.limit();
				source.limit(sourcePosition + amount);
				crc.update(source);
				source.limit(sourceLimit);
				this.envelopeBuffer.putBytes(
					AeronReplicationEnvelope.HEADER_LENGTH + copied, source, sourcePosition, amount);
				source.position(sourcePosition + amount);
				copied += amount;
			}
			this.offerEncoded(sequence, AeronReplicationEnvelope.Kind.STORE_BINARY,
				length, chunkIndex, count, logicalOffset, 0, this.envelopeBuffer,
				AeronReplicationEnvelope.HEADER_LENGTH, chunkLength);
			logicalOffset += chunkLength;
		}
		return (int)crc.getValue();
	}

	private int computeDataCrc(final ByteBuffer[] sources, final int length)
	{
		final CRC32C crc = this.dataCrc;
		crc.reset();
		int remaining = length;
		for (final ByteBuffer sourceBuffer : sources)
		{
			final ByteBuffer source = sourceBuffer.duplicate();
			final int amount = Math.min(remaining, source.remaining());
			if (amount > 0)
			{
				final int sourceLimit = source.limit();
				source.limit(source.position() + amount);
				crc.update(source);
				source.limit(sourceLimit);
				remaining -= amount;
			}
			if (remaining == 0) break;
		}
		if (remaining != 0) throw new IllegalArgumentException("data buffer length changed");
		return (int)crc.getValue();
	}

	/** Publishes the commit marker and waits for the configured durability boundary. */
	synchronized long commit(final PreparedTransaction transaction)
	{
		this.ensureOpen();
		this.validate(transaction);
		if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
		try
		{
			crashPoint("BEFORE_COMMIT_OFFER", transaction.sequence);
			final long commitPosition = this.offerMarker(transaction.sequence, AeronReplicationEnvelope.Kind.COMMIT,
				transaction.dataLength, transaction.dataChunkCount, transaction.crc32c);
			crashPoint("AFTER_COMMIT_OFFER", transaction.sequence);
			final long recordedPosition = this.commitPositionAwaiter.applyAsLong(commitPosition);
			crashPoint("AFTER_COMMIT_RECORDED", transaction.sequence);
			transaction.terminal = true;
			this.pendingTransaction = null;
			return recordedPosition;
		}
		catch (final RuntimeException | Error failure)
		{
			// Once the commit marker was offered it may still be delivered or
			// recorded.  Publishing an abort after an archive acknowledgement
			// timeout would create two terminal markers for one sequence and can
			// make readers apply a transaction that the writer later considers
			// failed.  Fail closed and require recovery instead.
			transaction.terminal = true;
			this.pendingTransaction = null;
			this.failed = true;
			throw failure;
		}
	}

	/** Publishes an abort marker after the local Store rejects the transaction. */
	long abort(final PreparedTransaction transaction)
	{
		final long position;
		synchronized (this)
		{
			position = this.abortLocked(transaction);
		}
		try
		{
			/* Notify outside the publisher monitor. Checkpoint listeners may acquire the
			 * provider lock, and invoking them while holding this lock would create a
			 * publisher/provider lock-order cycle during shutdown. */
			transaction.invokeAbortAction(position);
			return position;
		}
			catch (final RuntimeException | Error failure)
			{
				/* The marker may already have been offered when the Archive acknowledgement
				 * failed. Surface that attempted abort with its known-or-unknown position so
				 * the coordinator can persist an uncertain state instead of losing evidence. */
				try
				{
					transaction.invokeAbortAction(transaction.abortPosition);
				}
				catch (final RuntimeException | Error callbackFailure)
				{
					failure.addSuppressed(callbackFailure);
				}
				synchronized (this)
				{
				this.failed = true;
			}
			throw failure;
		}
	}

	/** Performs the marker offer while the publisher monitor is held. */
	private long abortLocked(final PreparedTransaction transaction)
	{
		this.ensureOpen();
		this.validate(transaction);
		if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
		try
		{
				final long offeredPosition = this.offerMarker(transaction.sequence, AeronReplicationEnvelope.Kind.ABORT,
					transaction.dataLength, transaction.dataChunkCount, 0);
				transaction.abortAttempted = true;
				final long position = this.commitPositionAwaiter.applyAsLong(offeredPosition);
			transaction.abortPosition = position;
			crashPoint("AFTER_ABORT_OFFERED", transaction.sequence);
			transaction.terminal = true;
			this.pendingTransaction = null;
			return position;
		}
		catch (final RuntimeException | Error failure)
		{
			transaction.terminal = true;
			this.pendingTransaction = null;
			this.failed = true;
			throw failure;
		}
	}

	private void publishDictionaryChunks(final long sequence, final org.agrona.DirectBuffer bytes, final int length)
	{
		if (length == 0)
		{
			return;
		}
		final int count = this.chunkCount(length);
		for (int index = 0, offset = 0; offset < length; index++)
		{
			final int chunkLength = Math.min(this.configuration.chunkSize(), length - offset);
			this.offerEncoded(sequence, AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, length, index, count,
				offset, 0, bytes, offset, chunkLength);
			offset += chunkLength;
		}
	}

	private int chunkCount(final int length)
	{
		return Math.max(1, (int)((length + (long)this.configuration.chunkSize() - 1L) /
			this.configuration.chunkSize()));
	}

	private long offerEncoded(final long sequence, final AeronReplicationEnvelope.Kind kind,
		final int payloadLength, final int chunkIndex, final int chunkCount, final int chunkOffset,
		final int commitCrc32c, final org.agrona.DirectBuffer payload, final int payloadOffset,
		final int payloadChunkLength)
	{
		final int encodedLength = AeronReplicationEnvelope.encode(this.envelopeBuffer, 0, this.clusterId,
			this.epoch, sequence, kind, payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c,
			payload == null ? EMPTY_BUFFER : payload, payloadOffset, payloadChunkLength);
		if (encodedLength > this.maxMessageLength)
		{
			throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
		}
		return this.offerer.offer(this.envelopeBuffer, encodedLength);
	}

	private long offerMarker(final long sequence, final AeronReplicationEnvelope.Kind kind,
		final int payloadLength, final int chunkCount, final int commitCrc32c)
	{
		return this.offerEncoded(sequence, kind, payloadLength, 0, Math.max(1, chunkCount), 0,
			commitCrc32c, EMPTY_BUFFER, 0, 0);
	}

	/** Advances the next sequence when an external cursor or promotion supplies a newer index. */
	synchronized void synchronizeNextSequence(final long next)
	{
		if (next < 0) throw new IllegalArgumentException("next sequence must be non-negative");
		this.ensureNoPendingTransaction();
		if (next > this.nextSequence) this.nextSequence = next;
	}

	private void validate(final PreparedTransaction transaction)
	{
		if (transaction == null || transaction.owner != this) throw new IllegalArgumentException("unknown prepared transaction");
	}

	private void ensureOpen()
	{
		if (this.closed || this.closeRequested) throw new IllegalStateException("Aeron publisher is closed or closing");
		if (this.failed) throw new IllegalStateException("Aeron publisher is failed closed");
	}

	private void ensureNoPendingTransaction()
	{
		if (this.pendingTransaction != null && !this.pendingTransaction.terminal)
		{
			throw new IllegalStateException("an Aeron prepared transaction is already pending");
		}
	}

	/** Returns whether an uncommitted prepared transaction owns this publisher. */
	synchronized boolean hasPendingTransaction()
	{
		return this.pendingTransaction != null && !this.pendingTransaction.terminal;
	}

	/** Claims the publisher for its single write coordinator. */
	synchronized void claimCoordinator(final Object coordinator)
	{
		this.ensureOpen();
		if (this.coordinatorOwner != null && this.coordinatorOwner != coordinator)
		{
			throw new IllegalStateException("an Aeron publisher already has a write coordinator");
		}
		this.coordinatorOwner = coordinator;
	}

	/** Releases the coordinator claim during owner disposal. */
	synchronized void releaseCoordinator(final Object coordinator)
	{
		if (this.coordinatorOwner == coordinator) this.coordinatorOwner = null;
	}

	/** Prevents further writes after an external durability or checkpoint failure. */
	synchronized void failClosed()
	{
		this.failed = true;
	}

	/** Returns whether a publication failure made this writer fail closed. */
	synchronized boolean isFailed()
	{
		return this.failed;
	}

	private static long totalRemaining(final ByteBuffer[] buffers)
	{
		if (buffers == null) throw new NullPointerException("dataBuffers");
		long length = 0;
		for (final ByteBuffer buffer : buffers)
		{
			if (buffer == null) throw new NullPointerException("dataBuffers contains null");
			length = Math.addExact(length, buffer.remaining());
		}
		return length;
	}

	/** Aborts a pending transaction when possible and releases the publication. */
	@Override
	public void close()
	{
		this.closeInternal(true);
	}

	/**
	 * Releases the publication without manufacturing an ABORT marker. This is
	 * used only when the Store has already accepted the transaction: the durable
	 * in-flight fence must remain unresolved so restart fails closed instead of
	 * pretending that a local acceptance was rejected.
	 */
	void closeWithoutAbort()
	{
		this.closeInternal(false);
	}

	private void closeInternal(final boolean abortPendingOnClose)
	{
		final PreparedTransaction pending;
		final boolean abortPending;
		synchronized (this)
		{
			if (this.closed) return;
			if (this.closeInProgress)
			{
				throw new IllegalStateException("Aeron publisher close is already in progress");
			}
			this.closeInProgress = true;
			this.closeRequested = true;
			pending = this.pendingTransaction;
			abortPending = abortPendingOnClose && pending != null && !pending.terminal;
			if (!abortPending && pending != null && !pending.terminal)
			{
				/* The caller explicitly chose the fail-closed shutdown path.  Detach the
				 * token before closing the publication, but leave any coordinator fence on
				 * disk to force reseed on the next writer process. */
				pending.terminal = true;
				this.pendingTransaction = null;
				this.failed = true;
			}
		}
		RuntimeException failure = null;
		Error fatalFailure = null;
		boolean abortCompleted = !abortPending;
		if (abortPending)
		{
			boolean abortMarkerOffered = false;
			try
			{
					final long offeredPosition = this.offerMarker(pending.sequence, AeronReplicationEnvelope.Kind.ABORT,
					pending.dataLength, pending.dataChunkCount, 0);
					abortMarkerOffered = true;
					pending.abortAttempted = true;
					final long position = this.commitPositionAwaiter.applyAsLong(offeredPosition);
				pending.abortPosition = position;
				abortCompleted = true;
				pending.invokeAbortAction(position);
			}
			catch (final RuntimeException abortFailure)
			{
				/* A failure before Aeron accepted the marker is retryable: keep the
				 * pending token and publication alive so a transient NOT_CONNECTED or
				 * back-pressure condition can be retried. Once the marker was accepted,
				 * its durability is ambiguous and the writer must fail closed. */
				if (abortMarkerOffered)
				{
					this.failed = true;
					try
					{
						/* The marker was accepted but its durable position is unknown. A
						 * coordinator callback records COMMITTING_UNCERTAIN and keeps restart
						 * on the safe reseed path. */
						pending.invokeAbortAction(pending.abortPosition);
					}
					catch (final RuntimeException | Error callbackFailure)
					{
						abortFailure.addSuppressed(callbackFailure);
					}
					/* Aeron accepted the marker, so a second close must never emit a
					 * contradictory terminal marker.  Its recorded position is unknown,
					 * therefore the checkpoint callback is intentionally not invoked and
					 * recovery remains fail-closed. */
					abortCompleted = true;
				}
				else
				{
					synchronized (this)
					{
						this.closeRequested = false;
					}
				}
				failure = abortFailure;
			}
			catch (final Error abortFailure)
			{
				this.failed = true;
				/* Fatal errors are not retryable.  Release the publication even when
				 * the marker was not accepted; retaining it would leak the native
				 * envelope storage during shutdown. */
				abortCompleted = true;
				/* Preserve fatal JVM errors for the caller.  They are still followed by
				 * best-effort publication cleanup below, but wrapping an OOME or
				 * StackOverflowError as an ordinary state failure obscures the cause. */
				fatalFailure = abortFailure;
			}
			finally
			{
				if (abortCompleted)
				{
					synchronized (this)
					{
						if (this.pendingTransaction == pending)
						{
							pending.terminal = true;
							this.pendingTransaction = null;
						}
					}
				}
			}
		}
		if (abortCompleted)
		{
			try
			{
				if (this.closeAction != null) this.closeAction.close();
			}
			catch (final Exception closeFailure)
			{
				final RuntimeException wrapped = new IllegalStateException("failed to close Aeron publication", closeFailure);
				if (failure == null) failure = wrapped;
				else failure.addSuppressed(wrapped);
			}
			catch (final Error closeFailure)
			{
				if (fatalFailure == null) fatalFailure = closeFailure;
				else fatalFailure.addSuppressed(closeFailure);
			}
			finally
			{
				boolean free;
				synchronized (this)
				{
					free = !this.envelopeFreed;
					this.envelopeFreed = true;
				}
				if (free) BufferUtil.free(this.envelopeStorage);
			}
		}
		try
		{
			if (fatalFailure != null)
			{
				if (failure != null) fatalFailure.addSuppressed(failure);
				throw fatalFailure;
			}
			if (failure != null)
			{
				throw failure;
			}
			synchronized (this)
			{
				this.closed = true;
			}
		}
		finally
		{
			synchronized (this)
			{
				this.closeInProgress = false;
			}
		}
	}

	/** Closeable handle that aborts an unfinished prepared transaction. */
	static final class PreparedTransaction implements AutoCloseable
	{
		private final AeronReplicationPublisher owner;
		private final long sequence;
		private final int dataLength;
		private final int dataChunkCount;
		private final int crc32c;
		private volatile java.util.function.LongConsumer abortAction;
		private boolean abortActionInvoked;
		private boolean abortAttempted;
		private volatile boolean terminal;
		private volatile long abortPosition = Aeron.NULL_VALUE;

		private PreparedTransaction(final AeronReplicationPublisher owner, final long sequence, final int dataLength,
			final int dataChunkCount, final int crc32c)
		{
			this.owner = owner;
			this.sequence = sequence;
			this.dataLength = dataLength;
			this.dataChunkCount = dataChunkCount;
			this.crc32c = crc32c;
		}

		long sequence() { return this.sequence; }
		int dataLength() { return this.dataLength; }
		int dataChunkCount() { return this.dataChunkCount; }
		/** Returns the CRC32C of the Store binary carried by this transaction. */
		int dataCrc32c() { return this.crc32c; }

		/**
	 * Registers a callback for an abort whose publication has been attempted.
	 * The callback runs synchronously on the caller that completes the abort; a
	 * late registration is invoked immediately when the publisher already
	 * completed that path. A position of {@code -1} means the marker was offered
	 * but its durable Archive position is unknown.
		 *
		 * @param action receives the recorded abort position
		 */
		void onAbort(final java.util.function.LongConsumer action)
		{
			if (action == null) throw new NullPointerException("action");
			boolean invoke;
			synchronized (this.owner)
			{
				if (this.abortAction != null && this.abortAction != action)
				{
					throw new IllegalStateException("abort callback is already registered");
				}
				this.abortAction = action;
				invoke = this.terminal && this.abortAttempted && !this.abortActionInvoked;
				if (invoke) this.abortActionInvoked = true;
			}
			if (invoke) action.accept(this.abortPosition);
		}

		private void invokeAbortAction(final long position)
		{
			final java.util.function.LongConsumer action;
			synchronized (this.owner)
			{
				if (!this.abortAttempted || this.abortActionInvoked || this.abortAction == null) return;
				this.abortActionInvoked = true;
				action = this.abortAction;
			}
			action.accept(position);
		}

		/**
		 * Aborts an abandoned transaction so its sequence is terminated in the log.
		 * Closing after commit or abort has no effect.
		 */
		@Override
		public void close()
		{
			final long position;
			synchronized (this.owner)
			{
				if (this.terminal) return;
				if (this.owner.closeRequested || this.owner.closed)
				{
					return;
				}
				if (this.owner.failed)
				{
					this.terminal = true;
					if (this.owner.pendingTransaction == this) this.owner.pendingTransaction = null;
					return;
				}
				position = this.owner.abortLocked(this);
			}
			/* Run the callback after releasing the publisher monitor. */
			try
			{
				this.invokeAbortAction(position);
			}
			catch (final RuntimeException | Error failure)
			{
				synchronized (this.owner)
				{
					this.owner.failed = true;
				}
				throw failure;
			}
		}
	}
}
