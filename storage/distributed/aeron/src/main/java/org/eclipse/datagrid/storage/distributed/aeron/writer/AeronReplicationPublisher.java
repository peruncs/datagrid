package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.ExclusivePublication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
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
	private final AeronOfferRetryer offerer;
	private final int maxMessageLength;
	private final AeronReplicationConfiguration configuration;
	private final UUID clusterId;
	private final long epoch;
	private final AtomicLong nextSequence;
	private final AutoCloseable closeAction;
	private final LongUnaryOperator commitPositionAwaiter;
	private final ByteBuffer envelopeStorage;
	private final UnsafeBuffer envelopeBuffer;
	private final UnsafeBuffer emptyBuffer = new UnsafeBuffer(new byte[0]);
	/* Publisher methods are synchronized, so one reusable CRC instance is enough
	 * and avoids retaining a ThreadLocal value on every caller thread. */
	private final CRC32C dataCrc = new CRC32C();
	private FailedPrepare failedPrepare;
	private PreparedTransaction pendingTransaction;
	private boolean failed;
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


	AeronReplicationPublisher(final ExclusivePublication publication,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final boolean closePublication)
	{
		this(publication::offer, configuration.maxMessageLength(), configuration, clusterId, epoch, initialSequence,
			closePublication ? publication : () -> { }, LongUnaryOperator.identity());
	}

	AeronReplicationPublisher(final ExclusivePublication publication,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final boolean closePublication, final LongUnaryOperator commitPositionAwaiter)
	{
		this(publication::offer, configuration.maxMessageLength(), configuration, clusterId, epoch, initialSequence,
			closePublication ? publication : () -> { }, commitPositionAwaiter);
	}

	AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence)
	{
		this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, () -> { });
	}

	AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
		final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
		final long initialSequence, final LongUnaryOperator commitPositionAwaiter)
	{
		this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, () -> { },
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
		this.nextSequence = new AtomicLong(initialSequence);
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
	 */
	synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers)
	{
		this.ensureOpen();
		this.failedPrepare = null;
		crashPoint("BEFORE_PREPARE", this.nextSequence.get());
		final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
		final long totalDataLength = totalRemaining(dataBuffers);
		if (totalDataLength > (long)this.configuration.maxTransactionBytes() - dictionaryLength)
		{
			throw new IllegalArgumentException("Store transaction exceeds maxTransactionBytes");
		}
		final int dataLength = (int)totalDataLength;
		final long sequence = this.nextSequence.get();
		if (sequence < 0 || sequence == Long.MAX_VALUE)
		{
			throw new IllegalStateException("Aeron replication sequence space is exhausted");
		}
		this.nextSequence.set(sequence + 1);
		final int dataChunks = chunkCount(dataLength);
		return this.prepareReserved(dictionary, dataBuffers, sequence, dataLength, dataChunks, -1, false);
	}

	/**
	 * Completes preparation for a sequence reserved before a durable fence was
	 * written. The explicit sequence prevents a crash between the fence write
	 * and publication from leaving two different sequence numbers in the log.
	 */
	synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers,
		final long reservedSequence, final TransactionMetadata metadata)
	{
		this.ensureOpen();
		this.failedPrepare = null;
		if (metadata == null || reservedSequence < 0 || reservedSequence == Long.MAX_VALUE ||
			this.nextSequence.get() != reservedSequence + 1)
		{
			throw new IllegalStateException("reserved replication sequence is no longer current");
		}
		final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
		if (metadata.dataLength() != totalRemaining(dataBuffers) ||
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
				this.publishChunks(sequence, new UnsafeBuffer(dictionary), dictionary.length);
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
			this.failed = true;
			throw failure;
		}
	}

	/** Returns metadata for the most recent failed prepare, if any. */
	synchronized FailedPrepare failedPrepare()
	{
		return this.failedPrepare;
	}

	/** Returns the next unreserved sequence for diagnostics and recovery checks. */
	synchronized long nextSequence()
	{
		return this.nextSequence.get();
	}

	/** Reserves the next sequence so a durable fence and publication share it. */
	synchronized long reserveSequence()
	{
		this.ensureOpen();
		final long sequence = this.nextSequence.get();
		if (sequence < 0 || sequence == Long.MAX_VALUE)
		{
			throw new IllegalStateException("Aeron replication sequence space is exhausted");
		}
		this.nextSequence.set(sequence + 1);
		return sequence;
	}

	/** Releases a reservation when the local Store rejects the fenced write. */
	synchronized void releaseReservedSequence(final long sequence)
	{
		if (this.nextSequence.get() != sequence + 1)
		{
			throw new IllegalStateException("replication sequence reservation is no longer current");
		}
		this.nextSequence.set(sequence);
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

	record TransactionMetadata(int dataLength, int dataChunkCount, int crc32c)
	{
	}

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
				0, 0, 1, 0, 0, this.emptyBuffer, 0, 0);
			return 0;
		}

		final int count = this.chunkCount(length);
		int sourceIndex = 0;
		ByteBuffer source = sources[sourceIndex] == null ? null : sources[sourceIndex].duplicate();
		int logicalOffset = 0;
		for (int chunkIndex = 0; chunkIndex < count; chunkIndex++)
		{
			final int chunkLength = Math.min(this.configuration.chunkSize(), length - logicalOffset);
			int copied = 0;
			while (copied < chunkLength)
			{
				while (source != null && !source.hasRemaining())
				{
					if (++sourceIndex >= sources.length) throw new IllegalArgumentException("data buffer length changed");
					source = sources[sourceIndex] == null ? null : sources[sourceIndex].duplicate();
				}
				if (source == null)
				{
					throw new IllegalArgumentException("data buffer is incomplete");
				}
				final int amount = Math.min(source.remaining(), chunkLength - copied);
				final ByteBuffer crcSlice = source.duplicate();
				crcSlice.limit(crcSlice.position() + amount);
				crc.update(crcSlice);
				this.envelopeBuffer.putBytes(
					AeronReplicationEnvelope.HEADER_LENGTH + copied, source, source.position(), amount);
				source.position(source.position() + amount);
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
				final ByteBuffer slice = source.duplicate();
				slice.limit(slice.position() + amount);
				crc.update(slice);
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
	synchronized long abort(final PreparedTransaction transaction)
	{
		this.ensureOpen();
		this.validate(transaction);
		if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
		try
		{
			final long position = this.offerMarker(transaction.sequence, AeronReplicationEnvelope.Kind.ABORT,
				transaction.dataLength, transaction.dataChunkCount, 0);
			crashPoint("AFTER_ABORT_OFFERED", transaction.sequence);
			transaction.terminal = true;
			this.pendingTransaction = null;
			return position;
		}
		catch (final RuntimeException | Error failure)
		{
			transaction.terminal = true;
			this.failed = true;
			throw failure;
		}
	}

	/** Emits an abort marker for a sequence that has not produced a token. */
	synchronized void publishAbort(final long sequence, final int dataLength, final int chunkCount)
	{
		this.ensureOpen();
		try
		{
			this.offerMarker(sequence, AeronReplicationEnvelope.Kind.ABORT,
				dataLength, Math.max(1, chunkCount), 0);
		}
		catch (final RuntimeException failure)
		{
			this.failed = true;
			throw failure;
		}
	}

	private void publishChunks(final long sequence, final org.agrona.DirectBuffer bytes, final int length)
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
			payload == null ? this.emptyBuffer : payload, payloadOffset, payloadChunkLength);
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
			commitCrc32c, this.emptyBuffer, 0, 0);
	}

	/** Advances the next sequence when an external cursor or promotion supplies a newer index. */
	synchronized void synchronizeNextSequence(final long next)
	{
		if (next < 0) throw new IllegalArgumentException("next sequence must be non-negative");
		this.nextSequence.accumulateAndGet(next, Math::max);
	}

	private void validate(final PreparedTransaction transaction)
	{
		if (transaction == null || transaction.owner != this) throw new IllegalArgumentException("unknown prepared transaction");
	}

	private void ensureOpen()
	{
		if (this.closed) throw new IllegalStateException("Aeron publisher is closed");
		if (this.failed) throw new IllegalStateException("Aeron publisher is failed closed");
	}

	/** Prevents further writes after an external durability or checkpoint failure. */
	synchronized void failClosed()
	{
		this.failed = true;
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
		final PreparedTransaction pending;
		final boolean abortPending;
		synchronized (this)
		{
			if (this.closed) return;
			this.closed = true;
			pending = this.pendingTransaction;
			this.pendingTransaction = null;
			abortPending = pending != null && !pending.terminal;
			if (abortPending)
			{
				/* Mark terminal before leaving the monitor so a concurrent token close
				 * cannot publish a second abort while this bounded offer is in flight. */
				pending.terminal = true;
			}
		}
		RuntimeException failure = null;
		if (abortPending)
		{
			try
			{
				this.offerMarker(pending.sequence, AeronReplicationEnvelope.Kind.ABORT,
					pending.dataLength, pending.dataChunkCount, 0);
				pending.invokeAbortAction();
			}
			catch (final RuntimeException abortFailure)
			{
				failure = abortFailure;
			}
			catch (final Error abortFailure)
			{
				failure = new IllegalStateException("pending transaction abort callback failed", abortFailure);
			}
		}
		try
		{
			this.closeAction.close();
		}
		catch (final Exception closeFailure)
		{
			final RuntimeException wrapped = new IllegalStateException("failed to close Aeron publication", closeFailure);
			if (failure == null) failure = wrapped;
			else failure.addSuppressed(wrapped);
		}
		finally
		{
			BufferUtil.free(this.envelopeStorage);
		}
		if (failure != null)
		{
			throw failure;
		}
	}

	static final class PreparedTransaction implements AutoCloseable
	{
		private final AeronReplicationPublisher owner;
		private final long sequence;
		private final int dataLength;
		private final int dataChunkCount;
		private final int crc32c;
		private volatile Runnable abortAction;
		private boolean abortActionInvoked;
		private volatile boolean terminal;

		private PreparedTransaction(final AeronReplicationPublisher owner, final long sequence, final int dataLength,
			final int dataChunkCount, final int crc32c)
		{
			this.owner = owner;
			this.sequence = sequence;
			this.dataLength = dataLength;
			this.dataChunkCount = dataChunkCount;
			this.crc32c = crc32c;
		}

		static PreparedTransaction checkpointOnly(final AeronReplicationPublisher owner, final long sequence,
			final int dataLength, final int dataChunkCount, final int crc32c)
		{
			return new PreparedTransaction(owner, sequence, dataLength, dataChunkCount, crc32c);
		}

		long sequence() { return this.sequence; }
		int dataLength() { return this.dataLength; }
		int dataChunkCount() { return this.dataChunkCount; }
		/** Returns the CRC32C of the Store binary carried by this transaction. */
		int dataCrc32c() { return this.crc32c; }

		/** Registers the callback notified after this token is aborted; a late registration
		 * is invoked immediately when publisher shutdown already completed the abort. */
		void onAbort(final Runnable action)
		{
			if (action == null) throw new NullPointerException("action");
			boolean invoke;
			synchronized (this.owner)
			{
				this.abortAction = action;
				invoke = this.terminal && this.owner.closed && !this.abortActionInvoked;
				if (invoke) this.abortActionInvoked = true;
			}
			if (invoke) action.run();
		}

		private void invokeAbortAction()
		{
			final Runnable action;
			synchronized (this.owner)
			{
				if (this.abortActionInvoked || this.abortAction == null) return;
				this.abortActionInvoked = true;
				action = this.abortAction;
			}
			action.run();
		}

		/**
		 * Aborts an abandoned transaction so its sequence is terminated in the log.
		 * Closing after commit or abort has no effect.
		 */
		@Override
		public void close()
		{
			synchronized (this.owner)
			{
				if (this.terminal) return;
				if (this.owner.closed || this.owner.failed)
				{
					this.terminal = true;
					return;
				}
				this.owner.abort(this);
				this.invokeAbortAction();
			}
		}
	}
}
