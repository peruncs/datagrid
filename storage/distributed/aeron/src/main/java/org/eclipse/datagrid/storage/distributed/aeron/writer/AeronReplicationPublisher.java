package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.ExclusivePublication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
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
 * Publishes ordered DataGrid transactions using prepare, commit, and abort
 * markers. Envelope frames are encoded into one reusable direct Agrona buffer;
 * Store data is copied from the caller's ByteBuffer sequence directly into
 * each outgoing frame and is never flattened into a heap transaction array.
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
	/* Publisher methods are synchronized and do not re-enter publication callbacks;
	 * this CRC instance is therefore confined to the owning publisher thread. */
	private final ThreadLocal<CRC32C> dataCrc = ThreadLocal.withInitial(CRC32C::new);
	private FailedPrepare failedPrepare;
	private PreparedTransaction pendingTransaction;
	private boolean failed;
	private boolean closed;


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

	/** Publishes and commits one transaction. */
	synchronized long publishTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers)
	{
		return this.commit(this.prepareTransaction(dictionary, dataBuffers));
	}

	/**
	 * Publishes dictionary and Store-data chunks and returns a token whose commit
	 * marker is pending. Callers must commit or close the token; a failed prepare
	 * emits an abort marker when possible and fails the publisher closed.
	 */
	synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers)
	{
		this.ensureOpen();
		this.failedPrepare = null;
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
		try
		{
			if (dictionary != null && dictionary.length != 0)
			{
				this.publishChunks(sequence, AeronReplicationEnvelope.Kind.TYPE_DICTIONARY,
					new UnsafeBuffer(dictionary), 0, dictionary.length);
			}
			final int dataCrc32c = this.publishDataChunks(sequence, dataBuffers, dataLength);
			final PreparedTransaction prepared = new PreparedTransaction(this, sequence, dataLength, dataChunks,
				dataCrc32c);
			this.pendingTransaction = prepared;
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

	record FailedPrepare(long sequence, int dataLength, int dataChunkCount, int crc32c)
	{
	}

	/** Publishes Store data directly from the caller's buffer sequence. */
	private int publishDataChunks(final long sequence, final ByteBuffer[] sources, final int length)
	{
		final CRC32C crc = this.dataCrc.get();
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
                    throw new IllegalArgumentException("data buffer is incomplete");
				final int amount = Math.min(source.remaining(), chunkLength - copied);
				final ByteBuffer crcSlice = source.duplicate();
				crcSlice.limit(crcSlice.position() + amount);
				crc.update(crcSlice);
				this.envelopeBuffer.putBytes(AeronReplicationEnvelope.HEADER_LENGTH + copied,
					source, source.position(), amount);
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
		final CRC32C crc = this.dataCrc.get();
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

	/** Publishes the commit marker for a prepared transaction. */
	synchronized long commit(final PreparedTransaction transaction)
	{
		this.ensureOpen();
		this.validate(transaction);
		if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
		try
		{
			final long commitPosition = this.offerMarker(transaction.sequence, AeronReplicationEnvelope.Kind.COMMIT,
				transaction.dataLength, transaction.dataChunkCount, transaction.crc32c);
			final long recordedPosition = this.commitPositionAwaiter.applyAsLong(commitPosition);
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

	/** Publishes an abort marker after local Store rejection. */
	synchronized long abort(final PreparedTransaction transaction)
	{
		this.ensureOpen();
		this.validate(transaction);
		if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
		try
		{
			final long position = this.offerMarker(transaction.sequence, AeronReplicationEnvelope.Kind.ABORT,
				transaction.dataLength, transaction.dataChunkCount, 0);
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

	synchronized long publishAbort(final long sequence, final int dataLength, final int chunkCount)
	{
		this.ensureOpen();
		try
		{
			return this.offerMarker(sequence, AeronReplicationEnvelope.Kind.ABORT,
				dataLength, Math.max(1, chunkCount), 0);
		}
		catch (final RuntimeException failure)
		{
			this.failed = true;
			throw failure;
		}
	}

	private void publishChunks(final long sequence, final AeronReplicationEnvelope.Kind kind,
		final org.agrona.DirectBuffer bytes, final int sourceOffset, final int length)
	{
		if (length == 0)
		{
			if (kind == AeronReplicationEnvelope.Kind.STORE_BINARY)
			{
				this.offerEncoded(sequence, kind, 0, 0, 1, 0, 0, bytes, 0, 0);
			}
			return;
		}
		final int count = this.chunkCount(length);
		for (int index = 0, offset = 0; offset < length; index++)
		{
			final int chunkLength = Math.min(this.configuration.chunkSize(), length - offset);
			this.offerEncoded(sequence, kind, length, index, count, offset, 0, bytes,
				sourceOffset + offset, chunkLength);
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

	/** Fails this publisher closed after an external durability/checkpoint failure. */
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
			}
			catch (final RuntimeException abortFailure)
			{
				failure = abortFailure;
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

		/**
		 * Aborts an abandoned prepared transaction so its sequence is terminated
		 * in the replication log. Closing after commit/abort is a no-op.
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
			}
		}
	}
}
