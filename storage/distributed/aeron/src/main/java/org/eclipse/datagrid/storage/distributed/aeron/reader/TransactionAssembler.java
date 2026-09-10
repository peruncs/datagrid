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

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Reassembles chunks and releases a Store binary only after commit validation.
 *
 * <p>The production Archive reader and the test-only live reader use this same
 * state machine. That keeps ordering, checksum, and cursor hand-off rules in
 * one place.</p>
 */
final class TransactionAssembler
{
	/* ChunksWrapper requires direct buffers even for an empty binary. */
	private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(1);
	private final AeronReplicationConfiguration configuration;
	private final UUID clusterId;
	private final long epoch;
	private final StorageBinaryDataReceiver receiver;
	private final Runnable transactionResolved;
	private final ReaderDeliveryListener deliveryListener;
	private volatile long lastResolvedSequence;
	/* The next sequence is reserved while the assembler monitor is held. It
	 * closes the gap between accepting a terminal marker and invoking the
	 * receiver callback (which deliberately runs outside the monitor). */
	private long nextExpectedSequence;
	private volatile long lastResolvedPosition = -1;
	/* The commit/abort witness for the last resolved sequence. A resumed
	 * assembler has no witness until it resolves one message locally. These
	 * fields are accessed only on the subscription owner thread; cursorSnapshot()
	 * is the synchronized cross-thread boundary. */
	private int lastResolutionCrc32c;
	private boolean hasLastResolutionCrc;
	private AeronReplicationEnvelope.Kind lastResolutionKind;
	private int lastResolutionDataLength;
	private int lastResolutionDataChunkCount;
	private Transaction transaction;
	private volatile RuntimeException failure;
	private final AeronReplicationEnvelope.EnvelopeView envelopeView =
		new AeronReplicationEnvelope.EnvelopeView();
	private final Delivery delivery = new Delivery();

	TransactionAssembler(
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final StorageBinaryDataReceiver receiver
	)
	{
		this(configuration, clusterId, epoch, -1, receiver, () -> { });
	}

	TransactionAssembler(
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver
	)
	{
		this(configuration, clusterId, epoch, initialSequence, receiver, () -> { });
	}

	TransactionAssembler(
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver,
		final Runnable transactionResolved
	)
	{
		this(configuration, clusterId, epoch, initialSequence, receiver, transactionResolved, null);
	}

	TransactionAssembler(
		final AeronReplicationConfiguration configuration,
		final UUID clusterId,
		final long epoch,
		final long initialSequence,
		final StorageBinaryDataReceiver receiver,
		final Runnable transactionResolved,
		final ReaderDeliveryListener deliveryListener
	)
	{
		this.configuration = configuration;
		this.clusterId = clusterId;
		this.epoch = epoch;
		this.receiver = receiver;
		this.transactionResolved = transactionResolved;
		this.deliveryListener = deliveryListener;
		if (initialSequence < -1 || initialSequence == Long.MAX_VALUE)
		{
			throw new IllegalArgumentException("initialSequence must be in [-1, Long.MAX_VALUE)");
		}
		this.lastResolvedSequence = initialSequence;
		this.nextExpectedSequence = initialSequence + 1;
	}

	void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
	{
		final boolean deliver;
		try
		{
			/* A single reusable Delivery carries the detached transaction. Keep its
			 * hand-off serialized so a future multi-consumer poller cannot overwrite
			 * it while the receiver is processing the prior commit. */
			synchronized (this.delivery)
			{
				synchronized (this)
				{
					final AeronReplicationEnvelope.EnvelopeView envelope =
						AeronReplicationEnvelope.decodeView(buffer, offset, length, this.envelopeView);
					if (this.failure != null) return;
					deliver = this.accept(envelope, header == null ? -1 : header.position());
				}
				if (deliver) this.delivery.run();
			}
		}
		catch (final RuntimeException e)
		{
			this.failure(e);
			throw e;
		}
		catch (final Error e)
		{
			this.failure(new IllegalStateException("Aeron envelope delivery failed", e));
			throw e;
		}
	}

	private boolean accept(final AeronReplicationEnvelope.EnvelopeView envelope, final long position)
	{
		if (!envelope.matches(this.clusterId) || this.epoch != envelope.epoch())
		{
			throw new IllegalArgumentException("cluster or epoch mismatch");
		}
		if (envelope.sequence() < this.lastResolvedSequence)
		{
			throw new IllegalStateException("replication sequence regressed: last resolved " +
			this.lastResolvedSequence + ", received " + envelope.sequence());
		}
		if (envelope.sequence() == this.lastResolvedSequence)
		{
			if (envelope.kind() != AeronReplicationEnvelope.Kind.COMMIT &&
				envelope.kind() != AeronReplicationEnvelope.Kind.ABORT)
			{
				throw new IllegalStateException("replayed data for an already resolved sequence " +
					this.lastResolvedSequence);
			}
			if (this.lastResolutionKind != null && envelope.kind() != this.lastResolutionKind)
			{
				throw new IllegalStateException("duplicate terminal has a different kind: expected " +
					this.lastResolutionKind + ", received " + envelope.kind());
			}
			if (this.lastResolutionKind != null &&
				(envelope.payloadLength() != this.lastResolutionDataLength ||
					envelope.chunkCount() != this.lastResolutionDataChunkCount))
			{
				throw new IllegalStateException("duplicate terminal has different transaction metadata");
			}
			if (envelope.kind() == AeronReplicationEnvelope.Kind.COMMIT &&
				this.hasLastResolutionCrc && envelope.commitCrc32c() != this.lastResolutionCrc32c)
			{
				throw new IllegalStateException("duplicate commit has a different payload checksum");
			}
			return false;
		}
		if (envelope.sequence() != this.nextExpectedSequence)
		{
			throw new IllegalStateException(
				"replication sequence gap: expected " + this.nextExpectedSequence +
				", received " + envelope.sequence());
		}
		if (envelope.kind() == AeronReplicationEnvelope.Kind.COMMIT)
		{
			this.commit(envelope, position);
			return true;
		}
		if (envelope.kind() == AeronReplicationEnvelope.Kind.ABORT)
		{
			if (this.transaction != null)
			{
				this.transaction.dispose();
				this.transaction = null;
			}
			this.nextExpectedSequence = envelope.sequence() + 1;
			this.delivery.prepare(null, null, null, envelope.sequence(), position, envelope.payloadLength(),
				envelope.chunkCount(), 0, AeronReplicationEnvelope.Kind.ABORT);
			return true;
		}
		if (envelope.kind() != AeronReplicationEnvelope.Kind.TYPE_DICTIONARY &&
			envelope.kind() != AeronReplicationEnvelope.Kind.STORE_BINARY)
		{
			throw new IllegalArgumentException("non-data envelope on replication data stream: " + envelope.kind());
		}
		if (this.transaction == null)
		{
			this.transaction = new Transaction(envelope.sequence(), this.configuration.maxTransactionBytes());
		}
		if (this.transaction.sequence != envelope.sequence())
		{
			throw new IllegalStateException("interleaved replication transaction");
		}
		this.transaction.add(envelope);
		return false;
	}

	private void commit(final AeronReplicationEnvelope.EnvelopeView envelope, final long position)
	{
		if (this.transaction == null)
		{
			if (envelope.payloadLength() != 0 || envelope.chunkCount() != 0)
			{
				throw new IllegalStateException("commit without data chunks");
			}
			this.nextExpectedSequence = envelope.sequence() + 1;
				this.delivery.prepare(null, null, null, envelope.sequence(), position, envelope.payloadLength(),
					envelope.chunkCount(), envelope.commitCrc32c(), AeronReplicationEnvelope.Kind.COMMIT);
			return;
		}
		if (this.transaction.dataLength != envelope.payloadLength() ||
			this.transaction.dataChunkCount != envelope.chunkCount() ||
			this.transaction.dataNextChunk != this.transaction.dataChunkCount ||
			this.transaction.dataOffset != this.transaction.dataLength ||
			this.transaction.dictionary != null &&
				(this.transaction.dictionaryOffset != this.transaction.dictionaryLength ||
				 this.transaction.dictionaryNextChunk != this.transaction.dictionaryChunkCount) ||
			(this.transaction.dataLength == 0 ? 0 :
				AeronReplicationEnvelope.crc32c(this.transaction.data, 0, this.transaction.dataLength)) !=
				envelope.commitCrc32c())
		{
			this.transaction.dispose();
			this.transaction = null;
			throw new IllegalStateException("commit does not match assembled Store binary");
		}
		final Transaction completed = this.transaction;
		final String dictionary;
		if (completed.dictionary == null)
		{
			dictionary = null;
		}
		else
		{
			final byte[] dictionaryBytes = new byte[completed.dictionaryLength];
			completed.dictionary.getBytes(0, dictionaryBytes);
			dictionary = new String(dictionaryBytes, StandardCharsets.UTF_8);
		}
		final ByteBuffer direct = completed.dataStorage == null
			? EMPTY_BUFFER.duplicate()
			: completed.dataStorage.duplicate();
		if (completed.dataStorage == null)
		{
			/* Keep the direct-buffer contract required by ChunksWrapper while
			 * representing an actually empty Store binary. */
			direct.clear();
			direct.limit(0);
		}
		else
		{
			direct.clear();
			direct.limit(completed.dataLength);
		}
		this.transaction = null;
		this.nextExpectedSequence = envelope.sequence() + 1;
		this.delivery.prepare(dictionary, direct, completed, envelope.sequence(), position, envelope.payloadLength(),
			envelope.chunkCount(), envelope.commitCrc32c(), AeronReplicationEnvelope.Kind.COMMIT);
	}

	private final class Delivery
	{
		private String dictionary;
		private ByteBuffer data;
		private Transaction completed;
		private long sequence;
		private long position;
		private int resolutionDataLength;
		private int resolutionDataChunkCount;
		private int resolutionCrc32c;
		private AeronReplicationEnvelope.Kind resolutionKind;

		void prepare(final String dictionary, final ByteBuffer data, final Transaction completed,
			final long sequence, final long position, final int resolutionDataLength,
			final int resolutionDataChunkCount, final int resolutionCrc32c,
			final AeronReplicationEnvelope.Kind resolutionKind)
		{
			this.dictionary = dictionary;
			this.data = data;
			this.completed = completed;
			this.sequence = sequence;
			this.position = position;
			this.resolutionDataLength = resolutionDataLength;
			this.resolutionDataChunkCount = resolutionDataChunkCount;
			this.resolutionCrc32c = resolutionCrc32c;
			this.resolutionKind = resolutionKind;
		}

		void run()
		{
			try
			{
				if (this.dictionary != null) receiver.receiveTypeDictionary(this.dictionary);
				if (this.data != null)
				{
					final int dataLength = this.completed.dataLength;
					final int dataChunkCount = this.completed.dataChunkCount;
					if (deliveryListener != null)
					{
						deliveryListener.beforeStoreImport(
							this.sequence, this.position, dataLength, dataChunkCount, this.resolutionCrc32c);
					}
					receiver.receiveData(ChunksWrapper.New(this.data));
				}
				synchronized (TransactionAssembler.this)
				{
					lastResolvedSequence = this.sequence;
					lastResolvedPosition = this.position;
					lastResolutionCrc32c = this.resolutionCrc32c;
					hasLastResolutionCrc = true;
					lastResolutionKind = this.resolutionKind;
					lastResolutionDataLength = this.resolutionDataLength;
					lastResolutionDataChunkCount = this.resolutionDataChunkCount;
				}
				transactionResolved.run();
				if (this.data != null && deliveryListener != null)
				{
					deliveryListener.afterStoreImport();
				}
			}
			finally
			{
				if (this.completed != null) this.completed.dispose();
				this.dictionary = null;
				this.data = null;
				this.completed = null;
				this.resolutionKind = null;
			}
		}
	}

	long lastResolvedSequence() { return this.lastResolvedSequence; }
	UUID clusterId() { return this.clusterId; }
	long epoch() { return this.epoch; }
	long lastResolvedPosition() { return this.lastResolvedPosition; }

	synchronized CursorSnapshot cursorSnapshot()
	{
		return new CursorSnapshot(this.lastResolvedSequence, this.lastResolvedPosition);
	}

	record CursorSnapshot(long sequence, long position) { }

	RuntimeException failure() { return this.failure; }

	synchronized void failure(final RuntimeException exception)
	{
		if (this.failure == null)
		{
			this.failure = exception;
			if (this.transaction != null)
			{
				this.transaction.dispose();
				this.transaction = null;
			}
		}
	}

	/** Releases native buffers retained by an incomplete transaction. */
	synchronized void dispose()
	{
		if (this.transaction != null)
		{
			this.transaction.dispose();
			this.transaction = null;
		}
	}

	static final class Transaction
	{
		private final long sequence;
		private UnsafeBuffer dictionary;
		private ByteBuffer dictionaryStorage;
		private UnsafeBuffer data;
		private ByteBuffer dataStorage;
		private final int maxBytes;
		private int dictionaryOffset;
		private int dataOffset;
		private int dictionaryNextChunk;
		private int dataNextChunk;
		private int dictionaryChunkCount = -1;
		private int dataChunkCount = -1;
		private int dictionaryLength;
		private int dataLength;

		Transaction(final long sequence, final int maxBytes)
		{
			this.sequence = sequence;
			this.maxBytes = maxBytes;
		}

		void add(final AeronReplicationEnvelope.EnvelopeView envelope)
		{
			final int payloadLength = envelope.payloadLength();
			final int wireLength = envelope.payloadLengthOnWire;
			if (payloadLength > this.maxBytes) throw new IllegalArgumentException("payload exceeds maxTransactionBytes");
			final boolean dictionary = envelope.kind() == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY;
			if (dictionary && this.dataNextChunk != 0) throw new IllegalStateException("type dictionary follows Store binary chunks");
			if (!dictionary && this.dictionary != null && this.dictionaryNextChunk != this.dictionaryChunkCount)
			{
				throw new IllegalStateException("Store binary chunk arrived before type dictionary completed");
			}
			if (dictionary && (long)payloadLength + this.dataLength > this.maxBytes ||
				!dictionary && this.dictionary != null && (long)payloadLength + this.dictionaryLength > this.maxBytes)
			{
				throw new IllegalArgumentException("dictionary and Store payload exceed maxTransactionBytes");
			}
			final int expectedCount = dictionary ? this.dictionaryChunkCount : this.dataChunkCount;
			if (expectedCount != -1 && expectedCount != envelope.chunkCount()) throw new IllegalArgumentException("chunk count changed within transaction");
			if (envelope.chunkIndex() != (dictionary ? this.dictionaryNextChunk : this.dataNextChunk)) throw new IllegalStateException("unexpected chunk index");
			final int offset = dictionary ? this.dictionaryOffset : this.dataOffset;
			if (envelope.chunkOffset() != offset || (long)envelope.chunkOffset() + wireLength > payloadLength)
			{
				throw new IllegalArgumentException("non-contiguous or oversized chunk");
			}
			if (dictionary)
			{
				if (this.dictionary == null)
				{
					this.dictionaryLength = payloadLength;
					if (payloadLength != 0)
					{
						this.dictionaryStorage = XMemory.allocateDirectNative(payloadLength);
						this.dictionary = new UnsafeBuffer(this.dictionaryStorage);
					}
				}
				if (this.dictionaryLength != payloadLength) throw new IllegalArgumentException("dictionary length changed within transaction");
				if (wireLength != 0)
				{
					if (this.dictionary == null) throw new IllegalStateException("dictionary storage is unavailable");
					this.dictionary.putBytes(offset, envelope.source, envelope.payloadOffset, wireLength);
				}
				this.dictionaryOffset += wireLength;
				this.dictionaryNextChunk++;
				this.dictionaryChunkCount = envelope.chunkCount();
			}
			else
			{
				if (this.data == null && payloadLength != 0)
				{
					this.dataStorage = XMemory.allocateDirectNative(payloadLength);
					this.data = new UnsafeBuffer(this.dataStorage);
				}
				if (this.dataLength != 0 && this.dataLength != payloadLength) throw new IllegalArgumentException("Store binary length changed within transaction");
				this.dataLength = payloadLength;
				if (wireLength != 0)
				{
					if (this.data == null) throw new IllegalStateException("Store data storage is unavailable");
					this.data.putBytes(offset, envelope.source, envelope.payloadOffset, wireLength);
				}
				this.dataOffset += wireLength;
				this.dataNextChunk++;
				this.dataChunkCount = envelope.chunkCount();
			}
		}

		void dispose()
		{
			if (this.dictionaryStorage != null)
			{
				XMemory.deallocateDirectByteBuffer(this.dictionaryStorage);
				this.dictionaryStorage = null;
			}
			if (this.dataStorage != null)
			{
				XMemory.deallocateDirectByteBuffer(this.dataStorage);
				this.dataStorage = null;
			}
			this.dictionary = null;
			this.data = null;
		}
	}
}
