package org.eclipse.datagrid.cache.clustered.aeron.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Aeron
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.datagrid.cache.clustered.types.TimestampsRegionUpdateMessage;
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.memory.XMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Publishes clustered-cache invalidations on one Aeron publication.
 *
 * <p>This sender has the same contract as the Kafka sender: it is synchronous
 * and fails the local cache operation when the invalidation cannot be
 * accepted. A listener callback offers the frame and waits until the
 * publication accepts it, retrying transient back pressure and reconnection
 * with an idle strategy for at most the configured publish timeout. A closed
 * or exhausted publication fails immediately, and a timeout fails with
 * {@link CacheEntryListenerException}, exactly as a Kafka send failure does.
 * Nothing is silently dropped.</p>
 *
 * <p>Observability: published and offer-retry counters are package-private
 * test seams; the public surface is the dispose debug log. The sequence is
 * shared per node identity when {@code node-id} is configured, and per
 * provider otherwise, so receivers never see false gaps when several providers
 * share one node id.</p>
 */
abstract class AeronClusteredCacheMessageSender implements ClusteredCacheMessageSender<Object, Object>
{
	private static final Logger logger = LoggerFactory.getLogger(AeronClusteredCacheMessageSender.class);
	/** A scratch buffer larger than this is released after the frame is offered. */
	private static final int MAX_RETAINED_SCRATCH_BYTES = 64 * 1024;

	private final AeronClusteredCacheResources resources;
	private final byte[] senderId;
	private final AtomicLong sequence;
	private final Serializer<byte[]> serializer;
	private final long publishTimeoutNanos;
	private final int maxPayloadBytes;
	/* JCache listeners may publish from several threads, and BackoffIdleStrategy
	 * is stateful, so each caller thread owns its idle strategy. The scratch is
	 * an off-heap direct buffer per thread, so no frame bytes are staged on the
	 * JVM heap. */
	private final ThreadLocal<IdleStrategy> idleStrategy = ThreadLocal.withInitial(BackoffIdleStrategy::new);
	private final ThreadLocal<UnsafeBuffer> scratch = new ThreadLocal<>();
	private final LongAdder published = new LongAdder();
	private final LongAdder offerRetries = new LongAdder();

	/* The publication is looked up once and cached; the resources monitor is
	 * only entered on the first publish of this sender. */
	private volatile ConcurrentPublication publication;
	private volatile boolean disposed;

	private AeronClusteredCacheMessageSender(
		final AeronClusteredCacheResources resources,
		final byte[] senderId,
		final AtomicLong sequence,
		final Serializer<byte[]> serializer,
		final long publishTimeoutNanos,
		final int maxPayloadBytes
	)
	{
		this.resources = resources;
		this.senderId = senderId;
		this.sequence = sequence;
		this.serializer = serializer;
		this.publishTimeoutNanos = publishTimeoutNanos;
		this.maxPayloadBytes = maxPayloadBytes;
	}

	/**
	 * Creates the sender that turns timestamp cache events into cluster messages.
	 *
	 * @param resources shared Aeron resources for this node
	 * @param senderId sender identity used so the node ignores its own frames
	 * @param sequence sequence source shared by every sender of this identity
	 * @param serializer serializer shared with the receiver
	 * @param publishTimeoutNanos maximum time to wait for the publication to accept a frame
	 * @param maxPayloadBytes maximum accepted serialized payload size
	 * @return timestamp-cache sender
	 */
	static ClusteredCacheMessageSender<Object, Object> UpdateTimestamps(
		final AeronClusteredCacheResources resources,
		final byte[] senderId,
		final AtomicLong sequence,
		final Serializer<byte[]> serializer,
		final long publishTimeoutNanos,
		final int maxPayloadBytes
	)
	{
		return new UpdateTimestamps(resources, senderId, sequence, serializer, publishTimeoutNanos, maxPayloadBytes);
	}

	/** Converts one cache event into a cluster update message. */
	protected abstract TimestampsRegionUpdateMessage createMessage(CacheEntryEvent<?, ?> event);

	/** Serializes and publishes each event in order. */
	protected void handleEvents(final Iterable<CacheEntryEvent<?, ?>> events)
		throws CacheEntryListenerException
	{
		for (final CacheEntryEvent<?, ?> event : events)
		{
			final byte[] payload;
			try
			{
				payload = this.serializer.serialize(this.createMessage(event));
			}
			catch (final Exception failure)
			{
				throw new CacheEntryListenerException("Failed to serialize clustered-cache message", failure);
			}
			this.publish(payload);
		}
	}

	private void publish(final byte[] payload)
	{
		if (this.disposed)
		{
			throw new CacheEntryListenerException("Aeron clustered-cache sender is disposed",
				new IllegalStateException("disposed"));
		}
		if (payload.length > this.maxPayloadBytes)
		{
			throw new CacheEntryListenerException(
				"Aeron clustered-cache payload of " + payload.length + " bytes exceeds the configured limit of " +
					this.maxPayloadBytes);
		}

		final UnsafeBuffer buffer = this.scratchFor(payload.length);
		final int length = AeronClusteredCacheMessageCodec.encode(
			buffer, this.senderId, this.sequence.getAndIncrement(), payload);
		final ConcurrentPublication publication = this.ensurePublication();
		try
		{
			this.offer(publication, buffer, length);
		}
		finally
		{
			/* A single oversized invalidation must not pin a large off-heap
			 * buffer on this listener thread forever. */
			if (buffer.capacity() > MAX_RETAINED_SCRATCH_BYTES)
			{
				this.releaseScratch();
			}
		}
	}

	/**
	 * Returns a thread-local off-heap buffer that fits the given payload,
	 * growing (and releasing the previous) buffer when needed.
	 */
	private UnsafeBuffer scratchFor(final int payloadLength)
	{
		final int required = AeronClusteredCacheMessageCodec.HEADER_LENGTH + payloadLength;
		UnsafeBuffer buffer = this.scratch.get();
		if (buffer == null || buffer.capacity() < required)
		{
			this.releaseScratch();
			buffer = new UnsafeBuffer(XMemory.allocateDirectNative(required));
			this.scratch.set(buffer);
		}
		return buffer;
	}

	/** Releases the thread-local off-heap scratch buffer. */
	private void releaseScratch()
	{
		final UnsafeBuffer buffer = this.scratch.get();
		if (buffer != null)
		{
			this.scratch.remove();
			XMemory.deallocateDirectByteBuffer(buffer.byteBuffer());
		}
	}

	private ConcurrentPublication ensurePublication()
	{
		ConcurrentPublication publication = this.publication;
		if (publication == null)
		{
			/* The resources monitor guards the single addPublication: without it,
			 * concurrent listener threads could each create and leak a
			 * publication. */
			synchronized (this.resources)
			{
				publication = this.publication;
				if (publication == null)
				{
					try
					{
						publication = this.resources.publication();
					}
					catch (final RuntimeException failure)
					{
						throw new CacheEntryListenerException(
							"Aeron clustered-cache publication is unavailable", failure);
					}
					this.publication = publication;
				}
			}
		}
		return publication;
	}

	/**
	 * Offers a frame, waiting for connection and back pressure up to the
	 * publish timeout. The caller is a synchronous JCache listener, so this
	 * method either succeeds or throws; it never returns without publishing.
	 */
	private void offer(final ConcurrentPublication publication, final UnsafeBuffer buffer, final int length)
	{
		final long deadline = saturatingDeadline(this.publishTimeoutNanos);
		try
		{
			while (true)
			{
				if (Thread.currentThread().isInterrupted())
				{
					throw new CacheEntryListenerException(
						"Aeron clustered-cache publish was interrupted", new InterruptedException());
				}
				final long result = publication.offer(buffer, 0, length);
				if (result > 0)
				{
					this.published.increment();
					return;
				}
				if (result == Publication.CLOSED)
				{
					throw new CacheEntryListenerException("Aeron clustered-cache publication is closed");
				}
				if (result == Publication.MAX_POSITION_EXCEEDED)
				{
					throw new CacheEntryListenerException(
						"Aeron clustered-cache publication reached its maximum position");
				}
				this.offerRetries.increment();
				if (System.nanoTime() >= deadline)
				{
					throw new CacheEntryListenerException(
						"Aeron clustered-cache publication did not accept the invalidation within " +
							(this.publishTimeoutNanos / 1_000_000L) + " ms (result=" + result + ")");
				}
				/* idle(0): a positive count means "work was done" and resets the
				 * strategy without parking, which would busy-spin for the whole
				 * publish timeout. */
				this.idleStrategy.get().idle(0);
			}
		}
		catch (final IllegalArgumentException failure)
		{
			throw new CacheEntryListenerException("Aeron clustered-cache frame is invalid", failure);
		}
	}

	@Override
	public void dispose()
	{
		if (this.disposed)
		{
			return;
		}
		this.disposed = true;
		this.resources.closePublication();
		logger.debug("Disposed Aeron clustered-cache sender: published={}, offerRetries={}",
			this.published.sum(), this.offerRetries.sum());
	}

	/** Package-private test seam for the published counter. */
	long published()
	{
		return this.published.sum();
	}

	/** Package-private test seam for the offer-retry counter. */
	long offerRetries()
	{
		return this.offerRetries.sum();
	}

	/**
	 * Returns the publish deadline, saturating at {@link Long#MAX_VALUE} so an
	 * extreme configured timeout cannot overflow the addition and fail
	 * immediately.
	 */
	private static long saturatingDeadline(final long timeoutNanos)
	{
		final long now = System.nanoTime();
		try
		{
			return Math.addExact(now, timeoutNanos);
		}
		catch (final ArithmeticException ignored)
		{
			return Long.MAX_VALUE;
		}
	}

	/** Converts timestamp cache events into cluster update messages. */
	private static final class UpdateTimestamps extends AeronClusteredCacheMessageSender
		implements CacheEntryCreatedListener<Object, Object>, CacheEntryUpdatedListener<Object, Object>
	{
		private UpdateTimestamps(
			final AeronClusteredCacheResources resources,
			final byte[] senderId,
			final AtomicLong sequence,
			final Serializer<byte[]> serializer,
			final long publishTimeoutNanos,
			final int maxPayloadBytes
		)
		{
			super(resources, senderId, sequence, serializer, publishTimeoutNanos, maxPayloadBytes);
		}

		@Override
		public void onCreated(final Iterable<CacheEntryEvent<?, ?>> events)
			throws CacheEntryListenerException
		{
			this.handleEvents(events);
		}

		@Override
		public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> events)
			throws CacheEntryListenerException
		{
			this.handleEvents(events);
		}

		@Override
		protected TimestampsRegionUpdateMessage createMessage(final CacheEntryEvent<?, ?> event)
		{
			return TimestampsRegionUpdateMessage.fromEvent(event);
		}
	}
}
