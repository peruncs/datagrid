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

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.datagrid.cache.clustered.types.TimestampsRegionUpdateMessage;
import org.eclipse.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Consumes clustered-cache invalidations from one Aeron subscription.
 *
 * <p>The receiver is an Agrona {@link Agent} run by an {@link AgentRunner} with
 * a {@link BackoffIdleStrategy}, which keeps the polling thread responsive
 * while spending almost no CPU when the cluster is idle. It polls the
 * subscription, reassembles fragmented frames, ignores frames published by its
 * own sender identity, and deserializes the rest into the neutral acceptor.</p>
 *
 * <p>Self-suppression compares the 16-byte sender identity in the frame
 * against the identity shared by this node's sender and receiver; a matching
 * frame is skipped without copying or deserializing its payload.</p>
 *
 * <p>A malformed or undeserializable frame is logged and skipped rather than
 * terminating the node. Invalidation is best-effort at the receiver: the
 * neutral timestamp region reconciles stale entries from the database, so one
 * corrupt datagram must not stop a node from consuming every later
 * invalidation. The sender, by contrast, fails the local write for parity with
 * Kafka.</p>
 *
 * <p>A receiver is single-use, like the sender and the Kafka receiver: after
 * {@link #dispose()} it cannot be started again. Create a new receiver from
 * the provider when a new lifecycle is needed.</p>
 *
 * <p>Observability: {@link #isRunning()} is the programmatic health surface,
 * and received/self-skipped/malformed counters are reported in the dispose
 * debug log; agent failures are logged through the agent error handler.</p>
 */
final class AeronClusteredCacheMessageReceiver implements ClusteredCacheMessageReceiver, Agent
{
	private static final Logger logger = LoggerFactory.getLogger(AeronClusteredCacheMessageReceiver.class);
	private static final int FRAGMENT_LIMIT = 10;
	private static final String ROLE_NAME = "eclipse-datagrid-cache-invalidation-aeron";

	private final AeronClusteredCacheResources resources;
	private final byte[] senderId;
	private final Serializer<byte[]> serializer;
	private final ClusteredCacheMessageAcceptor messageAcceptor;
	private final int maxPayloadBytes;
	private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
	private final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();
	private final LongAdder received = new LongAdder();
	private final LongAdder selfSkipped = new LongAdder();
	private final LongAdder malformed = new LongAdder();
	private final LongAdder gaps = new LongAdder();
	/* Accessed only on the agent thread. */
	private final Map<AeronClusteredCacheMessageCodec.SenderId, Long> lastSequenceBySender = new HashMap<>();

	private volatile Subscription subscription;
	private volatile AgentRunner runner;
	private volatile Thread agentThread;
	private volatile boolean disposed;
	private volatile RuntimeException agentFailure;
	private final ErrorHandler agentErrorHandler = failure ->
	{
		this.agentFailure = failure instanceof RuntimeException runtime
			? runtime : new IllegalStateException("Aeron receiver agent failed", failure);
		logger.error("Aeron clustered-cache receiver agent failed", failure);
	};

	/**
	 * Creates a receiver for one provider.
	 *
	 * @param resources shared Aeron resources for this node
	 * @param senderId sender identity used to ignore this node's own frames
	 * @param serializer serializer shared with the sender
	 * @param messageAcceptor target for accepted invalidations
	 * @param maxPayloadBytes maximum accepted serialized payload size
	 */
	AeronClusteredCacheMessageReceiver(
		final AeronClusteredCacheResources resources,
		final byte[] senderId,
		final Serializer<byte[]> serializer,
		final ClusteredCacheMessageAcceptor messageAcceptor,
		final int maxPayloadBytes
	)
	{
		this.resources = resources;
		this.senderId = senderId;
		this.serializer = serializer;
		this.messageAcceptor = messageAcceptor;
		this.maxPayloadBytes = maxPayloadBytes;
	}

	/**
	 * Starts the polling agent and its daemon thread.
	 *
	 * @throws IllegalStateException when the receiver was already started or disposed
	 */
	@Override
	public synchronized void start()
	{
		if (this.disposed)
		{
			throw new IllegalStateException("Aeron clustered-cache receiver is disposed");
		}
		if (this.runner != null)
		{
			throw new IllegalStateException("Aeron clustered-cache receiver is already started");
		}
		this.subscription = this.resources.subscription();
		try
		{
			final AgentRunner agentRunner = new AgentRunner(this.idleStrategy, this.agentErrorHandler, null, this);
			this.runner = agentRunner;
			AgentRunner.startOnThread(agentRunner, runnable ->
			{
				final Thread worker = new Thread(runnable, ROLE_NAME);
				worker.setDaemon(true);
				this.agentThread = worker;
				return worker;
			});
		}
		catch (final RuntimeException | Error failure)
		{
			/* A failed startup must not leak the subscription it already created. */
			this.runner = null;
			this.subscription = null;
			try
			{
				this.resources.closeSubscription();
			}
			catch (final RuntimeException closeFailure)
			{
				failure.addSuppressed(closeFailure);
			}
			throw failure;
		}
		logger.debug("Started Aeron clustered-cache receiver agent");
	}

	/**
	 * Polls one duty cycle of reassembled invalidation frames.
	 *
	 * @return number of fragments polled
	 */
	@Override
	public int doWork()
	{
		final Subscription current = this.subscription;
		if (current == null || this.disposed)
		{
			return 0;
		}
		return current.poll(this.assembler, FRAGMENT_LIMIT);
	}

	/** {@inheritDoc} */
	@Override
	public String roleName()
	{
		return ROLE_NAME;
	}

	@Override
	public boolean isRunning()
	{
		return this.runner != null && !this.disposed;
	}

	@Override
	public RuntimeException failure()
	{
		return this.agentFailure;
	}

	/** Package-private test seams for the counters. */
	long received() { return this.received.sum(); }
	long selfSkipped() { return this.selfSkipped.sum(); }
	long gaps() { return this.gaps.sum(); }

	private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
	{
		if (AeronClusteredCacheMessageCodec.senderIdMatches(buffer, offset, length, this.senderId))
		{
			this.selfSkipped.increment();
			return;
		}

		final byte[] payload;
		final long sequence;
		final AeronClusteredCacheMessageCodec.SenderId sender;
		try
		{
			/* Validate the length before reading the sender id so a truncated
			 * foreign fragment cannot read past the frame. */
			sequence = AeronClusteredCacheMessageCodec.sequenceOf(buffer, offset, length);
			sender = AeronClusteredCacheMessageCodec.senderIdOf(buffer, offset);
			payload = AeronClusteredCacheMessageCodec.decodePayload(buffer, offset, length, this.maxPayloadBytes);
		}
		catch (final RuntimeException failure)
		{
			this.malformed.increment();
			logger.error("Discarding malformed Aeron clustered-cache frame of {} bytes", length, failure);
			return;
		}

		final TimestampsRegionUpdateMessage message;
		try
		{
			message = this.serializer.deserialize(payload);
		}
		catch (final RuntimeException failure)
		{
			this.malformed.increment();
			logger.error("Discarding undeserializable Aeron clustered-cache invalidation", failure);
			return;
		}
		try
		{
			this.messageAcceptor.accept(message);
			this.received.increment();
		}
		catch (final RuntimeException failure)
		{
			logger.error("Failed to apply Aeron clustered-cache invalidation", failure);
		}
		finally
		{
			/* Record the sequence whenever the frame was received, even when
			 * applying it failed, so the next frame is not compared against a
			 * stale previous value. */
			this.observeSequence(sender, sequence);
		}
	}

	/**
	 * Tracks the per-sender sequence and reports a gap, which means a burst of
	 * invalidations was lost while this node was down or the sender restarted.
	 * The report is diagnostic: no automatic reconciliation is attempted.
	 */
	private void observeSequence(final AeronClusteredCacheMessageCodec.SenderId sender, final long sequence)
	{
		final Long previous = this.lastSequenceBySender.put(sender, sequence);
		if (previous != null && sequence != previous + 1)
		{
			this.gaps.increment();
			logger.warn("Detected a gap in Aeron clustered-cache invalidations from sender {}: " +
				"expected sequence {} after {}, received {}",
				sender, previous + 1, previous, sequence);
		}
	}

	/**
	 * Stops the polling agent and releases the subscription. Idempotent; the
	 * receiver is single-use and cannot be started again. The agent is stopped
	 * outside the receiver monitor so a slow agent thread cannot block other
	 * lifecycle calls.
	 */
	@Override
	public void dispose()
	{
		final AgentRunner current;
		synchronized (this)
		{
			if (this.disposed)
			{
				return;
			}
			this.disposed = true;
			current = this.runner;
			this.runner = null;
		}
		if (current == null)
		{
			/* Never started: no subscription was created, so the shared resources
			 * must stay open for a sender that may still be requested. */
			return;
		}
		try
		{
			/* AgentRunner.close() interrupts the agent thread and joins it with a
			 * bounded timeout, so a receiver blocked in the message acceptor
			 * cannot stall shutdown forever. */
			current.close();
		}
		finally
		{
			this.subscription = null;
			final Thread thread = this.agentThread;
			if (thread == null || !thread.isAlive())
			{
				this.resources.closeSubscription();
			}
			else
			{
				/* The agent did not stop within the bounded join. Closing the
				 * subscription now would race an in-flight poll on the agent
				 * thread, so leave it open (and logged) instead of corrupting
				 * the subscription. */
				logger.warn("Aeron receiver agent did not stop; leaving the subscription open to avoid a close/poll race");
			}
		}
		logger.debug("Disposed Aeron clustered-cache receiver: received={}, selfSkipped={}, malformed={}, gaps={}",
			this.received.sum(), this.selfSkipped.sum(), this.malformed.sum(), this.gaps.sum());
	}
}
