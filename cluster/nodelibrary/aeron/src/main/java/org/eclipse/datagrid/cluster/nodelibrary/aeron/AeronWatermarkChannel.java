package org.eclipse.datagrid.cluster.nodelibrary.aeron;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Small, latest-value Aeron control stream carrying durable reader progress to
 * the writer. Missing or superseded messages are safe: retention requires the
 * latest watermark from every active reader and watermarks are monotonic.
 */
final class AeronWatermarkChannel implements AutoCloseable
{
	@FunctionalInterface
	interface Receiver
	{
		void accept(DirectBuffer buffer, int offset, int length);
	}

	private final Publication publication;
	private final Subscription subscription;
	private final Receiver receiver;
	private final AtomicReference<byte[]> pending = new AtomicReference<>();
	private final UnsafeBuffer sendBuffer = new UnsafeBuffer(new byte[0]);
	private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
	private final AtomicBoolean running = new AtomicBoolean(true);
	private boolean closed;
	private final Thread worker;

	private AeronWatermarkChannel(
		final Publication publication,
		final Subscription subscription,
		final Receiver receiver
	)
	{
		this.publication = publication;
		this.subscription = subscription;
		this.receiver = receiver;
		this.worker = new Thread(this::run, "eclipse-datagrid-aeron-watermarks");
		this.worker.setDaemon(true);
		this.worker.start();
	}

	static AeronWatermarkChannel writer(
		final Aeron aeron, final String channel, final int streamId, final Receiver receiver)
	{
		return new AeronWatermarkChannel(null, aeron.addSubscription(channel, streamId), receiver);
	}

	static AeronWatermarkChannel reader(final Aeron aeron, final String channel, final int streamId)
	{
		return new AeronWatermarkChannel(aeron.addPublication(channel, streamId), null, null);
	}

	/**
	 * Transfers ownership of an encoded watermark, replacing an older unsent
	 * value with the latest durable progress.
	 */
	synchronized void publish(final byte[] encoded)
	{
		if (encoded == null) throw new NullPointerException("encoded");
		final RuntimeException terminal = this.failure.get();
		if (terminal != null) throw new IllegalStateException("Aeron watermark channel failed", terminal);
		if (!this.running.get()) throw new IllegalStateException("Aeron watermark channel is closed");
		this.pending.set(encoded);
	}

	boolean available()
	{
		return this.running.get() && this.failure.get() == null;
	}

	RuntimeException failure()
	{
		return this.failure.get();
	}

	private void run()
	{
		final IdleStrategy idle = new BackoffIdleStrategy(1, 10, 1, 1_000_000);
		try
		{
			while (this.running.get())
			{
				int work = 0;
				if (this.subscription != null)
				{
					work += this.subscription.poll((buffer, offset, length, header) ->
						this.receiver.accept(buffer, offset, length), 16);
				}
				if (this.publication != null)
				{
					final byte[] value = this.pending.get();
					if (value != null)
					{
						this.sendBuffer.wrap(value);
						final long result = this.publication.offer(this.sendBuffer);
						if (result >= 0 && this.pending.compareAndSet(value, null))
						{
							work++;
						}
						else if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
						{
							throw new IllegalStateException(
								"Aeron watermark publication became terminal: " + result);
						}
						else if (result < 0 && result != Publication.NOT_CONNECTED &&
							result != Publication.BACK_PRESSURED && result != Publication.ADMIN_ACTION)
						{
							throw new IllegalStateException("unknown Aeron watermark offer result: " + result);
						}
					}
				}
				idle.idle(work);
			}
		}
		catch (final RuntimeException failure)
		{
			this.failure.compareAndSet(null, failure);
		}
		catch (final Error failure)
		{
			this.failure.compareAndSet(null,
				new IllegalStateException("Aeron watermark worker failed", failure));
			throw failure;
		}
		finally
		{
			this.running.set(false);
		}
	}

	@Override
	public synchronized void close()
	{
		if (this.closed) return;
		RuntimeException closeFailure = this.failure.get();
		if (this.publication != null)
		{
			final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
			while (this.pending.get() != null && this.failure.get() == null &&
				this.worker.isAlive() && System.nanoTime() < deadline)
			{
				LockSupport.parkNanos(100_000L);
			}
			if (this.pending.get() != null)
			{
				closeFailure = append(closeFailure, new IllegalStateException(
					"Aeron watermark channel could not flush its last durable reader position"));
			}
		}
		this.running.set(false);
		this.worker.interrupt();
		try
		{
			this.worker.join(5_000L);
		}
		catch (final InterruptedException failure)
		{
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted while closing Aeron watermark channel", failure);
		}
		if (this.worker.isAlive())
		{
			throw new IllegalStateException("Aeron watermark channel did not stop");
		}
		try
		{
			if (this.publication != null) this.publication.close();
		}
		catch (final RuntimeException failure)
		{
			closeFailure = append(closeFailure, failure);
		}
		try
		{
			if (this.subscription != null) this.subscription.close();
		}
		catch (final RuntimeException failure)
		{
			closeFailure = append(closeFailure, failure);
		}
		if (closeFailure != null) throw closeFailure;
		this.closed = true;
	}

	private static RuntimeException append(final RuntimeException current, final RuntimeException additional)
	{
		if (current == null) return additional;
		if (current != additional) current.addSuppressed(additional);
		return current;
	}
}
