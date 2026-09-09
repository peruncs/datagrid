package org.eclipse.datagrid.storage.distributed.aeron.reader;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/** Shared polling and shutdown rules for the Aeron readers. */
final class AeronReaderLifecycle
{
	private AeronReaderLifecycle()
	{
	}

	/**
	 * Runs the subscription duty cycle used by both readers. Keeping idle and
	 * stop-at-tail handling here prevents the test reader and the production
	 * reader from acquiring different lifecycle semantics.
	 */
	static void runPollingLoop(
		final AtomicBoolean active,
		final BooleanSupplier stopPolling,
		final IntSupplier poller,
		final BooleanSupplier stopWhenIdle,
		final BooleanSupplier timedOut,
		final Runnable onTimeout
	)
	{
		while (active.get() && !stopPolling.getAsBoolean())
		{
			if (poller.getAsInt() == 0)
			{
				if (stopWhenIdle.getAsBoolean())
				{
					active.set(false);
					break;
				}
				if (timedOut.getAsBoolean())
				{
					onTimeout.run();
					active.set(false);
					break;
				}
				LockSupport.parkNanos(1_000_000L);
			}
		}
	}

	/**
	 * Stops polling, waits up to five seconds for a different polling thread, and
	 * closes the subscription. The callback runs after the wait so a fragment
	 * cannot use a subscription while it is being closed.
	 *
	 * @param active reader running flag
	 * @param thread reader polling thread, or {@code null}
	 * @param stopped latch released by the polling thread on exit
	 * @param closeSubscription callback that closes the reader subscription
	 */
	static void stopAndClose(
		final AtomicBoolean active,
		final Thread thread,
		final CountDownLatch stopped,
		final Runnable closeSubscription
	)
	{
		active.set(false);
		RuntimeException failure = null;
		if (thread != null)
		{
			thread.interrupt();
			if (thread != Thread.currentThread())
			{
				try
				{
					if (!stopped.await(5L, java.util.concurrent.TimeUnit.SECONDS))
					{
						failure = new IllegalStateException("Aeron reader polling thread did not stop");
					}
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
					failure = new IllegalStateException("interrupted while stopping Aeron reader", interrupted);
				}
			}
		}
		try
		{
			closeSubscription.run();
		}
		catch (final RuntimeException closeFailure)
		{
			if (failure == null) failure = closeFailure;
			else failure.addSuppressed(closeFailure);
		}
		if (failure != null)
		{
			throw failure;
		}
	}
}
