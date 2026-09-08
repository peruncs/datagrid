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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/** Shared shutdown sequence for the live and Archive-backed Aeron readers. */
final class AeronReaderLifecycle
{
	private AeronReaderLifecycle()
	{
	}

	/**
	 * Runs the common subscription duty cycle used by live and Archive readers.
	 * The poller owns the subscription and returns its fragment count; idle
	 * handling is centralized so stop-at-tail and park behavior cannot diverge.
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
	 * Stops polling, waits briefly for a different polling thread, and closes the
	 * owned subscription. The close callback is always invoked after the wait.
	 *
	 * @param active reader running flag
	 * @param thread reader polling thread, or {@code null}
	 * @param closeSubscription callback that closes the reader subscription
	 */
	static void stopAndClose(
		final AtomicBoolean active,
		final Thread thread,
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
					thread.join(5_000L);
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
				}
			}
		}
		try
		{
			closeSubscription.run();
		}
		catch (final RuntimeException closeFailure)
		{
			failure = closeFailure;
		}
		if (failure != null)
		{
			throw failure;
		}
	}
}
