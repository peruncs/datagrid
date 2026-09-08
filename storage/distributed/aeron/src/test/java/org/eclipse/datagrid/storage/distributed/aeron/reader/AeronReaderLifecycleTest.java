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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AeronReaderLifecycleTest
{
	@Test
	void closesSubscriptionWhenPollingThreadWaitIsInterrupted()
	{
		final AtomicBoolean active = new AtomicBoolean(true);
		final AtomicBoolean closed = new AtomicBoolean();
		final Thread pollingThread = new Thread(() ->
		{
			try
			{
				Thread.sleep(30_000L);
			}
			catch (final InterruptedException ignored)
			{
				// Expected shutdown path.
			}
		});
		pollingThread.start();

		AeronReaderLifecycle.stopAndClose(active, pollingThread, () -> closed.set(true));

		assertFalse(active.get());
		assertFalse(pollingThread.isAlive());
		org.junit.jupiter.api.Assertions.assertTrue(closed.get());
	}

	@Test
	void closesSubscriptionEvenWhenTheCloseCallbackFails()
	{
		final AtomicBoolean active = new AtomicBoolean(true);
		final IllegalStateException expected = new IllegalStateException("close failed");

		final IllegalStateException actual = assertThrows(
			IllegalStateException.class,
			() -> AeronReaderLifecycle.stopAndClose(active, null, () -> { throw expected; })
		);

		assertSame(expected, actual);
		assertFalse(active.get());
	}

	@Test
	void sharedPollingLoopStopsOnlyAfterAnIdlePoll()
	{
		final AtomicBoolean active = new AtomicBoolean(true);
		final AtomicInteger polls = new AtomicInteger();

		AeronReaderLifecycle.runPollingLoop(
			active, () -> false, () -> polls.incrementAndGet() == 1 ? 1 : 0,
			() -> polls.get() >= 2, () -> false, () -> { });

		assertFalse(active.get());
		assertEquals(2, polls.get());
	}

	@Test
	void sharedPollingLoopReportsArchiveTailTimeout()
	{
		final AtomicBoolean active = new AtomicBoolean(true);
		final AtomicBoolean timedOut = new AtomicBoolean();

		AeronReaderLifecycle.runPollingLoop(
			active, () -> false, () -> 0, () -> false, () -> true, () -> timedOut.set(true));

		assertFalse(active.get());
		assertTrue(timedOut.get());
	}
}
