package peruncs.datagrid.storage.distributed.aeron.reader;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import peruncs.datagrid.storage.distributed.types.ReplicationRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
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
		final IdleStrategy idleStrategy = new BackoffIdleStrategy();
		while (active.get() && !stopPolling.getAsBoolean())
		{
			if (timedOut.getAsBoolean())
			{
				try
				{
					onTimeout.run();
				}
				finally
				{
					active.set(false);
				}
				return;
			}
			final int work = poller.getAsInt();
			/* A live image only means the subscription is attached; it does not mean
			 * the current poll drained the replay.  Stop at the boundary only after an
			 * actually idle poll, otherwise a prepared chunk can be left without its
			 * terminal marker. */
			if (work == 0 && stopWhenIdle.getAsBoolean())
			{
				active.set(false);
				return;
			}
			if (timedOut.getAsBoolean())
			{
				try
				{
					onTimeout.run();
				}
				finally
				{
					active.set(false);
				}
				return;
			}
			idleStrategy.idle(work);
		}
	}

	/**
	 * Stops polling, waits up to five seconds for a different polling thread, and
	 * closes the subscription only after that thread has exited. On timeout the
	 * subscription remains open so the caller can retry without a use-after-close.
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
		stopAndClose(active, thread, stopped, closeSubscription,
			java.util.concurrent.TimeUnit.SECONDS.toNanos(5L));
	}

	/**
	 * Variant with an explicit wait budget used by deterministic lifecycle tests.
	 * Production callers should use the five-second overload above.
	 */
	static void stopAndClose(
		final AtomicBoolean active,
		final Thread thread,
		final CountDownLatch stopped,
		final Runnable closeSubscription,
		final long timeoutNanos
	)
	{
		if (timeoutNanos <= 0L) throw new IllegalArgumentException("timeoutNanos must be positive");
		if (active == null || closeSubscription == null)
			throw new NullPointerException("active and closeSubscription");
		if (thread != null && stopped == null)
			throw new NullPointerException("stopped latch is required for a polling thread");
		active.set(false);
		RuntimeException failure = null;
		if (thread != null)
		{
			if (thread == Thread.currentThread())
			{
				/* A delivery callback may request shutdown, but the polling thread must
				 * leave its duty cycle before its subscription is closed. Let the caller
				 * retry after this thread reaches its finally block. */
				throw new IllegalStateException("cannot dispose Aeron reader from its polling thread");
			}
			thread.interrupt();
			final long deadline = ReplicationRetry.deadlineNanos(timeoutNanos);
			try
			{
				final long remaining = ReplicationRetry.remainingNanos(deadline);
				if (remaining <= 0L || !stopped.await(remaining, java.util.concurrent.TimeUnit.NANOSECONDS))
				{
					failure = new IllegalStateException("Aeron reader polling thread did not stop");
				}
				else
				{
					/* The latch is released from the polling thread's finally block. Use
					 * the same deadline for the tiny interval between countDown() and
					 * Thread termination; an unbounded join defeats the shutdown budget. */
					final long joinNanos = ReplicationRetry.remainingNanos(deadline);
					if (joinNanos <= 0L)
					{
						failure = new IllegalStateException("Aeron reader polling thread did not stop");
					}
					else
					{
						final long joinMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(joinNanos);
						thread.join(Math.max(1L, joinMillis));
						if (thread.isAlive())
						{
							failure = new IllegalStateException("Aeron reader polling thread did not stop");
						}
					}
				}
			}
			catch (final InterruptedException interrupted)
			{
				Thread.currentThread().interrupt();
				failure = new IllegalStateException("interrupted while stopping Aeron reader", interrupted);
			}
		}
		/* Do not close a subscription while a polling thread is still able to
		 * access it. The caller retains ownership and may retry after it stops. */
		if (failure != null)
		{
			throw failure;
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
