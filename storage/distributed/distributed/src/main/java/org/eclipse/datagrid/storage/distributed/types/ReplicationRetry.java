package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

/**
 * Monotonic-clock helpers shared by bounded transport retry loops.
 *
 * <p>Replication code must use a monotonic deadline. A wall-clock deadline
 * can move backwards during clock correction and can turn a bounded retry into
 * an unbounded wait. The helpers also saturate addition so a very large,
 * explicitly configured timeout cannot wrap into an already-expired deadline.</p>
 */
public final class ReplicationRetry
{
	private ReplicationRetry()
	{
	}

	/**
	 * Returns a deadline measured by {@link System#nanoTime()}.
	 *
	 * @param timeoutNanos positive retry budget
	 * @return saturated monotonic deadline
	 * @throws IllegalArgumentException when the budget is not positive
	 */
	public static long deadlineNanos(final long timeoutNanos)
	{
		if (timeoutNanos <= 0L) throw new IllegalArgumentException("timeoutNanos must be positive");
		if (timeoutNanos == Long.MAX_VALUE) return Long.MAX_VALUE;
		try
		{
			return Math.addExact(System.nanoTime(), timeoutNanos);
		}
		catch (final ArithmeticException overflow)
		{
			return Long.MAX_VALUE;
		}
	}

	/**
	 * Returns the non-negative time left before a deadline.
	 *
	 * @param deadlineNanos saturated deadline returned by this class
	 * @return remaining nanoseconds, or zero after expiry
	 */
	public static long remainingNanos(final long deadlineNanos)
	{
		if (deadlineNanos == Long.MAX_VALUE) return Long.MAX_VALUE;
		final long now = System.nanoTime();
		try
		{
			final long remaining = Math.subtractExact(deadlineNanos, now);
			return Math.max(0L, remaining);
		}
		catch (final ArithmeticException overflow)
		{
			/* nanoTime has an arbitrary origin and may be negative. When subtraction
			 * overflows, ordering the operands still tells us whether the deadline is
			 * in the future; never expose a wrapped positive wait for an expired one. */
			return deadlineNanos > now ? Long.MAX_VALUE : 0L;
		}
	}

	/** Returns whether the supplied monotonic deadline has expired.
	 *
	 * @param deadlineNanos saturated deadline returned by this class
	 * @return {@code true} when no retry time remains
	 */
	public static boolean expired(final long deadlineNanos)
	{
		return remainingNanos(deadlineNanos) == 0L;
	}
}
