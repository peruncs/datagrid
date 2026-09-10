package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;

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

/**
 * The bounded retry policy used by the writer's Aeron publications.
 *
 * <p>The helper reuses its buffer and idle strategy, so it belongs to one
 * publisher and must be called by that publisher's serialized write path.</p>
 */
final class AeronOfferRetryer
{
	@FunctionalInterface
	interface Offerer
	{
		long offer(DirectBuffer buffer, int offset, int length);

		/**
		 * Returns the publication connectivity observed by the offerer. Test
		 * offerers may keep the default because they do not model a subscription.
		 */
		default boolean isConnected()
		{
			return true;
		}
	}

	private final Offerer offerer;
	private final AeronReplicationConfiguration configuration;
	private final BackoffIdleStrategy idle = new BackoffIdleStrategy(1, 10, 1, 1_000_000);

	AeronOfferRetryer(final Offerer offerer, final AeronReplicationConfiguration configuration)
	{
		this.offerer = java.util.Objects.requireNonNull(offerer, "offerer");
		this.configuration = java.util.Objects.requireNonNull(configuration, "configuration");
	}

	/**
	 * Offers the first {@code length} bytes of a buffer until Aeron accepts them
	 * or the configured deadline expires.
	 *
	 * <p>The buffer is read synchronously and is not retained after this method
	 * returns. The caller must keep it valid and unchanged for the duration of
	 * the call. No heap copy is made. The helper is not thread-safe.</p>
	 *
	 * @param source buffer containing the frame to offer
	 * @param length number of bytes to offer, starting at offset zero
	 * @return the Aeron publication position
	 * @throws IllegalArgumentException if {@code source} is null or the length
	 *         is outside the buffer capacity
	 * @throws IllegalStateException if the publication closes, exceeds its
	 *         maximum position, or does not accept the frame before timeout
	 */
	long offer(final DirectBuffer source, final int length)
	{
		if (source == null || length < 0 || length > source.capacity())
		{
			throw new IllegalArgumentException("invalid Aeron offer length");
		}
		return this.offerLoop(source, length);
	}

	private long offerLoop(final DirectBuffer source, final int length)
	{
		this.idle.reset();
		final long started = System.nanoTime();
		long backPressured = 0;
		long notConnected = 0;
		long adminActions = 0;
		while (true)
		{
			final long position = this.offerer.offer(source, 0, length);
			if (position >= 0) return position;
			if (position == Publication.CLOSED || position == Publication.MAX_POSITION_EXCEEDED)
			{
				throw new IllegalStateException("Aeron publication failed: " + position);
			}
			if (position == Publication.BACK_PRESSURED) backPressured++;
			else if (position == Publication.NOT_CONNECTED) notConnected++;
			else if (position == Publication.ADMIN_ACTION) adminActions++;
			if (System.nanoTime() - started >= this.configuration.offerTimeoutNanos())
			{
				final String reason;
				if (position == Publication.BACK_PRESSURED)
				{
					reason = "BACK_PRESSURED retries=" + backPressured;
				}
				else if (position == Publication.NOT_CONNECTED)
				{
					reason = "NOT_CONNECTED retries=" + notConnected;
				}
				else if (position == Publication.ADMIN_ACTION)
				{
					reason = "ADMIN_ACTION retries=" + adminActions;
				}
				else
				{
					reason = "status=" + position;
				}
				throw new IllegalStateException("Aeron offer timed out: " + reason +
					", connected=" + this.offerer.isConnected());
			}
			this.idle.idle();
		}
	}
}
