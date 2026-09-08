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
 * Shared bounded Aeron offer/retry loop for data and control publications.
 * The owner serializes calls; this helper is intentionally thread-confined
 * because it reuses one buffer and one idle strategy.
 */
final class AeronOfferRetryer
{
	@FunctionalInterface
	interface Offerer
	{
		long offer(DirectBuffer buffer, int offset, int length);
	}

	private final Offerer offerer;
	private final AeronReplicationConfiguration configuration;
	private final BackoffIdleStrategy idle = new BackoffIdleStrategy(1, 10, 1, 1_000_000);

	AeronOfferRetryer(final Offerer offerer, final AeronReplicationConfiguration configuration)
	{
		this.offerer = java.util.Objects.requireNonNull(offerer, "offerer");
		this.configuration = java.util.Objects.requireNonNull(configuration, "configuration");
	}

	/* The owning publisher serializes calls.  Keeping the retryer lock-free
	 * avoids a second monitor on every publication attempt. */
	/** Offers a caller-owned direct buffer without copying it to the heap. */
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
		while (true)
		{
			final long position = this.offerer.offer(source, 0, length);
			if (position >= 0) return position;
			if (position == Publication.CLOSED || position == Publication.MAX_POSITION_EXCEEDED)
			{
				throw new IllegalStateException("Aeron publication failed: " + position);
			}
			if (System.nanoTime() - started >= this.configuration.offerTimeoutNanos())
			{
				throw new IllegalStateException("Aeron offer timed out: " + position);
			}
			this.idle.idle();
		}
	}
}
