package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
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

import javax.cache.configuration.CacheEntryListenerConfiguration;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies the listener configuration owns its sender and disposes it once. */
class ClusteredCacheEntryListenerConfigurationTest
{
	@Test
	void disposeIsIdempotentThroughTheSender()
	{
		final CountingSender sender = new CountingSender();
		final ClusteredCacheEntryListenerConfiguration<Object, Object> configuration =
			new ClusteredCacheEntryListenerConfiguration<>(sender);

		configuration.dispose();
		configuration.dispose();

		assertEquals(1, sender.disposeCount, "the sender's idempotent dispose must be invoked once");
	}

	@Test
	void listenerFactoryReturnsTheSender()
	{
		final CountingSender sender = new CountingSender();
		final ClusteredCacheEntryListenerConfiguration<Object, Object> configuration =
			new ClusteredCacheEntryListenerConfiguration<>(sender);

		final CacheEntryListenerConfiguration<Object, Object> listener =
			configuration.getUpdateTimestampsCacheEntryListenerConfiguration();
		assertSame(sender, listener.getCacheEntryListenerFactory().create());
		assertTrue(listener.isSynchronous(), "clustered invalidations must be synchronous");
		assertFalse(listener.isOldValueRequired());
		assertNull(listener.getCacheEntryEventFilterFactory());
	}

	@Test
	void nullSenderIsRejected()
	{
		assertThrows(NullPointerException.class,
			() -> new ClusteredCacheEntryListenerConfiguration<>(null));
	}

	/** Sender stub whose dispose is idempotent, like every real transport sender. */
	private static final class CountingSender implements ClusteredCacheMessageSender<Object, Object>
	{
		private boolean disposed;
		private int disposeCount;

		@Override
		public void dispose()
		{
			if (this.disposed)
			{
				return;
			}
			this.disposed = true;
			this.disposeCount++;
		}
	}
}
