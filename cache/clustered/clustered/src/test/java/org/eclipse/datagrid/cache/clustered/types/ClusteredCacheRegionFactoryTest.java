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

import org.eclipse.serializer.Serializer;
import org.hibernate.cache.CacheException;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies the region factory resolves configured providers by public constructor. */
class ClusteredCacheRegionFactoryTest
{
	@Test
	void resolveComProviderAcceptsInstance()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();
		final PublicComProvider instance = new PublicComProvider();

		assertSame(instance, factory.resolveComProvider(null, instance));
	}

	@Test
	void resolveComProviderAcceptsPublicConstructor()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		final ClusteredCacheMessageComProvider provider =
			factory.resolveComProvider(null, PublicComProvider.class);

		assertTrue(provider instanceof PublicComProvider);
	}

	@Test
	void resolveComProviderRejectsPrivateConstructor()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		assertThrows(CacheException.class,
			() -> factory.resolveComProvider(null, PrivateComProvider.class),
			"a provider without a public no-argument constructor must be rejected");
	}

	@Test
	void resolveSerializationTypesProviderDefaultsWhenUnset()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		final SerializationTypesProvider provider = factory.resolveSerializationTypesProvider(null, Map.of());

		assertTrue(provider instanceof SerializationTypesProvider.Default);
	}

	@Test
	void resolveSerializationTypesProviderAcceptsPublicConstructor()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		final SerializationTypesProvider provider = factory.resolveSerializationTypesProvider(
			null, Map.of(ClusteredConfigurationPropertyNames.SERIALIZATION_TYPES_PROVIDER, PublicTypesProvider.class));

		assertTrue(provider instanceof PublicTypesProvider);
	}

	/** Provider with a public no-argument constructor. */
	public static final class PublicComProvider implements ClusteredCacheMessageComProvider
	{
		public PublicComProvider()
		{
		}

		@Override
		public ClusteredCacheMessageSender<Object, Object> provideUpdateTimestampsCacheMessageSender(
			@SuppressWarnings("rawtypes") final Map properties, final Serializer<byte[]> serializer)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public ClusteredCacheMessageReceiver provideMessageReceiver(
			@SuppressWarnings("rawtypes") final Map properties, final Serializer<byte[]> serializer,
			final ClusteredCacheMessageAcceptor messageAcceptor)
		{
			throw new UnsupportedOperationException();
		}
	}

	/** Provider with only a private constructor; must be rejected. */
	private static final class PrivateComProvider implements ClusteredCacheMessageComProvider
	{
		private PrivateComProvider()
		{
		}

		@Override
		public ClusteredCacheMessageSender<Object, Object> provideUpdateTimestampsCacheMessageSender(
			@SuppressWarnings("rawtypes") final Map properties, final Serializer<byte[]> serializer)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public ClusteredCacheMessageReceiver provideMessageReceiver(
			@SuppressWarnings("rawtypes") final Map properties, final Serializer<byte[]> serializer,
			final ClusteredCacheMessageAcceptor messageAcceptor)
		{
			throw new UnsupportedOperationException();
		}
	}

	/** Types provider with a public no-argument constructor. */
	public static final class PublicTypesProvider implements SerializationTypesProvider
	{
		public PublicTypesProvider()
		{
		}

		@Override
		public Collection<Class<?>> provideTypes()
		{
			return List.of();
		}
	}
}