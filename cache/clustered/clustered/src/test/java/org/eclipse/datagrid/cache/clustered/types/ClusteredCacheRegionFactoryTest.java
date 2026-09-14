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
import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.hibernate.cache.CacheException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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

		assertInstanceOf(PublicComProvider.class, provider);
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
	void resolveComProviderRejectsWrongClass()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		assertThrows(CacheException.class,
			() -> factory.resolveComProvider(null, String.class),
			"a configured class with the wrong contract must fail at configuration time");
	}

	@Test
	void failClosedStorageAccessRefusesOperationsAfterReceiverFailure()
	{
		final boolean[] healthy = {true};
		final boolean[] used = {false};
		final StorageAccess delegate = new StorageAccess()
		{
			@Override public Object getFromCache(final Object key, final SharedSessionContractImplementor session)
			{
				used[0] = true;
				return null;
			}
			@Override public void putIntoCache(final Object key, final Object value, final SharedSessionContractImplementor session)
			{
				used[0] = true;
			}
			@Override public boolean contains(final Object key) { used[0] = true; return false; }
			@Override public void evictData() { used[0] = true; }
			@Override public void evictData(final Object key) { used[0] = true; }
			@Override public void release() { used[0] = true; }
		};
		final StorageAccess guarded = new ClusteredCacheRegionFactory.FailClosedStorageAccess(
			delegate, () ->
			{
				if (!healthy[0])
				{
					throw new CacheException("receiver failed");
				}
			});

		assertFalse(guarded.contains("key"));
		assertTrue(used[0]);
		used[0] = false;
		healthy[0] = false;
		assertThrows(CacheException.class, () -> guarded.contains("key"));
		assertFalse(used[0], "failed receiver must prevent access to the local timestamps cache");
	}

	@Test
	void resolveSerializationTypesProviderDefaultsWhenUnset()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		final SerializationTypesProvider provider = factory.resolveSerializationTypesProvider(null, Map.of());

		assertInstanceOf(SerializationTypesProvider.Default.class, provider);
	}

	@Test
	void resolveSerializationTypesProviderAcceptsPublicConstructor()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		final SerializationTypesProvider provider = factory.resolveSerializationTypesProvider(
			null, Map.of(ClusteredConfigurationPropertyNames.SERIALIZATION_TYPES_PROVIDER, PublicTypesProvider.class));

		assertInstanceOf(PublicTypesProvider.class, provider);
	}

	@Test
	void resolveSerializationTypesProviderRejectsWrongClass()
	{
		final ClusteredCacheRegionFactory factory = new ClusteredCacheRegionFactory();

		assertThrows(CacheException.class,
			() -> factory.resolveSerializationTypesProvider(null,
				Map.of(ClusteredConfigurationPropertyNames.SERIALIZATION_TYPES_PROVIDER, String.class)),
			"a configured class with the wrong serialization contract must fail at configuration time");
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
