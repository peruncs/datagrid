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
import org.eclipse.serializer.SerializerFoundation;
import org.eclipse.store.cache.hibernate.types.CacheRegionFactory;
import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.eclipse.store.cache.types.CacheManager;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * This region factory adds clustered invalidation to the Store cache factory.
 *
 * <p>During preparation it creates one serializer, message provider, receiver,
 * and listener configuration for the session factory. During release it closes
 * those resources before the base factory releases the local caches.</p>
 */
public class ClusteredCacheRegionFactory extends CacheRegionFactory
{
	private static final Logger logger = LoggerFactory.getLogger(ClusteredCacheRegionFactory.class);

	/** Listener configuration created during session-factory preparation. */
	private ClusteredCacheEntryListenerConfiguration<Object, Object> cacheEntryListenerConfiguration;
	/** Receiver created during session-factory preparation. */
	private ClusteredCacheMessageReceiver cacheMessageReceiver;
	/** Local cache manager used by the message acceptor. */
	private CacheManager cacheManager;

	/** Creates a factory with Hibernate's default cache key strategy. */
	public ClusteredCacheRegionFactory()
    {
        this(DefaultCacheKeysFactory.INSTANCE);
    }

	/** Creates a factory with an explicit Hibernate cache key strategy.
	 *
	 * @param cacheKeysFactory cache key strategy
	 */
	public ClusteredCacheRegionFactory(final CacheKeysFactory cacheKeysFactory)
    {
        super(cacheKeysFactory);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void prepareForUse(final SessionFactoryOptions settings, final Map properties)
    {
        super.prepareForUse(settings, properties);

        final var typesProvider = this.resolveSerializationTypesProvider(settings, properties);
        final var serializer = Serializer.Bytes(SerializerFoundation.New()
            .registerEntityTypes(typesProvider.provideTypes()));

        final var comProviderSetting = properties.get(ClusteredConfigurationPropertyNames.COM_PROVIDER);
        final var comProvider =
            (ClusteredCacheMessageComProvider<Object, Object>)this.resolveComProvider(settings, comProviderSetting);

        final var messageAcceptor = new ClusteredCacheMessageAcceptor(this.cacheManager);

        this.cacheMessageReceiver = comProvider.provideMessageReceiver(properties, serializer, messageAcceptor);
        this.cacheEntryListenerConfiguration =
            createEntryListenerConfiguration(comProvider, properties, serializer);

        this.cacheMessageReceiver.start();
    }

    @Override
    protected CacheManager resolveCacheManager(final SessionFactoryOptions settings, final Map properties)
    {
        this.cacheManager = super.resolveCacheManager(settings, properties);
        return this.cacheManager;
    }

    private static <K, V> ClusteredCacheEntryListenerConfiguration<K, V> createEntryListenerConfiguration(
        final ClusteredCacheMessageComProvider<K, V> comProvider,
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer
    )
    {
        return new ClusteredCacheEntryListenerConfiguration<>(comProvider.provideUpdateTimestampsCacheMessageSender(
            properties,
            serializer
        ));
    }

	/** Resolves a communication provider instance from a Hibernate setting.
	 *
	 * @param settings session factory settings used for class loading
	 * @param comProviderSetting provider instance or provider class
	 * @return resolved communication provider
	 */
	@SuppressWarnings("unchecked")
	protected ClusteredCacheMessageComProvider<?, ?> resolveComProvider(
        final SessionFactoryOptions settings,
        final Object comProviderSetting
    )
    {
        if (comProviderSetting instanceof final ClusteredCacheMessageComProvider<?, ?> comProvider)
        {
            return comProvider;
        }

        try
        {
            final Class<? extends ClusteredCacheMessageComProvider<?, ?>> comProviderClass;
            comProviderClass = comProviderSetting instanceof Class
                               ? (Class<? extends ClusteredCacheMessageComProvider<?, ?>>)comProviderSetting
                               : this.loadClass(comProviderSetting.toString(), settings);

            return comProviderClass.getDeclaredConstructor().newInstance();
        }
        catch (final ClassNotFoundException | InstantiationException | IllegalAccessException
            | NoSuchMethodException | InvocationTargetException e)
        {
            throw new CacheException("Could not use ClusteredCacheMessageComProvider: " + comProviderSetting, e);
        }
    }

    @Override
    protected StorageAccess createTimestampsRegionStorageAccess(
        final String regionName,
        final SessionFactoryImplementor sessionFactory
    )
    {
        final String defaultedRegionName = this.defaultRegionName(
            regionName,
            sessionFactory,
            DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME,
            LEGACY_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAMES
        );
        final var cache = this.getOrCreateCache(defaultedRegionName, sessionFactory);
        cache.registerCacheEntryListener(this.cacheEntryListenerConfiguration.getUpdateTimestampsCacheEntryListenerConfiguration());
        return StorageAccess.New(cache);
    }

	/** Resolves the serializer type provider from a Hibernate setting.
	 *
	 * @param settings session factory settings used for class loading
	 * @param properties Hibernate cache properties
	 * @return resolved serializer type provider
	 */
	@SuppressWarnings("unchecked")
	protected SerializationTypesProvider resolveSerializationTypesProvider(
        final SessionFactoryOptions settings,
        @SuppressWarnings("rawtypes") // superclass uses raw type
        final Map properties
    )
    {
        final Object setting = properties.get(ClusteredConfigurationPropertyNames.SERIALIZATION_TYPES_PROVIDER);
        if (setting == null)
        {
            return new SerializationTypesProvider.Default();
        }
        if (setting instanceof final SerializationTypesProvider p)
        {
            return p;
        }

        try
        {
            final Class<? extends SerializationTypesProvider> typesProviderClass;
            if (setting instanceof Class)
            {
                typesProviderClass = (Class<? extends SerializationTypesProvider>)setting;
            }
            else
            {
                typesProviderClass = this.loadClass(setting.toString(), settings);
            }
            return typesProviderClass.getDeclaredConstructor().newInstance();
        }
        catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
            InvocationTargetException e)
        {
            throw new CacheException("Could not use explicit CacheManager : " + setting, e);
        }
    }

    @Override
    protected void releaseFromUse()
    {
        try
        {
            this.cacheEntryListenerConfiguration.dispose();
        }
        catch (final Exception e)
        {
            logger.error("Failed to close entry listeners.", e);
        }

        try
        {
            this.cacheMessageReceiver.dispose();
        }
        catch (final Exception e)
        {
            logger.error("Failed to close invalidation receiver.", e);
        }

        super.releaseFromUse();
    }
}
