package peruncs.datagrid.cache.clustered.types;

import peruncs.datagrid.cache.clustered.aeron.types.AeronClusteredCacheMessageComProvider;
import peruncs.datagrid.cache.clustered.aeron.types.AeronClusteredCacheMessageReceiver;
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
import org.hibernate.engine.spi.SharedSessionContractImplementor;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * This region factory adds clustered invalidation to the Store cache factory.
 *
 * <p>During preparation it creates one serializer, Aeron provider, receiver,
 * and listener configuration for the session factory. During release it closes
 * those resources before the base factory releases the local caches.</p>
 */
public class ClusteredCacheRegionFactory extends CacheRegionFactory
{
    private static final System.Logger LOGGER =
        System.getLogger(ClusteredCacheRegionFactory.class.getName());

	/** Listener configuration created during session-factory preparation. */
	private ClusteredCacheEntryListenerConfiguration cacheEntryListenerConfiguration;
	/** Receiver created during session-factory preparation. */
	private AeronClusteredCacheMessageReceiver cacheMessageReceiver;
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

    /**
     * Prepares the clustered resources for one session factory. The Aeron
     * provider is created and its receiver is started before local cache
     * events are redirected to it. A failure releases any partially created
     * resource.
     *
     * @param settings session factory settings
     * @param properties Hibernate cache properties
     */
    @Override
    protected void prepareForUse(final SessionFactoryOptions settings, final Map properties)
    {
        super.prepareForUse(settings, properties);
        try
        {
            final var typesProvider = this.resolveSerializationTypesProvider(settings, properties);
            final var serializer = Serializer.Bytes(SerializerFoundation.New()
                .registerEntityTypes(typesProvider.provideTypes()));

            final var comProvider = new AeronClusteredCacheMessageComProvider();
            final var messageAcceptor = new ClusteredCacheMessageAcceptor(this.cacheManager);

            this.cacheMessageReceiver = comProvider.provideMessageReceiver(properties, serializer, messageAcceptor);
            this.cacheEntryListenerConfiguration =
                createEntryListenerConfiguration(comProvider, properties, serializer);
            this.cacheMessageReceiver.start();
        }
		catch (final RuntimeException | Error failure)
		{
			boolean clusteredResourcesReleased = false;
			try
			{
				this.disposeClusteredResources();
				clusteredResourcesReleased = true;
			}
			catch (final Throwable cleanupFailure)
			{
				failure.addSuppressed(cleanupFailure);
			}
			if (clusteredResourcesReleased)
			{
				try
				{
					super.releaseFromUse();
				}
				catch (final Throwable releaseFailure)
				{
					failure.addSuppressed(releaseFailure);
				}
			}
			throw failure;
		}
    }

    @Override
    protected CacheManager resolveCacheManager(final SessionFactoryOptions settings, final Map properties)
    {
        this.cacheManager = super.resolveCacheManager(settings, properties);
        return this.cacheManager;
    }

    private static ClusteredCacheEntryListenerConfiguration createEntryListenerConfiguration(
        final AeronClusteredCacheMessageComProvider comProvider,
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer
    )
    {
        return new ClusteredCacheEntryListenerConfiguration(
            comProvider.provideUpdateTimestampsCacheMessageSender(properties, serializer));
    }

    @Override
    protected StorageAccess createTimestampsRegionStorageAccess(
        final String regionName,
        final SessionFactoryImplementor sessionFactory
    )
    {
        final ClusteredCacheEntryListenerConfiguration listenerConfiguration =
            this.cacheEntryListenerConfiguration;
        if (listenerConfiguration == null)
        {
            throw new CacheException(
                "Clustered cache resources are not prepared; cannot create the timestamps region " + regionName);
        }
        final String defaultedRegionName = this.defaultRegionName(
            regionName,
            sessionFactory,
            DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME,
            LEGACY_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAMES
        );
        final var cache = this.getOrCreateCache(defaultedRegionName, sessionFactory);
        cache.registerCacheEntryListener(listenerConfiguration.getUpdateTimestampsCacheEntryListenerConfiguration());
        return new FailClosedStorageAccess(StorageAccess.New(cache), this::ensureClusteredHealthy);
    }

	/**
	 * Prevents Hibernate from serving or mutating a timestamps cache after the
	 * invalidation broadcast has stopped. A volatile broadcast cannot repair a
	 * cache after a receiver gap, so continuing locally would silently serve
	 * stale query results.
	 */
	private void ensureClusteredHealthy()
	{
		final AeronClusteredCacheMessageReceiver receiver = this.cacheMessageReceiver;
		if (receiver == null)
		{
			throw new CacheException("Clustered cache invalidation receiver is not initialized");
		}
		final RuntimeException failure = receiver.failure();
		if (failure != null)
		{
			throw new CacheException("Clustered cache invalidation receiver has failed", failure);
		}
		if (!receiver.isRunning())
		{
			throw new CacheException("Clustered cache invalidation receiver is not running");
		}
	}

	/** Storage access that refuses all cache operations after receiver failure. */
	static final class FailClosedStorageAccess implements StorageAccess
	{
		private final StorageAccess delegate;
		private final Runnable healthCheck;

		FailClosedStorageAccess(final StorageAccess delegate, final Runnable healthCheck)
		{
			this.delegate = delegate;
			this.healthCheck = healthCheck;
		}

		private void check()
		{
			this.healthCheck.run();
		}

		@Override
		public Object getFromCache(
			final Object key,
			final SharedSessionContractImplementor session
		)
		{
			this.check();
			return this.delegate.getFromCache(key, session);
		}

		@Override
		public void putIntoCache(
			final Object key,
			final Object value,
			final SharedSessionContractImplementor session
		)
		{
			this.check();
			this.delegate.putIntoCache(key, value, session);
		}

		@Override
		public boolean contains(final Object key)
		{
			this.check();
			return this.delegate.contains(key);
		}

		@Override
		public void evictData()
		{
			this.check();
			this.delegate.evictData();
		}

		@Override
		public void evictData(final Object key)
		{
			this.check();
			this.delegate.evictData(key);
		}

		@Override
		public void release()
		{
			this.delegate.release();
		}
	}

	/** Resolves the serializer type provider from a Hibernate setting.
	 *
	 * <p>The provider class must expose a public no-argument constructor;
	 * private or package-private constructors are rejected.</p>
	 *
	 * @param settings session factory settings used for class loading
	 * @param properties Hibernate cache properties
	 * @return resolved serializer type provider
	 * @throws CacheException when the provider class cannot be instantiated
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
            if (setting instanceof Class<?> candidate)
            {
                if (!SerializationTypesProvider.class.isAssignableFrom(candidate))
                {
                    throw new CacheException(
                        "Configured serialization types provider does not implement " +
                            SerializationTypesProvider.class.getName() + ": " + candidate.getName());
                }
                typesProviderClass = (Class<? extends SerializationTypesProvider>)candidate;
            }
            else
            {
                typesProviderClass = this.loadClass(setting.toString(), settings);
            }
            return typesProviderClass.getConstructor().newInstance();
        }
        catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
            InvocationTargetException e)
        {
            throw new CacheException("Could not instantiate SerializationTypesProvider: " + setting, e);
        }
    }

	@Override
	protected void releaseFromUse()
	{
		Throwable failure = null;
		boolean clusteredResourcesReleased = false;
		try
		{
			this.disposeClusteredResources();
			clusteredResourcesReleased = true;
		}
		catch (final Throwable e)
		{
			LOGGER.log(System.Logger.Level.ERROR, "Failed to dispose clustered cache resources.", e);
			failure = e;
		}
		if (clusteredResourcesReleased) try
		{
			super.releaseFromUse();
		}
		catch (final Throwable releaseFailure)
		{
			if (failure == null) failure = releaseFailure;
			else if (failure != releaseFailure) failure.addSuppressed(releaseFailure);
		}
		if (failure instanceof final Error error)
		{
			throw error;
		}
		if (failure instanceof final RuntimeException runtime)
		{
			throw runtime;
		}
		if (failure != null)
		{
			throw new CacheException("Failed to release clustered cache resources", failure);
		}
	}

	/**
	 * Disposes the clustered resources, tolerating a partial preparation. Every
	 * resource is released even when an earlier dispose fails; the first failure
	 * is rethrown afterwards.
	 *
	 * @throws Throwable the first dispose failure, with later failures suppressed
	 */
	private void disposeClusteredResources() throws Throwable
	{
		Throwable failure = null;
		final ClusteredCacheEntryListenerConfiguration listenerConfiguration =
			this.cacheEntryListenerConfiguration;
		if (listenerConfiguration != null)
		{
			try
			{
				listenerConfiguration.dispose();
				this.cacheEntryListenerConfiguration = null;
			}
			catch (final Throwable e)
			{
				LOGGER.log(System.Logger.Level.ERROR, "Failed to close entry listeners.", e);
				failure = e;
			}
		}

		final AeronClusteredCacheMessageReceiver receiver = this.cacheMessageReceiver;
		if (receiver != null)
		{
			try
			{
				receiver.dispose();
				this.cacheMessageReceiver = null;
			}
			catch (final Throwable e)
			{
				LOGGER.log(System.Logger.Level.ERROR, "Failed to close invalidation receiver.", e);
				if (failure == null)
				{
					failure = e;
				}
				else if (failure != e)
				{
					failure.addSuppressed(e);
				}
			}
		}
		if (failure != null)
		{
			throw failure;
		}
	}
}
