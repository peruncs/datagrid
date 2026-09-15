package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.hibernate.types.CacheRegionFactory;
import org.eclipse.store.cache.hibernate.types.ConfigurationPropertyNames;
import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.eclipse.store.cache.types.CacheManager;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheConfiguration;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageCommunicationProvider;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageReceiver;

import java.util.Map;
import java.util.UUID;

/// This region factory adds clustered invalidation to the Store cache factory.
///
/// During preparation it creates one Aeron provider, receiver, and listener
/// configuration for the session factory. During release it closes those
/// resources before the base factory releases the local caches.
///
/// Hibernate hands settings to this factory as a raw string map. This factory
/// is the only place that reads that dialect: it translates the keys below
/// into an injected [AeronClusteredCacheConfiguration] once, and the Aeron
/// core never sees the map.
public class ClusteredCacheRegionFactory extends CacheRegionFactory {
    private static final System.Logger LOGGER =
            System.getLogger(ClusteredCacheRegionFactory.class.getName());

        /// Prefix shared by the clustered-cache Hibernate keys.
    private static final String CLUSTERED_PREFIX = ConfigurationPropertyNames.PREFIX + "clustered.";
        /// Prefix shared by the Aeron clustered-cache Hibernate keys.
    private static final String AERON_PREFIX = CLUSTERED_PREFIX + "aeron.";
        /// Key for the Aeron channel shared by all participants.
    private static final String KEY_CHANNEL = AERON_PREFIX + "channel";
        /// Key for the Aeron stream id shared by all participants.
    private static final String KEY_STREAM_ID = AERON_PREFIX + "stream-id";
        /// Key for the optional node identity shared by every provider of one node.
    private static final String KEY_NODE_ID = AERON_PREFIX + "node-id";
        /// Key for the optional Aeron driver directory.
    private static final String KEY_DIRECTORY = AERON_PREFIX + "directory";
        /// Key launching a private embedded MediaDriver.
    private static final String KEY_EMBEDDED_DRIVER = AERON_PREFIX + "embedded-driver";
        /// Key bounding the sender publication wait in millis.
    private static final String KEY_OFFER_TIMEOUT_MILLIS = AERON_PREFIX + "offer-timeout-millis";
        /// Key bounding the Aeron client driver connection wait in millis.
    private static final String KEY_DRIVER_TIMEOUT_MILLIS = AERON_PREFIX + "driver-timeout-millis";
        /// Key bounding the accepted serialized payload size.
    private static final String KEY_MAX_PAYLOAD_BYTES = AERON_PREFIX + "max-payload-bytes";

        /// Listener configuration created during session-factory preparation.
    private volatile ClusteredCacheEntryListenerConfiguration cacheEntryListenerConfiguration;
        /// Receiver created during session-factory preparation.
    private volatile AeronClusteredCacheMessageReceiver cacheMessageReceiver;
        /// Local cache manager used by the message acceptor.
    private volatile CacheManager cacheManager;

        /// Creates a factory with Hibernate's default cache key strategy.
    public ClusteredCacheRegionFactory() {
        this(DefaultCacheKeysFactory.INSTANCE);
    }

        /// Creates a factory with an explicit Hibernate cache key strategy.
    ///
    /// @param cacheKeysFactory cache key strategy
    public ClusteredCacheRegionFactory(final CacheKeysFactory cacheKeysFactory) {
        super(cacheKeysFactory);
    }

        /// Translates the Hibernate setting map into the injected Aeron
    /// configuration. Absent or blank values use the record defaults; malformed
    /// values fail before any Aeron resource is opened.
    ///
    /// @param properties Hibernate cache properties
    /// @return Aeron configuration for the sender and receiver
    static AeronClusteredCacheConfiguration clusteredCacheConfiguration(
            @SuppressWarnings("rawtypes") final Map properties
    ) {
        final String nodeId = stringProperty(properties, KEY_NODE_ID, null);
        return new AeronClusteredCacheConfiguration(
                stringProperty(properties, KEY_CHANNEL, AeronClusteredCacheConfiguration.DEFAULT_CHANNEL),
                intProperty(properties, KEY_STREAM_ID, AeronClusteredCacheConfiguration.DEFAULT_STREAM_ID, 0),
                nodeId == null ? null : parseNodeId(nodeId),
                stringProperty(properties, KEY_DIRECTORY, null),
                booleanProperty(properties, KEY_EMBEDDED_DRIVER, false),
                longProperty(properties, KEY_DRIVER_TIMEOUT_MILLIS,
                        AeronClusteredCacheConfiguration.DEFAULT_DRIVER_TIMEOUT_MILLIS, 1L),
                longProperty(properties, KEY_OFFER_TIMEOUT_MILLIS,
                        AeronClusteredCacheConfiguration.DEFAULT_OFFER_TIMEOUT_MILLIS, 0L),
                intProperty(properties, KEY_MAX_PAYLOAD_BYTES,
                        AeronClusteredCacheConfiguration.DEFAULT_MAX_PAYLOAD_BYTES, 1));
    }

    private static UUID parseNodeId(final String configured) {
        try {
            return UUID.fromString(configured);
        } catch (final IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "%s must be a UUID: %s".formatted(KEY_NODE_ID, configured), failure);
        }
    }

    private static String stringProperty(
            @SuppressWarnings("rawtypes") final Map properties, final String name, final String fallback) {
        final Object configured = properties.get(name);
        if (configured == null) return fallback;
        final String value = configured.toString().trim();
        return value.isEmpty() ? fallback : value;
    }

    private static int intProperty(
            @SuppressWarnings("rawtypes") final Map properties,
            final String name, final int fallback, final int minimum) {
        final long value = longProperty(properties, name, fallback, minimum);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("%s must be an integer: %s".formatted(name, value));
        }
        return (int) value;
    }

    private static long longProperty(
            @SuppressWarnings("rawtypes") final Map properties,
            final String name, final long fallback, final long minimum) {
        final String configured = stringProperty(properties, name, null);
        if (configured == null) return fallback;
        final long value;
        try {
            value = Long.parseLong(configured);
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException("%s must be a long: %s".formatted(name, configured), failure);
        }
        if (value < minimum) {
            throw new IllegalArgumentException("%s must be at least %s: %s".formatted(name, minimum, value));
        }
        return value;
    }

    private static boolean booleanProperty(
            @SuppressWarnings("rawtypes") final Map properties, final String name, final boolean fallback) {
        final String configured = stringProperty(properties, name, null);
        if (configured == null) return fallback;
        if (!"true".equalsIgnoreCase(configured) && !"false".equalsIgnoreCase(configured)) {
            throw new IllegalArgumentException("%s must be true or false: %s".formatted(name, configured));
        }
        return Boolean.parseBoolean(configured);
    }

        /// Prepares the clustered resources for one session factory. The Aeron
    /// provider is created and its receiver is started before local cache
    /// events are redirected to it. A failure releases any partially created
    /// resource.
    ///
    /// @param settings   session factory settings
    /// @param properties Hibernate cache properties
    @Override
    protected void prepareForUse(final SessionFactoryOptions settings, final Map properties) {
        try {
            super.prepareForUse(settings, properties);

            final var comProvider = new AeronClusteredCacheMessageCommunicationProvider();
            final var messageAcceptor = new ClusteredCacheMessageAcceptor(this.cacheManager);
            final var configuration = clusteredCacheConfiguration(properties);

            this.cacheMessageReceiver = comProvider.provideMessageReceiver(configuration, messageAcceptor);
            this.cacheEntryListenerConfiguration = new ClusteredCacheEntryListenerConfiguration(
                    comProvider.provideUpdateTimestampsCacheMessageSender(configuration));
            this.cacheMessageReceiver.start();
        } catch (final RuntimeException | Error failure) {
            /* super.prepareForUse assigns the CacheManager before it can throw
             * (bad configuration), so it must be released on every failure path,
             * not only when a clustered resource was already created. */
            try {
                this.disposeClusteredResources();
            } catch (final Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            try {
                super.releaseFromUse();
            } catch (final Throwable releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            throw failure;
        }
    }

    @Override
    protected CacheManager resolveCacheManager(final SessionFactoryOptions settings, final Map properties) {
        this.cacheManager = super.resolveCacheManager(settings, properties);
        return this.cacheManager;
    }

    @Override
    protected StorageAccess createTimestampsRegionStorageAccess(
            final String regionName,
            final SessionFactoryImplementor sessionFactory
    ) {
        final ClusteredCacheEntryListenerConfiguration listenerConfiguration =
                this.cacheEntryListenerConfiguration;
        if (listenerConfiguration == null) {
            throw new CacheException(
                    "Clustered cache resources are not prepared; cannot create the timestamps region %s".formatted(regionName));
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

        /// Prevents Hibernate from serving or mutating a timestamps cache after the
    /// invalidation broadcast has stopped. A volatile broadcast cannot repair a
    /// cache after a receiver gap, so continuing locally would silently serve
    /// stale query results.
    private void ensureClusteredHealthy() {
        final AeronClusteredCacheMessageReceiver receiver = this.cacheMessageReceiver;
        if (receiver == null) {
            throw new CacheException("Clustered cache invalidation receiver is not initialized");
        }
        final RuntimeException failure = receiver.failure();
        if (failure != null) {
            throw new CacheException("Clustered cache invalidation receiver has failed", failure);
        }
        if (!receiver.isRunning()) {
            throw new CacheException("Clustered cache invalidation receiver is not running");
        }
    }

    @Override
    protected void releaseFromUse() {
        Throwable failure = null;
        boolean clusteredResourcesReleased = false;
        try {
            this.disposeClusteredResources();
            clusteredResourcesReleased = true;
        } catch (final Throwable e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to dispose clustered cache resources.", e);
            failure = e;
        }
        if (clusteredResourcesReleased) try {
            super.releaseFromUse();
        } catch (final Throwable releaseFailure) {
            if (failure == null) failure = releaseFailure;
            else if (failure != releaseFailure) failure.addSuppressed(releaseFailure);
        }
        if (failure instanceof final Error error) {
            throw error;
        }
        if (failure instanceof final RuntimeException runtime) {
            throw runtime;
        }
        if (failure != null) {
            throw new CacheException("Failed to release clustered cache resources", failure);
        }
    }

        /// Disposes the clustered resources, tolerating a partial preparation. Every
    /// resource is released even when an earlier dispose fails; the first failure
    /// is rethrown afterwards.
    ///
    /// @throws Throwable the first dispose failure, with later failures suppressed
    private void disposeClusteredResources() throws Throwable {
        Throwable failure = null;
        final ClusteredCacheEntryListenerConfiguration listenerConfiguration =
                this.cacheEntryListenerConfiguration;
        if (listenerConfiguration != null) {
            try {
                listenerConfiguration.dispose();
                this.cacheEntryListenerConfiguration = null;
            } catch (final Throwable e) {
                LOGGER.log(System.Logger.Level.ERROR, "Failed to close entry listeners.", e);
                failure = e;
            }
        }

        final AeronClusteredCacheMessageReceiver receiver = this.cacheMessageReceiver;
        if (receiver != null) {
            try {
                receiver.dispose();
                this.cacheMessageReceiver = null;
            } catch (final Throwable e) {
                LOGGER.log(System.Logger.Level.ERROR, "Failed to close invalidation receiver.", e);
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

        /// Storage access that refuses all cache operations after receiver failure.
    static final class FailClosedStorageAccess implements StorageAccess {
        private final StorageAccess delegate;
        private final Runnable healthCheck;

        FailClosedStorageAccess(final StorageAccess delegate, final Runnable healthCheck) {
            this.delegate = delegate;
            this.healthCheck = healthCheck;
        }

        private void check() {
            this.healthCheck.run();
        }

        @Override
        public Object getFromCache(
                final Object key,
                final SharedSessionContractImplementor session
        ) {
            this.check();
            return this.delegate.getFromCache(key, session);
        }

        @Override
        public void putIntoCache(
                final Object key,
                final Object value,
                final SharedSessionContractImplementor session
        ) {
            this.check();
            this.delegate.putIntoCache(key, value, session);
        }

        @Override
        public boolean contains(final Object key) {
            this.check();
            return this.delegate.contains(key);
        }

        @Override
        public void evictData() {
            this.check();
            this.delegate.evictData();
        }

        @Override
        public void evictData(final Object key) {
            this.check();
            this.delegate.evictData(key);
        }

        @Override
        public void release() {
            this.delegate.release();
        }
    }
}
