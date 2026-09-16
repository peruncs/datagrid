package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.hibernate.types.CacheRegionFactory;
import org.eclipse.store.cache.hibernate.types.ConfigurationPropertyNames;
import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.eclipse.store.cache.types.CacheManager;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.support.DomainDataStorageAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheConfiguration;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageCommunicationProvider;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheMessageReceiver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

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
        /// Key setting the idle-sender heartbeat interval in millis.
    private static final String KEY_HEARTBEAT_INTERVAL_MILLIS = AERON_PREFIX + "heartbeat-interval-millis";
        /// Key setting the receiver silence tolerance in millis.
    private static final String KEY_FRESHNESS_TIMEOUT_MILLIS = AERON_PREFIX + "freshness-timeout-millis";
        /// Key setting how many distinct remote senders must prove liveness before reads are verified.
    private static final String KEY_EXPECTED_REMOTE_SENDERS = AERON_PREFIX + "expected-remote-senders";
        /// Key setting the directory holding the persisted per-sender cursors.
    private static final String KEY_CURSOR_DIRECTORY = AERON_PREFIX + "cursor-directory";
        /// Key setting the base64 HMAC secret authenticating every cache frame.
    private static final String KEY_HMAC_SECRET = AERON_PREFIX + "hmac-secret";
        /// Key setting a file holding the base64 HMAC secret.
    private static final String KEY_HMAC_SECRET_FILE = AERON_PREFIX + "hmac-secret-file";
        /// Key setting the base64 retiring HMAC secret accepted during rotation overlap.
    private static final String KEY_HMAC_SECRET_PREVIOUS = AERON_PREFIX + "hmac-secret-previous";
        /// Key setting a file holding the base64 retiring HMAC secret.
    private static final String KEY_HMAC_SECRET_PREVIOUS_FILE = AERON_PREFIX + "hmac-secret-previous-file";
        /// Key enabling production-only validation.
    private static final String KEY_PRODUCTION_MODE = AERON_PREFIX + "production-mode";
        /// Key explicitly acknowledging unsigned frames without a secret.
    private static final String KEY_ALLOW_UNSIGNED_FRAMES = AERON_PREFIX + "allow-unsigned-frames";
        /// Removed key that selected a serializer type provider.
    private static final String REMOVED_KEY_SERIALIZATION_TYPES_PROVIDER =
            CLUSTERED_PREFIX + "serialization-types-provider";

        /// Listener configuration created during session-factory preparation.
    private volatile ClusteredCacheEntryListenerConfiguration cacheEntryListenerConfiguration;
        /// Receiver created during session-factory preparation.
    private volatile AeronClusteredCacheMessageReceiver cacheMessageReceiver;
        /// Acceptor applying remote invalidations; retained to replay updates
    /// buffered while the timestamps cache was not open yet.
    private volatile ClusteredCacheMessageAcceptor clusteredCacheMessageAcceptor;
        /// Local cache manager used by the message acceptor.
    private volatile CacheManager cacheManager;
    /// Automatic receiver recovery is attempted at most once per this interval.
    private static final long RECOVERY_RETRY_INTERVAL_NANOS = 1_000_000_000L;
    /// `System.nanoTime()` reading of the last recovery attempt.
    private volatile long lastRecoveryAttemptNanos;

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
        /* The wire format is a fixed timestamp-update schema, so the old
         * serializer type provider no longer exists. Fail loudly instead of
         * silently ignoring a key an embedder still sets. */
        if (stringProperty(properties, REMOVED_KEY_SERIALIZATION_TYPES_PROVIDER, null) != null) {
            throw new IllegalArgumentException(
                    "%s was removed: clustered-cache invalidations use a fixed payload schema and no longer serialize entity types".formatted(REMOVED_KEY_SERIALIZATION_TYPES_PROVIDER));
        }
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
                        AeronClusteredCacheConfiguration.DEFAULT_MAX_PAYLOAD_BYTES, 1),
                longProperty(properties, KEY_HEARTBEAT_INTERVAL_MILLIS,
                        AeronClusteredCacheConfiguration.DEFAULT_HEARTBEAT_INTERVAL_MILLIS, 1L),
                longProperty(properties, KEY_FRESHNESS_TIMEOUT_MILLIS,
                        AeronClusteredCacheConfiguration.DEFAULT_FRESHNESS_TIMEOUT_MILLIS, 1L),
                intProperty(properties, KEY_EXPECTED_REMOTE_SENDERS,
                        AeronClusteredCacheConfiguration.DEFAULT_EXPECTED_REMOTE_SENDERS, 0),
                stringProperty(properties, KEY_CURSOR_DIRECTORY, null),
                hmacSecret(properties),
                booleanProperty(properties, KEY_PRODUCTION_MODE, false),
                booleanProperty(properties, KEY_ALLOW_UNSIGNED_FRAMES, false),
                previousHmacSecret(properties));
    }

        /// Resolves the HMAC secret from the inline key or the secret file.
    /// The two sources are mutually exclusive; both hold base64 decoding to at
    /// least 16 bytes. The file must be a regular, non-symbolic file of at
    /// most 4096 bytes, unreadable by group or others — mirroring the cluster
    /// retention-secret pattern.
    ///
    /// @param properties Hibernate cache properties
    /// @return decoded secret, or `null` for unsigned frames
    private static byte[] hmacSecret(@SuppressWarnings("rawtypes") final Map properties) {
        return secret(properties, KEY_HMAC_SECRET, KEY_HMAC_SECRET_FILE,
                ClusteredCacheRegionFactory::decodeHmacSecret,
                ClusteredCacheRegionFactory::decodeHmacSecretFile);
    }

        /// Resolves the retiring HMAC secret accepted during rotation overlap.
    ///
    /// The two sources are mutually exclusive, like the primary secret. Length
    /// validation rides with the decoders; the rotation-pair check lives in
    /// the configuration record once the primary is known.
    ///
    /// @param properties Hibernate cache properties
    /// @return decoded previous secret, or `null` when no rotation overlaps
    private static byte[] previousHmacSecret(@SuppressWarnings("rawtypes") final Map properties) {
        return secret(properties, KEY_HMAC_SECRET_PREVIOUS, KEY_HMAC_SECRET_PREVIOUS_FILE,
                ClusteredCacheRegionFactory::decodeHmacSecret,
                ClusteredCacheRegionFactory::decodeHmacSecretFile);
    }

        /// Resolves one HMAC secret from its mutually exclusive inline and file
    /// sources. Length validation rides with the decoders; the rotation-pair
    /// check lives in the configuration record once the primary is known.
    ///
    /// @param properties    Hibernate cache properties
    /// @param inlineKey     inline base64 property name
    /// @param fileKey       secret-file property name
    /// @param inlineDecoder decodes an inline value with its property label
    /// @param fileDecoder   decodes a secret file with its property label
    /// @return decoded secret, or `null` when neither source is configured
    private static byte[] secret(
            @SuppressWarnings("rawtypes") final Map properties,
            final String inlineKey,
            final String fileKey,
            final BiFunction<String, String, byte[]> inlineDecoder,
            final BiFunction<String, String, byte[]> fileDecoder
    ) {
        final String configured = stringProperty(properties, inlineKey, null);
        final String configuredFile = stringProperty(properties, fileKey, null);
        if (configured != null && configuredFile != null) {
            throw new IllegalArgumentException(
                    "%s and %s are mutually exclusive".formatted(inlineKey, fileKey));
        }
        if (configured != null) {
            return inlineDecoder.apply(configured, inlineKey);
        }
        if (configuredFile != null) {
            return fileDecoder.apply(configuredFile, fileKey);
        }
        return null;
    }

    private static byte[] decodeHmacSecret(final String configured, final String property) {
        try {
            final byte[] secret = Base64.getDecoder().decode(configured);
            if (secret.length < AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES) {
                throw new IllegalArgumentException(
                        "%s must decode to at least %s bytes".formatted(
                                property, AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES));
            }
            return secret;
        } catch (final IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "%s must be base64 and decode to at least %s bytes".formatted(
                            property, AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES),
                    failure);
        }
    }

    private static byte[] decodeHmacSecretFile(final String fileName, final String property) {
        final Path path = Paths.get(fileName).toAbsolutePath().normalize();
        try {
            final byte[] encoded = readSecretFile(path, 4_096, "HMAC secret");
            try {
                return decodeHmacSecret(
                        new String(encoded, StandardCharsets.US_ASCII).trim(), property);
            } finally {
                Arrays.fill(encoded, (byte) 0);
            }
        } catch (final IOException | IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "%s is invalid: %s".formatted(property, path), failure);
        }
    }

    /** Reads one HMAC secret through a bounded, stable descriptor snapshot. */
    private static byte[] readSecretFile(final Path path, final int maxBytes, final String description)
            throws IOException {
        final BasicFileAttributes before = Files.readAttributes(
                path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!before.isRegularFile() || Files.isSymbolicLink(path) || before.size() > maxBytes) {
            throw new IOException("%s file must be a regular, non-symbolic file <= %s bytes"
                    .formatted(description, maxBytes));
        }
        validateSecretFilePermissions(path, description);
        final byte[] encoded = new byte[maxBytes + 1];
        try {
            final int length;
            try (FileChannel channel = FileChannel.open(
                    path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                if (channel.size() > maxBytes) {
                    throw new IOException("%s file exceeds %s bytes".formatted(description, maxBytes));
                }
                final ByteBuffer destination = ByteBuffer.wrap(encoded);
                while (destination.hasRemaining()) {
                    final int read = channel.read(destination);
                    if (read < 0) break;
                    if (read == 0) throw new IOException("%s file read made no progress".formatted(description));
                }
                if (destination.position() > maxBytes) {
                    throw new IOException("%s file exceeds %s bytes".formatted(description, maxBytes));
                }
                length = destination.position();
            }
            final BasicFileAttributes after = Files.readAttributes(
                    path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (before.fileKey() == null || after.fileKey() == null ||
                    !Objects.equals(before.fileKey(), after.fileKey()) ||
                    !after.isRegularFile() || Files.isSymbolicLink(path)) {
                throw new IOException("%s file changed while it was being read".formatted(description));
            }
            return Arrays.copyOf(encoded, length);
        } finally {
            Arrays.fill(encoded, (byte) 0);
        }
    }

    private static void validateSecretFilePermissions(final Path path, final String description)
            throws IOException {
        try {
            final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(
                    path, LinkOption.NOFOLLOW_LINKS);
            if (permissions.stream().anyMatch(permission -> permission.name().startsWith("GROUP_") ||
                    permission.name().startsWith("OTHERS_"))) {
                throw new IOException("%s file must not be accessible by group or others".formatted(description));
            }
            final Path parent = path.getParent();
            if (parent != null) {
                final Set<PosixFilePermission> parentPermissions = Files.getPosixFilePermissions(
                        parent, LinkOption.NOFOLLOW_LINKS);
                if (parentPermissions.contains(PosixFilePermission.GROUP_WRITE) ||
                        parentPermissions.contains(PosixFilePermission.OTHERS_WRITE)) {
                    throw new IOException("%s file parent must not be writable by group or others".formatted(description));
                }
            }
        } catch (final UnsupportedOperationException unsupported) {
            throw new IOException("%s file permissions cannot be verified".formatted(description), unsupported);
        }
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
            this.clusteredCacheMessageAcceptor = messageAcceptor;
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

    /// Builds the entity and collection region access behind the same fail-closed
    /// health gate as the timestamps region. Remote entity writes invalidate
    /// through the broadcast timestamps, so serving entity entries while the
    /// receiver is down would silently expose stale state.
    @Override
    protected DomainDataStorageAccess createDomainDataStorageAccess(
            final DomainDataRegionConfig regionConfig,
            final DomainDataRegionBuildingContext buildingContext
    ) {
        /* Entity/collection cache entries have no table-level invalidation
         * key. Keeping them enabled would serve stale entities after a remote
         * writer updates the database. Until key eviction messages exist,
         * disable these regions so Hibernate always loads current state. */
        return new NonCachingStorageAccess();
    }

    /// Builds the query-results region access behind the same fail-closed
    /// health gate: query results are only trustworthy while timestamp
    /// invalidations are flowing.
    @Override
    protected StorageAccess createQueryResultsRegionStorageAccess(
            final String regionName,
            final SessionFactoryImplementor sessionFactory
    ) {
        final ClusteredCacheMessageAcceptor messageAcceptor = this.requireMessageAcceptor(regionName);
        final String defaultedRegionName = this.defaultRegionName(
                regionName,
                sessionFactory,
                DEFAULT_QUERY_RESULTS_REGION_UNQUALIFIED_NAME,
                LEGACY_QUERY_RESULTS_REGION_UNQUALIFIED_NAMES
        );
        return new FailClosedStorageAccess(
                StorageAccess.New(this.getOrCreateCache(defaultedRegionName, sessionFactory)),
                this::ensureClusteredHealthy, messageAcceptor.cacheReadLock());
    }

        /// Returns the clustered message acceptor, failing with a diagnostic
    /// [CacheException] when a region is created before preparation or after
    /// release instead of throwing a bare null dereference.
    ///
    /// @param regionName region being created, for the failure message
    /// @return clustered message acceptor, never `null`
    private ClusteredCacheMessageAcceptor requireMessageAcceptor(final String regionName) {
        final ClusteredCacheMessageAcceptor messageAcceptor = this.clusteredCacheMessageAcceptor;
        if (messageAcceptor == null) {
            throw new CacheException(
                    "Clustered cache resources are not prepared; cannot create the region %s".formatted(regionName));
        }
        return messageAcceptor;
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
        final ClusteredCacheMessageAcceptor messageAcceptor = this.requireMessageAcceptor(regionName);
        final String defaultedRegionName = this.defaultRegionName(
                regionName,
                sessionFactory,
                DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME,
                LEGACY_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAMES
        );
        final var cache = this.getOrCreateCache(defaultedRegionName, sessionFactory);
        cache.registerCacheEntryListener(listenerConfiguration.getUpdateTimestampsCacheEntryListenerConfiguration());
        /* The timestamps cache has just opened: apply any remote updates that
         * arrived while it was not open yet so no loss window remains. */
        messageAcceptor.replayPending();
        return new FailClosedStorageAccess(StorageAccess.New(cache), this::ensureClusteredHealthy,
                messageAcceptor.cacheReadLock());
    }

        /// Prevents Hibernate from serving or mutating any clustered cache region —
    /// entity and collection data, query results, or timestamps — after the
    /// invalidation broadcast has stopped. A volatile broadcast cannot repair a
    /// cache after a receiver gap, so continuing locally would silently serve
    /// stale entries.
    ///
    /// An unhealthy receiver first triggers one bounded-rate automatic
    /// re-synchronization, which invalidates every locally cached timestamp
    /// before declaring the receiver healthy again; only a recovery that
    /// fails (or one already tried inside the retry interval) leaves the
    /// region access refusing operations.
    private void ensureClusteredHealthy() {
        final AeronClusteredCacheMessageReceiver receiver = this.cacheMessageReceiver;
        if (receiver == null) {
            throw new CacheException("Clustered cache invalidation receiver is not initialized");
        }
        if (receiver.failure() == null && receiver.isRunning()) {
            return;
        }
        this.attemptReceiverRecovery(receiver);
        final RuntimeException failure = receiver.failure();
        if (failure != null) {
            throw new CacheException("Clustered cache invalidation receiver has failed", failure);
        }
        if (!receiver.isRunning()) {
            throw new CacheException("Clustered cache invalidation receiver is not running");
        }
    }

    /// Attempts one automatic receiver re-synchronization at most once per
    /// retry interval, so a persistently broken transport cannot turn every
    /// region access into a recovery storm.
    private void attemptReceiverRecovery(final AeronClusteredCacheMessageReceiver receiver) {
        final long now = System.nanoTime();
        final long last = this.lastRecoveryAttemptNanos;
        if (last != 0L && now - last < RECOVERY_RETRY_INTERVAL_NANOS) {
            return;
        }
        this.lastRecoveryAttemptNanos = now;
        try {
            /* Re-synchronization invalidates every cached timestamp before it
             * starts the receiver again, so a repaired receiver serves no
             * state that predates the recovery. */
            receiver.resynchronize();
            LOGGER.log(System.Logger.Level.WARNING,
                    "Clustered cache invalidation receiver re-synchronized after a failure; all cached timestamps were invalidated");
        } catch (final RuntimeException resyncFailure) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Clustered cache invalidation receiver could not be re-synchronized; cache operations stay refused",
                    resyncFailure);
        }
    }

    @Override
    protected void releaseFromUse() {
        Throwable failure = null;
        try {
            this.disposeClusteredResources();
        } catch (final Throwable e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to dispose clustered cache resources.", e);
            failure = e;
        }
        /* The local CacheManager and its regions must be released even when a
         * clustered resource failed to close, so the base release always runs. */
        try {
            super.releaseFromUse();
        } catch (final Throwable releaseFailure) {
            if (failure == null) failure = releaseFailure;
            else if (failure != releaseFailure) failure.addSuppressed(releaseFailure);
        }
        /* A re-prepared factory must not consult the manager released above. */
        this.cacheManager = null;
        this.clusteredCacheMessageAcceptor = null;
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
        this.clusteredCacheMessageAcceptor = null;
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
    /// Every read and mutation path is gated — including the defaulted
    /// `putFromLoad`, `removeFromCache`, and `clearCache` — so no region can be
    /// served or mutated while invalidations are not flowing. Only [#release]
    /// stays ungated: cleanup during shutdown must never throw.
    static final class FailClosedStorageAccess implements StorageAccess {
        private final StorageAccess delegate;
        private final Runnable healthCheck;
        private final Lock cacheReadLock;

        FailClosedStorageAccess(final StorageAccess delegate, final Runnable healthCheck) {
            this(delegate, healthCheck, new ReentrantLock());
        }

        FailClosedStorageAccess(final StorageAccess delegate, final Runnable healthCheck, final Lock cacheReadLock) {
            this.delegate = delegate;
            this.healthCheck = healthCheck;
            this.cacheReadLock = cacheReadLock;
        }

        private void check() {
            /* Recovery may invalidate all caches, so run it before acquiring
             * the read side of the acceptor's invalidation lock. */
            this.healthCheck.run();
        }

        @Override
        public Object getFromCache(
                final Object key,
                final SharedSessionContractImplementor session
        ) {
            this.check();
            this.cacheReadLock.lock();
            try {
                return this.delegate.getFromCache(key, session);
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public void putIntoCache(
                final Object key,
                final Object value,
                final SharedSessionContractImplementor session
        ) {
            this.check();
            this.cacheReadLock.lock();
            try {
                this.delegate.putIntoCache(key, value, session);
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public void putFromLoad(
                final Object key,
                final Object value,
                final SharedSessionContractImplementor session
        ) {
            this.check();
            this.cacheReadLock.lock();
            try {
                this.delegate.putFromLoad(key, value, session);
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public void removeFromCache(
                final Object key,
                final SharedSessionContractImplementor session
        ) {
            this.check();
            this.cacheReadLock.lock();
            try {
                this.delegate.removeFromCache(key, session);
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public void clearCache(final SharedSessionContractImplementor session) {
            this.check();
            this.cacheReadLock.lock();
            try {
                this.delegate.clearCache(session);
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public boolean contains(final Object key) {
            this.check();
            this.cacheReadLock.lock();
            try {
                return this.delegate.contains(key);
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public void evictData() {
            this.check();
            this.cacheReadLock.lock();
            try {
                this.delegate.evictData();
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public void evictData(final Object key) {
            this.check();
            this.cacheReadLock.lock();
            try {
                this.delegate.evictData(key);
            } finally {
                this.cacheReadLock.unlock();
            }
        }

        @Override
        public void release() {
            this.delegate.release();
        }
    }

    /** Disables a region whose entries cannot be invalidated by the wire schema. */
    static final class NonCachingStorageAccess implements StorageAccess {
        @Override public Object getFromCache(final Object key, final SharedSessionContractImplementor session) { return null; }
        @Override public void putIntoCache(final Object key, final Object value, final SharedSessionContractImplementor session) { }
        @Override public void putFromLoad(final Object key, final Object value, final SharedSessionContractImplementor session) { }
        @Override public void removeFromCache(final Object key, final SharedSessionContractImplementor session) { }
        @Override public void clearCache(final SharedSessionContractImplementor session) { }
        @Override public boolean contains(final Object key) { return false; }
        @Override public void evictData() { }
        @Override public void evictData(final Object key) { }
        @Override public void release() { }
    }
}
