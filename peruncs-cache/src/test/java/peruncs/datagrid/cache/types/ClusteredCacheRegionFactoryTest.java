package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.hibernate.types.StorageAccess;
import org.hibernate.cache.CacheException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cache.aeron.AeronClusteredCacheConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the region factory health gate and configuration translation.
class ClusteredCacheRegionFactoryTest {
    @Test
    void invalidationWaitsForAnAdmittedCacheRead() throws Exception {
        final var acceptor = new ClusteredCacheMessageAcceptor(null);
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var invalidationStarted = new CountDownLatch(1);
        final var invalidated = new AtomicBoolean();
        final StorageAccess delegate = new StorageAccess() {
            @Override public Object getFromCache(final Object key, final SharedSessionContractImplementor session) {
                entered.countDown();
                try {
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    fail(interrupted);
                }
                return "value";
            }
            @Override public void putIntoCache(final Object key, final Object value, final SharedSessionContractImplementor session) { }
            @Override public boolean contains(final Object key) { return false; }
            @Override public void evictData() { }
            @Override public void evictData(final Object key) { }
            @Override public void release() { }
        };
        final var access = new ClusteredCacheRegionFactory.FailClosedStorageAccess(
                delegate, () -> { }, acceptor.cacheReadLock());
        final var readFailure = new AtomicReference<Throwable>();
        final Thread reader = Thread.ofVirtual().start(() -> {
            try {
                assertEquals("value", access.getFromCache("key", null));
            } catch (final Throwable failure) {
                readFailure.set(failure);
            }
        });
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        final Thread invalidator = Thread.ofVirtual().start(() -> {
            invalidationStarted.countDown();
            acceptor.invalidateAll();
            invalidated.set(true);
        });
        try {
            assertTrue(invalidationStarted.await(10, TimeUnit.SECONDS));
            assertFalse(invalidator.join(java.time.Duration.ofMillis(100)),
                    "invalidation must wait until the admitted read finishes");
        } finally {
            release.countDown();
            reader.join();
            invalidator.join();
        }
        assertNull(readFailure.get(), "admitted cache read failed");
        assertTrue(invalidated.get());
    }

    @Test
    void failClosedStorageAccessRefusesOperationsAfterReceiverFailure() {
        final boolean[] healthy = {true};
        final boolean[] used = {false};
        final StorageAccess delegate = new StorageAccess() {
            @Override
            public Object getFromCache(final Object key, final SharedSessionContractImplementor session) {
                used[0] = true;
                return null;
            }

            @Override
            public void putIntoCache(final Object key, final Object value, final SharedSessionContractImplementor session) {
                used[0] = true;
            }

            @Override
            public boolean contains(final Object key) {
                used[0] = true;
                return false;
            }

            @Override
            public void evictData() {
                used[0] = true;
            }

            @Override
            public void evictData(final Object key) {
                used[0] = true;
            }

            @Override
            public void release() {
                used[0] = true;
            }
        };
        final StorageAccess guarded = new ClusteredCacheRegionFactory.FailClosedStorageAccess(
                delegate, () ->
        {
            if (!healthy[0]) {
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
    void clusteredCacheConfigurationTranslatesHibernateKeys() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.channel", "aeron:ipc");
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.stream-id", "2001");

        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties);

        assertEquals("aeron:ipc", configuration.channel());
        assertEquals(2001, configuration.streamId());
        assertNull(configuration.nodeId());
    }

    @Test
    void clusteredCacheConfigurationTranslatesDurabilityKeys() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.heartbeat-interval-millis", "100");
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.freshness-timeout-millis", "500");
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.cursor-directory", "/tmp/cursors");

        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties);

        assertEquals(100L, configuration.heartbeatIntervalMillis());
        assertEquals(500L, configuration.freshnessTimeoutMillis());
        assertEquals("/tmp/cursors", configuration.cursorDirectory());
    }

    @Test
    void clusteredCacheConfigurationRejectsFreshnessWithinHeartbeat() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.heartbeat-interval-millis", "500");
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.freshness-timeout-millis", "500");

        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties),
                "the freshness timeout must exceed the heartbeat interval");
    }

    @Test
    void clusteredCacheConfigurationUsesDurabilityDefaults() {
        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(new HashMap<>());

        assertEquals(AeronClusteredCacheConfiguration.DEFAULT_HEARTBEAT_INTERVAL_MILLIS,
                configuration.heartbeatIntervalMillis());
        assertEquals(AeronClusteredCacheConfiguration.DEFAULT_FRESHNESS_TIMEOUT_MILLIS,
                configuration.freshnessTimeoutMillis());
        assertNull(configuration.cursorDirectory());
    }

        /// The fixed payload schema removed the serializer type provider, so the
    /// old key must fail loudly instead of being silently ignored.
    @Test
    void clusteredCacheConfigurationRejectsRemovedSerializationTypesProviderKey() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.serialization-types-provider", "configured");

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties));
        assertTrue(failure.getMessage().contains("serialization-types-provider"),
                "the failure must name the removed key: " + failure.getMessage());
    }

    @Test
    void failClosedStorageAccessGatesEveryRegionOperation() {
        final boolean[] healthy = {true};
        final TrackingAccess delegate = new TrackingAccess();
        final StorageAccess guarded = new ClusteredCacheRegionFactory.FailClosedStorageAccess(
                delegate, () ->
        {
            if (!healthy[0]) {
                throw new CacheException("receiver failed");
            }
        });

        guarded.getFromCache("key", null);
        guarded.putIntoCache("key", "value", null);
        guarded.putFromLoad("key", "value", null);
        guarded.removeFromCache("key", null);
        guarded.clearCache(null);
        guarded.contains("key");
        guarded.evictData();
        guarded.evictData("key");
        assertTrue(delegate.allUsed(), "a healthy receiver must delegate every region operation");
        delegate.reset();

        healthy[0] = false;
        assertThrows(CacheException.class, () -> guarded.getFromCache("key", null));
        assertThrows(CacheException.class, () -> guarded.putIntoCache("key", "value", null));
        assertThrows(CacheException.class, () -> guarded.putFromLoad("key", "value", null));
        assertThrows(CacheException.class, () -> guarded.removeFromCache("key", null));
        assertThrows(CacheException.class, () -> guarded.clearCache(null));
        assertThrows(CacheException.class, () -> guarded.contains("key"));
        assertThrows(CacheException.class, guarded::evictData);
        assertThrows(CacheException.class, () -> guarded.evictData("key"));
        assertTrue(delegate.noneUsed(),
                "a failed receiver must prevent every region operation from reaching the local cache");

        guarded.release();
        assertTrue(delegate.released, "release must stay ungated so shutdown cleanup never throws");
    }

    @Test
    void regionFactoryGatesEveryRegionKind() throws Exception {
        assertEquals(ClusteredCacheRegionFactory.class,
                ClusteredCacheRegionFactory.class.getDeclaredMethod("createDomainDataStorageAccess",
                        org.hibernate.cache.cfg.spi.DomainDataRegionConfig.class,
                        org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext.class).getDeclaringClass(),
                "entity and collection regions must be health-gated, or remote writes would never invalidate them");
        assertEquals(ClusteredCacheRegionFactory.class,
                ClusteredCacheRegionFactory.class.getDeclaredMethod("createQueryResultsRegionStorageAccess",
                        String.class, org.hibernate.engine.spi.SessionFactoryImplementor.class).getDeclaringClass(),
                "query-result regions must be health-gated");
        assertEquals(ClusteredCacheRegionFactory.class,
                ClusteredCacheRegionFactory.class.getDeclaredMethod("createTimestampsRegionStorageAccess",
                        String.class, org.hibernate.engine.spi.SessionFactoryImplementor.class).getDeclaringClass(),
                "timestamps regions must stay health-gated");
    }

    @Test
    void clusteredCacheConfigurationTranslatesSecurityKeys() {
        final byte[] secret = new byte[32];
        for (int index = 0; index < secret.length; index++) {
            secret[index] = (byte) index;
        }
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret",
                java.util.Base64.getEncoder().encodeToString(secret));
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.production-mode", "true");

        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties);

        assertTrue(configuration.authenticated(), "a configured secret must authenticate frames");
        assertTrue(configuration.productionMode());
        assertFalse(configuration.allowUnsignedFrames());
        assertArrayEquals(secret, configuration.hmacSecret());
        assertNotNull(configuration.hmacSecret(), "a configured secret must be present");
        configuration.hmacSecret()[0] = (byte) 0xFF;
        assertArrayEquals(secret, configuration.hmacSecret(), "the accessor must return a copy");
    }

    @Test
    void clusteredCacheConfigurationRejectsInsecureProduction() {
        final Map<String, Object> insecureProd = new HashMap<>();
        insecureProd.put("hibernate.cache.eclipsestore.clustered.aeron.production-mode", "true");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(insecureProd),
                "production mode must reject unsigned frames without an explicit acknowledgement");

        final Map<String, Object> acknowledged = new HashMap<>(insecureProd);
        acknowledged.put("hibernate.cache.eclipsestore.clustered.aeron.allow-unsigned-frames", "true");
        assertTrue(ClusteredCacheRegionFactory.clusteredCacheConfiguration(acknowledged).allowUnsignedFrames());

        final Map<String, Object> contradictory = new HashMap<>();
        contradictory.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret",
                java.util.Base64.getEncoder().encodeToString(new byte[32]));
        contradictory.put("hibernate.cache.eclipsestore.clustered.aeron.allow-unsigned-frames", "true");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(contradictory),
                "a secret combined with an unsigned acknowledgement is contradictory");

        final Map<String, Object> shortSecret = new HashMap<>();
        shortSecret.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret",
                java.util.Base64.getEncoder().encodeToString(new byte[8]));
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(shortSecret),
                "a short secret must be rejected");

        final Map<String, Object> notBase64 = new HashMap<>();
        notBase64.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret", "not-base64!!");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(notBase64));

        final Map<String, Object> bothSources = new HashMap<>(contradictory);
        bothSources.remove("hibernate.cache.eclipsestore.clustered.aeron.allow-unsigned-frames");
        bothSources.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret-file", "/tmp/secret");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(bothSources),
                "the inline secret and the secret file are mutually exclusive");
    }

    @Test
    void clusteredCacheConfigurationTranslatesRotationOverlapKey() {
        final byte[] primary = new byte[32];
        final byte[] previous = new byte[32];
        for (int index = 0; index < 32; index++) {
            primary[index] = (byte) index;
            previous[index] = (byte) (31 - index);
        }
        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret",
                java.util.Base64.getEncoder().encodeToString(primary));
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret-previous",
                java.util.Base64.getEncoder().encodeToString(previous));

        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties);

        assertArrayEquals(previous, configuration.previousHmacSecret());
        configuration.previousHmacSecret()[0] = (byte) 0xFF;
        assertArrayEquals(previous, configuration.previousHmacSecret(), "the accessor must return a copy");

        final Map<String, Object> previousOnly = new HashMap<>();
        previousOnly.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret-previous",
                java.util.Base64.getEncoder().encodeToString(previous));
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(previousOnly),
                "a previous key without its primary must be rejected");

        final Map<String, Object> sameKey = new HashMap<>(properties);
        sameKey.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret-previous",
                java.util.Base64.getEncoder().encodeToString(primary));
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(sameKey),
                "a rotation to the same key must be rejected");

        final Map<String, Object> previousBothSources = new HashMap<>(properties);
        previousBothSources.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret-previous-file", "/tmp/secret");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(previousBothSources),
                "the inline previous secret and its file are mutually exclusive");
    }

    @Test
    void clusteredCacheConfigurationLoadsSecretFile(@org.junit.jupiter.api.io.TempDir final java.nio.file.Path root)
            throws Exception {
        final byte[] secret = new byte[32];
        for (int index = 0; index < secret.length; index++) {
            secret[index] = (byte) (index + 1);
        }
        final java.nio.file.Path guarded = root.resolve("secret");
        java.nio.file.Files.writeString(guarded, java.util.Base64.getEncoder().encodeToString(secret));
        java.nio.file.Files.setPosixFilePermissions(guarded,
                java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));

        final Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret-file", guarded.toString());
        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(properties);
        assertArrayEquals(secret, configuration.hmacSecret());

        final java.nio.file.Path exposed = root.resolve("exposed");
        java.nio.file.Files.writeString(exposed, java.util.Base64.getEncoder().encodeToString(secret));
        java.nio.file.Files.setPosixFilePermissions(exposed,
                java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
        final Map<String, Object> exposedProperties = new HashMap<>();
        exposedProperties.put("hibernate.cache.eclipsestore.clustered.aeron.hmac-secret-file", exposed.toString());
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(exposedProperties),
                "a group-readable secret file must be rejected");
    }

    @Test
    void clusteredCacheConfigurationNormalizesAndRejectsValues() {
        final UUID nodeId = UUID.randomUUID();
        final Map<String, Object> normalized = new HashMap<>();
        normalized.put("hibernate.cache.eclipsestore.clustered.aeron.node-id", "  %s  ".formatted(nodeId));
        normalized.put("hibernate.cache.eclipsestore.clustered.aeron.embedded-driver", "TRUE");

        final var configuration = ClusteredCacheRegionFactory.clusteredCacheConfiguration(normalized);

        assertEquals(nodeId, configuration.nodeId());
        assertTrue(configuration.embeddedDriver());

        final Map<String, Object> badBoolean = new HashMap<>();
        badBoolean.put("hibernate.cache.eclipsestore.clustered.aeron.embedded-driver", "maybe");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(badBoolean));

        final Map<String, Object> badNodeId = new HashMap<>();
        badNodeId.put("hibernate.cache.eclipsestore.clustered.aeron.node-id", "not-a-uuid");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(badNodeId),
                "an invalid node id must be rejected");

        final Map<String, Object> badStream = new HashMap<>();
        badStream.put("hibernate.cache.eclipsestore.clustered.aeron.stream-id", "-1");
        assertThrows(IllegalArgumentException.class,
                () -> ClusteredCacheRegionFactory.clusteredCacheConfiguration(badStream));
    }

        /// StorageAccess stub recording every delegated call.
    private static final class TrackingAccess implements StorageAccess {
        private boolean got;
        private boolean put;
        private boolean putFromLoad;
        private boolean removed;
        private boolean cleared;
        private boolean contained;
        private boolean evicted;
        private boolean evictedKey;
        private boolean released;

        private boolean allUsed() {
            return this.got && this.put && this.putFromLoad && this.removed && this.cleared &&
                   this.contained && this.evicted && this.evictedKey;
        }

        private boolean noneUsed() {
            return !this.got && !this.put && !this.putFromLoad && !this.removed && !this.cleared &&
                   !this.contained && !this.evicted && !this.evictedKey;
        }

        private void reset() {
            this.got = false;
            this.put = false;
            this.putFromLoad = false;
            this.removed = false;
            this.cleared = false;
            this.contained = false;
            this.evicted = false;
            this.evictedKey = false;
        }

        @Override
        public Object getFromCache(final Object key, final SharedSessionContractImplementor session) {
            this.got = true;
            return null;
        }

        @Override
        public void putIntoCache(final Object key, final Object value, final SharedSessionContractImplementor session) {
            this.put = true;
        }

        @Override
        public void putFromLoad(final Object key, final Object value, final SharedSessionContractImplementor session) {
            this.putFromLoad = true;
        }

        @Override
        public void removeFromCache(final Object key, final SharedSessionContractImplementor session) {
            this.removed = true;
        }

        @Override
        public void clearCache(final SharedSessionContractImplementor session) {
            this.cleared = true;
        }

        @Override
        public boolean contains(final Object key) {
            this.contained = true;
            return false;
        }

        @Override
        public void evictData() {
            this.evicted = true;
        }

        @Override
        public void evictData(final Object key) {
            this.evictedKey = true;
        }

        @Override
        public void release() {
            this.released = true;
        }
    }

    @Test
    void regionAccessBeforePreparationFailsClosed() {
        final var factory = new ClusteredCacheRegionFactory();
        final CacheException queryFailure = assertThrows(CacheException.class, () ->
                factory.createQueryResultsRegionStorageAccess("query-results", null));
        assertTrue(queryFailure.getMessage().contains("not prepared"),
                "unprepared query access must fail with a diagnostic message, was: %s"
                        .formatted(queryFailure.getMessage()));
        final CacheException timestampsFailure = assertThrows(CacheException.class, () ->
                factory.createTimestampsRegionStorageAccess("timestamps", null));
        assertTrue(timestampsFailure.getMessage().contains("not prepared"),
                "unprepared timestamps access must fail with a diagnostic message, was: %s"
                        .formatted(timestampsFailure.getMessage()));
    }
}
