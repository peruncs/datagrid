package peruncs.cluster.node.aeron;

import peruncs.cluster.test.DirectBufferReceiver;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.replication.ReplicationHealth;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.storage.ReplicationPosition;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies provider health reflects writer readiness and replication state.
class AeronReplicationMonitoringTest {
    @TempDir
    Path temporaryDirectory;

    /// Builds properties whose directories live under the test's own temporary
    /// directory, so every validation and runtime start is cleaned up by JUnit
    /// instead of accumulating in the system temporary directory.
    private NodeConfig properties(final String role) {
        return propertiesWith(role, null, null);
    }

    private NodeConfig propertiesWith(final String role, final String overrideName, final String overrideValue) {
        return propertiesWith(role, overrideName, overrideValue, false);
    }

    private NodeConfig propertiesWith(
            final String role, final String overrideName, final String overrideValue, final boolean production) {
        final Path root = temporaryDirectory.resolve("monitoring-%s".formatted(UUID.randomUUID()));
        final Map<String, String> overrides = overrideName == null ? Map.of() : Map.of(overrideName, overrideValue);
        return TestNodeConfig.aeron(root, role, production, overrides);
    }

    /// Verifies the provider creates a transport that identifies itself as aeron.
    @Test
    void aeronProviderCreatesAeronTransport() {
        try (final ClusterReplicationTransport transport = new AeronTransport(properties("writer"))) {
            assertNotNull(transport.replicationMark());
        }
    }

    @Test
    void registersOneStableReplicationMarkRootBeforeStoreStart() {
        final Path storePath = temporaryDirectory.resolve("mark-store");
        final EmbeddedStorageFoundation<?> foundation = EmbeddedStorage.Foundation(
                StorageConfiguration.Builder().setStorageFileProvider(Storage.FileProvider(storePath))
                        .createConfiguration());
        try (final AeronTransport transport = new AeronTransport(properties("reader"))) {
            transport.registerPersistentRoots(foundation);
            try (final var storage = foundation.start()) {
                final Object[] registered = new Object[1];
                storage.viewRoots().iterateEntries((identifier, root) -> {
                    if (ReplicationMark.ROOT_ID.equals(identifier)) registered[0] = root;
                });
                final ReplicationMark mark = assertInstanceOf(ReplicationMark.class, registered[0]);
                assertEquals(transport.settings().topology().clusterId(), mark.clusterId());
                assertEquals(transport.settings().topology().identity().storeGeneration(),
                        mark.storeGeneration());
            }

            final EmbeddedStorageFoundation<?> conflicting = EmbeddedStorage.Foundation(
                    StorageConfiguration.Builder().setStorageFileProvider(
                                    Storage.FileProvider(temporaryDirectory.resolve("conflicting-mark-store")))
                            .createConfiguration());
            conflicting.getConnectionFoundation().getRootResolverProvider()
                    .registerRoot(ReplicationMark.ROOT_ID, new Object());
            assertThrows(NodeException.class, () -> transport.registerPersistentRoots(conflicting));
        }
    }

    /// A writer never subscribes to its own recording: its reader transport hands back an inert applier.
    @Test
    void writerRoleGetsAnInertReaderClient() {
        try (final ClusterReplicationTransport transport = new AeronTransport(properties("writer"))) {
            final ReplicationMark mark = new ReplicationMark(UUID.randomUUID(), UUID.randomUUID(), 1L, -1L);
            final ReplicationApplier applier = transport.clientFromMark(new DirectBufferReceiver() {
                @Override
                public void receiveData(final Binary data) {
                    throw new AssertionError("a writer must not receive data");
                }

                @Override
                public void receiveTypeDictionary(final String typeDictionaryData) {
                    throw new AssertionError("a writer must not receive a dictionary");
                }
            }, mark);
            assertFalse(applier.isRunning());
            assertNull(applier.failure());
        }
    }

    /// Verifies writer provider exposes aeron and reports live without reader client.
    @Test
    void writerProviderExposesAeronAndReportsLiveWithoutReaderClient() {
        try (final ClusterReplicationTransport transport = new AeronTransport(properties("writer"))) {
            /* Health inspection must not start the runtime. Start it through the
             * explicit position-provider lifecycle first. */
            final ReplicationPositionProvider positionProvider = transport.positionProvider();
            positionProvider.init();
            positionProvider.latest();
            final ReplicationApplier client = ReplicationApplier.noOp();
            final ReplicationHealth health = transport.health(() -> true, client);
            assertNotNull(transport.replicationMark());
            assertTrue(health.isReady());
            assertTrue(health.isHealthy());
            assertEquals(ReplicationState.LIVE, health.state());
            health.close();
        }
    }

    /// Verifies position provider uses self describing recording position.
    @Test
    void positionProviderUsesSelfDescribingRecordingPosition() {
        try (final ClusterReplicationTransport transport = new AeronTransport(properties("writer"))) {
            final ReplicationPositionProvider positionProvider = transport.positionProvider();
            assertThrows(ReplicationPositionUnavailableException.class, positionProvider::latest);
            positionProvider.init();
            final ReplicationPosition cursor = positionProvider.latest();
            assertNotNull(cursor.clusterId());
            assertNotNull(cursor.storeGeneration());
            assertEquals(cursor.sequence(), positionProvider.latest().sequence());
        }
    }

    /// The writer rejects a Store transaction that omits its reserved mark.
    @Test
    void writerRejectsCommitWithoutStoreMark() {
        try (final ClusterReplicationTransport transport = new AeronTransport(properties("writer"))) {
            final EmbeddedStorageFoundation<?> foundation = EmbeddedStorage.Foundation(
                    StorageConfiguration.Builder()
                            .setStorageFileProvider(Storage.FileProvider(temporaryDirectory.resolve("unmarked-store")))
                            .createConfiguration());
            transport.registerPersistentRoots(foundation);
            transport.positionProvider().init();
            final Distribution distributor = new Distribution();
            final PersistenceTarget<Binary> target = transport.persistenceTargetFactory(distributor.outbox, distributor.enabled(), () -> null)
                    .apply(new PersistenceTarget<>() {
                        public void write(final Binary ignored) {
                        }

                        public boolean isWritable() {
                            return true;
                        }
                    });
            final WriteRejectedException failure = assertThrows(WriteRejectedException.class,
                    () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1, 2, 3}))));
            assertTrue(failure.getMessage().contains("does not contain its replication mark"));
        }
    }

    /// Retention stays unsupported until the configured reader quorum is present.
    @Test
    void retentionRejectsDeletionUntilWatermarksAreConfigured() {
        try (final ClusterReplicationTransport transport = new AeronTransport(properties("writer"))) {
            assertThrows(UnsupportedOperationException.class,
                    () -> transport.retention().deleteThrough(ReplicationPosition.NONE));
        }
    }

    /// Verifies reader provider surfaces replay and failure states.
    @Test
    void readerProviderSurfacesReplayAndFailureStates() {
        try (final ClusterReplicationTransport transport = new AeronTransport(properties("reader"))) {
            final TestClient replaying = new TestClient(true, null);
            final ReplicationHealth health = transport.health(() -> true, replaying);
            assertFalse(health.isReady(), "a replaying reader is not ready to serve traffic");
            assertTrue(health.isHealthy());
            assertEquals(ReplicationState.REPLAYING, health.state());

            final TestClient failed = new TestClient(false, new IllegalStateException("archive unavailable"));
            final ReplicationHealth failedHealth = transport.health(() -> true, failed);
            assertFalse(failedHealth.isReady());
            assertFalse(failedHealth.isHealthy());
            assertEquals(ReplicationState.FAILED, failedHealth.state());
            assertThrows(ReplicationPositionUnavailableException.class,
                    () -> transport.positionProvider().latest(),
                    "a reader cannot substitute its applied cursor for the writer's durable boundary");
            health.close();
            failedHealth.close();
        }
    }

    /// Verifies rejection of invalid aeron epoch and stream settings.
    @Test
    void rejectsInvalidAeronEpochAndStreamSettings() {
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_EPOCH", "-1")));
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_STREAM_ID", "-1")));
    }

    /// Rejects an invalid Archive free-space admission threshold.
    @Test
    void rejectsNegativeArchiveCapacityThreshold() {
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_MIN_ARCHIVE_FREE_BYTES", "-1")));
    }

    /// The writer admission gate and health state fail closed when usable space is below the threshold.
    @Test
    void reportsArchiveCapacityDegradationBeforeAcceptingWrites() {
        try (final ClusterReplicationTransport transport = new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_MIN_ARCHIVE_FREE_BYTES",
                        Long.toString(Long.MAX_VALUE)))) {
            final ReplicationApplier client = ReplicationApplier.noOp();
            final ReplicationHealth health = transport.health(() -> true, client);
            assertFalse(health.isReady());
            assertFalse(health.isHealthy());
            assertEquals(ReplicationState.DEGRADED, health.state());
            health.close();
        }
    }

    /// Rejects channel framing overrides that disagree with the shared configuration.
    @Test
    void rejectsConflictingChannelFraming() {
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=1m")));
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_REPLAY_CHANNEL",
                        "aeron:udp?endpoint=localhost:0|mtu=1024k")));
    }

    /// Writer topology validation is semantic, not a substring match.
    @Test
    void rejectsNonDynamicWriterTopology() {
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=localhost:40123|control-mode=manual|fc=max")));
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=min")));
    }

    /// Verifies rejection of malformed numeric and production temporary directory settings.
    @Test
    void rejectsMalformedNumericAndProductionTemporaryDirectorySettings() {
        assertThrows(IllegalArgumentException.class,
                () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_EPOCH", "not-a-number")));
        final NodeConfig production = TestNodeConfig.aeron(temporaryDirectory.resolve("production"),
                "writer", true, Map.of(NodeConfig.Setting.AERON_DIRECTORY.key(),
                        temporaryDirectory.resolve("production").toString()));
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(production));
    }

    /// Production mode rejects the two common configuration forms that weaken network/durability guarantees.
    @Test
    void rejectsProductionSyncLevelZeroAndIpv6Wildcard() {
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_FILE_SYNC_LEVEL", "0", true)));
        assertThrows(IllegalArgumentException.class, () -> new AeronTransport(propertiesWith("writer", "PERUNCS_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=[::]:40123|control-mode=dynamic|fc=max", true)));
    }

    private record TestClient(boolean isRunning, RuntimeException failure) implements ReplicationApplier {
        @Override
        public void start() {
        }

        @Override
        public void stopAtLatestMessage() {
        }

        @Override
        public ReplicationPosition position() {
            return ReplicationPosition.NONE;
        }

        @Override
        public void resume() {
        }

        @Override
        public boolean isLive() {
            return false;
        }

        @Override
        public void dispose() {
        }
    }
}
