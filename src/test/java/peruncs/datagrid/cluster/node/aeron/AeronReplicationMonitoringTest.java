package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies provider health reflects writer readiness and checkpoint state.
class AeronReplicationMonitoringTest {
    private static NodeLibraryPropertiesProvider properties(final String role) {
        return propertiesWith(role, null, null);
    }

    private static NodeLibraryPropertiesProvider propertiesWith(
            final String role, final String overrideName, final String overrideValue) {
        return propertiesWith(role, overrideName, overrideValue, false);
    }

    private static NodeLibraryPropertiesProvider propertiesWith(
            final String role, final String overrideName, final String overrideValue, final boolean production) {
        final String clusterId = UUID.randomUUID().toString();
        final Path root = Paths.get(System.getProperty("java.io.tmpdir"),
                "datagrid-aeron-monitoring-%s".formatted(UUID.randomUUID()));
        return new TestNodeProperties() {
            @Override
            public String replicationRole() {
                return role;
            }

            @Override
            public boolean isProdMode() {
                return production;
            }

            @Override
            public String replicationProperty(final String name) {
                if ("ECLIPSE_DATAGRID_AERON_CLUSTER_ID".equals(name)) return clusterId;
                if ("ECLIPSE_DATAGRID_AERON_NODE_ID".equals(name)) return UUID.randomUUID().toString();
                if ("ECLIPSE_DATAGRID_AERON_STORE_GENERATION".equals(name)) return UUID.randomUUID().toString();
                if ("ECLIPSE_DATAGRID_AERON_DIRECTORY".equals(name)) return root.resolve("driver").toString();
                if ("ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY".equals(name)) return root.resolve("archive").toString();
                if ("ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH".equals(name)) {
                    return root.resolve("checkpoint/writer.checkpoint").toString();
                }
                if ("ECLIPSE_DATAGRID_BACKUP_PATH".equals(name)) return root.resolve("backups").toString();
                return overrideName != null && overrideName.equals(name) ? overrideValue : null;
            }
        };
    }

    /// Verifies the provider creates a transport that identifies itself as aeron.
    @Test
    void aeronProviderCreatesAeronTransport() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties("writer"))) {
            assertEquals("aeron", transport.id());
        }
    }

        /// Verifies writer provider exposes aeron and reports live without reader client.
    @Test
    void writerProviderExposesAeronAndReportsLiveWithoutReaderClient() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties("writer"))) {
            /* Health inspection must not start the runtime. Start it through the
             * explicit position-provider lifecycle first. */
            final ReplicationPositionProvider positionProvider = transport.positionProvider("stream");
            positionProvider.init();
            positionProvider.latest();
            final StorageBinaryDataClient client = transport.client(null, "stream", null, null, false);
            final ReplicationHealth health = transport.health(() -> true, client);
            assertEquals("aeron", transport.id());
            assertTrue(health.isReady());
            assertTrue(health.isHealthy());
            assertEquals(ReplicationHealth.State.LIVE, health.state());
            health.close();
        }
    }

        /// Verifies position provider uses self describing recording position.
    @Test
    void positionProviderUsesSelfDescribingRecordingPosition() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties("writer"))) {
            final ReplicationPositionProvider positionProvider = transport.positionProvider("stream");
            assertThrows(ReplicationPositionUnavailableException.class, positionProvider::latest);
            positionProvider.init();
            final ReplicationCursor cursor = positionProvider.latest();
            assertEquals("aeron", cursor.transport());
            final AeronReplicationCursor aeronCursor = AeronReplicationCursor.decode(cursor.providerPositionBytes());
            assertEquals(cursor.logicalSequence(), aeronCursor.sequence());
            assertEquals(cursor.storeGeneration(), aeronCursor.storeGeneration());
        }
    }

        /// The embedded writer commits through its local Archive spy with no remote reader.
    @Test
    void writerCommitsWithoutRemoteReader() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties("writer"))) {
            transport.positionProvider("stream").init();
            final StorageBinaryDataDistributor distributor = transport.distributor("stream", false);
            final PersistenceTarget<Binary> target = transport.persistenceTargetFactory("stream", distributor)
                    .apply(new PersistenceTarget<>() {
                        public void write(final Binary ignored) {
                        }

                        public boolean isWritable() {
                            return true;
                        }
                    });
            target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1, 2, 3})));
            assertEquals(0L, transport.positionProvider("stream").latest().logicalSequence());
        }
    }

        /// Verifies Store binaries cannot bypass the fenced persistence target.
    @Test
    void distributorRejectsDataWithoutAStoreTarget() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties("writer"))) {
            final StorageBinaryDataDistributor distributor = transport.distributor("stream", false);
            assertThrows(IllegalStateException.class,
                    () -> distributor.distributeData(ChunksWrapper.New(
                            XMemory.toDirectByteBuffer(new byte[]{3, 2, 1}))));
        }
    }

    /// Verifies a reader-side gate rejects writer sequence synchronization and keeps its index unset.
    @Test
    void readerCannotChangeWriterMessageIndex() {
        final AeronDistributionGate distributor = new AeronDistributionGate(() -> false,
                ignored -> {
                    throw new AssertionError("reader must not synchronize a writer sequence");
                });
        assertThrows(IllegalStateException.class, () -> distributor.messageIndex(0L));
        assertEquals(-1L, distributor.messageIndex());
    }

        /// Retention must fail explicitly while authenticated watermarks are absent.
    @Test
    void retentionRejectsDeletionUntilWatermarksAreConfigured() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties("writer"))) {
            assertThrows(UnsupportedOperationException.class,
                    () -> transport.retention().deleteThrough(new ReplicationCursor("aeron", null, -1, "")));
        }
    }

        /// Verifies reader provider surfaces replay and failure states.
    @Test
    void readerProviderSurfacesReplayAndFailureStates() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties("reader"))) {
            final TestClient replaying = new TestClient(true, null);
            final ReplicationHealth health = transport.health(() -> true, replaying);
            assertFalse(health.isReady(), "a replaying reader is not ready to serve traffic");
            assertTrue(health.isHealthy());
            assertEquals(ReplicationHealth.State.REPLAYING, health.state());

            final TestClient failed = new TestClient(false, new IllegalStateException("archive unavailable"));
            final ReplicationHealth failedHealth = transport.health(() -> true, failed);
            assertFalse(failedHealth.isReady());
            assertFalse(failedHealth.isHealthy());
            assertEquals(ReplicationHealth.State.FAILED, failedHealth.state());
            assertThrows(ReplicationPositionUnavailableException.class,
                    () -> transport.positionProvider("stream").latest(),
                    "a reader cannot substitute its applied cursor for the writer's durable boundary");
            health.close();
            failedHealth.close();
        }
    }

        /// Verifies rejection of invalid aeron epoch and stream settings.
    @Test
    void rejectsInvalidAeronEpochAndStreamSettings() {
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_EPOCH", "-1")));
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_STREAM_ID", "-1")));
    }

        /// Rejects an invalid Archive free-space admission threshold.
    @Test
    void rejectsNegativeArchiveCapacityThreshold() {
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES", "-1")));
    }

        /// The writer admission gate and health state fail closed when usable space is below the threshold.
    @Test
    void reportsArchiveCapacityDegradationBeforeAcceptingWrites() {
        try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES",
                        Long.toString(Long.MAX_VALUE)))) {
            final StorageBinaryDataClient client = transport.client(null, "stream", null, null, false);
            final ReplicationHealth health = transport.health(() -> true, client);
            assertFalse(health.isReady());
            assertFalse(health.isHealthy());
            assertEquals(ReplicationHealth.State.DEGRADED_ARCHIVE, health.state());
            health.close();
        }
    }

        /// Rejects channel framing overrides that disagree with the shared configuration.
    @Test
    void rejectsConflictingChannelFraming() {
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=1m")));
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                        "aeron:udp?endpoint=localhost:0|mtu=1024k")));
    }

        /// Writer topology validation is semantic, not a substring match.
    @Test
    void rejectsNonDynamicWriterTopology() {
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=localhost:40123|control-mode=manual|fc=max")));
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=min")));
    }

        /// A lease stored inside the Aeron driver tree is deleted by the next
    /// driver start, so the wiring is rejected instead of silently losing it.
    @Test
    void rejectsLeaseDirectoryInsideAeronDirectory() {
        final Path root = Paths.get(System.getProperty("java.io.tmpdir"),
                "datagrid-lease-overlap-%s".formatted(UUID.randomUUID()));
        final NodeLibraryPropertiesProvider properties = new TestNodeProperties() {
            @Override
            public String replicationRole() {
                return "writer";
            }

            @Override
            public String replicationProperty(final String name) {
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID", "ECLIPSE_DATAGRID_AERON_NODE_ID",
                            "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> UUID.randomUUID().toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
                    case "ECLIPSE_DATAGRID_BACKUP_PATH" -> root.resolve("driver/backups").toString();
                    default -> null;
                };
            }
        };

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new AeronClusterReplicationTransportProvider().create(properties));
        assertTrue(failure.getMessage().contains("must not overlap"), failure.getMessage());
    }

        /// A writer without a shared lease directory cannot fence, so writer
    /// startup fails with an actionable error instead of publishing unfenced.
    @Test
    void writerWithoutSharedLeaseDirectoryFailsAtStartup() {
        final Path root = Paths.get(System.getProperty("java.io.tmpdir"),
                "datagrid-lease-absent-%s".formatted(UUID.randomUUID()));
        final NodeLibraryPropertiesProvider properties = new TestNodeProperties() {
            @Override
            public String replicationRole() {
                return "writer";
            }

            @Override
            public String replicationProperty(final String name) {
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID", "ECLIPSE_DATAGRID_AERON_NODE_ID",
                            "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> UUID.randomUUID().toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
                    default -> null;
                };
            }
        };
        try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
                .create(properties)) {
            final StorageBinaryDataDistributor distributor = transport.distributor("stream", false);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> transport.persistenceTargetFactory("stream", distributor));
            assertTrue(failure.getMessage().contains("shared lease directory"), failure.getMessage());
        }
    }

        /// Verifies rejection of malformed numeric and production temporary directory settings.
    @Test
    void rejectsMalformedNumericAndProductionTemporaryDirectorySettings() {        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_EPOCH", "not-a-number")));
        final NodeLibraryPropertiesProvider production = new TestNodeProperties() {
            @Override
            public String replicationRole() {
                return "writer";
            }

            @Override
            public boolean isProdMode() {
                return true;
            }

            @Override
            public String replicationProperty(final String name) {
                if ("ECLIPSE_DATAGRID_AERON_CLUSTER_ID".equals(name)) return UUID.randomUUID().toString();
                if ("ECLIPSE_DATAGRID_AERON_DIRECTORY".equals(name)) return "/tmp/datagrid-aeron-test";
                return null;
            }
        };
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider().create(production));
    }

        /// Production mode rejects the two common configuration forms that weaken network/durability guarantees.
    @Test
    void rejectsProductionSyncLevelZeroAndIpv6Wildcard() {
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL", "0", true)));
        assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
                .create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                        "aeron:udp?control=[::]:40123|control-mode=dynamic|fc=max", true)));
    }

    private record TestClient(boolean isRunning, RuntimeException failure) implements StorageBinaryDataClient {
        @Override
        public void start() {
        }

        @Override
        public void stopAtLatestMessage() {
        }

        @Override
        public ReplicationCursor cursor() {
            return new ReplicationCursor("aeron", null, -1, "");
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
