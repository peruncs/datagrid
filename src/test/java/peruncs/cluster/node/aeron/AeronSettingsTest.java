package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// Direct validation tests for configuration combinations that must fail before runtime startup.
class AeronSettingsTest {
    private static NodeConfig properties(final Map<String, String> overrides) {
        return properties(overrides, false);
    }

    private static NodeConfig properties(final Map<String, String> overrides, final boolean production) {
        return properties(overrides, production, "writer");
    }

    private static NodeConfig properties(
            final Map<String, String> overrides,
            final boolean production,
            final String role
    ) {
        final Path root = Path.of(System.getProperty("java.io.tmpdir"),
                "aeron-settings-%s".formatted(UUID.randomUUID()));
        return TestNodeConfig.aeron(root, role, production, overrides);
    }

    /// Verifies the validated embedded writer defaults parse without error.
    @Test
    void acceptsTheValidatedEmbeddedWriterDefaults() {
        assertDoesNotThrow(() -> AeronSettings.fromConfig(properties(Map.of())));
    }

    @Test
    void capturesDriverTimeoutInTheTypedSnapshot() {
        final String timeoutKey = NodeConfig.Setting.AERON_DRIVER_TIMEOUT_MILLIS.key();
        final Map<String, String> values = new HashMap<>();
        values.put(timeoutKey, "12000");
        final NodeConfig config = properties(values);
        values.put(timeoutKey, "1");
        assertEquals(12_000L, AeronSettings.fromConfig(config).timeouts().driverTimeoutMillis());
    }

    @Test
    void defaultsAeronDirectoriesBesideTheStoreAndRejectsOverlap() {
        final Path storageParent = Path.of(System.getProperty("java.io.tmpdir"),
                "aeron-storage-settings-%s".formatted(UUID.randomUUID()));
        final NodeConfig defaults = properties(Map.of(NodeConfig.Setting.STORAGE_PATH.key(), storageParent.toString()));
        final var directories = AeronSettings.fromConfig(defaults).topology().directories();
        assertEquals(storageParent.toAbsolutePath().resolve("aeron"), directories.aeronDirectory());
        assertEquals(storageParent.toAbsolutePath().resolve("aeron.archive"), directories.archiveDirectory());
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                NodeConfig.Setting.STORAGE_PATH.key(), "/node/data",
                NodeConfig.Setting.AERON_DIRECTORY.key(), "/node/data/storage"
        ))));
    }

    /// Production mode does not require an acknowledgement for the unauthenticated, unencrypted protocol.
    @Test
    void productionDoesNotRequireTrustedNetworkAcknowledgement() {
        final AeronSettings settings = AeronSettings.fromConfig(prodProperties(Map.of(
                "PERUNCS_AERON_RETENTION_READERS", UUID.randomUUID().toString()
        )));

        assertEquals(1, settings.archivePolicy().retentionReaders().size());
    }

    @Test
    void indexValidationBoundDefaultsAndCanBeConfigured() {
        assertEquals(65_536, properties(Map.of()).limits().maxValidatedIndexObjects());
        assertEquals(65_536, properties(Map.of("PERUNCS_INDEX_VALIDATION_MAX_OBJECTS", " "))
                .limits().maxValidatedIndexObjects(), "a blank override uses the shared default");
        assertEquals(128, properties(Map.of("PERUNCS_INDEX_VALIDATION_MAX_OBJECTS", "128"))
                .limits().maxValidatedIndexObjects());
        assertThrows(IllegalArgumentException.class,
                () -> properties(Map.of("PERUNCS_INDEX_VALIDATION_MAX_OBJECTS", "0"))
                        .limits().maxValidatedIndexObjects());
        assertThrows(IllegalArgumentException.class,
                () -> properties(Map.of("PERUNCS_INDEX_VALIDATION_MAX_OBJECTS", "many"))
                        .limits().maxValidatedIndexObjects());
    }

    /// Verifies retired secret settings are ignored without failing parsing.
    @Test
    void removedSecretSettingsAreIgnored() {
        /* A stale deployment environment may still export the removed
         * replication, retention, and rotation settings.
         * They are inert: parsing succeeds and no secret is retained. */
        final AeronSettings settings = AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_REPLICATION_SECRET",
                Base64.getEncoder().encodeToString("datagrid-replication-key".getBytes(StandardCharsets.US_ASCII)),
                "PERUNCS_AERON_REPLICATION_SECRET_PREVIOUS",
                Base64.getEncoder().encodeToString("datagrid-previous-key!".getBytes(StandardCharsets.US_ASCII)),
                "PERUNCS_AERON_REPLICATION_ALLOW_INSECURE", "true",
                "PERUNCS_AERON_RETENTION_SECRET",
                Base64.getEncoder().encodeToString("sixteen-byte-key".getBytes(StandardCharsets.US_ASCII)),
                "PERUNCS_AERON_RETENTION_SECRET_PREVIOUS",
                Base64.getEncoder().encodeToString("sixteen-byte-key".getBytes(StandardCharsets.US_ASCII))
        )));
        assertTrue(settings.archivePolicy().retentionReaders().isEmpty());
    }

    /// Verifies a recording id below the Aeron null value is rejected.
    @Test
    void rejectsRecordingIdsBelowAeronNullValue() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_RECORDING_ID", "-2"
        ))));
    }

    /// Verifies a data stream id with no room left for derived streams is rejected.
    @Test
    void rejectsADataStreamWithoutSpaceForDerivedStreams() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_STREAM_ID", Integer.toString(Integer.MAX_VALUE)
        ))));
    }

    /// Verifies a watermark stream id conflicting with derived streams is rejected.
    @Test
    void rejectsAConflictingWatermarkStream() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_WATERMARK_STREAM_ID", "1002"
        ))));
    }

    /// Verifies a duplicated retention reader entry is rejected.
    @Test
    void rejectsDuplicateRetentionReaders() {
        final String reader = UUID.randomUUID().toString();
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_RETENTION_READERS", "%s,%s".formatted(reader, reader)
        ))));
    }

    /// Verifies unsafe filesystem sync is rejected in production mode.
    @Test
    void rejectsUnsafeFilesystemSyncInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_FILE_SYNC_LEVEL", "0"), true)));
    }

    /// Verifies IPv6 wildcard channels are rejected in production mode.
    @Test
    void rejectsIpv6WildcardInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_LIVE_CHANNEL", "aeron:udp?control=[::]:40123|control-mode=dynamic|fc=max",
                "PERUNCS_AERON_REPLAY_CHANNEL", "aeron:udp?endpoint=[::]:0|control=[::]:40123|control-mode=dynamic"
        ), true)));
    }

    /// Verifies expanded-form IPv6 wildcard channels are rejected in production mode.
    @Test
    void rejectsExpandedIpv6WildcardInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_LIVE_CHANNEL",
                "aeron:udp?control=[0:0:0:0:0:0:0:0]:40123|control-mode=dynamic|fc=max",
                "PERUNCS_AERON_REPLAY_CHANNEL",
                "aeron:udp?endpoint=[0:0:0:0:0:0:0:0]:0|control=[0:0:0:0:0:0:0:0]:40123|control-mode=dynamic"
        ), true)));
    }

    /// Verifies a channel framing override disagreeing with replication settings is rejected.
    @Test
    void rejectsFramingOverrideThatDisagreesWithReplication() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_LIVE_CHANNEL",
                "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=8m"
        ))));
    }

    /// Verifies loopback endpoints are rejected in production mode.
    @Test
    void rejectsLoopbackEndpointsInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(), true)));
    }



    /// Retention without a reader set stays unconfigured rather than failing:
    /// the transport reports retention unsupported and preserves history.
    @Test
    void writerWithoutRetentionReadersLeavesRetentionUnconfigured() {
        final AeronSettings settings = AeronSettings.fromConfig(properties(Map.of()));
        assertTrue(settings.archivePolicy().retentionReaders().isEmpty());
    }

    /// Verifies the Archive control and watermark close budgets
    /// default independently of the publication offer timeout.
    @Test
    void perConcernTimeoutBudgetsDefaultIndependently() {
        final AeronSettings settings = AeronSettings.fromConfig(properties(Map.of()));
        assertEquals(TimeUnit.SECONDS.toNanos(5), settings.timeouts().archiveControlTimeoutNanos(),
                "Archive control requests must use their own 5 s budget");
        assertEquals(TimeUnit.SECONDS.toNanos(5), settings.timeouts().watermarkCloseTimeoutNanos(),
                "watermark channel close must use its own 5 s budget");
        assertEquals(TimeUnit.SECONDS.toNanos(30), settings.replication().offerTimeoutNanos(),
                "offerTimeoutNanos must stay the publication-offer budget");
        assertEquals(TimeUnit.SECONDS.toNanos(30), settings.replication().readerStopTimeoutNanos(),
                "readerStopTimeoutNanos must stay the shutdown budget");
        assertEquals(TimeUnit.SECONDS.toNanos(30), settings.replication().reconnectTimeoutNanos());
    }

    /// Verifies each per-concern timeout can be overridden independently.
    @Test
    void perConcernTimeoutBudgetsCanBeOverridden() {
        final AeronSettings settings = AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS", "123456",
                "PERUNCS_AERON_WATERMARK_CLOSE_TIMEOUT_NANOS", "654321",
                "PERUNCS_AERON_RECONNECT_TIMEOUT_NANOS", "345678"
        )));
        assertEquals(123456L, settings.timeouts().archiveControlTimeoutNanos());
        assertEquals(654321L, settings.timeouts().watermarkCloseTimeoutNanos());
        assertEquals(TimeUnit.SECONDS.toNanos(30), settings.replication().readerStopTimeoutNanos());
        assertEquals(345678L, settings.replication().reconnectTimeoutNanos());
    }

    /// Verifies non-positive per-concern budgets fail configuration validation.
    @Test
    void rejectsNonPositivePerConcernTimeouts() {
        for (final String key : List.of(
                "PERUNCS_AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS",
                "PERUNCS_AERON_WATERMARK_CLOSE_TIMEOUT_NANOS",
                "PERUNCS_AERON_RECONNECT_TIMEOUT_NANOS")) {
            assertThrows(IllegalArgumentException.class, () ->
                    AeronSettings.fromConfig(properties(Map.of(key, "0"))), key);
        }
    }

    /// Verifies a UDP channel without an endpoint or control is rejected by the
    /// ChannelUri-based parser rather than a hand-rolled option split.
    @Test
    void rejectsUdpChannelWithoutEndpointOrControl() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromConfig(properties(Map.of(
                "PERUNCS_AERON_WATERMARK_CHANNEL", "aeron:udp?alias=no-endpoint"
        ))));
    }

    /// Verifies production does not require or honor the removed shared-nonce setting.
    @Test
    void wireNonceIsDerivedFromClusterIdentity() {
        final AeronSettings defaults = AeronSettings.fromConfig(prodProperties(Map.of()));
        final AeronSettings settings = AeronSettings.fromConfig(prodProperties(Map.of(
                "PERUNCS_AERON_WIRE_NONCE", "731947"
        )));
        assertEquals(AeronReplicationEnvelope.defaultWireNonce(defaults.topology().clusterId()), defaults.wireNonce());
        assertEquals(AeronReplicationEnvelope.defaultWireNonce(settings.topology().clusterId()), settings.wireNonce());
    }


    /// Production-shaped settings with routable endpoints and home-directory paths.
    private static NodeConfig prodProperties(final Map<String, String> overrides) {
        final HashMap<String, String> merged = new HashMap<>(overrides);
        final Path root = Path.of(System.getProperty("user.home"),
                "datagrid-settings-test-%s".formatted(UUID.randomUUID()));
        merged.putIfAbsent(NodeConfig.Setting.AERON_DIRECTORY.key(), root.resolve("driver").toString());
        merged.putIfAbsent(NodeConfig.Setting.AERON_ARCHIVE_DIRECTORY.key(), root.resolve("archive").toString());
        merged.putIfAbsent(NodeConfig.Setting.AERON_LIVE_CHANNEL.key(),
                "aeron:udp?control=192.168.7.1:40123|control-mode=dynamic|fc=max");
        merged.putIfAbsent(NodeConfig.Setting.AERON_REPLAY_CHANNEL.key(),
                "aeron:udp?endpoint=192.168.7.1:0|control=192.168.7.1:40123|control-mode=dynamic");
        merged.putIfAbsent(NodeConfig.Setting.AERON_ARCHIVE_REPLICATION_CHANNEL.key(), "aeron:udp?endpoint=192.168.7.1:0");
        merged.putIfAbsent(NodeConfig.Setting.AERON_WATERMARK_CHANNEL.key(), "aeron:udp?endpoint=192.168.7.1:40125");
        merged.putIfAbsent(NodeConfig.Setting.AERON_CONTROL_CHANNEL.key(), "aeron:udp?endpoint=192.168.7.1:40124");
        merged.putIfAbsent(NodeConfig.Setting.AERON_CONTROL_RESPONSE_CHANNEL.key(), "aeron:udp?endpoint=192.168.7.1:0");
        return properties(merged, true);
    }
}
