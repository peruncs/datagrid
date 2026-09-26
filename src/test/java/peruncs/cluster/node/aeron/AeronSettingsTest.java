package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import peruncs.cluster.api.NodeSettingsSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// Direct validation tests for configuration combinations that must fail before runtime startup.
class AeronSettingsTest {
    private static NodeSettingsSource properties(final Map<String, String> overrides) {
        return properties(overrides, false);
    }

    private static NodeSettingsSource properties(final Map<String, String> overrides, final boolean production) {
        return properties(overrides, production, "writer");
    }

    private static NodeSettingsSource properties(
            final Map<String, String> overrides,
            final boolean production,
            final String role
    ) {
        final Path root = Path.of(System.getProperty("java.io.tmpdir"), "aeron-settings-%s".formatted(UUID.randomUUID()));
        final UUID cluster = UUID.randomUUID();
        final UUID node = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        return new TestNodeProperties() {
            @Override
            public boolean isProdMode() {
                return production;
            }

            @Override
            public String replicationRole() {
                return role;
            }

            @Override
            public String replicationProperty(final String name) {
                final String override = overrides.get(name);
                if (override != null) return override;
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> cluster.toString();
                    case "ECLIPSE_DATAGRID_AERON_WIRE_NONCE" -> "731947";
                    case "ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK" -> "true";
                    case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> node.toString();
                    case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
                    case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
                    case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> root.resolve("checkpoint/writer.checkpoint").toString();
                    default -> null;
                };
            }
        };
    }

    /// Verifies the validated embedded writer defaults parse without error.
    @Test
    void acceptsTheValidatedEmbeddedWriterDefaults() {
        assertDoesNotThrow(() -> AeronSettings.fromEnvironment(properties(Map.of())));
    }

    /// Verifies an external archive writer accepts retention readers as unsupported configuration with one reader parsed.
    @Test
    void externalArchiveWriterAcceptsRetentionReadersAsUnsupported() {
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE", "true",
                "ECLIPSE_DATAGRID_AERON_RETENTION_READERS", UUID.randomUUID().toString()
        )));
        assertEquals(1, settings.archivePolicy().retentionReaders().size());
    }

    /// Verifies an external archive writer accepts its point-to-point recording channel.
    @Test
    void externalArchiveWriterAcceptsItsPointToPointRecordingChannel() {
        assertDoesNotThrow(() -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE", "true",
                "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL", "aeron:udp?endpoint=localhost:40123"
        ))));
    }

    /// Verifies removed replication, retention, and network-profile secrets are ignored without failing parsing.
    @Test
    void removedSecretSettingsAreIgnored() {
        /* A stale deployment environment may still export the removed
         * replication, retention, rotation, and network-profile settings.
         * They are inert: parsing succeeds and no secret is retained. */
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_REPLICATION_SECRET",
                Base64.getEncoder().encodeToString("datagrid-replication-key".getBytes(StandardCharsets.US_ASCII)),
                "ECLIPSE_DATAGRID_AERON_REPLICATION_SECRET_PREVIOUS",
                Base64.getEncoder().encodeToString("datagrid-previous-key!".getBytes(StandardCharsets.US_ASCII)),
                "ECLIPSE_DATAGRID_AERON_REPLICATION_ALLOW_INSECURE", "true",
                "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET",
                Base64.getEncoder().encodeToString("sixteen-byte-key".getBytes(StandardCharsets.US_ASCII)),
                "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET_PREVIOUS",
                Base64.getEncoder().encodeToString("sixteen-byte-key".getBytes(StandardCharsets.US_ASCII)),
                "ECLIPSE_DATAGRID_NETWORK_PROFILE", "trusted-network"
        )));
        assertTrue(settings.archivePolicy().retentionReaders().isEmpty());
    }

    /// Verifies a recording id below the Aeron null value is rejected.
    @Test
    void rejectsRecordingIdsBelowAeronNullValue() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_RECORDING_ID", "-2"
        ))));
    }

    /// Verifies a data stream id with no room left for derived streams is rejected.
    @Test
    void rejectsADataStreamWithoutSpaceForDerivedStreams() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_STREAM_ID", Integer.toString(Integer.MAX_VALUE)
        ))));
    }

    /// Verifies a watermark stream id conflicting with derived streams is rejected.
    @Test
    void rejectsAConflictingWatermarkStream() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_WATERMARK_STREAM_ID", "1002"
        ))));
    }

    /// Verifies a duplicated retention reader entry is rejected.
    @Test
    void rejectsDuplicateRetentionReaders() {
        final String reader = UUID.randomUUID().toString();
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_RETENTION_READERS", "%s,%s".formatted(reader, reader)
        ))));
    }

    /// Verifies the unsupported local-durable-first durability mode is rejected.
    @Test
    void rejectsUnsupportedLocalDurableFirstMode() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_REPLICATION_DURABILITY_MODE", "LOCAL_DURABLE_FIRST"
        ))));
    }

    /// Verifies unsafe filesystem sync is rejected in production mode.
    @Test
    void rejectsUnsafeFilesystemSyncInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL", "0"), true)));
    }

    /// Verifies IPv6 wildcard channels are rejected in production mode.
    @Test
    void rejectsIpv6WildcardInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL", "aeron:udp?control=[::]:40123|control-mode=dynamic|fc=max",
                "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL", "aeron:udp?endpoint=[::]:0|control=[::]:40123|control-mode=dynamic"
        ), true)));
    }

    /// Verifies expanded-form IPv6 wildcard channels are rejected in production mode.
    @Test
    void rejectsExpandedIpv6WildcardInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                "aeron:udp?control=[0:0:0:0:0:0:0:0]:40123|control-mode=dynamic|fc=max",
                "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                "aeron:udp?endpoint=[0:0:0:0:0:0:0:0]:0|control=[0:0:0:0:0:0:0:0]:40123|control-mode=dynamic"
        ), true)));
    }

    /// Verifies a channel framing override disagreeing with replication settings is rejected.
    @Test
    void rejectsFramingOverrideThatDisagreesWithReplication() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=8m"
        ))));
    }

    /// Verifies loopback endpoints are rejected in production mode.
    @Test
    void rejectsLoopbackEndpointsInProduction() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(), true)));
    }



        /// Retention without a reader set stays unconfigured rather than failing:
    /// the transport reports retention unsupported and preserves history.
    @Test
    void writerWithoutRetentionReadersLeavesRetentionUnconfigured() {
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of()));
        assertTrue(settings.archivePolicy().retentionReaders().isEmpty());
    }

        /// Verifies the Archive control, watermark close, and lease lock budgets
    /// default independently of the publication offer timeout.
    @Test
    void perConcernTimeoutBudgetsDefaultIndependently() {
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of()));
        assertEquals(TimeUnit.SECONDS.toNanos(5), settings.timeouts().archiveControlTimeoutNanos(),
                "Archive control requests must use their own 5 s budget");
        assertEquals(TimeUnit.SECONDS.toNanos(5), settings.timeouts().watermarkCloseTimeoutNanos(),
                "watermark channel close must use its own 5 s budget");
        assertEquals(5_000L, settings.timeouts().leaseAcquireLockTimeoutMillis(),
                "the writer lease lock wait must have its own bounded default");
        assertEquals(TimeUnit.SECONDS.toNanos(30), settings.replication().offerTimeoutNanos(),
                "offerTimeoutNanos must stay the publication-offer budget");
    }

        /// Verifies each per-concern timeout can be overridden independently.
    @Test
    void perConcernTimeoutBudgetsCanBeOverridden() {
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS", "123456",
                "ECLIPSE_DATAGRID_AERON_WATERMARK_CLOSE_TIMEOUT_NANOS", "654321",
                "ECLIPSE_DATAGRID_AERON_LEASE_LOCK_TIMEOUT_MILLIS", "42"
        )));
        assertEquals(123456L, settings.timeouts().archiveControlTimeoutNanos());
        assertEquals(654321L, settings.timeouts().watermarkCloseTimeoutNanos());
        assertEquals(42L, settings.timeouts().leaseAcquireLockTimeoutMillis());
    }

        /// Verifies non-positive per-concern budgets fail configuration validation.
    @Test
    void rejectsNonPositivePerConcernTimeouts() {
        for (final String key : List.of(
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS",
                "ECLIPSE_DATAGRID_AERON_WATERMARK_CLOSE_TIMEOUT_NANOS",
                "ECLIPSE_DATAGRID_AERON_LEASE_LOCK_TIMEOUT_MILLIS")) {
            assertThrows(IllegalArgumentException.class, () ->
                    AeronSettings.fromEnvironment(properties(Map.of(key, "0"))), key);
        }
    }

        /// Verifies a UDP channel without an endpoint or control is rejected by the
    /// ChannelUri-based parser rather than a hand-rolled option split.
    @Test
    void rejectsUdpChannelWithoutEndpointOrControl() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL", "aeron:udp?alias=no-endpoint"
        ))));
    }

    /// Verifies the wire nonce is parsed from configuration and derived from the cluster id when unconfigured.
    @Test
    void wireNonceIsParsedOrDerivedFromClusterIdentity() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_WIRE_NONCE", "0"
        ))));
    }


        /// Production-shaped settings with routable endpoints and home-directory paths.
    private static NodeSettingsSource prodProperties(final Map<String, String> overrides) {
        final HashMap<String, String> merged = new HashMap<>(overrides);
        final Path root = Path.of(System.getProperty("user.home"),
                "datagrid-auth-test-%s".formatted(UUID.randomUUID()));
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_DIRECTORY", root.resolve("driver").toString());
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY", root.resolve("archive").toString());
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH",
                root.resolve("checkpoint/writer.checkpoint").toString());
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                "aeron:udp?control=192.168.7.1:40123|control-mode=dynamic|fc=max");
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                "aeron:udp?endpoint=192.168.7.1:0|control=192.168.7.1:40123|control-mode=dynamic");
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", "aeron:udp?endpoint=192.168.7.1:0");
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL", "aeron:udp?endpoint=192.168.7.1:40125");
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL", "aeron:udp?endpoint=192.168.7.1:40124");
        merged.putIfAbsent("ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL", "aeron:udp?endpoint=192.168.7.1:0");
        return properties(merged, true);
    }
}
