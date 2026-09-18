package peruncs.datagrid.cluster.node.aeron;

import io.aeron.archive.codecs.MessageHeaderDecoder;
import io.aeron.archive.codecs.ReplayRequestDecoder;
import io.aeron.archive.codecs.StartRecordingRequestDecoder;
import io.aeron.archive.codecs.TruncateRecordingRequestDecoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Direct validation tests for configuration combinations that must fail before runtime startup.
class AeronSettingsTest {
    private static NodeLibraryPropertiesProvider properties(final Map<String, String> overrides) {
        return properties(overrides, false);
    }

    private static NodeLibraryPropertiesProvider properties(final Map<String, String> overrides, final boolean production) {
        return properties(overrides, production, "writer");
    }

    private static NodeLibraryPropertiesProvider properties(
            final Map<String, String> overrides,
            final boolean production,
            final String role
    ) {
        final Path root = Path.of(System.getProperty("java.io.tmpdir"), "aeron-settings-%s".formatted(UUID.randomUUID()));
        final UUID cluster = UUID.randomUUID();
        final UUID node = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        return new NodeLibraryPropertiesProvider.Env() {
            @Override
            public boolean isProdMode() {
                return production;
            }

            @Override
            public String replicationRole() {
                return role;
            }

            @Override
            public boolean replicationRoleConfigured() {
                return true;
            }

            @Override
            public String replicationProperty(final String name) {
                final String override = overrides.get(name);
                if (override != null) return override;
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> cluster.toString();
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
        assertEquals(1, settings.retentionReaders().size());
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
        assertTrue(settings.retentionReaders().isEmpty());
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

    /// Verifies Aeron auth is disabled by default with no principal, credentials, or suppliers.
    @Test
    void aeronAuthIsDisabledByDefault() {
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of()));
        assertFalse(settings.authEnabled());
        assertNull(settings.authPrincipal());
        assertNull(settings.authCredentials());
        assertNull(settings.authenticatorSupplier());
        assertNull(settings.authorisationServiceSupplier());
        assertNull(settings.credentialsSupplier());
    }

    /// Verifies enabled Aeron auth parses its principal and credentials and wires authenticator and credential suppliers.
    @Test
    void aeronAuthEnabledParsesPrincipalAndCredentials() {
        final byte[] credentials = "datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII);
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS", Base64.getEncoder().encodeToString(credentials)
        )));
        assertTrue(settings.authEnabled());
        assertEquals("datagrid-node", settings.authPrincipal());
        assertArrayEquals(credentials, settings.authCredentials());
        final var authenticatorSupplier = settings.authenticatorSupplier();
        assertNotNull(authenticatorSupplier);
        assertNotNull(authenticatorSupplier.get());
        final var authorisationServiceSupplier = settings.authorisationServiceSupplier();
        assertNotNull(authorisationServiceSupplier);
        assertNotNull(authorisationServiceSupplier.get());
        final var credentialsSupplier = settings.credentialsSupplier();
        assertNotNull(credentialsSupplier);
        assertArrayEquals(credentials, credentialsSupplier.encodedCredentials());
        assertArrayEquals(credentials, credentialsSupplier.onChallenge(new byte[0]));
    }

    /// Verifies the authorisation service grants recording control to the node and replay only to the reader.
    @Test
    void aeronAuthAuthorisesAnAuthenticatedPrincipal() {
        final byte[] credentials = "datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII);
        final byte[] readerCredentials = "datagrid-reader-secret".getBytes(StandardCharsets.US_ASCII);
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS", Base64.getEncoder().encodeToString(credentials),
                "ECLIPSE_DATAGRID_AERON_AUTH_READER_PRINCIPAL", "datagrid-reader",
                "ECLIPSE_DATAGRID_AERON_AUTH_READER_CREDENTIALS", Base64.getEncoder().encodeToString(readerCredentials)
        )));
        final var authorisationServiceSupplier = settings.authorisationServiceSupplier();
        assertNotNull(authorisationServiceSupplier);
        assertTrue(authorisationServiceSupplier.get()
                .isAuthorised(MessageHeaderDecoder.SCHEMA_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null,
                        "datagrid-node".getBytes(StandardCharsets.US_ASCII)));
        assertFalse(authorisationServiceSupplier.get()
                .isAuthorised(MessageHeaderDecoder.SCHEMA_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null,
                        "unexpected-principal".getBytes(StandardCharsets.US_ASCII)));
        assertTrue(authorisationServiceSupplier.get()
                .isAuthorised(MessageHeaderDecoder.SCHEMA_ID, ReplayRequestDecoder.TEMPLATE_ID, null,
                        "datagrid-reader".getBytes(StandardCharsets.US_ASCII)));
        assertFalse(authorisationServiceSupplier.get()
                .isAuthorised(MessageHeaderDecoder.SCHEMA_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, null,
                        "datagrid-reader".getBytes(StandardCharsets.US_ASCII)));
    }

    /// Verifies a reader principal may replay the Archive but cannot truncate or otherwise mutate it.
    @Test
    void readerPrincipalCannotMutateArchive() {
        final byte[] credentials = "datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII);
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-reader",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS", Base64.getEncoder().encodeToString(credentials)
        ), false, "reader"));
        final var service = settings.authorisationServiceSupplier().get();
        assertNotNull(service);
        final byte[] principal = "datagrid-reader".getBytes(StandardCharsets.US_ASCII);
        assertTrue(service.isAuthorised(MessageHeaderDecoder.SCHEMA_ID, ReplayRequestDecoder.TEMPLATE_ID, null, principal));
        assertFalse(service.isAuthorised(MessageHeaderDecoder.SCHEMA_ID,
                TruncateRecordingRequestDecoder.TEMPLATE_ID, null, principal));
    }

    /// Verifies a non-printable auth principal is rejected.
    @Test
    void aeronAuthRejectsNonPrintablePrincipal() {
        final byte[] credentials = "datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII);
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "node\n",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS",
                Base64.getEncoder().encodeToString(credentials)
        ))));
    }

    /// Verifies auth credentials load from an owner-only credentials file with matching supplier output.
    @Test
    void aeronAuthReadsCredentialsFromOwnerOnlyFile(@TempDir final Path temporaryDirectory) throws Exception {
        final byte[] credentials = "datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII);
        final Path file = temporaryDirectory.resolve("aeron-auth.credentials");
        Files.writeString(file, Base64.getEncoder().encodeToString(credentials), StandardCharsets.US_ASCII);
        Files.setPosixFilePermissions(file, Set.of(PosixFilePermission.OWNER_READ));

        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS_FILE", file.toString()
        )));

        assertTrue(settings.authEnabled());
        assertArrayEquals(credentials, settings.authCredentials());
        final var credentialsSupplier = settings.credentialsSupplier();
        assertNotNull(credentialsSupplier);
        assertArrayEquals(credentials, credentialsSupplier.encodedCredentials());
    }

    /// Verifies enabled auth without a principal is rejected.
    @Test
    void aeronAuthRequiresPrincipalWhenEnabled() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS",
                Base64.getEncoder().encodeToString("datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII))
        ))));
    }

    /// Verifies enabled auth without credentials is rejected.
    @Test
    void aeronAuthRequiresCredentialsWhenEnabled() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node"
        ))));
    }

    /// Verifies auth principal and credentials without the enabled flag are rejected.
    @Test
    void aeronAuthRejectsCredentialsWithoutEnabled() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS",
                Base64.getEncoder().encodeToString("datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII))
        ))));
    }

    /// Verifies specifying both inline credentials and a credentials file is rejected.
    @Test
    void aeronAuthRejectsBothCredentialsAndCredentialsFile(@TempDir final Path temporaryDirectory) throws Exception {
        final Path file = temporaryDirectory.resolve("aeron-auth.credentials");
        Files.writeString(file, Base64.getEncoder().encodeToString("datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII)),
                StandardCharsets.US_ASCII);
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS",
                Base64.getEncoder().encodeToString("datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII)),
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS_FILE", file.toString()
        ))));
    }

    /// Verifies undersized auth credentials are rejected.
    @Test
    void aeronAuthRejectsShortCredentials() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS", Base64.getEncoder().encodeToString(new byte[4])
        ))));
    }

    /// Verifies a non-boolean auth enabled value is rejected.
    @Test
    void aeronAuthRejectsNonBooleanEnabled() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "yes",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS",
                Base64.getEncoder().encodeToString("datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII))
        ))));
    }

        /// Production mode rejects unauthenticated operation without an explicit acknowledgement.
    @Test
    void prodModeRejectsDisabledAuthWithoutAcknowledgement() {
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSettings.fromEnvironment(prodProperties(Map.of())));
        assertTrue(failure.getMessage().contains("ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE"));
    }

        /// The insecure acknowledgement explicitly opts a production node out of auth.
    @Test
    void prodModeAcceptsAcknowledgedInsecureAuth() {
        final AeronSettings settings = AeronSettings.fromEnvironment(prodProperties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE", "true")));

        assertFalse(settings.authEnabled());
        assertNull(settings.authenticatorSupplier());
    }

        /// The acknowledgement is strictly `true`/`false`, like every other boolean setting.
    @Test
    void prodModeRejectsNonBooleanAcknowledgement() {
        assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(prodProperties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE", "yes"))));
    }

        /// Enabled auth in production mode needs no acknowledgement.
    @Test
    void prodModeWithEnabledAuthNeedsNoAcknowledgement() {
        final AeronSettings settings = AeronSettings.fromEnvironment(prodProperties(Map.of(
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", "true",
                "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", "datagrid-node",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS",
                Base64.getEncoder().encodeToString("datagrid-auth-secret".getBytes(StandardCharsets.US_ASCII)),
                "ECLIPSE_DATAGRID_AERON_AUTH_READER_PRINCIPAL", "datagrid-reader",
                "ECLIPSE_DATAGRID_AERON_AUTH_READER_CREDENTIALS",
                Base64.getEncoder().encodeToString("datagrid-reader-secret".getBytes(StandardCharsets.US_ASCII)))));

        assertTrue(settings.authEnabled());
        assertEquals("datagrid-node", settings.authPrincipal());
    }

        /// Retention without a reader set stays unconfigured rather than failing:
    /// the transport reports retention unsupported and preserves history.
    @Test
    void writerWithoutRetentionReadersLeavesRetentionUnconfigured() {
        final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of()));
        assertTrue(settings.retentionReaders().isEmpty());
    }

        /// Production-shaped settings with routable endpoints and home-directory paths.
    private static NodeLibraryPropertiesProvider prodProperties(final Map<String, String> overrides) {
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
