package peruncs.datagrid.cluster.node.aeron;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.driver.ThreadingMode;
import org.agrona.SystemUtil;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

/// Validated configuration for one Aeron transport. Structural values are
/// immutable; the internal retention-key copy is erased by its owning transport
/// during shutdown.
///
/// @param replication                     validated publication framing and timeout settings
/// @param clusterId                       stable cluster identity shared by all members
/// @param epoch                           writer epoch used to reject stale frames
/// @param streamId                        data stream id; replay and watermark ids are derived from it
/// @param recordingId                     configured or discovered Archive recording id
/// @param liveChannel                     live data publication channel
/// @param replayChannel                   reader replay channel
/// @param controlChannel                  embedded Archive control request channel
/// @param controlResponseChannel          Archive control response channel
/// @param aeronDirectory                  MediaDriver directory
/// @param archiveDirectory                embedded Archive directory
/// @param checkpointPath                  durable writer checkpoint path
/// @param nodeId                          stable node identity used in writer recovery
/// @param storeGeneration                 identity of the Store image handled by this node
/// @param archiveFileSyncLevel            Archive file and catalog synchronization level
/// @param minimumArchiveFreeBytes         minimum embedded-Archive free space
/// @param externalArchive                 whether an external Archive owns the recording
/// @param role                            configured replication role
/// @param productionMode                  whether production-only validation is enabled
/// @param threadingMode                   MediaDriver threading mode
/// @param archiveThreadingMode            embedded Archive threading mode
/// @param archiveSegmentFileLength        Archive segment length in bytes
/// @param archiveLowStorageSpaceThreshold Archive low-storage threshold in bytes
/// @param maxConcurrentReplays            maximum simultaneous Archive replays
/// @param driverTimeoutMillis             MediaDriver timeout in milliseconds
/// @param archiveReplicationChannel       Archive replication channel
/// @param watermarkChannel                reader-watermark channel
/// @param watermarkStreamId               reader-watermark stream id
/// @param retentionSecret                 copied HMAC key for authenticated retention, or `null`
/// @param retentionReaders                configured reader identities required for retention
record AeronSettings(
        AeronReplicationConfiguration replication,
        UUID clusterId,
        long epoch,
        int streamId,
        long recordingId,
        String liveChannel,
        String replayChannel,
        String controlChannel,
        String controlResponseChannel,
        Path aeronDirectory,
        Path archiveDirectory,
        Path checkpointPath,
        UUID nodeId,
        UUID storeGeneration,
        int archiveFileSyncLevel,
        long minimumArchiveFreeBytes,
        boolean externalArchive,
        String role,
        boolean productionMode,
        ThreadingMode threadingMode,
        ArchiveThreadingMode archiveThreadingMode,
        int archiveSegmentFileLength,
        long archiveLowStorageSpaceThreshold,
        int maxConcurrentReplays,
        long driverTimeoutMillis,
        String archiveReplicationChannel,
        String watermarkChannel,
        int watermarkStreamId,
        byte[] retentionSecret,
        Set<UUID> retentionReaders
) {
    AeronSettings {
        retentionSecret = retentionSecret == null ? null : retentionSecret.clone();
        retentionReaders = retentionReaders == null ? Set.of() : Set.copyOf(retentionReaders);
    }

    static AeronSettings fromEnvironment(final NodeLibraryPropertiesProvider properties) {
        if (properties == null) throw new NullPointerException("properties");
        if (!properties.replicationRoleConfigured()) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_REPLICATION_ROLE must be explicitly configured for Aeron");
        }
        final String configuredRole = properties.replicationRole();
        if (configuredRole == null || configuredRole.isBlank()) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_REPLICATION_ROLE must be writer, reader, or backup-reader");
        }
        final String role = configuredRole.trim().toLowerCase(java.util.Locale.ROOT);
        if (!role.equals("writer") && !role.equals("reader") && !role.equals("backup-reader")) {
            throw new IllegalArgumentException("ECLIPSE_DATAGRID_REPLICATION_ROLE must be writer, reader, or backup-reader");
        }
        final Properties values = new Properties();
        put(values, properties, AeronReplicationConfiguration.TERM_LENGTH_PROPERTY, "ECLIPSE_DATAGRID_AERON_TERM_LENGTH");
        put(values, properties, AeronReplicationConfiguration.MTU_LENGTH_PROPERTY, "ECLIPSE_DATAGRID_AERON_MTU_LENGTH");
        put(values, properties, AeronReplicationConfiguration.CHUNK_SIZE_PROPERTY, "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE");
        put(values, properties, AeronReplicationConfiguration.MAX_TRANSACTION_BYTES_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES");
        put(values, properties, AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_REPLICATION_DURABILITY_MODE");
        put(values, properties, AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_DURABILITY_MODE");
        put(values, properties, AeronReplicationConfiguration.OFFER_TIMEOUT_NANOS_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_OFFER_TIMEOUT_NANOS");
        put(values, properties, AeronReplicationConfiguration.RECORDING_START_TIMEOUT_NANOS_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_RECORDING_START_TIMEOUT_NANOS");
        put(values, properties, AeronReplicationConfiguration.RECORDED_POSITION_TIMEOUT_NANOS_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_RECORDED_POSITION_TIMEOUT_NANOS");
        put(values, properties, AeronReplicationConfiguration.RECORDING_STOP_TIMEOUT_NANOS_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_RECORDING_STOP_TIMEOUT_NANOS");
        put(values, properties, AeronReplicationConfiguration.READER_STOP_TIMEOUT_NANOS_PROPERTY,
                "ECLIPSE_DATAGRID_AERON_READER_STOP_TIMEOUT_NANOS");
        final AeronReplicationConfiguration replication = AeronReplicationConfiguration.from(values);
        final String cluster = value(properties, "ECLIPSE_DATAGRID_AERON_CLUSTER_ID", null);
        if (cluster == null) throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_CLUSTER_ID is required");
        final long epoch = parseLong(properties, "ECLIPSE_DATAGRID_AERON_EPOCH", "1");
        final int streamId = parseInt(properties, "ECLIPSE_DATAGRID_AERON_STREAM_ID", "1001");
        final long recordingId = parseLong(properties, "ECLIPSE_DATAGRID_AERON_RECORDING_ID", "-1");
        if (epoch < 0 || streamId < 0 || recordingId < -1) {
            throw new IllegalArgumentException("Aeron epoch/stream/recording settings are out of range");
        }
        final Path aeronDirectory = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_DIRECTORY", "/tmp/eclipse-datagrid-aeron"));
        final Path archiveDirectory = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY",
                aeronDirectory.resolveSibling("%s.archive".formatted(aeronDirectory.getFileName())).toString()));
        if (properties.isProdMode() && (temporaryPath(aeronDirectory) || temporaryPath(archiveDirectory))) {
            throw new IllegalArgumentException("Aeron directories must not use /tmp in production mode");
        }
        final String configuredNodeId = value(properties, "ECLIPSE_DATAGRID_AERON_NODE_ID", null);
        final String configuredStoreGeneration = value(
                properties, "ECLIPSE_DATAGRID_AERON_STORE_GENERATION", null);
        if (configuredNodeId == null || configuredStoreGeneration == null) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_NODE_ID and ECLIPSE_DATAGRID_AERON_STORE_GENERATION are required");
        }
        final UUID nodeId = parseUuid(configuredNodeId, "ECLIPSE_DATAGRID_AERON_NODE_ID");
        final UUID storeGeneration = parseUuid(
                configuredStoreGeneration, "ECLIPSE_DATAGRID_AERON_STORE_GENERATION");
        // MediaDriver recreates its directory on startup, so checkpoints must live outside it.
        final Path checkpointPath = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH",
                aeronDirectory.resolveSibling("%s.writer.checkpoint".formatted(aeronDirectory.getFileName())).toString()));
        final Path normalizedAeronDirectory = aeronDirectory.toAbsolutePath().normalize();
        final Path normalizedArchiveDirectory = archiveDirectory.toAbsolutePath().normalize();
        final Path normalizedCheckpointPath = checkpointPath.toAbsolutePath().normalize();
        if (overlaps(normalizedAeronDirectory, normalizedArchiveDirectory) ||
            overlaps(normalizedAeronDirectory, normalizedCheckpointPath) ||
            overlaps(normalizedArchiveDirectory, normalizedCheckpointPath)) {
            throw new IllegalArgumentException(
                    "Aeron driver, archive, and checkpoint paths must not overlap: driver=%s, archive=%s, checkpoint=%s".formatted(aeronDirectory, archiveDirectory, checkpointPath));
        }
        if (properties.isProdMode() && (!aeronDirectory.isAbsolute() || !archiveDirectory.isAbsolute() || !checkpointPath.isAbsolute())) {
            throw new IllegalArgumentException("Aeron directories and checkpoint path must be absolute in production mode");
        }
        final int archiveFileSyncLevel = parseInt(properties, "ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL", "1");
        if (archiveFileSyncLevel < 0 || archiveFileSyncLevel > 2) {
            throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL must be 0, 1, or 2");
        }
        if (properties.isProdMode() && archiveFileSyncLevel == 0) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL=0 is only allowed outside production mode");
        }
        final long minimumArchiveFreeBytes = parseLong(
                properties, "ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES", "0");
        if (minimumArchiveFreeBytes < 0) {
            throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES must be >= 0");
        }
        final String externalArchiveValue = value(
                properties, "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE", "false");
        if (!"true".equalsIgnoreCase(externalArchiveValue) &&
            !"false".equalsIgnoreCase(externalArchiveValue)) {
            throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE must be true or false");
        }
        final boolean externalArchive = Boolean.parseBoolean(externalArchiveValue);
        if (externalArchive && minimumArchiveFreeBytes > 0) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES is only supported for embedded Archive writers");
        }
        /* Keep development defaults self-contained, but make them match the
         * production topology: explicit framing, dynamic MDC, and a stable
         * channel alias. Production deployments must override localhost endpoints
         * and are rejected below when they do not. */
        final String channelAlias = "datagrid-%s".formatted(cluster);
        final String liveChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=16m|alias=%s".formatted(channelAlias));
        final String replayChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                "aeron:udp?endpoint=localhost:0|control=localhost:40123|control-mode=dynamic");
        final String archiveReplicationChannel = channel(properties,
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", "aeron:udp?endpoint=localhost:0");
        final String watermarkChannel = channel(properties,
                "ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL", "aeron:udp?endpoint=localhost:40125");
        if (streamId > Integer.MAX_VALUE - 2) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_STREAM_ID must leave room for replay and watermark streams");
        }
        final int replayStreamId = streamId + 1;
        final int watermarkStreamId = parseInt(properties,
                "ECLIPSE_DATAGRID_AERON_WATERMARK_STREAM_ID", Integer.toString(streamId + 2));
        if (watermarkStreamId < 0 || watermarkStreamId == streamId || watermarkStreamId == replayStreamId) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_WATERMARK_STREAM_ID must be non-negative and distinct from data/replay streams");
        }
        final byte[] retentionSecret = retentionSecret(properties);
        final Set<UUID> retentionReaders = retentionReaders(properties);
        if ("writer".equals(role) && (retentionSecret != null) != !retentionReaders.isEmpty()) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET and ECLIPSE_DATAGRID_AERON_RETENTION_READERS must be configured together");
        }
        if (externalArchive && "writer".equals(role) &&
            (retentionSecret != null || !retentionReaders.isEmpty())) {
            throw new IllegalArgumentException(
                    "authenticated retention requires an embedded Aeron Archive writer");
        }
        final String controlChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL", "aeron:udp?endpoint=localhost:40124");
        final String controlResponseChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL", "aeron:udp?endpoint=localhost:0");
        final ThreadingMode threadingMode = threadingMode(properties);
        final ArchiveThreadingMode archiveThreadingMode = threadingMode == ThreadingMode.DEDICATED
                ? ArchiveThreadingMode.DEDICATED : ArchiveThreadingMode.SHARED;
        final int archiveSegmentFileLength = parseInt(properties, "ECLIPSE_DATAGRID_AERON_ARCHIVE_SEGMENT_FILE_LENGTH",
                Integer.toString(Archive.Configuration.segmentFileLength()));
        final long archiveLowStorageSpaceThreshold = parseLong(properties,
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD",
                Long.toString(Archive.Configuration.lowStorageSpaceThreshold()));
        final int maxConcurrentReplays = parseInt(properties, "ECLIPSE_DATAGRID_AERON_MAX_CONCURRENT_REPLAYS",
                Integer.toString(Archive.Configuration.maxConcurrentReplays()));
        final long driverTimeoutMillis = parseLong(properties,
                "ECLIPSE_DATAGRID_AERON_DRIVER_TIMEOUT_MILLIS", "10000");
        if (archiveSegmentFileLength <= 0 || Integer.bitCount(archiveSegmentFileLength) != 1 ||
            archiveSegmentFileLength < replication.termLength() ||
            archiveLowStorageSpaceThreshold < 0 || maxConcurrentReplays <= 0 || driverTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Archive segment length must be a positive power of two; low-storage threshold must not be negative; max concurrent replays must be positive");
        }
        validateFraming(liveChannel, "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL", replication);
        validateFraming(replayChannel, "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL", replication);
        validateFraming(archiveReplicationChannel,
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", replication);
        ChannelUri.parse(watermarkChannel);
        /* An embedded writer publishes directly to N readers and therefore requires
         * dynamic MDC. The external topology uses this URI for both the publication
         * and the remote Archive recording subscription; that Aeron pairing is a
         * point-to-point endpoint and cannot reuse the embedded MDC URI. */
        if ("writer".equals(role) && !externalArchive) {
            validateWriterTopology(liveChannel);
        }
        if (properties.isProdMode() && (wildcardEndpoint(liveChannel) || wildcardEndpoint(replayChannel) ||
                                        wildcardEndpoint(controlChannel) || wildcardEndpoint(controlResponseChannel) ||
                                        wildcardEndpoint(archiveReplicationChannel) || wildcardEndpoint(watermarkChannel))) {
            throw new IllegalArgumentException("wildcard Aeron endpoints are not allowed in production mode");
        }
        if (properties.isProdMode() && (loopbackEndpoint(liveChannel) || loopbackEndpoint(replayChannel) ||
                                        loopbackEndpoint(controlChannel) || loopbackEndpoint(controlResponseChannel) ||
                                        loopbackEndpoint(archiveReplicationChannel) || loopbackEndpoint(watermarkChannel))) {
            throw new IllegalArgumentException(
                    "loopback Aeron endpoints are not allowed in production mode; configure routable node addresses");
        }
        final AeronSettings settings = new AeronSettings(
                replication,
                parseUuid(cluster, "ECLIPSE_DATAGRID_AERON_CLUSTER_ID"),
                epoch,
                streamId,
                recordingId,
                liveChannel,
                replayChannel,
                controlChannel,
                controlResponseChannel,
                aeronDirectory,
                archiveDirectory,
                checkpointPath,
                nodeId,
                storeGeneration,
                archiveFileSyncLevel,
                minimumArchiveFreeBytes,
                externalArchive,
                role,
                properties.isProdMode(),
                threadingMode,
                archiveThreadingMode,
                archiveSegmentFileLength,
                archiveLowStorageSpaceThreshold,
                maxConcurrentReplays,
                driverTimeoutMillis,
                archiveReplicationChannel,
                watermarkChannel,
                watermarkStreamId,
                retentionSecret,
                retentionReaders
        );
        /* The record constructor keeps its own defensive copy. Erase the parser's
         * temporary immediately so configuration loading does not leave an extra
         * long-lived HMAC key on the heap. */
        if (retentionSecret != null) Arrays.fill(retentionSecret, (byte) 0);
        return settings;
    }

    private static Set<UUID> retentionReaders(final NodeLibraryPropertiesProvider properties) {
        final String configured = value(properties, "ECLIPSE_DATAGRID_AERON_RETENTION_READERS", null);
        if (configured == null || configured.isBlank()) return Set.of();
        final HashSet<UUID> readers = new HashSet<>();
        for (final String value : configured.split(",")) {
            final String text = value.trim();
            if (text.isEmpty()) throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_RETENTION_READERS contains an empty reader id");
            if (!readers.add(parseUuid(text, "ECLIPSE_DATAGRID_AERON_RETENTION_READERS")))
                throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_RETENTION_READERS contains a duplicate reader id");
        }
        return Set.copyOf(readers);
    }

    private static byte[] retentionSecret(final NodeLibraryPropertiesProvider properties) {
        final String configured = value(properties, "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET", null);
        final String configuredFile = value(properties, "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET_FILE", null);
        if (configured != null && !configured.isBlank() && configuredFile != null && !configuredFile.isBlank()) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET and *_SECRET_FILE are mutually exclusive");
        }
        if (configured == null || configured.isBlank()) {
            return configuredFile == null || configuredFile.isBlank()
                    ? null : decodeRetentionSecretFile(configuredFile.trim());
        }
        return decodeRetentionSecret(configured.trim(), "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET");
    }

    private static byte[] decodeRetentionSecretFile(final String fileName) {
        final Path path = Paths.get(fileName).toAbsolutePath().normalize();
        try {
            final BasicFileAttributes attributes = Files.readAttributes(
                    path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile() || Files.isSymbolicLink(path) || attributes.size() > 4096L) {
                throw new IOException("retention secret file must be a regular, non-symbolic file <= 4096 bytes");
            }
            try {
                final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS);
                if (permissions.contains(PosixFilePermission.GROUP_READ) ||
                    permissions.contains(PosixFilePermission.GROUP_WRITE) ||
                    permissions.contains(PosixFilePermission.GROUP_EXECUTE) ||
                    permissions.contains(PosixFilePermission.OTHERS_READ) ||
                    permissions.contains(PosixFilePermission.OTHERS_WRITE) ||
                    permissions.contains(PosixFilePermission.OTHERS_EXECUTE)) {
                    throw new IOException("retention secret file must not be readable by group or others");
                }
            } catch (final UnsupportedOperationException unsupported) {
                throw new IOException("retention secret file permissions cannot be verified", unsupported);
            }
            final byte[] encoded = new byte[(int) attributes.size()];
            try (var input = Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                final byte[] buffer = new byte[256];
                int total = 0;
                int read;
                while ((read = input.read(buffer)) != -1) {
                    if (read > encoded.length - total) throw new IOException("retention secret file is too large");
                    System.arraycopy(buffer, 0, encoded, total, read);
                    total += read;
                }
                try {
                    return decodeRetentionSecret(
                            new String(encoded, 0, total, StandardCharsets.US_ASCII).trim(),
                            "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET_FILE");
                } finally {
                    Arrays.fill(encoded, (byte) 0);
                }
            }
        } catch (final IOException | IllegalArgumentException failure) {
            throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_RETENTION_SECRET_FILE is invalid: %s".formatted(path),
                    failure);
        }
    }

    private static byte[] decodeRetentionSecret(final String configured, final String property) {
        try {
            final byte[] secret = Base64.getDecoder().decode(configured);
            if (secret.length < 16) throw new IllegalArgumentException(
                    "%s must decode to at least 16 bytes".formatted(property));
            return secret;
        } catch (final IllegalArgumentException failure) {
            throw new IllegalArgumentException("%s must be base64 and decode to at least 16 bytes".formatted(property), failure);
        }
    }

    private static ThreadingMode threadingMode(final NodeLibraryPropertiesProvider properties) {
        final String configured = value(properties, "ECLIPSE_DATAGRID_AERON_THREADING_MODE",
                properties.isProdMode() ? "DEDICATED" : "SHARED");
        return switch (configured.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "SHARED" -> ThreadingMode.SHARED;
            case "DEDICATED" -> ThreadingMode.DEDICATED;
            default -> throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_THREADING_MODE must be SHARED or DEDICATED");
        };
    }

    private static void put(final Properties values, final NodeLibraryPropertiesProvider properties,
                            final String property, final String environment) {
        final String value = value(properties, environment, null);
        if (value != null) {
            final String previous = values.getProperty(property);
            if (previous != null && !equivalentSetting(previous, value)) {
                throw new IllegalArgumentException(
                        "Conflicting Aeron settings for %s: %s and %s".formatted(property, previous, value));
            }
            values.setProperty(property, value);
        }
    }

    private static boolean equivalentSetting(final String left, final String right) {
        return left.trim().replace('-', '_').equalsIgnoreCase(right.trim().replace('-', '_'));
    }

    private static String value(final NodeLibraryPropertiesProvider properties, final String name, final String fallback) {
        /* The provider owns the precedence rule.  Falling back directly to the
         * process environment here would let an ambient variable override a
         * deliberately isolated application/test provider. NodeLibrary's Env
         * implementation already reads environment variables at its boundary. */
        final String configured = properties.replicationProperty(name);
        return configured == null || configured.isBlank() ? fallback : configured;
    }

    private static String channel(final NodeLibraryPropertiesProvider properties, final String name, final String fallback) {
        final String channel = value(properties, name, fallback).trim();
        if (channel.isEmpty() || channel.chars().anyMatch(Character::isWhitespace) ||
            channel.equals("aeron:udp") ||
            !(channel.startsWith("aeron:udp?") ||
              channel.equals("aeron:ipc") || channel.startsWith("aeron:ipc?"))) {
            throw new IllegalArgumentException("Invalid Aeron channel for %s: %s".formatted(name, channel));
        }
        try {
            ChannelUri.parse(channel);
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Aeron channel for %s: %s".formatted(name, channel), failure);
        }
        if (channel.startsWith("aeron:udp?")) {
            boolean endpoint = false;
            boolean control = false;
            for (final String option : channel.substring(channel.indexOf('?') + 1).split("\\|")) {
                if (option.startsWith("control=")) {
                    control = true;
                    validateUdpAddress(option.substring("control=".length()), name);
                } else if (option.startsWith("endpoint=")) {
                    endpoint = true;
                    validateUdpAddress(option.substring("endpoint=".length()), name);
                }
            }
            if (!endpoint && !control) {
                throw new IllegalArgumentException("Aeron UDP channel must specify endpoint= or control= for %s".formatted(name));
            }
        }
        return channel;
    }

        /// Returns whether the configured channel explicitly disables Aeron spy
    /// connection simulation. The driver-level setting must not silently
    /// override an operator's explicit `ssc=false` choice.
    private static void validateUdpAddress(final String address, final String name) {
        final String portText;
        if (address.startsWith("[")) {
            final int closingBracket = address.indexOf(']');
            if (closingBracket <= 1 || closingBracket + 1 >= address.length() ||
                address.charAt(closingBracket + 1) != ':') {
                throw new IllegalArgumentException("Aeron UDP address must include host and port for %s".formatted(name));
            }
            portText = address.substring(closingBracket + 2);
        } else {
            final int colon = address.lastIndexOf(':');
            if (colon <= 0 || colon == address.length() - 1) {
                throw new IllegalArgumentException("Aeron UDP address must include host and port for %s".formatted(name));
            }
            portText = address.substring(colon + 1);
        }
        if (portText.isEmpty()) {
            throw new IllegalArgumentException("Aeron UDP address must include host and port for %s".formatted(name));
        }
        try {
            final int port = Integer.parseInt(portText);
            if (port < 0 || port > 65535) throw new NumberFormatException();
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException("Invalid Aeron UDP address for %s: %s".formatted(name, address), failure);
        }
    }

    private static boolean wildcardEndpoint(final String channel) {
        return endpointMatches(channel, AeronSettings::wildcardHost);
    }

    private static boolean loopbackEndpoint(final String channel) {
        return endpointMatches(channel, AeronSettings::loopbackHost);
    }

        /// Checks all endpoint-bearing URI options instead of relying on textual
    /// substrings.  The latter misses case/format variants (for example expanded
    /// IPv6 wildcards) and can match an unrelated option value.
    private static boolean endpointMatches(final String channel,
                                           final java.util.function.Predicate<String> hostPredicate) {
        final ChannelUri uri = ChannelUri.parse(channel);
        return hostPredicate.test(endpointHost(uri.get(CommonContext.ENDPOINT_PARAM_NAME))) ||
               hostPredicate.test(endpointHost(uri.get(CommonContext.MDC_CONTROL_PARAM_NAME)));
    }

    private static String endpointHost(final String endpoint) {
        if (endpoint == null || endpoint.isBlank()) return null;
        final String value = endpoint.trim();
        if (value.charAt(0) == '[') {
            final int closing = value.indexOf(']');
            return closing > 1 ? value.substring(1, closing) : value;
        }
        final int separator = value.lastIndexOf(':');
        return separator > 0 ? value.substring(0, separator) : value;
    }

    private static boolean wildcardHost(final String host) {
        if (host == null) return false;
        final String normalized = host.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("*") || normalized.equals("0.0.0.0") ||
               normalized.equals("::") || normalized.equals("0:0:0:0:0:0:0:0");
    }

    private static boolean loopbackHost(final String host) {
        if (host == null) return false;
        final String normalized = host.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("localhost") || normalized.equals("127.0.0.1") ||
               normalized.equals("::1") || normalized.equals("0:0:0:0:0:0:0:1");
    }

    private static boolean overlaps(final Path left, final Path right) {
        return left.startsWith(right) || right.startsWith(left);
    }

    private static boolean temporaryPath(final Path path) {
        final Path normalized = path.toAbsolutePath().normalize();
        return normalized.startsWith(Path.of("/tmp")) || normalized.startsWith(Path.of("/private/tmp"));
    }

        /// Reject channel-level framing overrides that disagree with the values used
    /// to configure the MediaDriver and replication envelope. Without this check
    /// the channel silently wins and a writer and reader can use different term or
    /// MTU limits even though they share one replication configuration.
    private static void validateFraming(final String channel, final String name,
                                        final AeronReplicationConfiguration replication) {
        final ChannelUri uri;
        try {
            uri = ChannelUri.parse(channel);
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Aeron channel for %s: %s".formatted(name, channel), failure);
        }
        validateFramingOption(uri, CommonContext.TERM_LENGTH_PARAM_NAME, replication.termLength(), name);
        if (uri.isUdp()) {
            validateFramingOption(uri, CommonContext.MTU_LENGTH_PARAM_NAME, replication.mtuLength(), name);
        }
    }

    private static void validateFramingOption(final ChannelUri uri, final String option,
                                              final int expected, final String name) {
        final String configured = uri.get(option);
        if (configured == null) return;
        final long value;
        try {
            value = SystemUtil.parseSize(option, configured);
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("Invalid %s in %s: %s".formatted(option, name, configured), failure);
        }
        if (value != expected) {
            throw new IllegalArgumentException("%s %s=%s conflicts with replication configuration value %s".formatted(name, option, configured, expected));
        }
    }

    private static void validateWriterTopology(final String channel) {
        final ChannelUri uri = ChannelUri.parse(channel);
        if (!uri.isUdp() || !CommonContext.MDC_CONTROL_MODE_DYNAMIC.equalsIgnoreCase(
                uri.get(CommonContext.MDC_CONTROL_MODE_PARAM_NAME)) ||
            !"max".equalsIgnoreCase(uri.get(CommonContext.FLOW_CONTROL_PARAM_NAME))) {
            throw new IllegalArgumentException(
                    "Aeron writer live channel must use dynamic MDC with fc=max: %s".formatted(channel));
        }
    }

    private static int parseInt(final NodeLibraryPropertiesProvider properties, final String name, final String fallback) {
        try {
            return Integer.parseInt(value(properties, name, fallback).trim());
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException("Invalid %s".formatted(name), failure);
        }
    }

    private static long parseLong(final NodeLibraryPropertiesProvider properties, final String name, final String fallback) {
        try {
            return Long.parseLong(value(properties, name, fallback).trim());
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException("Invalid %s".formatted(name), failure);
        }
    }

    private static UUID parseUuid(final String value, final String name) {
        try {
            final UUID parsed = UUID.fromString(value);
            if (parsed.equals(new UUID(0L, 0L))) {
                throw new IllegalArgumentException("%s must not be the zero UUID".formatted(name));
            }
            return parsed;
        } catch (final IllegalArgumentException failure) {
            throw new IllegalArgumentException("Invalid %s".formatted(name), failure);
        }
    }

    @Override
    public byte[] retentionSecret() {
        return this.retentionSecret == null ? null : this.retentionSecret.clone();
    }

    @Override
    public Set<UUID> retentionReaders() {
        return this.retentionReaders;
    }

        /// Erases the in-memory retention key when the owning transport closes.
    void clearRetentionSecret() {
        if (this.retentionSecret != null) Arrays.fill(this.retentionSecret, (byte) 0);
    }
}
