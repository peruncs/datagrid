package peruncs.cluster.node.aeron;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.driver.ThreadingMode;
import org.agrona.SystemUtil;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.api.NodeRole;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.config.AeronRetryPolicy;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;

import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;


/// Validated configuration for one Aeron transport. Structural values are
/// immutable and grouped by concern into composed sub-records.
///
/// The public [NodeConfig] parses external values once; this package validates
/// transport-specific combinations before resources are created.
///
/// @param replication          validated publication framing and offer-timeout settings
/// @param topology             cluster identity, role, channels, stream ids, and directories
/// @param archivePolicy        Archive recording, retention, and capacity policy
/// @param timeouts             operation deadlines for driver, Archive control, and watermark close
/// @param wireNonce            public cluster-id-derived framing value; it is not a credential
/// @param threadingMode        MediaDriver threading mode
/// @param archiveThreadingMode embedded Archive threading mode
/// @param productionMode       whether production-only validation is enabled
record AeronSettings(
        AeronReplicationConfiguration replication,
        Topology topology,
        ArchivePolicy archivePolicy,
        Timeouts timeouts,
        long wireNonce,
        ThreadingMode threadingMode,
        ArchiveThreadingMode archiveThreadingMode,
        boolean productionMode
) {
    /// The cluster-wide and per-node wiring one node joins and publishes on.
    ///
    /// @param clusterId         stable cluster identity shared by all members
    /// @param role              normalized replication role
    /// @param epoch             writer epoch used to reject stale frames
    /// @param streamId          data stream id; replay and watermark ids are derived from it
    /// @param watermarkStreamId reader-watermark stream id
    /// @param recordingId       configured or discovered Archive recording id
    /// @param identity          node and Store-generation identities
    /// @param channels          live, replay, Archive control, and watermark channels
    /// @param directories       MediaDriver and Archive directories
    record Topology(
            UUID clusterId,
            NodeRole role,
            long epoch,
            int streamId,
            int watermarkStreamId,
            long recordingId,
            StorageIdentity identity,
            Channels channels,
            Directories directories
    ) {
        Topology {
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(channels, "channels");
            Objects.requireNonNull(directories, "directories");
        }
    }

    /// The Archive recording, retention, and capacity policy.
    ///
    /// Copies the mutable reader set so the record never shares caller-owned state.
    ///
    /// @param fileSyncLevel            Archive file and catalog synchronization level
    /// @param minimumFreeBytes         minimum Archive free space
    /// @param segmentFileLength        Archive segment length in bytes
    /// @param lowStorageSpaceThreshold Archive low-storage threshold in bytes
    /// @param maxConcurrentReplays     maximum simultaneous Archive replays
    /// @param retentionReaders         configured reader identities required for retention
    record ArchivePolicy(
            int fileSyncLevel,
            long minimumFreeBytes,
            int segmentFileLength,
            long lowStorageSpaceThreshold,
            int maxConcurrentReplays,
            Set<UUID> retentionReaders
    ) {
        ArchivePolicy {
            retentionReaders = retentionReaders == null ? Set.of() : Set.copyOf(retentionReaders);
        }

        @Override
        public Set<UUID> retentionReaders() {
            return this.retentionReaders;
        }
    }

    /// The operation deadlines of one transport. Every budget must be positive.
    ///
    /// @param driverTimeoutMillis           MediaDriver timeout in milliseconds
    /// @param archiveControlTimeoutNanos    timeout for one synchronous Archive control request
    /// @param watermarkCloseTimeoutNanos    flush budget when a watermark channel closes
    /// @param retentionOperationTimeoutMillis bound of one retention command and of stopping its agent
    /// @param writerRecoveryAttempts          consecutive transient writer-recovery failures before `FAILED`
    record Timeouts(
            long driverTimeoutMillis,
            long archiveControlTimeoutNanos,
            long watermarkCloseTimeoutNanos,
            long retentionOperationTimeoutMillis,
            int writerRecoveryAttempts
    ) {
        Timeouts {
            if (driverTimeoutMillis <= 0L || archiveControlTimeoutNanos <= 0L ||
                watermarkCloseTimeoutNanos <= 0L || retentionOperationTimeoutMillis <= 0L ||
                writerRecoveryAttempts <= 0) {
                throw new IllegalArgumentException("Aeron per-concern timeouts must be positive");
            }
        }
    }

    /// The channel set of one transport.
    ///
    /// @param live               live data publication channel
    /// @param replay             reader replay channel
    /// @param control            embedded Archive control request channel
    /// @param controlResponse    Archive control response channel
    /// @param archiveReplication Archive replication channel
    /// @param watermark          reader-watermark channel
    record Channels(
            String live,
            String replay,
            String control,
            String controlResponse,
            String archiveReplication,
            String watermark
    ) {
        Channels {
            Objects.requireNonNull(live, "live");
            Objects.requireNonNull(replay, "replay");
            Objects.requireNonNull(control, "control");
            Objects.requireNonNull(controlResponse, "controlResponse");
            Objects.requireNonNull(archiveReplication, "archiveReplication");
            Objects.requireNonNull(watermark, "watermark");
        }
    }

    /// The stable identities of one node and the Store image it handles.
    ///
    /// @param nodeId          stable node identity used in writer recovery
    /// @param storeGeneration identity of the Store image handled by this node
    record StorageIdentity(UUID nodeId, UUID storeGeneration) {
        StorageIdentity {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(storeGeneration, "storeGeneration");
        }
    }

    /// The filesystem directories one node owns.
    ///
    /// Grouping the adjacent paths into one type keeps the settings
    /// constructor free of same-typed neighbors that can be transposed
    /// silently at a call site.
    ///
    /// @param aeronDirectory  MediaDriver directory, recreated by Aeron on startup
    /// @param archiveDirectory embedded Archive directory, never a child of the driver directory
    record Directories(Path aeronDirectory, Path archiveDirectory) {
        Directories {
            Objects.requireNonNull(aeronDirectory, "aeronDirectory");
            Objects.requireNonNull(archiveDirectory, "archiveDirectory");
        }
    }

    static AeronSettings fromConfig(final NodeConfig config) {
        Objects.requireNonNull(config, "config");
        if (config.replicationTransport() != NodeConfig.ReplicationTransport.AERON) {
            throw new IllegalArgumentException("Aeron settings require the Aeron transport");
        }
        final NodeConfig.AeronConfig configured = config.aeron();
        final UUID clusterId = requiredId(configured.clusterId(), NodeConfig.Setting.AERON_CLUSTER_ID);
        final UUID nodeId = requiredId(configured.nodeId(), NodeConfig.Setting.AERON_NODE_ID);
        final UUID generation = requiredId(configured.storeGeneration(), NodeConfig.Setting.AERON_STORE_GENERATION);
        final NodeConfig.Limits limits = config.limits();
        final NodeConfig.Timeouts budgets = config.timeouts();
        final AeronReplicationConfiguration replication = AeronReplicationConfiguration.builder()
                .termLength(limits.termLength())
                .mtuLength(limits.mtuLength())
                .chunkSize(limits.chunkSize())
                .maxTransactionBytes(limits.maxTransactionBytes())
                .offerTimeoutNanos(budgets.offer().toNanos())
                .recordingStartTimeoutNanos(budgets.recordingStart().toNanos())
                .recordedPositionTimeoutNanos(budgets.recordedPosition().toNanos())
                .abortRecordedPositionTimeoutNanos(budgets.abortRecordedPosition().toNanos())
                .recordingStopTimeoutNanos(budgets.recordingStop().toNanos())
                .readerStopTimeoutNanos(budgets.readerStop().toNanos())
                .reconnectTimeoutNanos(budgets.reconnect().toNanos())
                .retryPolicy(retryPolicy(configured.retryPacing()))
                .build();

        final NodeConfig.ArchivePolicy configuredArchive = configured.archivePolicy();
        if (config.productionMode() && configuredArchive.fileSyncLevel() == 0) {
            throw new IllegalArgumentException("%s=0 is only allowed outside production mode"
                    .formatted(NodeConfig.Setting.AERON_FILE_SYNC_LEVEL.key()));
        }
        final int segmentLength = configuredArchive.segmentFileLength();
        if (Integer.bitCount(segmentLength) != 1 || segmentLength < replication.termLength()) {
            throw new IllegalArgumentException(
                    "Archive segment length must be a power of two at least as large as the term length");
        }
        final ArchivePolicy archivePolicy = new ArchivePolicy(configuredArchive.fileSyncLevel(),
                configuredArchive.minimumFreeBytes(), segmentLength, configuredArchive.lowStorageSpaceThreshold(),
                configuredArchive.maxConcurrentReplays(), configuredArchive.retentionReaders());

        final int streamId = configured.streamId();
        if (streamId > Integer.MAX_VALUE - 2) {
            throw new IllegalArgumentException("%s must leave room for replay and watermark streams"
                    .formatted(NodeConfig.Setting.AERON_STREAM_ID.key()));
        }
        final int watermarkStreamId = configured.watermarkStreamId();
        if (watermarkStreamId == streamId || watermarkStreamId == streamId + 1) {
            throw new IllegalArgumentException("%s must be distinct from data and replay streams"
                    .formatted(NodeConfig.Setting.AERON_WATERMARK_STREAM_ID.key()));
        }
        final NodeRole role = config.role();
        final boolean production = config.productionMode();
        final Channels channels = validateChannels(configured.channels(), role, production, replication);
        final Directories directories = directories(config.storage().root(), configured.directories(), production);
        final Topology topology = new Topology(clusterId, role, configured.epoch(), streamId, watermarkStreamId,
                configured.recordingId(), new StorageIdentity(nodeId, generation), channels, directories);
        final ThreadingMode threading = configured.threadingMode() == NodeConfig.ThreadingMode.DEDICATED
                ? ThreadingMode.DEDICATED : ThreadingMode.SHARED;
        return new AeronSettings(replication, topology, archivePolicy,
                new Timeouts(budgets.driver().toMillis(), budgets.archiveControl().toNanos(),
                        budgets.watermarkClose().toNanos(), config.operations().retentionOperationTimeout().toMillis(),
                        config.operations().writerRecoveryAttempts()),
                AeronReplicationEnvelope.defaultWireNonce(clusterId), threading,
                threading == ThreadingMode.DEDICATED ? ArchiveThreadingMode.DEDICATED : ArchiveThreadingMode.SHARED,
                production);
    }

    private static AeronRetryPolicy retryPolicy(final NodeConfig.RetryPacing pacing) {
        final AeronRetryPolicy defaults = AeronRetryPolicy.defaults();
        return new AeronRetryPolicy(defaults.idleMaxSpins(), defaults.idleMaxYields(), defaults.idleMinParkNanos(),
                pacing.idleMaxPark().toNanos(), pacing.jitterBase().toNanos(), pacing.jitterCap().toNanos(),
                pacing.archiveProbeDelay().toNanos(), defaults.catalogProbeInitialDelayNanos(),
                defaults.catalogProbeMaxDelayNanos());
    }

    private static UUID requiredId(final UUID value, final NodeConfig.Setting setting) {
        if (value == null || value.equals(new UUID(0L, 0L))) {
            throw new IllegalArgumentException("%s must be configured as a non-zero UUID".formatted(setting.key()));
        }
        return value;
    }

    private static Directories directories(final Path storageRoot, final NodeConfig.Directories configured,
                                           final boolean production) {
        final Path rawDriver = configured.aeron();
        final Path rawArchive = configured.archive();
        if (production && (!rawDriver.isAbsolute() || !rawArchive.isAbsolute())) {
            throw new IllegalArgumentException("Aeron directories must be absolute in production mode");
        }
        if (production && (temporaryPath(rawDriver) || temporaryPath(rawArchive))) {
            throw new IllegalArgumentException("Aeron directories must not use /tmp in production mode");
        }
        final Path driver = rawDriver.toAbsolutePath().normalize();
        final Path archive = rawArchive.toAbsolutePath().normalize();
        final Path store = storageRoot.toAbsolutePath().normalize().resolve("storage");
        if (overlaps(driver, archive) || overlaps(store, driver) || overlaps(store, archive)) {
            throw new IllegalArgumentException(
                    "Store, Aeron driver, and Archive paths must not overlap: store=%s, driver=%s, archive=%s"
                            .formatted(store, driver, archive));
        }
        return new Directories(driver, archive);
    }

    private static Channels validateChannels(final NodeConfig.Channels configured, final NodeRole role,
                                             final boolean production,
                                             final AeronReplicationConfiguration replication) {
        final String live = channel(configured.live(), NodeConfig.Setting.AERON_LIVE_CHANNEL.key());
        final String replay = channel(configured.replay(), NodeConfig.Setting.AERON_REPLAY_CHANNEL.key());
        final String control = channel(configured.control(), NodeConfig.Setting.AERON_CONTROL_CHANNEL.key());
        final String response = channel(configured.controlResponse(),
                NodeConfig.Setting.AERON_CONTROL_RESPONSE_CHANNEL.key());
        final String archive = channel(configured.archiveReplication(),
                NodeConfig.Setting.AERON_ARCHIVE_REPLICATION_CHANNEL.key());
        final String watermark = channel(configured.watermark(), NodeConfig.Setting.AERON_WATERMARK_CHANNEL.key());
        validateFraming(live, NodeConfig.Setting.AERON_LIVE_CHANNEL.key(), replication);
        validateFraming(replay, NodeConfig.Setting.AERON_REPLAY_CHANNEL.key(), replication);
        validateFraming(archive, NodeConfig.Setting.AERON_ARCHIVE_REPLICATION_CHANNEL.key(), replication);
        if (role.isWriter()) validateWriterTopology(live);
        final List<String> channels = List.of(live, replay, control, response, archive, watermark);
        if (production && channels.stream().anyMatch(AeronSettings::wildcardEndpoint)) {
            throw new IllegalArgumentException("wildcard Aeron endpoints are not allowed in production mode");
        }
        if (production && channels.stream().anyMatch(AeronSettings::loopbackEndpoint)) {
            throw new IllegalArgumentException(
                    "loopback Aeron endpoints are not allowed in production mode; configure routable node addresses");
        }
        return new Channels(live, replay, control, response, archive, watermark);
    }

    private static String channel(final String channel, final String name) {
        final String value = channel.trim();
        if (value.isEmpty() || value.chars().anyMatch(Character::isWhitespace) || value.equals("aeron:udp") ||
            !(value.startsWith("aeron:udp?") || value.equals("aeron:ipc") || value.startsWith("aeron:ipc?"))) {
            throw new IllegalArgumentException("Invalid Aeron channel for %s: %s".formatted(name, value));
        }
        final ChannelUri uri;
        try {
            uri = ChannelUri.parse(value);
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Aeron channel for %s: %s".formatted(name, value), failure);
        }
        if (uri.isUdp()) {
            final String endpoint = uri.get(CommonContext.ENDPOINT_PARAM_NAME);
            final String control = uri.get(CommonContext.MDC_CONTROL_PARAM_NAME);
            final boolean hasEndpoint = endpoint != null && !endpoint.isBlank();
            final boolean hasControl = control != null && !control.isBlank();
            if (!hasEndpoint && !hasControl) {
                throw new IllegalArgumentException(
                        "Aeron UDP channel must specify endpoint= or control= for %s".formatted(name));
            }
            if (hasEndpoint) validateUdpAddress(endpoint, name);
            if (hasControl) validateUdpAddress(control, name);
        }
        return value;
    }

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
                                           final Predicate<String> hostPredicate) {
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


}
