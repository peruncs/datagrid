package peruncs.datagrid.cluster.node.aeron;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.codecs.*;
import io.aeron.driver.ThreadingMode;
import io.aeron.security.*;
import org.agrona.SystemUtil;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.NodeRole;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

/// Validated configuration for one Aeron transport. Structural values are
/// immutable.
///
/// The environment keys are grouped into four concerns: topology and channels,
/// authentication, Archive policy, and runtime threading/timeouts. Each group
/// is parsed and validated by its own step in [#fromEnvironment], and the
/// result is assembled once. `offerTimeoutNanos` on the nested replication
/// configuration bounds publication offers only; Archive control requests and
/// watermark-channel shutdown have their own budgets below.
///
/// @param replication                     validated publication framing and timeout settings
/// @param clusterId                       stable cluster identity shared by all members
/// @param wireNonce                       shared non-authenticating nonce that rejects accidental cross-wiring
/// @param epoch                           writer epoch used to reject stale frames
/// @param streamId                        data stream id; replay and watermark ids are derived from it
/// @param recordingId                     configured or discovered Archive recording id
/// @param channels                        live, replay, Archive control, and watermark channels
/// @param directories                      MediaDriver, Archive, and checkpoint directories
/// @param identity                         node and Store-generation identities
/// @param archiveFileSyncLevel            Archive file and catalog synchronization level
/// @param minimumArchiveFreeBytes         minimum embedded-Archive free space
/// @param externalArchive                 whether an external Archive owns the recording
/// @param role                            normalized replication role
/// @param productionMode                  whether production-only validation is enabled
/// @param threadingMode                   MediaDriver threading mode
/// @param archiveThreadingMode            embedded Archive threading mode
/// @param archiveSegmentFileLength        Archive segment length in bytes
/// @param archiveLowStorageSpaceThreshold Archive low-storage threshold in bytes
/// @param maxConcurrentReplays            maximum simultaneous Archive replays
/// @param driverTimeoutMillis             MediaDriver timeout in milliseconds
/// @param archiveControlTimeoutNanos      timeout for one synchronous Archive control request
/// @param watermarkCloseTimeoutNanos      flush budget when a watermark channel closes
/// @param leaseAcquireLockTimeoutMillis   bounded wait for the writer lease interprocess lock
/// @param watermarkStreamId               reader-watermark stream id
/// @param retentionReaders                configured reader identities required for retention
/// @param auth                             Archive control-session authentication group; disabled auth
///                                          in production mode requires the explicit
///                                          `ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE=true` acknowledgement
record AeronSettings(
        AeronReplicationConfiguration replication,
        UUID clusterId,
        long wireNonce,
        long epoch,
        int streamId,
        long recordingId,
        Channels channels,
        Directories directories,
        StorageIdentity identity,
        int archiveFileSyncLevel,
        long minimumArchiveFreeBytes,
        boolean externalArchive,
        NodeRole role,
        boolean productionMode,
        ThreadingMode threadingMode,
        ArchiveThreadingMode archiveThreadingMode,
        int archiveSegmentFileLength,
        long archiveLowStorageSpaceThreshold,
        int maxConcurrentReplays,
        long driverTimeoutMillis,
        long archiveControlTimeoutNanos,
        long watermarkCloseTimeoutNanos,
        long leaseAcquireLockTimeoutMillis,
        int watermarkStreamId,
        Set<UUID> retentionReaders,
        Auth auth
) {
    private static final int MAX_SECRET_FILE_BYTES = 4096;
    private static final int ARCHIVE_PROTOCOL_ID = MessageHeaderDecoder.SCHEMA_ID;
    private static final long DEFAULT_ARCHIVE_CONTROL_TIMEOUT_NANOS = 5_000_000_000L;
    private static final long DEFAULT_WATERMARK_CLOSE_TIMEOUT_NANOS = 5_000_000_000L;
    private static final long DEFAULT_LEASE_LOCK_TIMEOUT_MILLIS = 5_000L;
    /* Reader clients only need discovery, position queries, and replay. Keep
     * every mutating Archive command out of this allow-list. */
    private static final int[] READER_ARCHIVE_ACTIONS = {
            ArchiveIdRequestDecoder.TEMPLATE_ID,
            ReplayRequestDecoder.TEMPLATE_ID,
            BoundedReplayRequestDecoder.TEMPLATE_ID,
            StopReplayRequestDecoder.TEMPLATE_ID,
            ListRecordingsRequestDecoder.TEMPLATE_ID,
            ListRecordingsForUriRequestDecoder.TEMPLATE_ID,
            ListRecordingRequestDecoder.TEMPLATE_ID,
            StartPositionRequestDecoder.TEMPLATE_ID,
            StopPositionRequestDecoder.TEMPLATE_ID,
            RecordingPositionRequestDecoder.TEMPLATE_ID,
            MaxRecordedPositionRequestDecoder.TEMPLATE_ID,
            KeepAliveRequestDecoder.TEMPLATE_ID,
            CloseSessionRequestDecoder.TEMPLATE_ID
    };
    /* The writer owns the recording lifecycle and retention cleanup. It still
     * receives an explicit list so adding a new Archive command does not
     * silently widen either role's authority. */
    private static final int[] WRITER_ARCHIVE_ACTIONS = {
            ArchiveIdRequestDecoder.TEMPLATE_ID,
            StartRecordingRequestDecoder.TEMPLATE_ID,
            StopRecordingRequestDecoder.TEMPLATE_ID,
            ExtendRecordingRequestDecoder.TEMPLATE_ID,
            StopRecordingByIdentityRequestDecoder.TEMPLATE_ID,
            PurgeSegmentsRequestDecoder.TEMPLATE_ID,
            ListRecordingsRequestDecoder.TEMPLATE_ID,
            ListRecordingsForUriRequestDecoder.TEMPLATE_ID,
            ListRecordingRequestDecoder.TEMPLATE_ID,
            StartPositionRequestDecoder.TEMPLATE_ID,
            StopPositionRequestDecoder.TEMPLATE_ID,
            RecordingPositionRequestDecoder.TEMPLATE_ID,
            MaxRecordedPositionRequestDecoder.TEMPLATE_ID,
            KeepAliveRequestDecoder.TEMPLATE_ID,
            CloseSessionRequestDecoder.TEMPLATE_ID
    };

    /// Copies mutable inputs so the record never shares caller-owned arrays or sets.
    AeronSettings {
        retentionReaders = retentionReaders == null ? Set.of() : Set.copyOf(retentionReaders);
        if (archiveControlTimeoutNanos <= 0L || watermarkCloseTimeoutNanos <= 0L ||
            leaseAcquireLockTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("Aeron per-concern timeouts must be positive");
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
    /// @param checkpointPath   durable writer-checkpoint path, owner-only and local
    record Directories(Path aeronDirectory, Path archiveDirectory, Path checkpointPath) {
        Directories {
            Objects.requireNonNull(aeronDirectory, "aeronDirectory");
            Objects.requireNonNull(archiveDirectory, "archiveDirectory");
            Objects.requireNonNull(checkpointPath, "checkpointPath");
        }
    }

    /// The Archive control-session authentication group.
    ///
    /// Credential arrays are copied in and copied out so the settings never
    /// share caller-owned state; [erase] zeroes the held copies when the
    /// owning transport closes.
    ///
    /// @param enabled           whether Aeron Archive control authentication is enabled
    /// @param principal         configured principal, or `null` when disabled
    /// @param credentials        copied credentials, or `null` when disabled
    /// @param readerPrincipal   optional reader principal accepted by a writer Archive
    /// @param readerCredentials copied credentials for the optional reader principal
    record Auth(
            boolean enabled,
            String principal,
            byte[] credentials,
            String readerPrincipal,
            byte[] readerCredentials
    ) {
        Auth {
            credentials = credentials == null ? null : credentials.clone();
            readerCredentials = readerCredentials == null ? null : readerCredentials.clone();
        }

        @Override
        public byte[] credentials() {
            return this.credentials == null ? null : this.credentials.clone();
        }

        @Override
        public byte[] readerCredentials() {
            return this.readerCredentials == null ? null : this.readerCredentials.clone();
        }

        /// Zeroes the credential copies held by this group.
        ///
        /// The principal is a non-secret identity string and has no
        /// erasable form.
        void erase() {
            if (this.credentials != null) java.util.Arrays.fill(this.credentials, (byte) 0);
            if (this.readerCredentials != null) java.util.Arrays.fill(this.readerCredentials, (byte) 0);
        }
    }

    static AeronSettings fromEnvironment(final NodeLibraryPropertiesProvider properties) {
        Objects.requireNonNull(properties, "properties");
        final String configuredRole = properties.replicationRole();
        if (configuredRole == null || configuredRole.isBlank()) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_REPLICATION_ROLE must be explicitly configured for Aeron");
        }
        /* One normalized role for transport setup; a legacy/new conflict
         * fails here, before any gate reads it. */
        final NodeRole role = properties.nodeRole();
        final boolean productionMode = properties.isProdMode();
        final boolean trustedNetwork = Boolean.parseBoolean(value(
                properties, "ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK", "false"));
        if (productionMode && !trustedNetwork) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK=true is required in production; "
                            + "the replication protocol is not authenticated");
        }
        final AeronReplicationConfiguration replication = replication(properties);
        final String cluster = value(properties, "ECLIPSE_DATAGRID_AERON_CLUSTER_ID", null);
        if (cluster == null) throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_CLUSTER_ID is required");
        final UUID clusterId = parseUuid(cluster, "ECLIPSE_DATAGRID_AERON_CLUSTER_ID");
        final String configuredNonce = value(properties, "ECLIPSE_DATAGRID_AERON_WIRE_NONCE", null);
        if (productionMode && (configuredNonce == null || configuredNonce.isBlank())) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_WIRE_NONCE is required in production");
        }
        final long wireNonce = configuredNonce == null || configuredNonce.isBlank()
                ? AeronReplicationEnvelope.defaultWireNonce(clusterId)
                : parseLong(properties, "ECLIPSE_DATAGRID_AERON_WIRE_NONCE", null);
        if (wireNonce == 0L) {
            throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_WIRE_NONCE must not be zero");
        }
        final long epoch = parseLong(properties, "ECLIPSE_DATAGRID_AERON_EPOCH", "1");
        final int streamId = parseInt(properties, "ECLIPSE_DATAGRID_AERON_STREAM_ID", "1001");
        final long recordingId = parseLong(properties, "ECLIPSE_DATAGRID_AERON_RECORDING_ID", "-1");
        if (epoch < 0 || streamId < 0 || recordingId < -1) {
            throw new IllegalArgumentException("Aeron epoch/stream/recording settings are out of range");
        }
        final Directories directories = directories(properties, productionMode);
        final StorageIdentity identity = identity(properties);
        final ArchivePolicy archivePolicy = archivePolicy(properties, productionMode);
        final Channels channels = channels(properties, cluster, replication, role,
                archivePolicy.externalArchive(), productionMode);
        final Set<UUID> retentionReaders = trustedNetwork ? retentionReaders(properties) : Set.of();
        final Authentication authentication = authentication(properties, role, productionMode,
                archivePolicy.externalArchive());
        final RuntimeSettings runtime = runtimeSettings(properties, streamId, replication);
        final AeronSettings settings = new AeronSettings(
                replication,
                clusterId,
                wireNonce,
                epoch,
                streamId,
                recordingId,
                channels,
                directories,
                identity,
                archivePolicy.fileSyncLevel(),
                archivePolicy.minimumFreeBytes(),
                archivePolicy.externalArchive(),
                role,
                productionMode,
                runtime.threadingMode(),
                runtime.archiveThreadingMode(),
                runtime.archiveSegmentFileLength(),
                runtime.archiveLowStorageSpaceThreshold(),
                runtime.maxConcurrentReplays(),
                runtime.driverTimeoutMillis(),
                runtime.archiveControlTimeoutNanos(),
                runtime.watermarkCloseTimeoutNanos(),
                runtime.leaseAcquireLockTimeoutMillis(),
                runtime.watermarkStreamId(),
                retentionReaders,
                new Auth(
                        authentication.enabled(),
                        authentication.enabled() ? authentication.principal() : null,
                        authentication.credentials(),
                        authentication.enabled() && authentication.readerPrincipal() != null
                                && !authentication.readerPrincipal().isBlank()
                                ? authentication.readerPrincipal() : null,
                        authentication.readerCredentials()
                )
        );
        /* The record constructor keeps its own defensive copy. Erase the parser's
         * temporaries immediately so configuration loading does not leave extra
         * long-lived credential copies on the heap. */
        final byte[] credentials = authentication.credentials();
        final byte[] readerCredentials = authentication.readerCredentials();
        if (credentials != null) Arrays.fill(credentials, (byte) 0);
        if (readerCredentials != null) Arrays.fill(readerCredentials, (byte) 0);
        return settings;
    }

    /// Parses the replication framing and timeout builder settings.
    private static AeronReplicationConfiguration replication(final NodeLibraryPropertiesProvider properties) {
        final AeronReplicationConfiguration.Builder builder = AeronReplicationConfiguration.builder();
        integerSetting(properties, "ECLIPSE_DATAGRID_AERON_TERM_LENGTH", builder::termLength);
        integerSetting(properties, "ECLIPSE_DATAGRID_AERON_MTU_LENGTH", builder::mtuLength);
        integerSetting(properties, "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE", builder::chunkSize);
        integerSetting(properties, "ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES", builder::maxTransactionBytes);
        longSetting(properties, "ECLIPSE_DATAGRID_AERON_OFFER_TIMEOUT_NANOS", builder::offerTimeoutNanos);
        longSetting(properties, "ECLIPSE_DATAGRID_AERON_RECORDING_START_TIMEOUT_NANOS",
                builder::recordingStartTimeoutNanos);
        longSetting(properties, "ECLIPSE_DATAGRID_AERON_RECORDED_POSITION_TIMEOUT_NANOS",
                builder::recordedPositionTimeoutNanos);
        longSetting(properties, "ECLIPSE_DATAGRID_AERON_RECORDING_STOP_TIMEOUT_NANOS",
                builder::recordingStopTimeoutNanos);
        longSetting(properties, "ECLIPSE_DATAGRID_AERON_READER_STOP_TIMEOUT_NANOS",
                builder::readerStopTimeoutNanos);
        builder.durabilityMode(durabilityMode(properties));
        return builder.build();
    }

    /// Resolves and validates the driver, Archive, and checkpoint directories.
    private static Directories directories(final NodeLibraryPropertiesProvider properties,
                                           final boolean productionMode) {
        final Path aeronDirectory = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_DIRECTORY", "/tmp/eclipse-datagrid-aeron"));
        final Path archiveDirectory = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY",
                aeronDirectory.resolveSibling("%s.archive".formatted(aeronDirectory.getFileName())).toString()));
        if (productionMode && (temporaryPath(aeronDirectory) || temporaryPath(archiveDirectory))) {
            throw new IllegalArgumentException("Aeron directories must not use /tmp in production mode");
        }
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
        if (productionMode && (!aeronDirectory.isAbsolute() || !archiveDirectory.isAbsolute() || !checkpointPath.isAbsolute())) {
            throw new IllegalArgumentException("Aeron directories and checkpoint path must be absolute in production mode");
        }
        return new Directories(normalizedAeronDirectory, normalizedArchiveDirectory, normalizedCheckpointPath);
    }

    /// Resolves the required node and Store-generation identities.
    private static StorageIdentity identity(final NodeLibraryPropertiesProvider properties) {
        final String configuredNodeId = value(properties, "ECLIPSE_DATAGRID_AERON_NODE_ID", null);
        final String configuredStoreGeneration = value(
                properties, "ECLIPSE_DATAGRID_AERON_STORE_GENERATION", null);
        if (configuredNodeId == null || configuredStoreGeneration == null) {
            throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_NODE_ID and ECLIPSE_DATAGRID_AERON_STORE_GENERATION are required");
        }
        return new StorageIdentity(
                parseUuid(configuredNodeId, "ECLIPSE_DATAGRID_AERON_NODE_ID"),
                parseUuid(configuredStoreGeneration, "ECLIPSE_DATAGRID_AERON_STORE_GENERATION"));
    }

    /// Parses the Archive durability and capacity policy.
    private static ArchivePolicy archivePolicy(final NodeLibraryPropertiesProvider properties,
                                               final boolean productionMode) {
        final int archiveFileSyncLevel = parseInt(properties, "ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL", "1");
        if (archiveFileSyncLevel < 0 || archiveFileSyncLevel > 2) {
            throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL must be 0, 1, or 2");
        }
        if (productionMode && archiveFileSyncLevel == 0) {
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
        return new ArchivePolicy(archiveFileSyncLevel, minimumArchiveFreeBytes, externalArchive);
    }

    /// Parses and validates every channel, including production endpoint policy.
    private static Channels channels(final NodeLibraryPropertiesProvider properties, final String cluster,
                                     final AeronReplicationConfiguration replication, final NodeRole role,
                                     final boolean externalArchive, final boolean productionMode) {
        /* Keep development defaults self-contained, but make them match the
         * production topology: explicit framing, dynamic MDC, and a stable
         * channel alias. Production deployments must override localhost endpoints
         * and are rejected below when they do not. */
        final String channelAlias = "datagrid-%s".formatted(cluster);
        final String liveChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
                "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=%d|mtu=%d|alias=%s"
                        .formatted(replication.termLength(), replication.mtuLength(), channelAlias));
        final String replayChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                "aeron:udp?endpoint=localhost:0|control=localhost:40123|control-mode=dynamic");
        final String archiveReplicationChannel = channel(properties,
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", "aeron:udp?endpoint=localhost:0");
        final String watermarkChannel = channel(properties,
                "ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL", "aeron:udp?endpoint=localhost:40125");
        final String controlChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL", "aeron:udp?endpoint=localhost:40124");
        final String controlResponseChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL", "aeron:udp?endpoint=localhost:0");
        validateFraming(liveChannel, "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL", replication);
        validateFraming(replayChannel, "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL", replication);
        validateFraming(archiveReplicationChannel,
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", replication);
        /* An embedded writer publishes directly to N readers and therefore requires
         * dynamic MDC. The external topology uses this URI for both the publication
         * and the remote Archive recording subscription; that Aeron pairing is a
         * point-to-point endpoint and cannot reuse the embedded MDC URI. */
        if (role.isWriter() && !externalArchive) {
            validateWriterTopology(liveChannel);
        }
        if (productionMode && (wildcardEndpoint(liveChannel) || wildcardEndpoint(replayChannel) ||
                               wildcardEndpoint(controlChannel) || wildcardEndpoint(controlResponseChannel) ||
                               wildcardEndpoint(archiveReplicationChannel) || wildcardEndpoint(watermarkChannel))) {
            throw new IllegalArgumentException("wildcard Aeron endpoints are not allowed in production mode");
        }
        if (productionMode && (loopbackEndpoint(liveChannel) || loopbackEndpoint(replayChannel) ||
                               loopbackEndpoint(controlChannel) || loopbackEndpoint(controlResponseChannel) ||
                               loopbackEndpoint(archiveReplicationChannel) || loopbackEndpoint(watermarkChannel))) {
            throw new IllegalArgumentException(
                    "loopback Aeron endpoints are not allowed in production mode; configure routable node addresses");
        }
        return new Channels(liveChannel, replayChannel, controlChannel, controlResponseChannel,
                archiveReplicationChannel, watermarkChannel);
    }

    /// Parses and validates the Archive authentication contract.
    private static Authentication authentication(final NodeLibraryPropertiesProvider properties, final NodeRole role,
                                                 final boolean productionMode, final boolean externalArchive) {
        final boolean authEnabled = booleanSetting(properties,
                "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED", false);
        /* Archive control authentication needs its own explicit
         * acknowledgement: control operations are a separate protection
         * domain from replication traffic. */
        if (!authEnabled && productionMode
            && !booleanSetting(properties, "ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE", false)) {
            throw new IllegalArgumentException(
                    "Aeron Archive authentication must be enabled in production mode "
                    + "(ECLIPSE_DATAGRID_AERON_AUTH_ENABLED=true), or explicitly acknowledge unauthenticated "
                    + "operation with ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE=true");
        }
        final String authPrincipal = value(properties, "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL", null);
        final byte[] authCredentials = authCredentials(properties);
        final String authReaderPrincipal = value(
                properties, "ECLIPSE_DATAGRID_AERON_AUTH_READER_PRINCIPAL", null);
        final byte[] authReaderCredentials = authCredentials(
                properties,
                "ECLIPSE_DATAGRID_AERON_AUTH_READER_CREDENTIALS",
                "ECLIPSE_DATAGRID_AERON_AUTH_READER_CREDENTIALS_FILE");
        if (!authEnabled) {
            if ((authPrincipal != null && !authPrincipal.isBlank()) || authCredentials != null ||
                (authReaderPrincipal != null && !authReaderPrincipal.isBlank()) || authReaderCredentials != null) {
                throw new IllegalArgumentException(
                        "Aeron auth principals and credentials require ECLIPSE_DATAGRID_AERON_AUTH_ENABLED=true");
            }
        } else {
            if (authPrincipal == null || authPrincipal.isBlank()) {
                throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL is required when auth is enabled");
            }
            if (authPrincipal.chars().anyMatch(character -> character < 0x21 || character > 0x7e)) {
                throw new IllegalArgumentException(
                        "ECLIPSE_DATAGRID_AERON_AUTH_PRINCIPAL must contain printable ASCII characters");
            }
            if (authCredentials == null) {
                throw new IllegalArgumentException(
                        "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS or *_CREDENTIALS_FILE is required when auth is enabled");
            }
            if (authReaderPrincipal != null && !authReaderPrincipal.isBlank()) {
                if (authReaderPrincipal.chars().anyMatch(character -> character < 0x21 || character > 0x7e)) {
                    throw new IllegalArgumentException(
                            "ECLIPSE_DATAGRID_AERON_AUTH_READER_PRINCIPAL must contain printable ASCII characters");
                }
                if (authReaderCredentials == null) {
                    throw new IllegalArgumentException(
                            "ECLIPSE_DATAGRID_AERON_AUTH_READER_CREDENTIALS or *_CREDENTIALS_FILE is required when a reader principal is configured");
                }
                if (authReaderPrincipal.trim().equals(authPrincipal.trim()) ||
                    Arrays.equals(authReaderCredentials, authCredentials)) {
                    throw new IllegalArgumentException(
                            "writer and reader Archive identities must use different principals and credentials");
                }
            } else if (authReaderCredentials != null) {
                throw new IllegalArgumentException(
                        "ECLIPSE_DATAGRID_AERON_AUTH_READER_PRINCIPAL is required when reader credentials are configured");
            }
            if (productionMode && !externalArchive && role.isWriter() &&
                (authReaderPrincipal == null || authReaderPrincipal.isBlank())) {
                throw new IllegalArgumentException(
                        "production writers require a separate ECLIPSE_DATAGRID_AERON_AUTH_READER_PRINCIPAL and credentials");
            }
        }
        return new Authentication(authEnabled,
                authEnabled ? authPrincipal.trim() : null,
                authCredentials,
                authReaderPrincipal == null || authReaderPrincipal.isBlank() ? null : authReaderPrincipal.trim(),
                authReaderCredentials);
    }

    /// Parses threading, Archive sizing, and the per-concern timeout budgets.
    private static RuntimeSettings runtimeSettings(final NodeLibraryPropertiesProvider properties,
                                                   final int streamId,
                                                   final AeronReplicationConfiguration replication) {
        /* Distinct streams must be reservable for replay and watermarks. */
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
        final long archiveControlTimeoutNanos = positiveLongSetting(properties,
                "ECLIPSE_DATAGRID_AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS", DEFAULT_ARCHIVE_CONTROL_TIMEOUT_NANOS);
        final long watermarkCloseTimeoutNanos = positiveLongSetting(properties,
                "ECLIPSE_DATAGRID_AERON_WATERMARK_CLOSE_TIMEOUT_NANOS", DEFAULT_WATERMARK_CLOSE_TIMEOUT_NANOS);
        final long leaseAcquireLockTimeoutMillis = positiveLongSetting(properties,
                "ECLIPSE_DATAGRID_AERON_LEASE_LOCK_TIMEOUT_MILLIS", DEFAULT_LEASE_LOCK_TIMEOUT_MILLIS);
        if (archiveSegmentFileLength <= 0 || Integer.bitCount(archiveSegmentFileLength) != 1 ||
            archiveSegmentFileLength < replication.termLength() ||
            archiveLowStorageSpaceThreshold < 0 || maxConcurrentReplays <= 0 || driverTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Archive segment length must be a positive power of two; low-storage threshold must not be negative; max concurrent replays must be positive");
        }
        return new RuntimeSettings(threadingMode, archiveThreadingMode, archiveSegmentFileLength,
                archiveLowStorageSpaceThreshold, maxConcurrentReplays, driverTimeoutMillis,
                archiveControlTimeoutNanos, watermarkCloseTimeoutNanos, leaseAcquireLockTimeoutMillis,
                watermarkStreamId);
    }

    /** Reads one secret file through a bounded, stable descriptor snapshot. */
    private static byte[] readSecretFile(final Path path) throws IOException {
        final int maxBytes = MAX_SECRET_FILE_BYTES;
        final String description = "auth credentials";
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

        /// Decodes an inline secret value, labelling failures with its property.
    private interface InlineSecretDecoder {
        byte[] decode(String configured, String property);
    }

        /// Decodes a secret file, labelling failures with its property.
    private interface FileSecretDecoder {
        byte[] decode(String fileName, String property);
    }

        /// Reads credentials from an inline value or a secret file, which stay
    /// mutually exclusive. The two decoder types are distinct so swapping them
    /// at a call site fails compilation instead of decoding silently wrong.
    ///
    /// @param properties    property provider
    /// @param inlineKey     inline base64 property name
    /// @param fileKey       secret-file property name
    /// @param inlineDecoder decodes an inline value with its property label
    /// @param fileDecoder   decodes a secret file with its property label
    /// @return decoded credentials, or `null` when unconfigured
    private static byte[] credentialsSetting(
            final NodeLibraryPropertiesProvider properties,
            final String inlineKey,
            final String fileKey,
            final InlineSecretDecoder inlineDecoder,
            final FileSecretDecoder fileDecoder
    ) {
        final String configured = value(properties, inlineKey, null);
        final String configuredFile = value(properties, fileKey, null);
        if (configured != null && !configured.isBlank() && configuredFile != null && !configuredFile.isBlank()) {
            throw new IllegalArgumentException("%s and %s are mutually exclusive".formatted(inlineKey, fileKey));
        }
        if (configured == null || configured.isBlank()) {
            return configuredFile == null || configuredFile.isBlank()
                    ? null : fileDecoder.decode(configuredFile.trim(), fileKey);
        }
        return inlineDecoder.decode(configured.trim(), inlineKey);
    }

    private static boolean booleanSetting(final NodeLibraryPropertiesProvider properties,
                                          final String name, final boolean fallback) {
        final String configured = value(properties, name, null);
        if (configured == null || configured.isBlank()) return fallback;
        if ("true".equalsIgnoreCase(configured.trim())) return true;
        if ("false".equalsIgnoreCase(configured.trim())) return false;
        throw new IllegalArgumentException("%s must be true or false".formatted(name));
    }

    private static byte[] authCredentials(final NodeLibraryPropertiesProvider properties) {
        return authCredentials(properties,
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS",
                "ECLIPSE_DATAGRID_AERON_AUTH_CREDENTIALS_FILE");
    }

    private static byte[] authCredentials(
            final NodeLibraryPropertiesProvider properties,
            final String credentialsProperty,
            final String credentialsFileProperty
    ) {
        return credentialsSetting(properties, credentialsProperty, credentialsFileProperty,
                AeronSettings::decodeAuthCredentials, AeronSettings::decodeAuthCredentialsFile);
    }

    private static byte[] decodeAuthCredentialsFile(final String fileName, final String property) {
        final Path path = Paths.get(fileName).toAbsolutePath().normalize();
        try {
            final byte[] encoded = readSecretFile(path);
            try {
                return decodeAuthCredentials(
                        new String(encoded, StandardCharsets.US_ASCII).trim(),
                        property);
            } finally {
                Arrays.fill(encoded, (byte) 0);
            }
        } catch (final IOException | IllegalArgumentException failure) {
            throw new IllegalArgumentException("%s is invalid: %s".formatted(property, path),
                    failure);
        }
    }

    private static byte[] decodeAuthCredentials(final String configured, final String property) {
        try {
            final byte[] credentials = Base64.getDecoder().decode(configured);
            if (credentials.length < 8) throw new IllegalArgumentException(
                    "%s must decode to at least 8 bytes".formatted(property));
            return credentials;
        } catch (final IllegalArgumentException failure) {
            throw new IllegalArgumentException("%s must be base64 and decode to at least 8 bytes".formatted(property), failure);
        }
    }

    private static ThreadingMode threadingMode(final NodeLibraryPropertiesProvider properties) {
        final String configured = value(properties, "ECLIPSE_DATAGRID_AERON_THREADING_MODE",
                properties.isProdMode() ? "DEDICATED" : "SHARED");
        return switch (configured.trim().toUpperCase(Locale.ROOT)) {
            case "SHARED" -> ThreadingMode.SHARED;
            case "DEDICATED" -> ThreadingMode.DEDICATED;
            default -> throw new IllegalArgumentException(
                    "ECLIPSE_DATAGRID_AERON_THREADING_MODE must be SHARED or DEDICATED");
        };
    }

    private static void integerSetting(final NodeLibraryPropertiesProvider properties,
                                       final String environment, final IntConsumer setter) {
        final String configured = value(properties, environment, null);
        if (configured == null) return;
        try {
            setter.accept(Integer.parseInt(configured.trim()));
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "Invalid integer for %s: %s".formatted(environment, configured), failure);
        }
    }

    private static void longSetting(final NodeLibraryPropertiesProvider properties,
                                    final String environment, final LongConsumer setter) {
        final String configured = value(properties, environment, null);
        if (configured == null) return;
        try {
            setter.accept(Long.parseLong(configured.trim()));
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "Invalid long for %s: %s".formatted(environment, configured), failure);
        }
    }

        /// Accepts only archive-first durability.
    private static ReplicationDurabilityMode durabilityMode(final NodeLibraryPropertiesProvider properties) {
        final String primary = value(properties, "ECLIPSE_DATAGRID_AERON_REPLICATION_DURABILITY_MODE", null);
        final String legacy = value(properties, "ECLIPSE_DATAGRID_AERON_DURABILITY_MODE", null);
        if (primary != null && legacy != null && !equivalentSetting(primary, legacy)) {
            throw new IllegalArgumentException(
                    "Conflicting Aeron settings for durability mode: %s and %s".formatted(primary, legacy));
        }
        final String selected = primary != null ? primary : legacy;
        if (selected == null || selected.isBlank()) return ReplicationDurabilityMode.ARCHIVE_FIRST;
        return switch (selected.trim().toLowerCase(Locale.ROOT)) {
            case "archive-first", "archive_first" -> ReplicationDurabilityMode.ARCHIVE_FIRST;
            default -> throw new IllegalArgumentException(
                    "Unsupported replication durability mode '%s'; only archive-first is supported".formatted(selected));
        };
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
        final ChannelUri uri;
        try {
            uri = ChannelUri.parse(channel);
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Aeron channel for %s: %s".formatted(name, channel), failure);
        }
        if (uri.isUdp()) {
            final String endpoint = uri.get(CommonContext.ENDPOINT_PARAM_NAME);
            final String control = uri.get(CommonContext.MDC_CONTROL_PARAM_NAME);
            final boolean hasEndpoint = endpoint != null && !endpoint.isBlank();
            final boolean hasControl = control != null && !control.isBlank();
            if (!hasEndpoint && !hasControl) {
                throw new IllegalArgumentException("Aeron UDP channel must specify endpoint= or control= for %s".formatted(name));
            }
            if (hasEndpoint) validateUdpAddress(endpoint, name);
            if (hasControl) validateUdpAddress(control, name);
        }
        return channel;
    }

        /// Validates one host:port address option from an already-parsed URI.
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

    /// Parses a positive long environment setting with a typed default.
    private static long positiveLongSetting(final NodeLibraryPropertiesProvider properties,
                                            final String name, final long fallback) {
        final long configured = parseLong(properties, name, Long.toString(fallback));
        if (configured <= 0L) {
            throw new IllegalArgumentException("%s must be positive".formatted(name));
        }
        return configured;
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
    public Set<UUID> retentionReaders() {
        return this.retentionReaders;
    }

    /// Erases the settings-held Aeron auth credentials when the owning transport closes.
    ///
    /// The owning [AeronRuntime] calls this once its driver, client, and Archive
    /// are all released, so a closed node keeps no long-lived credential copy.
    /// Copies previously issued through [#authenticatorSupplier()] and
    /// [#credentialsSupplier()] are owned by the Aeron Archive and client
    /// contexts they were issued to and are released together with those
    /// contexts by the same close; only this settings-held copy is erased
    /// here. The principal is a non-secret identity string and has no
    /// erasable form.
    void clearAuthCredentials() {
        this.auth.erase();
    }

        /// Builds the Archive authenticator for the embedded Archive, or `null` when auth is disabled.
    ///
    /// Authentication is enforced at the Archive control protocol: unauthenticated
    /// sessions are rejected before any recording or replay is authorized. This never
    /// replaces network policy — the control, replication, and watermark channels must
    /// still sit on an isolated network, which remains the defense-in-depth boundary
    /// against observers and denial-of-service.
    ///
    /// @return authenticator supplier, or `null`
    AuthenticatorSupplier authenticatorSupplier() {
        if (!this.auth.enabled()) return null;
        final byte[] principal = this.auth.principal().getBytes(StandardCharsets.US_ASCII);
        final byte[] credentials = this.auth.credentials();
        final byte[] readerPrincipal = this.auth.readerPrincipal() == null
                ? null : this.auth.readerPrincipal().getBytes(StandardCharsets.US_ASCII);
        final byte[] readerCredentials = this.auth.readerCredentials();
        return () -> {
            final SimpleAuthenticator.Builder builder = new SimpleAuthenticator.Builder()
                    .principal(principal, credentials);
            if (readerPrincipal != null) builder.principal(readerPrincipal, readerCredentials);
            return builder.newInstance();
        };
    }

        /// Builds the Archive authorisation service for the embedded Archive, or `null` when auth is disabled.
    ///
    /// Only the configured principal is authorized, and only for the
    /// role-specific Archive actions required by this node. Reader principals
    /// receive discovery and replay actions; writer principals additionally
    /// receive recording and retention-maintenance actions. Unknown actions
    /// and principals are denied by default.
    ///
    /// @return authorisation service supplier, or `null`
    AuthorisationServiceSupplier authorisationServiceSupplier() {
        if (!this.auth.enabled()) return null;
        final byte[] principal = this.auth.principal().getBytes(StandardCharsets.US_ASCII);
        final byte[] readerPrincipal = this.auth.readerPrincipal() == null
                ? null : this.auth.readerPrincipal().getBytes(StandardCharsets.US_ASCII);
        final int[] actions = this.role.isWriter()
                ? WRITER_ARCHIVE_ACTIONS : READER_ARCHIVE_ACTIONS;
        return () -> {
            final SimpleAuthorisationService.Builder builder = new SimpleAuthorisationService.Builder()
                    .defaultAuthorisation(AuthorisationService.DENY_ALL);
            addArchiveRules(builder, actions, principal);
            if (readerPrincipal != null) addArchiveRules(builder, READER_ARCHIVE_ACTIONS, readerPrincipal);
            return builder.newInstance();
        };
    }

    private static void addArchiveRules(
            final SimpleAuthorisationService.Builder builder,
            final int[] actions,
            final byte[] principal
    ) {
        for (final int action : actions) {
            builder.addPrincipalRule(ARCHIVE_PROTOCOL_ID, action, principal, true);
        }
    }

        /// Builds the client credentials presented to the Archive, or `null` when auth is disabled.
    ///
    /// @return credentials supplier, or `null`
    CredentialsSupplier credentialsSupplier() {
        if (!this.auth.enabled()) return null;
        final byte[] credentials = Objects.requireNonNull(this.auth.credentials(),
                "auth credentials must be set when auth is enabled");
        return new CredentialsSupplier() {
            @Override
            public byte[] encodedCredentials() {
                return credentials.clone();
            }

            @Override
            public byte[] onChallenge(final byte[] encodedChallenge) {
                /* SimpleAuthenticator performs no challenge/response round-trip;
                 * re-present the same credentials if one is ever issued. */
                return credentials.clone();
            }
        };
    }

    /// Local parse holder for the Archive policy group.
    private record ArchivePolicy(int fileSyncLevel, long minimumFreeBytes, boolean externalArchive) {
    }

    /// Local parse holder for the authentication group.
    private record Authentication(boolean enabled, String principal, byte[] credentials,
                                  String readerPrincipal, byte[] readerCredentials) {
    }

    /// Local parse holder for the runtime threading and timeout group.
    private record RuntimeSettings(ThreadingMode threadingMode, ArchiveThreadingMode archiveThreadingMode,
                                   int archiveSegmentFileLength, long archiveLowStorageSpaceThreshold,
                                   int maxConcurrentReplays, long driverTimeoutMillis,
                                   long archiveControlTimeoutNanos, long watermarkCloseTimeoutNanos,
                                   long leaseAcquireLockTimeoutMillis, int watermarkStreamId) {
    }
}
