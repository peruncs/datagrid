package peruncs.cluster.api;

import io.aeron.archive.Archive;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

/// Immutable, typed configuration for one PerunCS node.
///
/// Environment variables and external configuration maps are parsed once at
/// startup. Applications can instead construct the same value with
/// [#builder()].
///
/// @param role replication role, or standalone
/// @param storage Store path and disk-maintenance policy
/// @param backup backup volume and retention policy
/// @param timeouts bounded waits used by lifecycle, replication, and apply
/// @param limits bounded memory, queue, scan, and wire sizes
/// @param aeron Aeron topology and Archive policy
/// @param productionMode whether production-only validation is enabled
/// @param replicationTransport selected transport
/// @param operations bounded retries and shutdown waits
/// @since 1.0
public record NodeConfig(
        NodeRole role,
        StorageConfig storage,
        BackupConfig backup,
        Timeouts timeouts,
        Limits limits,
        AeronConfig aeron,
        boolean productionMode,
        ReplicationTransport replicationTransport,
        Operations operations
) {
    /// All supported environment/configuration keys, defaults, and parsers.
    public enum Setting {
        PROD_MODE("PERUNCS_PROD_MODE", "false", NodeConfig::parseBoolean),
        REPLICATION_TRANSPORT("PERUNCS_REPLICATION_TRANSPORT", "none", NodeConfig::parseTransport),
        REPLICATION_ROLE("PERUNCS_REPLICATION_ROLE", null, NodeConfig::parseRole),
        STORAGE_PATH("PERUNCS_STORAGE_PATH", "storage", Path::of),
        BACKUP_PATH("PERUNCS_BACKUP_PATH", "backups", Path::of),
        BACKUP_WORKSPACE_PATH("PERUNCS_BACKUP_WORKSPACE_PATH", null, Path::of),
        BACKUP_MAX_ENTRIES("PERUNCS_BACKUP_MAX_ENTRIES", "1048576", NodeConfig::parsePositiveInt),
        BACKUP_PUBLICATION_LOCK_TIMEOUT_MILLIS(
                "PERUNCS_BACKUP_PUBLICATION_LOCK_TIMEOUT_MILLIS", "30000", NodeConfig::parsePositiveMillis),
        KEPT_BACKUPS_COUNT("PERUNCS_KEPT_BACKUPS_COUNT", "3", NodeConfig::parsePositiveInt),
        STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES(
                "PERUNCS_STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES", null, NodeConfig::parsePositiveMinutes),
        GC_INTERVAL_MINUTES("PERUNCS_GC_INTERVAL_MINUTES", null, NodeConfig::parsePositiveMinutes),
        BACKUP_INTERVAL_MINUTES("PERUNCS_BACKUP_INTERVAL_MINUTES", "120", NodeConfig::parsePositiveMinutes),
        STORAGE_LIMIT_GB("PERUNCS_STORAGE_LIMIT_GB", null, NodeConfig::parseStorageLimitGb),
        DATA_MERGER_CACHE_TIMEOUT_MILLIS(
                "PERUNCS_DATA_MERGER_TIMEOUT", "10000", NodeConfig::parseNonNegativeMillis),
        DATA_MERGER_APPLY_TIMEOUT_MILLIS(
                "PERUNCS_DATA_MERGER_APPLY_TIMEOUT", "60000", NodeConfig::parsePositiveMillis),
        DATA_MERGER_APPLY_QUEUE_BYTES(
                "PERUNCS_DATA_MERGER_LIMIT", Long.toString(Limits.DEFAULT_APPLY_QUEUE_BYTES),
                NodeConfig::parsePositiveLong),
        DATA_MERGER_BUFFER_POOL_RETAINED_BYTES(
                "PERUNCS_POOL_MAX_RETAINED_BYTES",
                Long.toString(Limits.DEFAULT_BUFFER_POOL_RETAINED_BYTES), NodeConfig::parseNonNegativeLong),
        GRAPH_DRAIN_TIMEOUT_MILLIS("PERUNCS_GRAPH_DRAIN_TIMEOUT_MILLIS", "5000", NodeConfig::parsePositiveMillis),
        BACKUP_CLOSE_TIMEOUT_MILLIS("PERUNCS_BACKUP_CLOSE_TIMEOUT_MILLIS", "60000", NodeConfig::parsePositiveMillis),
        BACKUP_STOP_TIMEOUT_MILLIS("PERUNCS_BACKUP_STOP_TIMEOUT_MILLIS", "60000", NodeConfig::parsePositiveMillis),
        BACKUP_STOP_POLL_INTERVAL_MILLIS(
                "PERUNCS_BACKUP_STOP_POLL_INTERVAL_MILLIS", "100", NodeConfig::parsePositiveMillis),
        BACKUP_RETENTION_RETRY_ATTEMPTS(
                "PERUNCS_BACKUP_RETENTION_RETRY_ATTEMPTS", "3", NodeConfig::parsePositiveInt),
        BACKUP_RETENTION_RETRY_DELAY_MILLIS(
                "PERUNCS_BACKUP_RETENTION_RETRY_DELAY_MILLIS", "100", NodeConfig::parsePositiveMillis),
        BACKUP_PUBLICATION_RETRY_ATTEMPTS(
                "PERUNCS_BACKUP_PUBLICATION_RETRY_ATTEMPTS", "3", NodeConfig::parsePositiveInt),
        MAINTENANCE_FAILURE_THRESHOLD(
                "PERUNCS_MAINTENANCE_FAILURE_THRESHOLD", "3", NodeConfig::parsePositiveInt),
        WRITER_RECOVERY_ATTEMPTS("PERUNCS_WRITER_RECOVERY_ATTEMPTS", "3", NodeConfig::parsePositiveInt),
        MAINTENANCE_CLOSE_TIMEOUT_MILLIS(
                "PERUNCS_MAINTENANCE_CLOSE_TIMEOUT_MILLIS", "5000", NodeConfig::parsePositiveMillis),
        STORAGE_CHECK_CLOSE_TIMEOUT_MILLIS(
                "PERUNCS_STORAGE_CHECK_CLOSE_TIMEOUT_MILLIS", "5000", NodeConfig::parsePositiveMillis),
        INDEX_REFRESH_TIMEOUT_MILLIS(
                "PERUNCS_INDEX_REFRESH_TIMEOUT_MILLIS", "600000", NodeConfig::parsePositiveMillis),
        INDEX_VALIDATION_MAX_OBJECTS("PERUNCS_INDEX_VALIDATION_MAX_OBJECTS",
                Integer.toString(Limits.DEFAULT_MAX_VALIDATED_INDEX_OBJECTS), NodeConfig::parsePositiveInt),
        AERON_ARCHIVE_DIRECTORY("PERUNCS_AERON_ARCHIVE_DIRECTORY", null, Path::of),
        AERON_DIRECTORY("PERUNCS_AERON_DIRECTORY", null, Path::of),
        AERON_RETENTION_INTERVAL_MINUTES("PERUNCS_AERON_RETENTION_INTERVAL_MINUTES", "1", NodeConfig::parsePositiveMinutes),
        AERON_TERM_LENGTH("PERUNCS_AERON_TERM_LENGTH", "16777216", NodeConfig::parsePositiveInt),
        AERON_MTU_LENGTH("PERUNCS_AERON_MTU_LENGTH", "1408", NodeConfig::parsePositiveInt),
        AERON_CHUNK_SIZE("PERUNCS_AERON_CHUNK_SIZE", "131072", NodeConfig::parsePositiveInt),
        AERON_MAX_TRANSACTION_BYTES("PERUNCS_AERON_MAX_TRANSACTION_BYTES", "67108864", NodeConfig::parsePositiveInt),
        AERON_OFFER_TIMEOUT_NANOS("PERUNCS_AERON_OFFER_TIMEOUT_NANOS", "30000000000", NodeConfig::parsePositiveNanos),
        AERON_RECORDING_START_TIMEOUT_NANOS(
                "PERUNCS_AERON_RECORDING_START_TIMEOUT_NANOS", "30000000000", NodeConfig::parsePositiveNanos),
        AERON_RECORDED_POSITION_TIMEOUT_NANOS(
                "PERUNCS_AERON_RECORDED_POSITION_TIMEOUT_NANOS", "30000000000", NodeConfig::parsePositiveNanos),
        AERON_ABORT_RECORDED_POSITION_TIMEOUT_NANOS(
                "PERUNCS_AERON_ABORT_RECORDED_POSITION_TIMEOUT_NANOS", "5000000000", NodeConfig::parsePositiveNanos),
        AERON_RETRY_IDLE_MAX_PARK_NANOS(
                "PERUNCS_AERON_RETRY_IDLE_MAX_PARK_NANOS", "1000000", NodeConfig::parsePositiveNanos),
        AERON_RETRY_JITTER_CAP_NANOS(
                "PERUNCS_AERON_RETRY_JITTER_CAP_NANOS", "1000000", NodeConfig::parsePositiveNanos),
        AERON_RETRY_ARCHIVE_PROBE_DELAY_NANOS(
                "PERUNCS_AERON_RETRY_ARCHIVE_PROBE_DELAY_NANOS", "10000000", NodeConfig::parsePositiveNanos),
        AERON_RETENTION_OPERATION_TIMEOUT_MILLIS(
                "PERUNCS_AERON_RETENTION_OPERATION_TIMEOUT_MILLIS", "60000", NodeConfig::parsePositiveMillis),
        AERON_RECORDING_STOP_TIMEOUT_NANOS(
                "PERUNCS_AERON_RECORDING_STOP_TIMEOUT_NANOS", "30000000000", NodeConfig::parsePositiveNanos),
        AERON_READER_STOP_TIMEOUT_NANOS(
                "PERUNCS_AERON_READER_STOP_TIMEOUT_NANOS", "30000000000", NodeConfig::parsePositiveNanos),
        AERON_RECONNECT_TIMEOUT_NANOS(
                "PERUNCS_AERON_RECONNECT_TIMEOUT_NANOS", "30000000000", NodeConfig::parsePositiveNanos),
        AERON_NODE_ID("PERUNCS_AERON_NODE_ID", null, UUID::fromString),
        AERON_STORE_GENERATION("PERUNCS_AERON_STORE_GENERATION", null, UUID::fromString),
        AERON_CLUSTER_ID("PERUNCS_AERON_CLUSTER_ID", null, UUID::fromString),
        AERON_EPOCH("PERUNCS_AERON_EPOCH", "1", NodeConfig::parseNonNegativeLong),
        AERON_STREAM_ID("PERUNCS_AERON_STREAM_ID", "1001", NodeConfig::parseNonNegativeInt),
        AERON_RECORDING_ID("PERUNCS_AERON_RECORDING_ID", "-1", NodeConfig::parseRecordingId),
        AERON_WATERMARK_STREAM_ID("PERUNCS_AERON_WATERMARK_STREAM_ID", null, NodeConfig::parseNonNegativeInt),
        AERON_FILE_SYNC_LEVEL("PERUNCS_AERON_FILE_SYNC_LEVEL", "1", NodeConfig::parseNonNegativeInt),
        AERON_MIN_ARCHIVE_FREE_BYTES("PERUNCS_AERON_MIN_ARCHIVE_FREE_BYTES", "0", NodeConfig::parseNonNegativeLong),
        AERON_ARCHIVE_SEGMENT_FILE_LENGTH(
                "PERUNCS_AERON_ARCHIVE_SEGMENT_FILE_LENGTH",
                Integer.toString(Archive.Configuration.segmentFileLength()), NodeConfig::parsePositiveInt),
        AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD(
                "PERUNCS_AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD",
                Long.toString(Archive.Configuration.lowStorageSpaceThreshold()), NodeConfig::parseNonNegativeLong),
        AERON_MAX_CONCURRENT_REPLAYS(
                "PERUNCS_AERON_MAX_CONCURRENT_REPLAYS",
                Integer.toString(Archive.Configuration.maxConcurrentReplays()), NodeConfig::parsePositiveInt),
        AERON_DRIVER_TIMEOUT_MILLIS("PERUNCS_AERON_DRIVER_TIMEOUT_MILLIS", "10000", NodeConfig::parsePositiveMillis),
        AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS(
                "PERUNCS_AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS", "5000000000", NodeConfig::parsePositiveNanos),
        AERON_WATERMARK_CLOSE_TIMEOUT_NANOS(
                "PERUNCS_AERON_WATERMARK_CLOSE_TIMEOUT_NANOS", "5000000000", NodeConfig::parsePositiveNanos),
        AERON_LIVE_CHANNEL("PERUNCS_AERON_LIVE_CHANNEL", null, Function.identity()),
        AERON_REPLAY_CHANNEL("PERUNCS_AERON_REPLAY_CHANNEL", null, Function.identity()),
        AERON_ARCHIVE_REPLICATION_CHANNEL(
                "PERUNCS_AERON_ARCHIVE_REPLICATION_CHANNEL", null, Function.identity()),
        AERON_WATERMARK_CHANNEL("PERUNCS_AERON_WATERMARK_CHANNEL", null, Function.identity()),
        AERON_CONTROL_CHANNEL("PERUNCS_AERON_CONTROL_CHANNEL", null, Function.identity()),
        AERON_CONTROL_RESPONSE_CHANNEL("PERUNCS_AERON_CONTROL_RESPONSE_CHANNEL", null, Function.identity()),
        AERON_RETENTION_READERS("PERUNCS_AERON_RETENTION_READERS", "", NodeConfig::parseReaderIds),
        AERON_THREADING_MODE("PERUNCS_AERON_THREADING_MODE", null, NodeConfig::parseThreadingMode);

        private final String key;
        private final String defaultValue;
        private final Function<String, ?> parser;

        Setting(final String key, final String defaultValue, final Function<String, ?> parser) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.parser = parser;
        }

        /// Returns the environment or external-map key.
        public String key() {
            return this.key;
        }

        /// Returns the raw default value, or `null` when the setting is optional.
        public String defaultValue() {
            return this.defaultValue;
        }

        private Object read(final Map<String, String> source) {
            String raw = source.get(this.key);
            if (raw == null || raw.isBlank()) raw = this.defaultValue;
            if (raw == null) return null;
            try {
                return this.parser.apply(raw.trim());
            } catch (final RuntimeException failure) {
                throw new IllegalArgumentException("Invalid %s value: %s".formatted(this.key, raw), failure);
            }
        }
    }

    /// Transport selected by the node configuration.
    public enum ReplicationTransport { NONE, AERON }

    /// Store location and local disk checks.
    public record StorageConfig(
            Path root,
            Long limitBytes,
            Duration limitCheckInterval,
            Duration gcInterval
    ) {
        public StorageConfig {
            Objects.requireNonNull(root, "root");
            positiveOrNull(limitCheckInterval, "limitCheckInterval");
            positiveOrNull(gcInterval, "gcInterval");
            if (limitBytes != null && limitBytes <= 0L) throw new IllegalArgumentException("limitBytes must be positive");
        }
    }

    /// Backup volume and retention behavior.
    ///
    /// @param volume                 backup volume; the only path that may be shared between nodes
    /// @param workspace              node-local directory where backups are exported and compressed
    /// @param kept                   scheduled backups to keep
    /// @param interval               time between scheduled backups
    /// @param closeTimeout           longest wait for a running backup when the node closes
    /// @param maxArchiveEntries      most files one backup may hold; a Store with more cannot be restored
    /// @param publicationLockTimeout longest wait for another publisher on the shared volume
    public record BackupConfig(Path volume, Path workspace, int kept, Duration interval, Duration closeTimeout,
                               int maxArchiveEntries, Duration publicationLockTimeout) {
        public BackupConfig {
            Objects.requireNonNull(volume, "volume");
            Objects.requireNonNull(workspace, "workspace");
            if (kept <= 0 || maxArchiveEntries <= 0) {
                throw new IllegalArgumentException("kept and maxArchiveEntries must be positive");
            }
            positive(interval, "interval");
            positive(closeTimeout, "closeTimeout");
            positive(publicationLockTimeout, "publicationLockTimeout");
        }
    }

    /// Bounded retries and shutdown waits for maintenance work.
    public record Operations(
            Duration backupStopTimeout,
            Duration backupStopPollInterval,
            int backupRetentionRetryAttempts,
            Duration backupRetentionRetryDelay,
            int backupPublicationRetryAttempts,
            int maintenanceFailureThreshold,
            Duration maintenanceCloseTimeout,
            Duration storageCheckCloseTimeout,
            Duration retentionOperationTimeout,
            int writerRecoveryAttempts
    ) {
        public static final Operations DEFAULT = new Operations(
                Duration.ofMinutes(1), Duration.ofMillis(100), 3, Duration.ofMillis(100), 3, 3,
                Duration.ofMillis(5_000), Duration.ofMillis(5_000), Duration.ofMinutes(1), 3);

        public Operations {
            positive(backupStopTimeout, "backupStopTimeout");
            positive(backupStopPollInterval, "backupStopPollInterval");
            if (backupRetentionRetryAttempts <= 0 || backupPublicationRetryAttempts <= 0 ||
                maintenanceFailureThreshold <= 0 || writerRecoveryAttempts <= 0) {
                throw new IllegalArgumentException("operation retry counts and failure threshold must be positive");
            }
            positive(backupRetentionRetryDelay, "backupRetentionRetryDelay");
            positive(maintenanceCloseTimeout, "maintenanceCloseTimeout");
            positive(storageCheckCloseTimeout, "storageCheckCloseTimeout");
            positive(retentionOperationTimeout, "retentionOperationTimeout");
        }
    }

    /// Bounded waits across graph close, apply, and Aeron operations.
    public record Timeouts(
            Duration graphDrain,
            Duration mergerCache,
            Duration applyBudget,
            Duration offer,
            Duration recordingStart,
            Duration recordedPosition,
            Duration abortRecordedPosition,
            Duration recordingStop,
            Duration readerStop,
            Duration reconnect,
            Duration archiveControl,
            Duration watermarkClose,
            Duration driver,
            Duration indexRefresh
    ) {
        public Timeouts {
            positive(graphDrain, "graphDrain");
            nonNegativeOrNull(mergerCache, "mergerCache");
            positive(applyBudget, "applyBudget");
            positive(offer, "offer");
            positive(recordingStart, "recordingStart");
            positive(recordedPosition, "recordedPosition");
            positive(abortRecordedPosition, "abortRecordedPosition");
            positive(recordingStop, "recordingStop");
            positive(readerStop, "readerStop");
            positive(reconnect, "reconnect");
            positive(archiveControl, "archiveControl");
            positive(watermarkClose, "watermarkClose");
            positive(driver, "driver");
            positive(indexRefresh, "indexRefresh");
        }
    }

    /// Bounds for graph scans, queued data, and Aeron frames.
    public record Limits(
            int maxValidatedIndexObjects,
            long bufferPoolRetainedBytes,
            long applyQueueBytes,
            long applyQueueMaxBytes,
            int termLength,
            int mtuLength,
            int chunkSize,
            int maxTransactionBytes
    ) {
        public static final int DEFAULT_MAX_VALIDATED_INDEX_OBJECTS = 65_536;
        /* 32 MiB is the default ceiling; raise it only if profiling shows larger buffers recur. */
        public static final long DEFAULT_BUFFER_POOL_RETAINED_BYTES = 32L << 20;
        public static final long DEFAULT_APPLY_QUEUE_BYTES = 64L << 20;
        public static final long DEFAULT_APPLY_QUEUE_MAX_BYTES = 1L << 30;

        public Limits {
            if (maxValidatedIndexObjects <= 0 || bufferPoolRetainedBytes < 0L ||
                applyQueueBytes <= 0L || applyQueueMaxBytes <= 0L || termLength <= 0 ||
                mtuLength <= 0 || chunkSize <= 0 || maxTransactionBytes <= 0) {
                throw new IllegalArgumentException("Node limits must be positive (buffer retention may be zero)");
            }
            if (applyQueueBytes > applyQueueMaxBytes) {
                throw new IllegalArgumentException("applyQueueBytes must not exceed applyQueueMaxBytes");
            }
        }
    }

    /// Aeron node identity, channels, directories, and Archive policy.
    public record AeronConfig(
            UUID clusterId,
            UUID nodeId,
            UUID storeGeneration,
            long epoch,
            int streamId,
            long recordingId,
            int watermarkStreamId,
            Duration retentionInterval,
            Channels channels,
            Directories directories,
            ArchivePolicy archivePolicy,
            ThreadingMode threadingMode,
            RetryPacing retryPacing
    ) {
        public AeronConfig {
            Objects.requireNonNull(retryPacing, "retryPacing");
            Objects.requireNonNull(channels, "channels");
            Objects.requireNonNull(directories, "directories");
            Objects.requireNonNull(archivePolicy, "archivePolicy");
            Objects.requireNonNull(threadingMode, "threadingMode");
            positive(retentionInterval, "retentionInterval");
            if (epoch < 0L || streamId < 0 || recordingId < -1L || watermarkStreamId < 0) {
                throw new IllegalArgumentException("Aeron epoch, stream, or recording id is out of range");
            }
        }
    }

    /// How fast the writer's bounded retry loops spin: trades CPU against reaction time on slow Archives.
    ///
    /// @param idleMaxPark       longest park of one idle step while waiting for Aeron
    /// @param jitterCap         longest delay between two offer retries
    /// @param archiveProbeDelay spacing of Archive progress probes while waiting for a recorded position
    public record RetryPacing(Duration idleMaxPark, Duration jitterCap, Duration archiveProbeDelay) {
        /// Documented defaults: 1 ms, 1 ms and 10 ms.
        public static final RetryPacing DEFAULT = new RetryPacing(
                Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofMillis(10));

        public RetryPacing {
            positive(idleMaxPark, "idleMaxPark");
            positive(jitterCap, "jitterCap");
            positive(archiveProbeDelay, "archiveProbeDelay");
        }
    }

    /// Aeron and Archive channels.
    public record Channels(
            String live,
            String replay,
            String archiveReplication,
            String watermark,
            String control,
            String controlResponse
    ) {
        public Channels {
            Objects.requireNonNull(live, "live");
            Objects.requireNonNull(replay, "replay");
            Objects.requireNonNull(archiveReplication, "archiveReplication");
            Objects.requireNonNull(watermark, "watermark");
            Objects.requireNonNull(control, "control");
            Objects.requireNonNull(controlResponse, "controlResponse");
        }
    }

    /// Archive durability, capacity, and reader quorum.
    public record ArchivePolicy(
            int fileSyncLevel,
            long minimumFreeBytes,
            int segmentFileLength,
            long lowStorageSpaceThreshold,
            int maxConcurrentReplays,
            Set<UUID> retentionReaders
    ) {
        public ArchivePolicy {
            retentionReaders = Set.copyOf(retentionReaders);
            if (fileSyncLevel < 0 || fileSyncLevel > 2 || minimumFreeBytes < 0L ||
                segmentFileLength <= 0 || lowStorageSpaceThreshold < 0L || maxConcurrentReplays <= 0) {
                throw new IllegalArgumentException("Invalid Archive policy");
            }
        }
    }

    /// Local Aeron process and Archive directories.
    public record Directories(Path aeron, Path archive) {
        public Directories {
            Objects.requireNonNull(aeron, "aeron");
            Objects.requireNonNull(archive, "archive");
        }
    }

    /// MediaDriver threading mode.
    public enum ThreadingMode { SHARED, DEDICATED }

    public NodeConfig {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(backup, "backup");
        Objects.requireNonNull(timeouts, "timeouts");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(aeron, "aeron");
        Objects.requireNonNull(replicationTransport, "replicationTransport");
        Objects.requireNonNull(operations, "operations");
        if ((replicationTransport == ReplicationTransport.NONE) != (role == NodeRole.STANDALONE)) {
            throw new IllegalArgumentException("transport and role disagree");
        }
    }

    /// Parses the process environment once into immutable configuration.
    public static NodeConfig fromEnvironment() {
        return fromMap(System.getenv());
    }

    /// Parses recognized settings from a framework-neutral map.
    public static NodeConfig fromMap(final Map<String, String> values) {
        final Map<String, String> source = Map.copyOf(Objects.requireNonNull(values, "values"));
        final EnumMap<Setting, Object> parsed = new EnumMap<>(Setting.class);
        for (final Setting setting : Setting.values()) parsed.put(setting, setting.read(source));

        final boolean production = value(parsed, Setting.PROD_MODE);
        final ReplicationTransport transport = value(parsed, Setting.REPLICATION_TRANSPORT);
        final String configuredRole = value(parsed, Setting.REPLICATION_ROLE);
        final NodeRole role = transport == ReplicationTransport.NONE
                ? NodeRole.STANDALONE : NodeRole.of(configuredRole);
        if (configuredRole != null && transport == ReplicationTransport.NONE) {
            throw new IllegalArgumentException("replication role requires PERUNCS_REPLICATION_TRANSPORT=aeron");
        }
        if (source.getOrDefault(Setting.PROD_MODE.key(), "").isBlank() &&
            !source.getOrDefault(Setting.AERON_ARCHIVE_DIRECTORY.key(), "").isBlank()) {
            throw new IllegalArgumentException("PERUNCS_PROD_MODE must be explicitly set when production-only settings are configured");
        }
        if (transport == ReplicationTransport.NONE && source.entrySet().stream().anyMatch(entry ->
                entry.getKey().startsWith("PERUNCS_AERON_") && !entry.getValue().isBlank())) {
            throw new IllegalArgumentException("PERUNCS_AERON_* settings require PERUNCS_REPLICATION_TRANSPORT=aeron");
        }

        final Path root = value(parsed, Setting.STORAGE_PATH);
        final Path backupVolume = value(parsed, Setting.BACKUP_PATH);
        final Integer limitCheckMinutes = value(parsed, Setting.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES);
        final Integer gcMinutes = value(parsed, Setting.GC_INTERVAL_MINUTES);
        final Integer backupIntervalMinutes = value(parsed, Setting.BACKUP_INTERVAL_MINUTES);
        final Integer retentionIntervalMinutes = value(parsed, Setting.AERON_RETENTION_INTERVAL_MINUTES);
        final Integer storageLimitGigabytes = value(parsed, Setting.STORAGE_LIMIT_GB);
        final long applyTimeoutMillis = value(parsed, Setting.DATA_MERGER_APPLY_TIMEOUT_MILLIS);
        final long mergerCacheMillis = value(parsed, Setting.DATA_MERGER_CACHE_TIMEOUT_MILLIS);
        final int termLength = value(parsed, Setting.AERON_TERM_LENGTH);
        final int mtuLength = value(parsed, Setting.AERON_MTU_LENGTH);
        final int chunkSize = value(parsed, Setting.AERON_CHUNK_SIZE);
        final int maxTransactionBytes = value(parsed, Setting.AERON_MAX_TRANSACTION_BYTES);
        final UUID clusterId = value(parsed, Setting.AERON_CLUSTER_ID);
        final Path aeronDirectory = defaulted(value(parsed, Setting.AERON_DIRECTORY), root.resolve("aeron"));
        final Path archiveDirectory = defaulted(value(parsed, Setting.AERON_ARCHIVE_DIRECTORY),
                aeronDirectory.resolveSibling(aeronDirectory.getFileName() + ".archive"));
        final long epoch = value(parsed, Setting.AERON_EPOCH);
        final int streamId = value(parsed, Setting.AERON_STREAM_ID);
        if (streamId > Integer.MAX_VALUE - 2) {
            throw new IllegalArgumentException("PERUNCS_AERON_STREAM_ID must leave room for replay and watermark streams");
        }
        final int watermarkStreamId = defaulted(value(parsed, Setting.AERON_WATERMARK_STREAM_ID), streamId + 2);
        final String alias = "peruncs-%s".formatted(clusterId);
        final Channels channels = new Channels(
                defaulted(value(parsed, Setting.AERON_LIVE_CHANNEL),
                        "aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=%d|mtu=%d|alias=%s"
                                .formatted(termLength, mtuLength, alias)),
                defaulted(value(parsed, Setting.AERON_REPLAY_CHANNEL),
                        "aeron:udp?endpoint=localhost:0|control=localhost:40123|control-mode=dynamic"),
                defaulted(value(parsed, Setting.AERON_ARCHIVE_REPLICATION_CHANNEL), "aeron:udp?endpoint=localhost:0"),
                defaulted(value(parsed, Setting.AERON_WATERMARK_CHANNEL), "aeron:udp?endpoint=localhost:40125"),
                defaulted(value(parsed, Setting.AERON_CONTROL_CHANNEL), "aeron:udp?endpoint=localhost:40124"),
                defaulted(value(parsed, Setting.AERON_CONTROL_RESPONSE_CHANNEL), "aeron:udp?endpoint=localhost:0"));
        final ThreadingMode threading = defaulted(value(parsed, Setting.AERON_THREADING_MODE),
                production ? ThreadingMode.DEDICATED : ThreadingMode.SHARED);

        final StorageConfig storage = new StorageConfig(root,
                storageLimitGigabytes == null ? null : Math.multiplyExact((long) storageLimitGigabytes, 1_000_000_000L),
                durationMinutes(limitCheckMinutes), durationMinutes(gcMinutes));
        final BackupConfig backup = new BackupConfig(backupVolume,
                defaulted(value(parsed, Setting.BACKUP_WORKSPACE_PATH), root.resolve("backup-workspace")),
                value(parsed, Setting.KEPT_BACKUPS_COUNT),
                Duration.ofMinutes(backupIntervalMinutes),
                Duration.ofMillis(value(parsed, Setting.BACKUP_CLOSE_TIMEOUT_MILLIS)),
                value(parsed, Setting.BACKUP_MAX_ENTRIES),
                Duration.ofMillis(value(parsed, Setting.BACKUP_PUBLICATION_LOCK_TIMEOUT_MILLIS)));
        final Timeouts timeouts = new Timeouts(
                Duration.ofMillis(value(parsed, Setting.GRAPH_DRAIN_TIMEOUT_MILLIS)),
                Duration.ofMillis(mergerCacheMillis),
                Duration.ofMillis(applyTimeoutMillis),
                Duration.ofNanos(value(parsed, Setting.AERON_OFFER_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_RECORDING_START_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_RECORDED_POSITION_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_ABORT_RECORDED_POSITION_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_RECORDING_STOP_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_READER_STOP_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_RECONNECT_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_ARCHIVE_CONTROL_TIMEOUT_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_WATERMARK_CLOSE_TIMEOUT_NANOS)),
                Duration.ofMillis(value(parsed, Setting.AERON_DRIVER_TIMEOUT_MILLIS)),
                Duration.ofMillis(value(parsed, Setting.INDEX_REFRESH_TIMEOUT_MILLIS)));
        final Limits limits = new Limits(
                value(parsed, Setting.INDEX_VALIDATION_MAX_OBJECTS),
                value(parsed, Setting.DATA_MERGER_BUFFER_POOL_RETAINED_BYTES),
                value(parsed, Setting.DATA_MERGER_APPLY_QUEUE_BYTES),
                Limits.DEFAULT_APPLY_QUEUE_MAX_BYTES,
                termLength,
                mtuLength,
                chunkSize,
                maxTransactionBytes);
        final ArchivePolicy archivePolicy = new ArchivePolicy(
                value(parsed, Setting.AERON_FILE_SYNC_LEVEL),
                value(parsed, Setting.AERON_MIN_ARCHIVE_FREE_BYTES),
                value(parsed, Setting.AERON_ARCHIVE_SEGMENT_FILE_LENGTH),
                value(parsed, Setting.AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD),
                value(parsed, Setting.AERON_MAX_CONCURRENT_REPLAYS),
                value(parsed, Setting.AERON_RETENTION_READERS));
        final Operations operations = new Operations(
                Duration.ofMillis(value(parsed, Setting.BACKUP_STOP_TIMEOUT_MILLIS)),
                Duration.ofMillis(value(parsed, Setting.BACKUP_STOP_POLL_INTERVAL_MILLIS)),
                value(parsed, Setting.BACKUP_RETENTION_RETRY_ATTEMPTS),
                Duration.ofMillis(value(parsed, Setting.BACKUP_RETENTION_RETRY_DELAY_MILLIS)),
                value(parsed, Setting.BACKUP_PUBLICATION_RETRY_ATTEMPTS),
                value(parsed, Setting.MAINTENANCE_FAILURE_THRESHOLD),
                Duration.ofMillis(value(parsed, Setting.MAINTENANCE_CLOSE_TIMEOUT_MILLIS)),
                Duration.ofMillis(value(parsed, Setting.STORAGE_CHECK_CLOSE_TIMEOUT_MILLIS)),
                Duration.ofMillis(value(parsed, Setting.AERON_RETENTION_OPERATION_TIMEOUT_MILLIS)),
                value(parsed, Setting.WRITER_RECOVERY_ATTEMPTS));
        final RetryPacing retryPacing = new RetryPacing(
                Duration.ofNanos(value(parsed, Setting.AERON_RETRY_IDLE_MAX_PARK_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_RETRY_JITTER_CAP_NANOS)),
                Duration.ofNanos(value(parsed, Setting.AERON_RETRY_ARCHIVE_PROBE_DELAY_NANOS)));
        final AeronConfig aeron = new AeronConfig(
                clusterId,
                value(parsed, Setting.AERON_NODE_ID),
                value(parsed, Setting.AERON_STORE_GENERATION),
                epoch,
                streamId,
                value(parsed, Setting.AERON_RECORDING_ID),
                watermarkStreamId,
                Duration.ofMinutes(retentionIntervalMinutes),
                channels,
                new Directories(aeronDirectory, archiveDirectory),
                archivePolicy,
                threading,
                retryPacing);
        return new NodeConfig(role, storage, backup, timeouts, limits, aeron, production, transport, operations);
    }

    /// Creates a builder with standalone, local-development defaults.
    public static Builder builder() {
        return new Builder(fromMap(Map.of()));
    }

    /// Returns the config keys and defaults as a Markdown table for the README.
    public static String settingsMarkdown() {
        final StringBuilder result = new StringBuilder("| Setting | Default |\n| --- | --- |\n");
        for (final Setting setting : Setting.values()) {
            result.append("| `").append(setting.key()).append("` | `")
                    .append(setting.defaultValue() == null ? "unset" : setting.defaultValue())
                    .append("` |\n");
        }
        return result.toString();
    }

    /// Fluent builder for programmatic configuration.
    ///
    /// Starts from the standalone, local-development defaults; every setter replaces one section,
    /// and the configuration is validated once, by [#build()].
    public static final class Builder {
        private NodeRole role;
        private StorageConfig storage;
        private BackupConfig backup;
        private Timeouts timeouts;
        private Limits limits;
        private AeronConfig aeron;
        private boolean productionMode;
        private ReplicationTransport replicationTransport;
        private Operations operations;

        private Builder(final NodeConfig defaults) {
            this.role = defaults.role;
            this.storage = defaults.storage;
            this.backup = defaults.backup;
            this.timeouts = defaults.timeouts;
            this.limits = defaults.limits;
            this.aeron = defaults.aeron;
            this.productionMode = defaults.productionMode;
            this.replicationTransport = defaults.replicationTransport;
            this.operations = defaults.operations;
        }

        /// Selects a topology role and its matching transport.
        /// @param role node role
        /// @return this builder
        public Builder role(final NodeRole role) {
            this.role = role;
            this.replicationTransport = role == NodeRole.STANDALONE
                    ? ReplicationTransport.NONE : ReplicationTransport.AERON;
            return this;
        }

        /// Enables or disables production validation.
        /// @param enabled whether production checks apply
        /// @return this builder
        public Builder productionMode(final boolean enabled) {
            this.productionMode = enabled;
            return this;
        }

        /// Replaces the Store path and storage checks.
        /// @param storage storage configuration
        /// @return this builder
        public Builder storage(final StorageConfig storage) {
            this.storage = storage;
            return this;
        }

        /// Replaces backup volume and retention settings.
        /// @param backup backup configuration
        /// @return this builder
        public Builder backup(final BackupConfig backup) {
            this.backup = backup;
            return this;
        }

        /// Replaces lifecycle and transport deadlines.
        /// @param timeouts timeout configuration
        /// @return this builder
        public Builder timeouts(final Timeouts timeouts) {
            this.timeouts = timeouts;
            return this;
        }

        /// Replaces memory, queue, scan, and frame limits.
        /// @param limits node limits
        /// @return this builder
        public Builder limits(final Limits limits) {
            this.limits = limits;
            return this;
        }

        /// Replaces the Aeron topology and Archive policy.
        /// @param aeron Aeron configuration
        /// @return this builder
        public Builder aeron(final AeronConfig aeron) {
            this.aeron = aeron;
            return this;
        }

        /// Replaces maintenance retry limits and shutdown waits.
        /// @param operations bounded operation policy
        /// @return this builder
        public Builder operations(final Operations operations) {
            this.operations = operations;
            return this;
        }

        /// Validates and returns the configuration assembled so far.
        /// @return node configuration
        public NodeConfig build() {
            return new NodeConfig(this.role, this.storage, this.backup, this.timeouts, this.limits, this.aeron,
                    this.productionMode, this.replicationTransport, this.operations);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T value(final EnumMap<Setting, Object> values, final Setting setting) {
        return (T) values.get(setting);
    }

    private static void positive(final Duration duration, final String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isZero() || duration.isNegative()) throw new IllegalArgumentException(name + " must be positive");
    }

    private static void positiveOrNull(final Duration duration, final String name) {
        if (duration != null && (duration.isZero() || duration.isNegative())) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void nonNegativeOrNull(final Duration duration, final String name) {
        if (duration != null && duration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }

    private static Duration durationMinutes(final Integer minutes) {
        return minutes == null ? null : Duration.ofMinutes(minutes);
    }

    private static <T> T defaulted(final T value, final T fallback) {
        return value == null ? fallback : value;
    }

    private static boolean parseBoolean(final String value) {
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        throw new IllegalArgumentException("expected true or false");
    }

    private static ReplicationTransport parseTransport(final String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "none" -> ReplicationTransport.NONE;
            case "aeron" -> ReplicationTransport.AERON;
            default -> throw new IllegalArgumentException("must be 'aeron' or 'none'");
        };
    }

    private static String parseRole(final String value) {
        NodeRole.of(value);
        return value;
    }

    private static Integer parsePositiveInt(final String value) {
        final int parsed = Integer.parseInt(value);
        if (parsed <= 0) throw new IllegalArgumentException("must be positive");
        return parsed;
    }

    private static Integer parseNonNegativeInt(final String value) {
        final int parsed = Integer.parseInt(value);
        if (parsed < 0) throw new IllegalArgumentException("must not be negative");
        return parsed;
    }

    private static Long parsePositiveLong(final String value) {
        final long parsed = Long.parseLong(value);
        if (parsed <= 0L) throw new IllegalArgumentException("must be positive");
        return parsed;
    }

    private static Long parseNonNegativeLong(final String value) {
        final long parsed = Long.parseLong(value);
        if (parsed < 0L) throw new IllegalArgumentException("must not be negative");
        return parsed;
    }

    private static Long parseNonNegativeMillis(final String value) {
        return parseNonNegativeLong(value);
    }

    private static Long parsePositiveMillis(final String value) {
        return parsePositiveLong(value);
    }

    private static Long parsePositiveNanos(final String value) {
        return parsePositiveLong(value);
    }

    private static Integer parsePositiveMinutes(final String value) {
        return parsePositiveInt(value);
    }

    private static Long parseRecordingId(final String value) {
        final long parsed = Long.parseLong(value);
        if (parsed < -1L) throw new IllegalArgumentException("must be -1 or non-negative");
        return parsed;
    }

    private static Integer parseStorageLimitGb(final String value) {
        String number = value.trim();
        final String upper = number.toUpperCase(Locale.ROOT);
        if (upper.endsWith("GB")) number = number.substring(0, number.length() - 2).trim();
        else if (upper.endsWith("G")) number = number.substring(0, number.length() - 1).trim();
        final int gibibytes = Integer.parseInt(number);
        if (gibibytes <= 0L) throw new IllegalArgumentException("must be positive");
        return gibibytes;
    }

    private static Set<UUID> parseReaderIds(final String value) {
        if (value.isBlank()) return Set.of();
        final HashSet<UUID> ids = new HashSet<>();
        for (final String item : value.split(",", -1)) {
            final UUID id = UUID.fromString(item.trim());
            if (!ids.add(id)) throw new IllegalArgumentException("contains a duplicate reader id");
        }
        return Set.copyOf(ids);
    }

    private static ThreadingMode parseThreadingMode(final String value) {
        return ThreadingMode.valueOf(value.toUpperCase(Locale.ROOT));
    }

}
