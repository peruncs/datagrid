package peruncs.cluster.api;

import java.util.Map;
import java.util.Objects;

/// Configuration contract shared by cluster lifecycle code and the Aeron provider.
///
/// [#replicationTransport()] selects an explicit `aeron` or `none` selection.
///
/// The default provider reads the process environment; embedding applications
/// may implement this interface directly to supply configuration.
///
/// @since 1.0
public interface NodeSettingsSource {
    /// Default limit for index-relevant objects and collection entries in one validation scan.
    int DEFAULT_INDEX_VALIDATION_MAX_OBJECTS = 65_536;
    /// Creates an environment-backed provider using the process environment.
    ///
    /// @return settings provider
    static NodeSettingsSource env() {
        return new EnvironmentNodeSettingsSource();
    }

    /// Creates an environment-backed provider using a supplied variable map.
    ///
    /// @param environment environment values used instead of the process environment
    /// @return settings provider
    /// @throws NullPointerException if `environment` is `null`
    static NodeSettingsSource env(final Map<String, String> environment) {
        return new EnvironmentNodeSettingsSource(Objects.requireNonNull(environment, "environment"));
    }

    /// Returns the logical replication stream name.
    ///
    /// @return stream name, or `null` when unset
    default String replicationStreamName() {
        return null;
    }

    /// Returns the selected replication transport.
    ///
    /// @return transport name
    default String replicationTransport() {
        return "none";
    }

    /// Optional provider-specific setting, allowing embedded applications to avoid
    /// environment variables. `ECLIPSE_DATAGRID_STORAGE_PATH` overrides the
    /// default `/storage` root used for the Store and durable replication
    /// cursor file.
    ///
    /// @param name provider-specific property name
    /// @return property value, or `null`
    default String replicationProperty(final String name) {
        return null;
    }

    /// Maximum number of index-relevant objects and collection entries per validation scan.
    ///
    /// @return configured positive bound, or 65,536
    default int indexValidationMaxObjects() {
        return this.indexValidationMaxObjects(DEFAULT_INDEX_VALIDATION_MAX_OBJECTS);
    }

    /// Returns the configured validation bound or the supplied default.
    ///
    /// A blank value uses the supplied default; malformed or non-positive values are rejected.
    ///
    /// @param defaultValue bound to use when the setting is absent
    /// @return configured or default positive bound
    default int indexValidationMaxObjects(final int defaultValue) {
        if (defaultValue <= 0) throw new IllegalArgumentException("defaultValue must be positive");
        final String key = EnvKeys.INDEX_VALIDATION_MAX_OBJECTS;
        final String configured = this.replicationProperty(key);
        if (configured == null || configured.isBlank()) return defaultValue;
        try {
            final int maximum = Integer.parseInt(configured.trim());
            if (maximum > 0) return maximum;
        } catch (final NumberFormatException ignored) {
            throw new IllegalArgumentException(key + " must be a positive integer", ignored);
        }
        throw new IllegalArgumentException(key + " must be a positive integer");
    }

    /// Maximum time to wait for active Store graph sections during close.
    ///
    /// @return positive drain timeout in milliseconds
    default long graphDrainTimeoutMillis() {
        return 5_000L;
    }

    /// Maximum time node close waits for an active backup export to finish.
    default long backupCloseTimeoutMillis() {
        return 60_000L;
    }

    /// Fixed-topology node role: `writer`, `reader`, or `backup-reader`.
    ///
    /// A blank value inherits the legacy backup flag. The normalized role is
    /// resolved by internal node code; this contract only reports the raw
    /// setting so the role type never enters the application contract.
    ///
    /// @return configured role value, or `null` when unconfigured
    default String replicationRole() {
        return null;
    }

    /// Single writer role: owns the Store and the Aeron Archive recording.
    String WRITER_ROLE = "writer";

    /// Plain reader role: replays the recording without recording.
    String READER_ROLE = "reader";

    /// Backup reader role: replays like a reader and additionally serves backups.
    String BACKUP_READER_ROLE = "backup-reader";

    /// Reports whether this node restores backups.
    ///
    /// @return `true` for a backup node
    boolean isBackupNode();

    /// Returns the number of backups to retain.
    ///
    /// @return retained backup count, or `null` when unset
    Integer keptBackupsCount();

    /// Returns the storage check interval in minutes.
    ///
    /// @return interval in minutes, or `null` when unset
    Integer storageLimitCheckerIntervalMinutes();

    /// Returns the storage cleanup interval in minutes.
    ///
    /// @return interval in minutes, or `null` for the node default
    default Integer gcIntervalMinutes() {
        return null;
    }

    /// Returns the automatic backup interval in minutes.
    ///
    /// @return interval in minutes, or `null` for the node default
    default Integer backupIntervalMinutes() {
        return null;
    }

    /// Returns the Archive retention maintenance interval in minutes.
    ///
    /// @return configured interval, or `null` for the one-minute default
    default Integer aeronRetentionIntervalMinutes() {
        return null;
    }

    /// Returns the storage limit in gigabytes.
    ///
    /// Accepts a bare number (`20`) or a number with a `G`/`GB` suffix
    /// (`20G`, `20GB`), case-insensitively.
    ///
    /// @return storage limit in gigabytes, or `null` when unset
    Integer storageLimitGB();

    /// Returns a stable node identity for transports that need to retain a
    /// consumer-group identity across process restarts.
    ///
    /// @return configured node identity, or `null` when none was supplied
    default String replicationNodeIdentity() {
        return this.replicationProperty(EnvKeys.NODE_ID);
    }

    /// Reports whether production mode is enabled.
    ///
    /// @return `true` in production mode
    boolean isProdMode();

    /// Returns the merger timeout in milliseconds.
    ///
    /// @return timeout in milliseconds, or `null` when unset
    Long dataMergerTimeoutMs();

    /// Returns the merger cache limit.
    ///
    /// @return cache limit in bytes, or `null` when unset
    Long dataMergerCachedDataLimit();

    /// Returns the maximum wait for one materialization batch, in milliseconds.
    ///
    /// @return apply timeout, or `null` for the built-in default
    Long dataMergerApplyTimeoutMs();

    /// Writer fencing lease heartbeat staleness bound in millis, `null` for the default.
    ///
    /// @return staleness bound in millis, or `null` when unset
    Long writerLeaseStalenessMillis();

    /// Environment variable names recognized by the environment-backed settings source.
    ///
    /// Every key carries the `ECLIPSE_DATAGRID_` prefix. Earlier bare and `MSCNL_` names remain
    /// accepted as fallbacks when a prefixed name is unset; when both are set, the prefixed name wins.
    final class EnvKeys {
        /// Stable node identity environment variable.
        public static final String NODE_ID = "ECLIPSE_DATAGRID_NODE_ID";
        /// Replication stream environment variable.
        public static final String REPLICATION_STREAM_NAME = "ECLIPSE_DATAGRID_REPLICATION_STREAM";
        /// Replication transport environment variable.
        public static final String REPLICATION_TRANSPORT = "ECLIPSE_DATAGRID_REPLICATION_TRANSPORT";
        /// Replication role environment variable.
        public static final String REPLICATION_ROLE = "ECLIPSE_DATAGRID_REPLICATION_ROLE";
        /// Store path environment variable.
        public static final String STORAGE_PATH = "ECLIPSE_DATAGRID_STORAGE_PATH";
        /// Filesystem backup volume environment variable.
        public static final String BACKUP_PATH = "ECLIPSE_DATAGRID_BACKUP_PATH";
        /// Backup-node environment variable.
        public static final String IS_BACKUP_NODE = "ECLIPSE_DATAGRID_IS_BACKUP_NODE";
        /// Retained-backups environment variable.
        public static final String KEPT_BACKUPS_COUNT = "ECLIPSE_DATAGRID_KEPT_BACKUPS_COUNT";
        /// Storage-check interval environment variable.
        public static final String STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES =
                "ECLIPSE_DATAGRID_STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES";
        /// Storage cleanup interval environment variable.
        public static final String GC_INTERVAL_MINUTES = "ECLIPSE_DATAGRID_GC_INTERVAL_MINUTES";
        /// Automatic backup interval environment variable.
        public static final String BACKUP_INTERVAL_MINUTES = "ECLIPSE_DATAGRID_BACKUP_INTERVAL_MINUTES";
        /// Storage limit environment variable.
        public static final String STORAGE_LIMIT_GB = "ECLIPSE_DATAGRID_STORAGE_LIMIT_GB";
        /// Production-mode environment variable.
        public static final String IS_PROD_MODE = "ECLIPSE_DATAGRID_PROD_MODE";
        /// Merger timeout environment variable.
        public static final String DATA_MERGER_TIMEOUT_MS = "ECLIPSE_DATAGRID_DATA_MERGER_TIMEOUT";
        /// Merger cache limit environment variable.
        public static final String DATA_MERGER_LIMIT = "ECLIPSE_DATAGRID_DATA_MERGER_LIMIT";
        /// Merger materialization timeout environment variable.
        public static final String DATA_MERGER_APPLY_TIMEOUT = "ECLIPSE_DATAGRID_DATA_MERGER_APPLY_TIMEOUT";
        /// Graph drain timeout used during node close.
        public static final String GRAPH_DRAIN_TIMEOUT_MILLIS = "ECLIPSE_DATAGRID_GRAPH_DRAIN_TIMEOUT_MILLIS";
        /// Maximum close wait for an active backup export.
        public static final String BACKUP_CLOSE_TIMEOUT_MILLIS = "ECLIPSE_DATAGRID_BACKUP_CLOSE_TIMEOUT_MILLIS";
        /// Maximum index-relevant objects and collection entries visited in one validation scan.
        public static final String INDEX_VALIDATION_MAX_OBJECTS = "ECLIPSE_DATAGRID_INDEX_VALIDATION_MAX_OBJECTS";
        /// Writer lease staleness environment variable.
        public static final String WRITER_LEASE_STALENESS_MILLIS = "ECLIPSE_DATAGRID_AERON_LEASE_STALENESS_MILLIS";
        /// Durable checkpoint path.
        public static final String AERON_CHECKPOINT_PATH = "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH";
        /// Archive directory.
        public static final String AERON_ARCHIVE_DIRECTORY = "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY";
        /// Shared writer lease path.
        public static final String AERON_LEASE_PATH = "ECLIPSE_DATAGRID_AERON_LEASE_PATH";
        /// Archive retention maintenance interval in minutes.
        public static final String AERON_RETENTION_INTERVAL_MINUTES = "ECLIPSE_DATAGRID_AERON_RETENTION_INTERVAL_MINUTES";

        private EnvKeys() {
        }
    }
}
