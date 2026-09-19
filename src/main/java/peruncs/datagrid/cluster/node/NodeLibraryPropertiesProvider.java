package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/// Configuration contract shared by cluster lifecycle code and the Aeron provider.
///
/// [#replicationTransport()] selects an explicit `aeron` or `none` selection.
///
/// @since 1.0
/// The nested [Env] implementation reads the process environment; embedding
/// applications may implement this interface directly to supply configuration
/// without environment variables.
public interface NodeLibraryPropertiesProvider {
        /// Creates an environment-backed provider.
    ///
    /// @return properties provider
    static NodeLibraryPropertiesProvider Env() {
        return new Env();
    }

        /// Returns the logical replication stream name.
    ///
    /// @return stream name
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

        /// Fixed-topology node role: `writer`, `reader`, or `backup-reader`.
    ///
    /// A blank value inherits the legacy backup flag via [NodeRole#resolve].
    ///
    /// @return node role, or `null` when unconfigured
    default String replicationRole() {
        return null;
    }

        /// Single writer role: owns the Store and the Aeron Archive recording.
    String WRITER_ROLE = "writer";

        /// Plain reader role: replays the recording without recording.
    String READER_ROLE = "reader";

        /// Backup reader role: replays like a reader and additionally serves backups.
    String BACKUP_READER_ROLE = "backup-reader";

        /// Returns the single normalized role every decision point uses.
    ///
    /// This reconciles the legacy [NodeLibraryPropertiesProvider#isBackupNode]
    /// flag with the `ECLIPSE_DATAGRID_REPLICATION_ROLE` value and rejects a
    /// conflicting combination, so startup, guards, and transport setup can
    /// never disagree about the role.
    ///
    /// @return effective role
    /// @throws IllegalArgumentException for an unknown value or a legacy/new conflict
    default NodeRole nodeRole() {
        return NodeRole.of(this);
    }

        /// Reports whether this node restores backups.
    ///
    /// @return `true` for a backup node
    boolean isBackupNode();

        /// Returns the number of backups to retain.
    ///
    /// @return retained backup count
    Integer keptBackupsCount();

        /// Returns the storage check interval in minutes.
    ///
    /// @return interval in minutes
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

        /// Returns the storage limit in gigabytes.
    ///
    /// Accepts a bare number (`20`) or a number with a `G`/`GB` suffix
    /// (`20G`, `20GB`), case-insensitively.
    ///
    /// @return storage limit in gigabytes
    Integer storageLimitGB();

        /// Returns a stable node identity for transports that need to retain a
    /// consumer-group identity across process restarts.
    ///
    /// @return configured node identity, or `null` when none was supplied
    default String replicationNodeIdentity() {
        return this.replicationProperty(Env.EnvKeys.NODE_ID);
    }

        /// Reports whether production mode is enabled.
    ///
    /// @return `true` in production mode
    boolean isProdMode();

        /// Returns the merger timeout in milliseconds.
    ///
    /// @return timeout, or `null`
    Long dataMergerTimeoutMs();

        /// Returns the merger cache limit.
    ///
    /// @return cache limit, or `null`
    Long dataMergerCachedDataLimit();

        /// Returns the maximum wait for one materialization batch, in milliseconds.
    ///
    /// @return apply timeout, or `null` for the built-in default
    Long dataMergerApplyTimeoutMs();

        /// Writer fencing lease heartbeat staleness bound in millis, `null` for the default.
    ///
    /// @return staleness bound in millis, or `null` when unset
    Long writerLeaseStalenessMillis();

        /// Reads node properties from environment variables.
    class Env implements NodeLibraryPropertiesProvider {
        private static final System.Logger LOGGER = System.getLogger(NodeLibraryPropertiesProvider.class.getName());

        private final Map<String, String> environment;

        /// Legacy names already reported for this provider instance, so each is
        /// warned about once without leaking state across provider instances.
        private final Set<String> legacyWarned = ConcurrentHashMap.newKeySet();

        private final AtomicBoolean prodModeWarningLogged = new AtomicBoolean();

        /// Creates an environment-backed provider reading the process environment.
        public Env() {
            this(null);
        }

                /// Creates a provider reading a fixed environment, for tests.
        ///
        /// @param environment variable source, or `null` for the process environment
        public Env(final Map<String, String> environment) {
            this.environment = environment == null ? null : Map.copyOf(environment);
        }

        @Override
        public String replicationStreamName() {
            return this.envString(EnvKeys.REPLICATION_STREAM_NAME);
        }

        @Override
        public String replicationTransport() {
            final String transport = this.envString(EnvKeys.REPLICATION_TRANSPORT);
            return transport == null ? "none" : transport;
        }

        @Override
        public boolean isBackupNode() {
            return this.envBoolean(EnvKeys.IS_BACKUP_NODE);
        }

        @Override
        public String replicationProperty(final String name) {
            return this.envString(name);
        }

        @Override
        public String replicationRole() {
            return this.envString(EnvKeys.REPLICATION_ROLE);
        }

        @Override
        public Integer keptBackupsCount() {
            return this.envInteger(EnvKeys.KEPT_BACKUPS_COUNT);
        }

        @Override
        public Integer storageLimitCheckerIntervalMinutes() {
            return this.envInteger(EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES);
        }

        @Override
        public Integer gcIntervalMinutes() {
            return this.envInteger(EnvKeys.GC_INTERVAL_MINUTES);
        }

        @Override
        public Integer backupIntervalMinutes() {
            return this.envInteger(EnvKeys.BACKUP_INTERVAL_MINUTES);
        }

        @Override
        public Integer storageLimitGB() {
            final String configured = this.envString(EnvKeys.STORAGE_LIMIT_GB);
            if (configured == null || configured.isBlank()) {
                return null;
            }
            final String value = configured.trim();
            final String upper = value.toUpperCase(Locale.ROOT);
            final String number;
            if (upper.endsWith("GB")) {
                number = value.substring(0, value.length() - 2).trim();
            } else if (upper.endsWith("G")) {
                number = value.substring(0, value.length() - 1).trim();
            } else {
                number = value;
            }
            return this.envNumber(EnvKeys.STORAGE_LIMIT_GB, configured, number, Integer::valueOf);
        }

        @Override
        public boolean isProdMode() {
            final String configured = this.envString(EnvKeys.IS_PROD_MODE);
            if (configured == null || configured.isBlank()) {
                if (this.hasProductionOnlySetting()) {
                    throw new NodeLibraryException(
                            "%s must be explicitly set when production-only settings are configured"
                                    .formatted(EnvKeys.IS_PROD_MODE));
                }
                if (this.prodModeWarningLogged.compareAndSet(false, true)) {
                    LOGGER.log(System.Logger.Level.WARNING,
                            "%s is unset; using development mode".formatted(EnvKeys.IS_PROD_MODE));
                }
                return false;
            }
            if ("true".equalsIgnoreCase(configured.trim())) return true;
            if ("false".equalsIgnoreCase(configured.trim())) return false;
            throw new NodeLibraryException("Invalid %s value: %s".formatted(EnvKeys.IS_PROD_MODE, configured));
        }

        private boolean hasProductionOnlySetting() {
            return this.envString(EnvKeys.AERON_AUTH_ENABLED) != null
                    || this.envString(EnvKeys.AERON_CHECKPOINT_PATH) != null
                    || this.envString(EnvKeys.AERON_ARCHIVE_DIRECTORY) != null
                    || this.envString(EnvKeys.AERON_LEASE_PATH) != null;
        }

        @Override
        public Long dataMergerTimeoutMs() {
            return this.envLong(EnvKeys.DATA_MERGER_TIMEOUT_MS);
        }

        @Override
        public Long dataMergerCachedDataLimit() {
            return this.envLong(EnvKeys.DATA_MERGER_LIMIT);
        }

        @Override
        public Long dataMergerApplyTimeoutMs() {
            return this.envLong(EnvKeys.DATA_MERGER_APPLY_TIMEOUT);
        }

        @Override
        public Long writerLeaseStalenessMillis() {
            return this.envLong(EnvKeys.WRITER_LEASE_STALENESS_MILLIS);
        }

        private Integer envInteger(final String envKey) {
            final String env = this.envString(envKey);
            if (env == null || env.isBlank()) return null;
            return this.envNumber(envKey, env, env.trim(), Integer::valueOf);
        }

        private Long envLong(final String envKey) {
            final String env = this.envString(envKey);
            if (env == null || env.isBlank()) return null;
            return this.envNumber(envKey, env, env.trim(), Long::valueOf);
        }

        /// Parses one numeric environment value, reporting the raw text and key
        /// in a single error type shared with every other configuration failure.
        private <T> T envNumber(
                final String envKey,
                final String rawValue,
                final String number,
                final Function<String, T> parser
        ) {
            try {
                return parser.apply(number);
            } catch (final NumberFormatException failure) {
                throw new NodeLibraryException(
                        "Invalid %s value: %s".formatted(envKey, rawValue), failure);
            }
        }

        private boolean envBoolean(final String envKey) {
            final String env = this.envString(envKey);
            if (env == null || env.isBlank()) return false;
            if ("true".equalsIgnoreCase(env.trim())) return true;
            if ("false".equalsIgnoreCase(env.trim())) return false;
            throw new NodeLibraryException("Invalid %s value: %s".formatted(envKey, env));
        }

        private String envString(final String envKey) {
            return this.resolve(envKey);
        }

        /// Resolves one variable against this provider's environment, falling
        /// back to its pre-prefix name when the prefixed name is unset.
        ///
        /// A consumed legacy name is reported once per provider instance so an
        /// operator can migrate without the fallback staying silent forever.
        ///
        /// @param envKey prefixed variable name
        /// @return the prefixed value, else the legacy value, else `null`
        String resolve(final String envKey) {
            final Map<String, String> source = this.environment == null ? System.getenv() : this.environment;
            final String value = source.get(envKey);
            if (value != null) return value;
            final String legacy = EnvKeys.LEGACY_ENV_KEYS.get(envKey);
            if (legacy == null) return null;
            final String legacyValue = source.get(legacy);
            if (legacyValue != null && this.legacyWarned.add(legacy)) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Environment variable %s is deprecated; use %s instead".formatted(legacy, envKey));
            }
            return legacyValue;
        }

                /// Names of the environment variables understood by the provider.
        ///
        /// Every key carries the `ECLIPSE_DATAGRID_` prefix. The earlier bare
        /// and `MSCNL_` names remain accepted as a fallback when the prefixed
        /// name is unset, so existing deployments keep working; when both are
        /// set, the prefixed name wins.
        public static final class EnvKeys {
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
                        /// Writer lease staleness environment variable.
            public static final String WRITER_LEASE_STALENESS_MILLIS = "ECLIPSE_DATAGRID_AERON_LEASE_STALENESS_MILLIS";
                        /// Live-channel authentication setting.
            public static final String AERON_AUTH_ENABLED = "ECLIPSE_DATAGRID_AERON_AUTH_ENABLED";
                        /// Durable checkpoint path.
            public static final String AERON_CHECKPOINT_PATH = "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH";
                        /// Archive directory.
            public static final String AERON_ARCHIVE_DIRECTORY = "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY";
                        /// Shared writer lease path.
            public static final String AERON_LEASE_PATH = "ECLIPSE_DATAGRID_AERON_LEASE_PATH";

            /// Pre-prefix variable names, kept working while operators migrate.
            private static final Map<String, String> LEGACY_ENV_KEYS = Map.ofEntries(
                    Map.entry(IS_BACKUP_NODE, "IS_BACKUP_NODE"),
                    Map.entry(KEPT_BACKUPS_COUNT, "KEPT_BACKUPS_COUNT"),
                    Map.entry(STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES, "STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES"),
                    Map.entry(GC_INTERVAL_MINUTES, "GC_INTERVAL_MINUTES"),
                    Map.entry(BACKUP_INTERVAL_MINUTES, "BACKUP_INTERVAL_MINUTES"),
                    Map.entry(STORAGE_LIMIT_GB, "STORAGE_LIMIT_GB"),
                    Map.entry(IS_PROD_MODE, "MSCNL_PROD_MODE"),
                    Map.entry(DATA_MERGER_TIMEOUT_MS, "MSCNL_DATA_MERGER_TIMEOUT"),
                    Map.entry(DATA_MERGER_LIMIT, "MSCNL_DATA_MERGER_LIMIT")
            );

            private EnvKeys() {
            }
        }
    }
}
