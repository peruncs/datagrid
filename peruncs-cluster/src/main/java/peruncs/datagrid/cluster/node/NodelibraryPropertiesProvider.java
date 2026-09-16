package peruncs.datagrid.cluster.node;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/// Configuration contract shared by cluster lifecycle code and the Aeron provider.
/// [#replicationTransport()] selects an explicit `aeron` or `none` selection.
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
    /// @return node role
    default String replicationRole() {
        return isBackupNode() ? BACKUP_READER_ROLE : WRITER_ROLE;
    }

        /// Single writer role: owns the Store and the Aeron Archive recording.
    String WRITER_ROLE = "writer";

        /// Plain reader role: replays the recording without recording.
    String READER_ROLE = "reader";

        /// Backup reader role: replays like a reader and additionally serves backups.
    String BACKUP_READER_ROLE = "backup-reader";

        /// Returns whether the role was explicitly configured rather than inherited from the default.
    ///
    /// @return `true` when explicitly configured
    default boolean replicationRoleConfigured() {
        return false;
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
    /// @return storage limit
    Integer storageLimitGB();

        /// Returns the pod name.
    ///
    /// @return pod name
    String myPodName();

        /// Returns a stable node identity for transports that need to retain a
    /// consumer-group identity across process restarts.
    ///
    /// @return configured node identity, or `null` when none was supplied
    default String replicationNodeIdentity() {
        return this.replicationProperty("ECLIPSE_DATAGRID_NODE_ID");
    }

        /// Returns the pod namespace.
    ///
    /// @return namespace
    String myNamespace();

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

        /// Reads node properties from environment variables.
    class Env implements NodeLibraryPropertiesProvider {
                /// Creates an environment-backed provider.
        public Env() {
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
            final String role = this.envString(EnvKeys.REPLICATION_ROLE);
            return role == null || role.isBlank() ? NodeLibraryPropertiesProvider.super.replicationRole() : role;
        }

        @Override
        public boolean replicationRoleConfigured() {
            final String role = this.envString(EnvKeys.REPLICATION_ROLE);
            return role != null && !role.isBlank();
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
            final String number = value.endsWith("G") || value.endsWith("g")
                    ? value.substring(0, value.length() - 1).trim()
                    : value;
            try {
                return Integer.valueOf(number);
            } catch (final NumberFormatException failure) {
                throw new IllegalArgumentException(
                        "Invalid %s value: %s".formatted(EnvKeys.STORAGE_LIMIT_GB, configured), failure);
            }
        }

        @Override
        public String myPodName() {
            return this.envString(EnvKeys.MY_POD_NAME);
        }

        @Override
        public String replicationNodeIdentity() {
            return this.envString("ECLIPSE_DATAGRID_NODE_ID");
        }

        @Override
        public String myNamespace() {
            return this.envString(EnvKeys.MY_NAMESPACE);
        }

        @Override
        public boolean isProdMode() {
            return this.envBoolean(EnvKeys.IS_PROD_MODE);
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

        private Integer envInteger(final String envKey) {
            final String env = this.envString(envKey);
            if (env == null || env.isBlank()) return null;
            try {
                return Integer.valueOf(env.trim());
            } catch (final NumberFormatException failure) {
                throw new IllegalArgumentException("Invalid %s value: %s".formatted(envKey, env), failure);
            }
        }

        private Long envLong(final String envKey) {
            final String env = this.envString(envKey);
            if (env == null || env.isBlank()) return null;
            try {
                return Long.valueOf(env.trim());
            } catch (final NumberFormatException failure) {
                throw new IllegalArgumentException("Invalid %s value: %s".formatted(envKey, env), failure);
            }
        }

        private boolean envBoolean(final String envKey) {
            return Boolean.parseBoolean(this.envString(envKey));
        }

        private String envString(final String envKey) {
            return resolve(System.getenv(), envKey);
        }

        /// Resolves one variable against an explicit environment, falling back
        /// to its pre-prefix name when the prefixed name is unset.
        ///
        /// A consumed legacy name is reported once per process so an operator
        /// can migrate without the fallback staying silent forever.
        ///
        /// @param environment variable source, normally the process environment
        /// @param envKey      prefixed variable name
        /// @return the prefixed value, else the legacy value, else `null`
        static String resolve(final Map<String, String> environment, final String envKey) {
            final String value = environment.get(envKey);
            if (value != null) return value;
            final String legacy = LEGACY_ENV_KEYS.get(envKey);
            if (legacy == null) return null;
            final String legacyValue = environment.get(legacy);
            if (legacyValue != null && LEGACY_WARNED.add(legacy)) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Environment variable %s is deprecated; use %s instead".formatted(legacy, envKey));
            }
            return legacyValue;
        }

        private static final System.Logger LOGGER = System.getLogger(NodeLibraryPropertiesProvider.class.getName());

        /// Legacy names already reported, so each is warned about once.
        private static final Set<String> LEGACY_WARNED = ConcurrentHashMap.newKeySet();

        /// Pre-prefix variable names, kept working while operators migrate.
        private static final Map<String, String> LEGACY_ENV_KEYS = Map.ofEntries(
                Map.entry(EnvKeys.IS_BACKUP_NODE, "IS_BACKUP_NODE"),
                Map.entry(EnvKeys.KEPT_BACKUPS_COUNT, "KEPT_BACKUPS_COUNT"),
                Map.entry(
                        EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES, "STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES"),
                Map.entry(EnvKeys.GC_INTERVAL_MINUTES, "GC_INTERVAL_MINUTES"),
                Map.entry(EnvKeys.BACKUP_INTERVAL_MINUTES, "BACKUP_INTERVAL_MINUTES"),
                Map.entry(EnvKeys.STORAGE_LIMIT_GB, "STORAGE_LIMIT_GB"),
                Map.entry(EnvKeys.MY_POD_NAME, "MY_POD_NAME"),
                Map.entry(EnvKeys.MY_NAMESPACE, "MY_NAMESPACE"),
                Map.entry(EnvKeys.IS_PROD_MODE, "MSCNL_PROD_MODE"),
                Map.entry(EnvKeys.DATA_MERGER_TIMEOUT_MS, "MSCNL_DATA_MERGER_TIMEOUT"),
                Map.entry(EnvKeys.DATA_MERGER_LIMIT, "MSCNL_DATA_MERGER_LIMIT")
        );

                /// Names of the environment variables understood by the provider.
        ///
        /// Every key carries the `ECLIPSE_DATAGRID_` prefix. The earlier bare
        /// and `MSCNL_` names remain accepted as a fallback when the prefixed
        /// name is unset, so existing deployments keep working; when both are
        /// set, the prefixed name wins.
        public static final class EnvKeys {
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
                        /// Pod name environment variable.
            public static final String MY_POD_NAME = "ECLIPSE_DATAGRID_MY_POD_NAME";
                        /// Pod namespace environment variable.
            public static final String MY_NAMESPACE = "ECLIPSE_DATAGRID_MY_NAMESPACE";
                        /// Production-mode environment variable.
            public static final String IS_PROD_MODE = "ECLIPSE_DATAGRID_PROD_MODE";
                        /// Merger timeout environment variable.
            public static final String DATA_MERGER_TIMEOUT_MS = "ECLIPSE_DATAGRID_DATA_MERGER_TIMEOUT";
                        /// Merger cache limit environment variable.
            public static final String DATA_MERGER_LIMIT = "ECLIPSE_DATAGRID_DATA_MERGER_LIMIT";
                        /// Merger materialization timeout environment variable.
            public static final String DATA_MERGER_APPLY_TIMEOUT = "ECLIPSE_DATAGRID_DATA_MERGER_APPLY_TIMEOUT";

            private EnvKeys() {
            }
        }
    }
}
