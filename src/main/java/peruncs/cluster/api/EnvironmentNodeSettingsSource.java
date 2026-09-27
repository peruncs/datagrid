package peruncs.cluster.api;

import peruncs.cluster.errors.NodeException;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/// Reads node configuration from the process environment or a supplied map.
final class EnvironmentNodeSettingsSource implements NodeSettingsSource {
    private static final System.Logger LOGGER = System.getLogger(NodeSettingsSource.class.getName());
    private static final Map<String, String> LEGACY_ENV_KEYS = Map.ofEntries(
            Map.entry(NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "IS_BACKUP_NODE"),
            Map.entry(NodeSettingsSource.EnvKeys.KEPT_BACKUPS_COUNT, "KEPT_BACKUPS_COUNT"),
            Map.entry(NodeSettingsSource.EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES, "STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES"),
            Map.entry(NodeSettingsSource.EnvKeys.GC_INTERVAL_MINUTES, "GC_INTERVAL_MINUTES"),
            Map.entry(NodeSettingsSource.EnvKeys.BACKUP_INTERVAL_MINUTES, "BACKUP_INTERVAL_MINUTES"),
            Map.entry(NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, "STORAGE_LIMIT_GB"),
            Map.entry(NodeSettingsSource.EnvKeys.IS_PROD_MODE, "MSCNL_PROD_MODE"),
            Map.entry(NodeSettingsSource.EnvKeys.DATA_MERGER_TIMEOUT_MS, "MSCNL_DATA_MERGER_TIMEOUT"),
            Map.entry(NodeSettingsSource.EnvKeys.DATA_MERGER_LIMIT, "MSCNL_DATA_MERGER_LIMIT")
    );

    private final Map<String, String> environment;

    /// Legacy names already reported for this provider instance, so each is
    /// warned about once without leaking state across provider instances.
    private final Set<String> legacyWarned = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean prodModeWarningLogged = new AtomicBoolean();

    /// Creates an environment-backed provider reading the process environment.
    EnvironmentNodeSettingsSource() {
        this.environment = null;
    }

    /// Creates a provider reading a fixed environment.
    EnvironmentNodeSettingsSource(final Map<String, String> environment) {
        this.environment = Map.copyOf(environment);
    }

    @Override
    public String replicationStreamName() {
        return this.envString(NodeSettingsSource.EnvKeys.REPLICATION_STREAM_NAME);
    }

    @Override
    public String replicationTransport() {
        final String transport = this.envString(NodeSettingsSource.EnvKeys.REPLICATION_TRANSPORT);
        return transport == null ? "none" : transport;
    }

    @Override
    public long graphDrainTimeoutMillis() {
        final Long configured = this.envLong(NodeSettingsSource.EnvKeys.GRAPH_DRAIN_TIMEOUT_MILLIS);
        if (configured == null) return 5_000L;
        if (configured <= 0L) {
            throw new NodeException("%s must be positive".formatted(NodeSettingsSource.EnvKeys.GRAPH_DRAIN_TIMEOUT_MILLIS));
        }
        return configured;
    }

    @Override
    public long backupCloseTimeoutMillis() {
        final Long configured = this.envLong(NodeSettingsSource.EnvKeys.BACKUP_CLOSE_TIMEOUT_MILLIS);
        if (configured == null) return 60_000L;
        if (configured <= 0L) {
            throw new NodeException("%s must be positive".formatted(NodeSettingsSource.EnvKeys.BACKUP_CLOSE_TIMEOUT_MILLIS));
        }
        return configured;
    }

    @Override
    public boolean isBackupNode() {
        return this.envBoolean(NodeSettingsSource.EnvKeys.IS_BACKUP_NODE);
    }

    @Override
    public String replicationProperty(final String name) {
        return this.envString(name);
    }

    @Override
    public String replicationRole() {
        return this.envString(NodeSettingsSource.EnvKeys.REPLICATION_ROLE);
    }

    @Override
    public Integer keptBackupsCount() {
        return this.envInteger(NodeSettingsSource.EnvKeys.KEPT_BACKUPS_COUNT);
    }

    @Override
    public Integer storageLimitCheckerIntervalMinutes() {
        return this.envInteger(NodeSettingsSource.EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES);
    }

    @Override
    public Integer gcIntervalMinutes() {
        return this.envInteger(NodeSettingsSource.EnvKeys.GC_INTERVAL_MINUTES);
    }

    @Override
    public Integer backupIntervalMinutes() {
        return this.envInteger(NodeSettingsSource.EnvKeys.BACKUP_INTERVAL_MINUTES);
    }

    @Override
    public Integer storageLimitGB() {
        final String configured = this.envString(NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB);
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
        return this.envNumber(NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, configured, number, Integer::valueOf);
    }

    @Override
    public boolean isProdMode() {
        final String configured = this.envString(NodeSettingsSource.EnvKeys.IS_PROD_MODE);
        if (configured == null || configured.isBlank()) {
            if (this.hasProductionOnlySetting()) {
                throw new NodeException(
                        "%s must be explicitly set when production-only settings are configured"
                                .formatted(NodeSettingsSource.EnvKeys.IS_PROD_MODE));
            }
            if (this.prodModeWarningLogged.compareAndSet(false, true)) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "%s is unset; using development mode".formatted(NodeSettingsSource.EnvKeys.IS_PROD_MODE));
            }
            return false;
        }
        if ("true".equalsIgnoreCase(configured.trim())) return true;
        if ("false".equalsIgnoreCase(configured.trim())) return false;
        throw new NodeException("Invalid %s value: %s".formatted(NodeSettingsSource.EnvKeys.IS_PROD_MODE, configured));
    }

    private boolean hasProductionOnlySetting() {
        return this.envString(NodeSettingsSource.EnvKeys.AERON_CHECKPOINT_PATH) != null
                || this.envString(NodeSettingsSource.EnvKeys.AERON_ARCHIVE_DIRECTORY) != null
                || this.envString(NodeSettingsSource.EnvKeys.AERON_LEASE_PATH) != null;
    }

    @Override
    public Long dataMergerTimeoutMs() {
        return this.envLong(NodeSettingsSource.EnvKeys.DATA_MERGER_TIMEOUT_MS);
    }

    @Override
    public Long dataMergerCachedDataLimit() {
        return this.envLong(NodeSettingsSource.EnvKeys.DATA_MERGER_LIMIT);
    }

    @Override
    public Long dataMergerApplyTimeoutMs() {
        return this.envLong(NodeSettingsSource.EnvKeys.DATA_MERGER_APPLY_TIMEOUT);
    }

    @Override
    public Long writerLeaseStalenessMillis() {
        return this.envLong(NodeSettingsSource.EnvKeys.WRITER_LEASE_STALENESS_MILLIS);
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
            throw new NodeException(
                    "Invalid %s value: %s".formatted(envKey, rawValue), failure);
        }
    }

    private boolean envBoolean(final String envKey) {
        final String env = this.envString(envKey);
        if (env == null || env.isBlank()) return false;
        if ("true".equalsIgnoreCase(env.trim())) return true;
        if ("false".equalsIgnoreCase(env.trim())) return false;
        throw new NodeException("Invalid %s value: %s".formatted(envKey, env));
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
    private String resolve(final String envKey) {
        final Map<String, String> source = this.environment == null ? System.getenv() : this.environment;
        final String value = source.get(envKey);
        if (value != null) return value;
        final String legacy = LEGACY_ENV_KEYS.get(envKey);
        if (legacy == null) return null;
        final String legacyValue = source.get(legacy);
        if (legacyValue != null && this.legacyWarned.add(legacy)) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Environment variable %s is deprecated; use %s instead".formatted(legacy, envKey));
        }
        return legacyValue;
    }
}
