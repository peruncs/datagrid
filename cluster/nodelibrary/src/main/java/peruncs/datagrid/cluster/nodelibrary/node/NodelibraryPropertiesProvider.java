package peruncs.datagrid.cluster.nodelibrary.node;

import peruncs.datagrid.cluster.nodelibrary.backup.BackupTarget;


/**
 * Configuration contract shared by cluster lifecycle code and the Aeron provider.
 * {@link #replicationTransport()} selects an explicit {@code aeron} or {@code none} selection.
 */
public interface NodelibraryPropertiesProvider
{
	/** Returns the logical replication stream name.
	 * @return stream name
	 */
	default String replicationStreamName()
	{
		return null;
	}

	/** Returns the selected replication transport.
	 * @return transport name
	 */
	default String replicationTransport()
	{
		return "none";
	}

	/**
	 * Optional provider-specific setting, allowing embedded applications to avoid
	 * environment variables. {@code ECLIPSE_DATAGRID_STORAGE_PATH} overrides the
	 * default {@code /storage} root used for the Store and durable replication
	 * cursor file.
	 *
	 * @param name provider-specific property name
	 * @return property value, or {@code null}
	 */
	default String replicationProperty(final String name)
	{
		return null;
	}

	/** Fixed-topology node role: {@code writer}, {@code reader}, or {@code backup-reader}.
	 * @return node role
	 */
	default String replicationRole()
	{
		return isBackupNode() ? "backup-reader" : "writer";
	}

	/** Returns whether the role was explicitly configured rather than inherited from the default.
	 * @return {@code true} when explicitly configured
	 */
	default boolean replicationRoleConfigured()
	{
		return false;
	}

	/** Reports whether this node restores backups.
	 * @return {@code true} for a backup node
	 */
	boolean isBackupNode();

	/** Returns the number of backups to retain.
	 * @return retained backup count
	 */
	Integer keptBackupsCount();

	/** Returns the backup target.
	 * @return backup target
	 */
	BackupTarget backupTarget();

	/** Returns the backup proxy URL.
	 * @return proxy URL
	 */
	String backupProxyServiceUrl();

	/** Returns the storage check interval in minutes.
	 * @return interval in minutes
	 */
	Integer storageLimitCheckerIntervalMinutes();

	/** Returns the storage cleanup interval in minutes.
	 * @return interval in minutes, or {@code null} for the node default
	 */
	default Integer gcIntervalMinutes()
	{
		return null;
	}

	/** Returns the automatic backup interval in minutes.
	 * @return interval in minutes, or {@code null} for the node default
	 */
	default Integer backupIntervalMinutes()
	{
		return null;
	}

	/** Returns the storage limit in gigabytes.
	 * @return storage limit
	 */
	Integer storageLimitGB();

	/** Returns the pod name.
	 * @return pod name
	 */
	String myPodName();

	/**
	 * Returns a stable node identity for transports that need to retain a
	 * consumer-group identity across process restarts.
	 *
	 * @return configured node identity, or {@code null} when none was supplied
	 */
	default String replicationNodeIdentity()
	{
		return this.replicationProperty("ECLIPSE_DATAGRID_NODE_ID");
	}

	/** Returns the pod namespace.
	 * @return namespace
	 */
	String myNamespace();

	/** Reports whether production mode is enabled.
	 * @return {@code true} in production mode
	 */
	boolean isProdMode();

	/** Returns the merger timeout in milliseconds.
	 * @return timeout, or {@code null}
	 */
	Long dataMergerTimeoutMs();

	/** Returns the merger cache limit.
	 * @return cache limit, or {@code null}
	 */
	Long dataMergerCachedDataLimit();

	/** Creates an environment-backed provider.
	 * @return properties provider
	 */
	static NodelibraryPropertiesProvider Env()
	{
		return new Env();
	}

	/** Reads node properties from environment variables. */
	class Env implements NodelibraryPropertiesProvider
	{
		/** Creates an environment-backed provider. */
		public Env()
		{
		}

		/** Names of the environment variables understood by the provider. */
		public static final class EnvKeys
		{
			/** Replication stream environment variable. */
			public static final String REPLICATION_STREAM_NAME = "ECLIPSE_DATAGRID_REPLICATION_STREAM";
			/** Replication transport environment variable. */
			public static final String REPLICATION_TRANSPORT = "ECLIPSE_DATAGRID_REPLICATION_TRANSPORT";
			/** Store path environment variable. */
			public static final String STORAGE_PATH = "ECLIPSE_DATAGRID_STORAGE_PATH";
			/** Filesystem backup volume environment variable. */
			public static final String BACKUP_PATH = "ECLIPSE_DATAGRID_BACKUP_PATH";
			/** Backup-node environment variable. */
			public static final String IS_BACKUP_NODE = "IS_BACKUP_NODE";
			/** Backup-target environment variable. */
			public static final String BACKUP_TARGET = "BACKUP_TARGET";
			/** Retained-backups environment variable. */
			public static final String KEPT_BACKUPS_COUNT = "KEPT_BACKUPS_COUNT";
			/** Backup proxy URL environment variable. */
			public static final String BACKUP_PROXY_SERVICE_URL = "BACKUP_PROXY_SERVICE_URL";
			/** Storage-check interval environment variable. */
			public static final String STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES =
				"STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES";
			/** Storage cleanup interval environment variable. */
			public static final String GC_INTERVAL_MINUTES = "GC_INTERVAL_MINUTES";
			/** Automatic backup interval environment variable. */
			public static final String BACKUP_INTERVAL_MINUTES = "BACKUP_INTERVAL_MINUTES";
			/** Storage limit environment variable. */
			public static final String STORAGE_LIMIT_GB = "STORAGE_LIMIT_GB";
			/** Pod name environment variable. */
			public static final String MY_POD_NAME = "MY_POD_NAME";
			/** Pod namespace environment variable. */
			public static final String MY_NAMESPACE = "MY_NAMESPACE";
			/** Production-mode environment variable. */
			public static final String IS_PROD_MODE = "MSCNL_PROD_MODE";
			/** Merger timeout environment variable. */
			public static final String DATA_MERGER_TIMEOUT_MS = "MSCNL_DATA_MERGER_TIMEOUT";
			/** Merger cache limit environment variable. */
			public static final String DATA_MERGER_LIMIT = "MSCNL_DATA_MERGER_LIMIT";

			private EnvKeys()
			{
			}
		}

		@Override
		public String replicationStreamName()
		{
			return this.envString(EnvKeys.REPLICATION_STREAM_NAME);
		}

		@Override
		public String replicationTransport()
		{
			final String transport = this.envString(EnvKeys.REPLICATION_TRANSPORT);
			return transport == null ? "none" : transport;
		}

		@Override
		public boolean isBackupNode()
		{
			return this.envBoolean(EnvKeys.IS_BACKUP_NODE);
		}

		@Override
		public String replicationProperty(final String name)
		{
			return this.envString(name);
		}

		@Override
		public String replicationRole()
		{
			final String role = this.envString("ECLIPSE_DATAGRID_REPLICATION_ROLE");
			return role == null || role.isBlank() ? NodelibraryPropertiesProvider.super.replicationRole() : role;
		}

		@Override
		public boolean replicationRoleConfigured()
		{
			final String role = this.envString("ECLIPSE_DATAGRID_REPLICATION_ROLE");
			return role != null && !role.isBlank();
		}

		@Override
		public BackupTarget backupTarget()
		{
			return BackupTarget.parse(this.envString(EnvKeys.BACKUP_TARGET));
		}

		@Override
		public Integer keptBackupsCount()
		{
			return this.envInteger(EnvKeys.KEPT_BACKUPS_COUNT);
		}

		@Override
		public String backupProxyServiceUrl()
		{
			return this.envString(EnvKeys.BACKUP_PROXY_SERVICE_URL);
		}

		@Override
		public Integer storageLimitCheckerIntervalMinutes()
		{
			return this.envInteger(EnvKeys.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES);
		}

		@Override
		public Integer gcIntervalMinutes()
		{
			return this.envInteger(EnvKeys.GC_INTERVAL_MINUTES);
		}

		@Override
		public Integer backupIntervalMinutes()
		{
			return this.envInteger(EnvKeys.BACKUP_INTERVAL_MINUTES);
		}

		@Override
		public Integer storageLimitGB()
		{
			final String configured = this.envString(EnvKeys.STORAGE_LIMIT_GB);
			if (configured == null || configured.isBlank())
			{
				return null;
			}
			final String value = configured.trim();
			final String number = value.endsWith("G") || value.endsWith("g")
				? value.substring(0, value.length() - 1).trim()
				: value;
			try
			{
				return Integer.valueOf(number);
			}
			catch (final NumberFormatException failure)
			{
				throw new IllegalArgumentException(
					"Invalid " + EnvKeys.STORAGE_LIMIT_GB + " value: " + configured, failure);
			}
		}

		@Override
		public String myPodName()
		{
			return this.envString(EnvKeys.MY_POD_NAME);
		}

		@Override
		public String replicationNodeIdentity()
		{
			return this.envString("ECLIPSE_DATAGRID_NODE_ID");
		}

		@Override
		public String myNamespace()
		{
			return this.envString(EnvKeys.MY_NAMESPACE);
		}

		@Override
		public boolean isProdMode()
		{
			return this.envBoolean(EnvKeys.IS_PROD_MODE);
		}

		@Override
		public Long dataMergerTimeoutMs()
		{
			return this.envLong(EnvKeys.DATA_MERGER_TIMEOUT_MS);
		}

		@Override
		public Long dataMergerCachedDataLimit()
		{
			return this.envLong(EnvKeys.DATA_MERGER_LIMIT);
		}

		private Integer envInteger(final String envKey)
		{
			final String env = this.envString(envKey);
			if (env == null || env.isBlank()) return null;
			try
			{
				return Integer.valueOf(env.trim());
			}
			catch (final NumberFormatException failure)
			{
				throw new IllegalArgumentException("Invalid " + envKey + " value: " + env, failure);
			}
		}

		private Long envLong(final String envKey)
		{
			final String env = this.envString(envKey);
			if (env == null || env.isBlank()) return null;
			try
			{
				return Long.valueOf(env.trim());
			}
			catch (final NumberFormatException failure)
			{
				throw new IllegalArgumentException("Invalid " + envKey + " value: " + env, failure);
			}
		}

		private boolean envBoolean(final String envKey)
		{
			return Boolean.parseBoolean(this.envString(envKey));
		}

		private String envString(final String envKey)
		{
			return System.getenv(envKey);
		}
	}
}
