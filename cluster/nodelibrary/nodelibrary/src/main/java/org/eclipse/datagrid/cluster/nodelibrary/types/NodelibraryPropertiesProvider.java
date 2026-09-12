package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */


/**
 * Configuration contract shared by cluster lifecycle code and optional
 * providers. Environment-backed defaults preserve the existing Kafka
 * deployment while {@link #replicationTransport()} allows explicit
 * {@code kafka}, {@code aeron}, or {@code none} selection.
 */
public interface NodelibraryPropertiesProvider
{
	/** Existing Kafka topic contract retained for source compatibility. */
	default String kafkaTopicName()
	{
		return null;
	}

	default String replicationStreamName()
	{
		return this.kafkaTopicName();
	}

	default String replicationTransport()
	{
		return "none";
	}

	/**
	 * Optional provider-specific setting, allowing embedded applications to avoid
	 * environment variables. {@code ECLIPSE_DATAGRID_STORAGE_PATH} overrides the
	 * default {@code /storage} root used for the Store and durable offset file.
	 */
	default String replicationProperty(final String name)
	{
		return null;
	}

	/** Fixed-topology node role: {@code writer}, {@code reader}, or {@code backup-reader}. */
	default String replicationRole()
	{
		return isBackupNode() ? "backup-reader" : "writer";
	}

	/** Returns whether the role was explicitly configured rather than inherited from the Kafka-era default. */
	default boolean replicationRoleConfigured()
	{
		return false;
	}

	boolean isBackupNode();

	Integer keptBackupsCount();

	BackupTarget backupTarget();

	String backupProxyServiceUrl();

	Integer storageLimitCheckerIntervalMinutes();

	Integer storageLimitGB();

	String myPodName();

	String myNamespace();

	boolean isProdMode();

	Long dataMergerTimeoutMs();

	Long dataMergerCachedDataLimit();

	static NodelibraryPropertiesProvider Env()
	{
		return new Env();
	}

	/** Reads node properties from environment variables. */
	class Env implements NodelibraryPropertiesProvider
	{
		/** Names of the environment variables understood by the provider. */
		public static final class EnvKeys
		{
			public static final String KAFKA_TOPIC_NAME = "MSCNL_KAFKA_TOPIC_NAME";
			public static final String REPLICATION_STREAM_NAME = "ECLIPSE_DATAGRID_REPLICATION_STREAM";
			public static final String REPLICATION_TRANSPORT = "ECLIPSE_DATAGRID_REPLICATION_TRANSPORT";
			public static final String STORAGE_PATH = "ECLIPSE_DATAGRID_STORAGE_PATH";
			public static final String IS_BACKUP_NODE = "IS_BACKUP_NODE";
			public static final String BACKUP_TARGET = "BACKUP_TARGET";
			public static final String KEPT_BACKUPS_COUNT = "KEPT_BACKUPS_COUNT";
			public static final String BACKUP_PROXY_SERVICE_URL = "BACKUP_PROXY_SERVICE_URL";
			public static final String STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES =
				"STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES";
			public static final String STORAGE_LIMIT_GB = "STORAGE_LIMIT_GB";
			public static final String MY_POD_NAME = "MY_POD_NAME";
			public static final String MY_NAMESPACE = "MY_NAMESPACE";
			public static final String IS_PROD_MODE = "MSCNL_PROD_MODE";
			public static final String DATA_MERGER_TIMEOUT_MS = "MSCNL_DATA_MERGER_TIMEOUT";
			public static final String DATA_MERGER_LIMIT = "MSCNL_DATA_MERGER_LIMIT";

			private EnvKeys()
			{
			}
		}

		@Override
		public String replicationStreamName()
		{
			final String stream = this.envString(EnvKeys.REPLICATION_STREAM_NAME);
			return stream == null ? this.envString(EnvKeys.KAFKA_TOPIC_NAME) : stream;
		}

		@Override
		public String kafkaTopicName()
		{
			return this.replicationStreamName();
		}

		@Override
		public String replicationTransport()
		{
			final String transport = this.envString(EnvKeys.REPLICATION_TRANSPORT);
			return transport == null
				? (this.envString(EnvKeys.KAFKA_TOPIC_NAME) == null ? "none" : "kafka")
				: transport;
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
		public Integer storageLimitGB()
		{
			// TODO: Change behaviour to set an integer instead of a formatted string like this
			return Integer.parseInt(this.envString(EnvKeys.STORAGE_LIMIT_GB).replace("G", ""));
		}

		@Override
		public String myPodName()
		{
			return this.envString(EnvKeys.MY_POD_NAME);
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
			return env == null ? null : Integer.parseInt(env);
		}

		private Long envLong(final String envKey)
		{
			final String env = this.envString(envKey);
			return env == null ? null : Long.parseLong(env);
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
