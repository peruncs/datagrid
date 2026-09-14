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


import org.eclipse.datagrid.cluster.nodelibrary.exceptions.BadRequestException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.InternalServerErrorException;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.PostBackup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static org.eclipse.serializer.util.X.notNull;
import static org.eclipse.serializer.util.X.unbox;

/**
 * Transport-neutral HTTP operation facade used by the framework adapters.
 * Implementations translate storage, replication, and provider failures into
 * the nodelibrary's HTTP exception types; adapters only supply annotations and
 * asynchronous execution policy.
 */
public interface ClusterRestRequestController extends AutoCloseable
{
	/** Reports whether the distributor is active.
	 * @return distributor state
	 * @throws HttpResponseException if the request fails
	 */
	boolean getDistributor() throws HttpResponseException;

	/** Starts distributor activation.
	 * @throws HttpResponseException if the request fails
	 */
	void postActivateDistributorStart() throws HttpResponseException;

	/** Finishes distributor activation.
	 * @return whether activation finished
	 * @throws HttpResponseException if the request fails
	 */
	boolean postActivateDistributorFinish() throws HttpResponseException;

	/** Checks node health.
	 * @throws HttpResponseException if the request fails
	 */
	void getHealth() throws HttpResponseException;

	/** Checks node readiness.
	 * @throws HttpResponseException if the request fails
	 */
	void getHealthReady() throws HttpResponseException;

	// TODO: Rename to get statistics or monitoring etc.
	/** Returns current storage size.
	 * @return storage size text
	 * @throws HttpResponseException if the request fails
	 */
	String getStorageBytes() throws HttpResponseException;

	/** Returns Prometheus metrics including provider id, state, sequence, and lag.
	 * @return metrics text
	 * @throws HttpResponseException if the request fails
	 */
	default String getReplicationMetrics() throws HttpResponseException
	{
		return "";
	}

	/** Starts a backup.
	 * @param body backup request body
	 * @throws HttpResponseException if the request fails
	 */
	void postBackup(PostBackup.Body body) throws HttpResponseException;

	/** Reports whether a backup is running.
	 * @return backup state
	 * @throws HttpResponseException if the request fails
	 */
	boolean getBackup() throws HttpResponseException;

	/** Stops or pauses updates.
	 * @throws HttpResponseException if the request fails
	 */
	void postUpdates() throws HttpResponseException;

	/** Reports whether updates are active.
	 * @return update state
	 * @throws HttpResponseException if the request fails
	 */
	boolean getUpdates() throws HttpResponseException;

	/** Resumes updates.
	 * @throws HttpResponseException if the request fails
	 */
	void postResumeUpdates() throws HttpResponseException;

	/** Starts storage checks.
	 * @throws HttpResponseException if the request fails
	 */
	void postGc() throws HttpResponseException;

	/** Reports whether storage checks are running.
	 * @return check state
	 * @throws HttpResponseException if the request fails
	 */
	boolean getGc() throws HttpResponseException;

	@Override
	void close();

	/** Creates a controller for a storage node.
	 *
	 * @param storageNodeManager storage node manager
	 * @param properties node properties
	 * @return request controller
	 */
	static ClusterRestRequestController StorageNode(
		final StorageNodeManager storageNodeManager,
		final NodelibraryPropertiesProvider properties
	)
	{
		return new StorageNode(notNull(storageNodeManager), notNull(properties));
	}

	/** Creates a controller for a development node.
	 * @return request controller
	 */
	static ClusterRestRequestController DevNode()
	{
		return new DevNode();
	}

	/** Creates a controller for a backup node.
	 *
	 * @param backupNodeManager backup node manager
	 * @param properties node properties
	 * @return request controller
	 */
	static ClusterRestRequestController BackupNode(
		final BackupNodeManager backupNodeManager,
		final NodelibraryPropertiesProvider properties
	)
	{
		return new BackupNode(notNull(backupNodeManager), notNull(properties));
	}

	/** Shares validation, error mapping, and common monitoring requests. */
	abstract class Abstract implements ClusterRestRequestController
	{
		private static final Logger LOG = LoggerFactory.getLogger(ClusterRestRequestController.class);

		private final ClusterNodeManager nodeManager;
		private final NodelibraryPropertiesProvider properties;

		/** Creates the shared request controller.
		 *
		 * @param nodeManager node manager
		 * @param properties node properties
		 */
		protected Abstract(final ClusterNodeManager nodeManager, final NodelibraryPropertiesProvider properties)
		{
			this.nodeManager = nodeManager;
			this.properties = properties;
		}

		@Override
		public void postGc() throws HttpResponseException
		{
			LOG.trace("Handling postDataGridGc request");
			this.handleRequest(this.nodeManager::startStorageChecks);
		}

		@Override
		public boolean getGc() throws HttpResponseException
		{
			LOG.trace("Handling getDataGridGc request");
			return this.handleRequest(this.nodeManager::isRunningStorageChecks);
		}

		@Override
		public void getHealth() throws HttpResponseException
		{
			this.handleRequest(() ->
			{
				if (!this.nodeManager.isHealthy())
				{
					throw new InternalServerErrorException();
				}
			});
		}

		@Override
		public void getHealthReady() throws HttpResponseException
		{
			this.handleRequest(() ->
			{
				if (!this.nodeManager.isReady())
				{
					throw new InternalServerErrorException();
				}
			});
		}

		@Override
		public String getStorageBytes() throws HttpResponseException
		{
			return this.handleRequest(() ->
			{
				final long storageSizeBytes = this.nodeManager.readStorageSizeBytes();
					return String.format(
						"""
						# HELP cluster_storage_used_bytes How many bytes are currently used up by the storage.
						# TYPE cluster_storage_used_bytes gauge
						cluster_storage_used_bytes{namespace="%s",pod="%s"} %s""",
						metricLabel(this.properties.myNamespace()),
						metricLabel(this.properties.myPodName()),
					storageSizeBytes
				);
			});
		}

		@Override
		public String getReplicationMetrics() throws HttpResponseException
		{
			return this.handleRequest(() ->
			{
				final long current = this.nodeManager.getCurrentMessageIndex();
				final long latest = this.nodeManager.getLatestMessageIndex();
				final long lag = Math.max(0, latest - current);
				final long archiveFree = this.nodeManager.getArchiveUsableSpaceBytes();
				final long durablePosition = this.nodeManager.getWriterDurablePosition();
				final long durableSequence = this.nodeManager.getWriterDurableSequence();
				final long appliedSequence = this.nodeManager.getAppliedSequence();
				final String transport = metricLabel(this.nodeManager.getReplicationTransport());
				final String state = this.nodeManager.getReplicationState().name().toLowerCase(java.util.Locale.ROOT);
				return String.format(
					"# HELP cluster_replication_current_sequence Last committed sequence applied locally.\n" +
					"# TYPE cluster_replication_current_sequence gauge\n" +
					"cluster_replication_current_sequence{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_latest_sequence Latest writer sequence observed.\n" +
					"# TYPE cluster_replication_latest_sequence gauge\n" +
					"cluster_replication_latest_sequence{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_lag_transactions Transactions behind latest.\n" +
					"# TYPE cluster_replication_lag_transactions gauge\n" +
					"cluster_replication_lag_transactions{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_state Provider lifecycle state (one label is 1).\n" +
					"# TYPE cluster_replication_state gauge\n" +
					"cluster_replication_state{transport=\"%s\",state=\"%s\"} 1\n" +
					"# HELP cluster_replication_ready Whether the node is ready.\n" +
					"# TYPE cluster_replication_ready gauge\n" +
					"cluster_replication_ready{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_healthy Whether the node is healthy.\n" +
					"# TYPE cluster_replication_healthy gauge\n" +
					"cluster_replication_healthy{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_archive_usable_space_bytes Archive free bytes, or -1 when unavailable.\n" +
					"# TYPE cluster_replication_archive_usable_space_bytes gauge\n" +
					"cluster_replication_archive_usable_space_bytes{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_writer_durable_position Last terminal recording position, or -1.\n" +
					"# TYPE cluster_replication_writer_durable_position gauge\n" +
					"cluster_replication_writer_durable_position{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_writer_durable_sequence Last terminal writer sequence, or -1.\n" +
					"# TYPE cluster_replication_writer_durable_sequence gauge\n" +
					"cluster_replication_writer_durable_sequence{transport=\"%s\"} %d\n" +
					"# HELP cluster_replication_applied_sequence Last sequence applied by this node, or -1.\n" +
					"# TYPE cluster_replication_applied_sequence gauge\n" +
					"cluster_replication_applied_sequence{transport=\"%s\"} %d",
					transport, current, transport, latest, transport, lag, transport, state,
					transport, this.nodeManager.isReady() ? 1 : 0,
					transport, this.nodeManager.isHealthy() ? 1 : 0,
					transport, archiveFree, transport, durablePosition, transport, durableSequence,
					transport, appliedSequence
				);
			});
		}

		private static String metricLabel(final String value)
		{
			return (value == null ? "unknown" : value)
				.replace("\\", "\\\\")
				.replace("\"", "\\\"")
				.replace("\n", "\\n")
				.replace("\r", "\\r");
		}

		@Override
		public boolean postActivateDistributorFinish() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public boolean getDistributor() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public boolean getUpdates() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postResumeUpdates() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postActivateDistributorStart() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postBackup(PostBackup.Body body) throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public boolean getBackup() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postUpdates() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		/** Runs a request and maps failures to HTTP exceptions.
		 *
		 * @param request request action
		 * @throws HttpResponseException if the request fails
		 */
		protected void handleRequest(final Runnable request) throws HttpResponseException
		{
			try
			{
				request.run();
			}
			catch (final Exception e)
			{
				// the exception has already been handled
				if (e instanceof HttpResponseException)
				{
					throw e;
				}

				LOG.error("Failed to handle request", e);
				throw new InternalServerErrorException();
			}
		}

		/** Runs a value request and maps failures to HTTP exceptions.
		 *
		 * @param <T> result type
		 * @param request request action
		 * @return request result
		 * @throws HttpResponseException if the request fails
		 */
		protected <T> T handleRequest(final Supplier<T> request) throws HttpResponseException
		{
			try
			{
				return request.get();
			}
			catch (final Exception e)
			{
				// the exception has already been handled
				if (e instanceof HttpResponseException)
				{
					throw e;
				}

				LOG.error("Failed to handle request", e);
				throw new InternalServerErrorException();
			}
		}
	}

	/** Serves requests for a node that can become the distributor. */
	final class StorageNode extends Abstract
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageNode.class);
		private final StorageNodeManager storageNodeManager;

		private StorageNode(final StorageNodeManager storageNodeManager, final NodelibraryPropertiesProvider properties)
		{
			super(storageNodeManager, properties);
			this.storageNodeManager = storageNodeManager;
		}

		@Override
		public boolean postActivateDistributorFinish() throws HttpResponseException
		{
			return this.handleRequest(this.storageNodeManager::finishDistributionSwitch);
		}

		@Override
		public boolean getDistributor() throws HttpResponseException
		{
			return this.handleRequest(this.storageNodeManager::isDistributor);
		}

		@Override
		public void postActivateDistributorStart() throws HttpResponseException
		{
			LOG.trace("Handling postDataGridActivateDistributorStart request");
			this.handleRequest(() ->
			{
				if (!this.storageNodeManager.isDistributor())
				{
					this.storageNodeManager.switchToDistribution();
				}
			});
		}

		@Override
		public void close()
		{
			this.storageNodeManager.close();
		}
	}

	/** Serves requests for a node that reads from a backup. */
	final class BackupNode extends Abstract
	{
		private static final Logger LOG = LoggerFactory.getLogger(BackupNode.class);
		private final BackupNodeManager backupNodeManager;

		private BackupNode(final BackupNodeManager backupNodeManager, final NodelibraryPropertiesProvider properties)
		{
			super(backupNodeManager, properties);
			this.backupNodeManager = backupNodeManager;
		}

		@Override
		public void postBackup(PostBackup.Body body) throws HttpResponseException
		{
			LOG.trace("Handling postDataGridBackup request");
			this.handleRequest(() -> this.backupNodeManager.createStorageBackup(unbox(body.getUseManualSlot())));
		}

		@Override
		public boolean getBackup() throws HttpResponseException
		{
			return this.handleRequest(this.backupNodeManager::isBackupRunning);

		}

		@Override
		public void postUpdates() throws HttpResponseException
		{
			LOG.trace("Handling postDataGridUpdates request");
			this.handleRequest(this.backupNodeManager::stopReadingAtLatestMessage);
		}

		@Override
		public boolean getUpdates() throws HttpResponseException
		{
			return this.handleRequest(() ->
			{
				final boolean isReading = this.backupNodeManager.isReading();
				return !isReading;
			});
		}

		@Override
		public void postResumeUpdates() throws HttpResponseException
		{
			LOG.trace("Handling postDataGridResumeUpdates request");
			this.handleRequest(this.backupNodeManager::resumeReading);
		}

		@Override
		public void close()
		{
			this.backupNodeManager.close();

		}
	}

	/**
	 * Dev Nodes are just dummy implementations so that the nodelibrary can be
	 * tested and run in local development environments.
	 */
	final class DevNode implements ClusterRestRequestController
	{
		private DevNode()
		{
		}

		@Override
		public boolean getDistributor() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postActivateDistributorStart() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public boolean postActivateDistributorFinish() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void getHealth() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void getHealthReady() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public String getStorageBytes() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postBackup(PostBackup.Body body) throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public boolean getBackup() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postUpdates() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public boolean getUpdates() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postResumeUpdates() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void postGc() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public boolean getGc() throws HttpResponseException
		{
			throw new BadRequestException();
		}

		@Override
		public void close()
		{
			// no-op
		}
	}
}
