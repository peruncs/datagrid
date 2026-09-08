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


import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NotADistributorException;
import org.eclipse.store.storage.types.StorageController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageNodeManager extends ClusterNodeManager
{
	boolean isDistributor();

	void switchToDistribution();

	boolean finishDistributonSwitch() throws NotADistributorException;

	long getCurrentMessageIndex();

	long getLatestMessageIndex();

	/** Returns the selected transport id for monitoring (for example {@code aeron}). */
	String getReplicationTransport();

	/** Returns the provider lifecycle state shown by monitoring endpoints. */
	ReplicationHealth.State getReplicationState();

	static StorageNodeManager New(
		final ClusterStorageBinaryDataDistributor dataDistributor,
		final StorageTaskExecutor storageTaskExecutor,
		final ClusterStorageBinaryDataClient dataClient,
		final StorageNodeHealthCheck healthCheck,
		final StorageController storageController,
		final StorageDiskSpaceReader storageDiskSpaceReader,
		final ReplicationPositionProvider positionProvider
	)
	{
		return new Default(
			notNull(dataDistributor),
			notNull(storageTaskExecutor),
			notNull(dataClient),
			notNull(healthCheck),
			notNull(storageController),
			notNull(storageDiskSpaceReader),
			notNull(positionProvider)
		);
	}

	/** Creates a manager with an explicit transport id for monitoring labels. */
	static StorageNodeManager New(
		final ClusterStorageBinaryDataDistributor dataDistributor,
		final StorageTaskExecutor storageTaskExecutor,
		final ClusterStorageBinaryDataClient dataClient,
		final StorageNodeHealthCheck healthCheck,
		final StorageController storageController,
		final StorageDiskSpaceReader storageDiskSpaceReader,
		final ReplicationPositionProvider positionProvider,
		final String replicationTransport
	)
	{
		return new Default(
			notNull(dataDistributor), notNull(storageTaskExecutor), notNull(dataClient), notNull(healthCheck),
			notNull(storageController), notNull(storageDiskSpaceReader), notNull(positionProvider),
			replicationTransport
		);
	}

	final class Default implements StorageNodeManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageNodeManager.class);

		private final ClusterStorageBinaryDataDistributor dataDistributor;
		private final StorageTaskExecutor storageTaskExecutor;
		private final ClusterStorageBinaryDataClient dataClient;
		private final StorageNodeHealthCheck healthCheck;
		private final StorageController storageController;
		private final StorageDiskSpaceReader storageDiskSpaceReader;
		private final ReplicationPositionProvider positionProvider;
		private final String replicationTransport;

		private boolean isDistributor;
		private boolean isSwitchingToDistributor;
		private boolean closed;
		private boolean positionProviderClosed;

		public Default(
			final ClusterStorageBinaryDataDistributor dataDistributor,
			final StorageTaskExecutor storageTaskExecutor,
			final ClusterStorageBinaryDataClient dataClient,
			final StorageNodeHealthCheck healthCheck,
			final StorageController storageController,
			final StorageDiskSpaceReader storageDiskSpaceReader,
			final ReplicationPositionProvider positionProvider
		)
		{
			this(dataDistributor, storageTaskExecutor, dataClient, healthCheck, storageController,
				storageDiskSpaceReader, positionProvider, "unknown");
		}

		public Default(
			final ClusterStorageBinaryDataDistributor dataDistributor,
			final StorageTaskExecutor storageTaskExecutor,
			final ClusterStorageBinaryDataClient dataClient,
			final StorageNodeHealthCheck healthCheck,
			final StorageController storageController,
			final StorageDiskSpaceReader storageDiskSpaceReader,
			final ReplicationPositionProvider positionProvider,
			final String replicationTransport
		)
		{
			this.dataDistributor = dataDistributor;
			this.dataClient = dataClient;
			this.healthCheck = healthCheck;
			this.storageController = storageController;
			this.storageDiskSpaceReader = storageDiskSpaceReader;
			this.storageTaskExecutor = storageTaskExecutor;
			this.positionProvider = positionProvider;
			this.replicationTransport = replicationTransport == null || replicationTransport.isBlank()
				? "unknown" : replicationTransport;
		}

		@Override
		public void startStorageChecks()
		{
			this.storageTaskExecutor.runChecks();
		}

		@Override
		public boolean isRunningStorageChecks()
		{
			return this.storageTaskExecutor.isRunningChecks();
		}

		@Override
		public boolean isReady() throws NodelibraryException
		{
			if (this.isDistributor)
			{
				return this.storageController.isRunning() && !this.storageController.isStartingUp();
			}
			else
			{
				return this.healthCheck.isReady();
			}
		}

		@Override
		public boolean isHealthy()
		{
			if (this.isDistributor)
			{
				return this.storageController.isRunning() && !this.storageController.isStartingUp();
			}
			else
			{
				return this.healthCheck.isHealthy();
			}
		}

		@Override
		public long readStorageSizeBytes() throws NodelibraryException
		{
			return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
		}

		@Override
		public boolean isDistributor()
		{
			return this.isDistributor;
		}

		@Override
		public void switchToDistribution()
		{
			if (this.isDistributor() || this.isSwitchingToDistributor)
			{
				return;
			}

			LOG.info("Turning on distribution.");
			this.isSwitchingToDistributor = true;
			this.dataClient.stopAtLatestMessage();
		}

		@Override
		public boolean finishDistributonSwitch() throws NotADistributorException
		{
			if (!this.isSwitchingToDistributor)
			{
				throw new NotADistributorException("switchToDistribution() has to be called first");
			}

			if (this.isDistributor)
			{
				return true;
			}

			if (this.dataClient.isRunning())
			{
				return false;
			}

			final var messageInfo = this.dataClient.messageInfo();

			// once a node has been switched to distribution it will never become a reader node anymore
			RuntimeException failure = null;
			try { this.healthCheck.close(); }
			catch (final RuntimeException closeFailure) { failure = closeFailure; }
			try { this.dataClient.dispose(); }
			catch (final RuntimeException closeFailure)
			{
				if (failure == null) failure = closeFailure;
				else failure.addSuppressed(closeFailure);
			}
			if (failure != null)
			{
				throw new IllegalStateException("failed to close reader resources during promotion", failure);
			}
			this.dataDistributor.messageIndex(messageInfo.messageIndex());

			this.isDistributor = true;
			this.isSwitchingToDistributor = false;
			return true;
		}

		@Override
		public long getCurrentMessageIndex()
		{
			if (this.isDistributor())
			{
				return this.dataDistributor.messageIndex();
			}
			else
			{
				return this.dataClient.messageInfo().messageIndex();
			}
		}

		@Override
		public long getLatestMessageIndex()
		{
			try
			{
				return this.positionProvider.latestSequence();
			}
			catch (final NodelibraryException e)
			{
				throw new IllegalStateException("Failed to read latest replication position", e);
			}
		}

		@Override
		public String getReplicationTransport()
		{
			return this.replicationTransport;
		}

		@Override
		public ReplicationHealth.State getReplicationState()
		{
			return this.isDistributor
				? (this.isHealthy() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.STARTING)
				: this.healthCheck.replicationState();
		}

		@Override
		public void close()
		{
			LOG.info("Closing StorageNodeManager");
			if (this.closed)
			{
				return;
			}
			this.closed = true;
			RuntimeException failure = null;
			try { this.dataDistributor.dispose(); }
			catch (final RuntimeException closeFailure) { failure = closeFailure; }
			try { this.dataClient.dispose(); }
			catch (final RuntimeException closeFailure)
			{
				if (failure == null) failure = closeFailure;
				else failure.addSuppressed(closeFailure);
			}
			try { this.healthCheck.close(); }
			catch (final RuntimeException closeFailure)
			{
				if (failure == null) failure = closeFailure;
				else failure.addSuppressed(closeFailure);
			}
			try { this.closePositionProvider(); }
			catch (final RuntimeException closeFailure)
			{
				if (failure == null) failure = closeFailure;
				else failure.addSuppressed(closeFailure);
			}
			if (failure != null)
			{
				throw new IllegalStateException("failed to close storage node resources", failure);
			}
		}

		private void closePositionProvider()
		{
			if (this.positionProviderClosed)
			{
				return;
			}
			this.positionProviderClosed = true;
			this.positionProvider.close();
		}
	}
}
