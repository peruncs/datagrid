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
import org.eclipse.store.storage.types.StorageController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This manager exposes node health while coordinating backups and replication.
 *
 * <p>A backup stops the reader at a safe message boundary, creates the backup,
 * and then resumes reading. Callers must not close the storage while either
 * operation is active.</p>
 */
public interface BackupNodeManager extends ClusterNodeManager
{
	void stopReadingAtLatestMessage();

	void resumeReading() throws NodelibraryException;

	boolean isReading();

	void createStorageBackup(final boolean useManualSlot) throws NodelibraryException;

	boolean isBackupRunning();

	static BackupNodeManager New(
		final StorageBackupTaskExecutor storageBackupTaskExecutor,
		final ClusterStorageBinaryDataClient dataClient,
		final StorageController storageController,
		final StorageDiskSpaceReader storageDiskSpaceReader
	)
	{
		return new Default(
			notNull(storageBackupTaskExecutor),
			notNull(dataClient),
			notNull(storageController),
			notNull(storageDiskSpaceReader)
		);
	}

	/** Coordinates backup work with the replication reader and storage controller. */
	final class Default implements BackupNodeManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(BackupNodeManager.class);

		private final StorageBackupTaskExecutor tasks;
		private final ClusterStorageBinaryDataClient dataClient;
		private final StorageController storageController;
		private final StorageDiskSpaceReader storageDiskSpaceReader;

		private Default(
			final StorageBackupTaskExecutor storageBackupTaskExecutor,
			final ClusterStorageBinaryDataClient dataClient,
			final StorageController storageController,
			final StorageDiskSpaceReader storageDiskSpaceReader
		)
		{
			this.tasks = storageBackupTaskExecutor;
			this.dataClient = dataClient;
			this.storageController = storageController;
			this.storageDiskSpaceReader = storageDiskSpaceReader;
		}

		@Override
		public void stopReadingAtLatestMessage()
		{
			this.dataClient.stopAtLatestMessage();
		}

		@Override
		public void resumeReading() throws NodelibraryException
		{
			this.dataClient.resume();
		}

		@Override
		public boolean isReading()
		{
			return this.dataClient.isRunning();
		}

		@Override
		public void createStorageBackup(final boolean useManualSlot) throws NodelibraryException
		{
			this.tasks.runBackup(useManualSlot);
		}

		@Override
		public boolean isBackupRunning()
		{
			return this.tasks.isRunningBackup();
		}

		@Override
		public boolean isHealthy()
		{
			return this.storageController.isRunning() && !this.storageController.isStartingUp();
		}

		@Override
		public boolean isReady() throws NodelibraryException
		{
			return this.storageController.isRunning() && !this.storageController.isStartingUp();
		}

		@Override
		public boolean isRunningStorageChecks()
		{
			return this.tasks.isRunningChecks();
		}

		@Override
		public long readStorageSizeBytes() throws NodelibraryException
		{
			return this.storageDiskSpaceReader.readUsedDiskSpaceBytes();
		}

		@Override
		public void startStorageChecks()
		{
			this.tasks.runChecks();
		}

		@Override
		public void close()
		{
			LOG.info("Closing BackupNodeManager.");
			this.dataClient.dispose();
			//this.backupManager.close();
		}

	}
}
