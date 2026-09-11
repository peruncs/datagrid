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
import org.eclipse.serializer.concurrency.XThreads;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

public interface StorageBackupManager
{
    void createStorageBackup(boolean useManualSlot) throws NodelibraryException;

    void downloadLatestBackup(Path targetRootPath) throws NodelibraryException;

    List<BackupMetadata> listBackups() throws NodelibraryException;

    default BackupMetadata latestBackup(final boolean ignoreManualSlot) throws NodelibraryException
    {
        return this.listBackups()
            .stream()
            .filter(b -> !ignoreManualSlot || !b.manualSlot())
            .max(Comparator.comparingLong(BackupMetadata::timestamp))
            .orElse(null);
    }

    void deleteBackup(BackupMetadata backup) throws NodelibraryException;

    void downloadBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodelibraryException;

    boolean hasUserUploadedStorage() throws NodelibraryException;

    void downloadUserUploadedStorage(Path storageDestinationParentPath) throws NodelibraryException;

    void deleteUserUploadedStorage() throws NodelibraryException;

    static StorageBackupManager New(
        final StorageConnection storageConnection,
        final int maxBackupCount,
        final StorageBackupBackend storageBackupBackend,
        final Supplier<MessageInfo> messageInfoSupplier,
        final ClusterStorageBinaryDataClient dataClient,
        final ReplicationLogRetention retention
    )
    {
        return new Default(
            notNull(storageConnection),
            positive(maxBackupCount),
            notNull(storageBackupBackend),
            notNull(messageInfoSupplier),
            notNull(dataClient),
            notNull(retention)
        );
    }

    class Default implements StorageBackupManager
    {
		private static final Logger LOG = LoggerFactory.getLogger(StorageBackupManager.class);
		private static final long STOP_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(1);

        private final StorageConnection storageConnection;
        private final int maxBackupCount;
        private final StorageBackupBackend backend;
        private final Supplier<MessageInfo> messageInfoSupplier;
        private final ClusterStorageBinaryDataClient dataClient;
        private final ReplicationLogRetention retention;

        private Default(
            final StorageConnection storageConnection,
            final int maxBackupCount,
            final StorageBackupBackend backupBackend,
            final Supplier<MessageInfo> messageInfoSupplier,
            final ClusterStorageBinaryDataClient dataClient,
            final ReplicationLogRetention retention
        )
        {
            this.storageConnection = storageConnection;
            this.maxBackupCount = maxBackupCount;
            this.backend = backupBackend;
            this.messageInfoSupplier = messageInfoSupplier;
            this.dataClient = dataClient;
            this.retention = retention;
        }

        @Override
        public synchronized void createStorageBackup(final boolean useManualSlot) throws NodelibraryException
        {
            LOG.trace("Creating new storage backup");

			final var newBackup = new BackupMetadata(System.currentTimeMillis(), useManualSlot);
			final List<BackupMetadata> backups = this.listBackups();
			final RuntimeException readerFailure = this.dataClient.failure();
			if (readerFailure != null)
			{
				throw new IllegalStateException("Cannot create backup after replication reader failure", readerFailure);
			}

			if (!backups.isEmpty())
            {
                if (useManualSlot)
                {
                    backups.stream().filter(BackupMetadata::manualSlot).forEach(this::deleteBackup);
                }
                else
                {
                    // just in case there are multiple backups too many
                    final int toDeleteCount = (int)backups.stream().filter(b -> !b.manualSlot()).count()
                        - this.maxBackupCount + 1;
                    LOG.debug("Deleting {} oldest backup(s)", toDeleteCount);
                    for (int i = 0; i < toDeleteCount; i++)
                    {
                        this.deleteBackup(this.oldestBackup(backups));
                    }
                }
            }

			final boolean isRunning = this.dataClient.isRunning();

			if (isRunning)
			{
				this.stopDataClient();
			}

            try
            {
                this.backend.createAndUploadBackup(this.storageConnection, this.messageInfoSupplier.get(), newBackup);

                if (!useManualSlot)
                {
                    // delete up to the previous backup to save on Kafka log storage
					if (this.retention.isSupported())
					{
						this.backend.getMessageInfoFromPreviousBackup(1)
							.ifPresent(info -> this.retention.deleteThrough(new ReplicationCursor(
								info.transport(), info.storeGeneration(), info.messageIndex(), info.providerPosition()
							)));
					}
					else
					{
						LOG.warn("Replication retention is unsupported; preserving Archive history");
					}
                }
            }
            finally
            {
                // only resume if the data client was running previously
                if (isRunning)
                {
                    this.dataClient.resume();
                }
            }
        }

        @Override
        public synchronized void downloadLatestBackup(final Path targetRootPath)
        {
            final var backup = this.latestBackup(false);
            if (backup == null)
            {
                throw new NodelibraryException("No backups are available to download");
            }
            this.downloadBackup(targetRootPath, backup);
        }

        @Override
        public synchronized void deleteBackup(final BackupMetadata backup) throws NodelibraryException
        {
            this.backend.deleteBackup(backup);
        }

        @Override
        public synchronized void deleteUserUploadedStorage() throws NodelibraryException
        {
            this.backend.deleteUserUploadedStorage();
        }

        @Override
        public void downloadBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
            throws NodelibraryException
        {
            this.backend.downloadBackup(storageDestinationParentPath, backup);
        }

        @Override
        public synchronized void downloadUserUploadedStorage(final Path storageDestinationParentPath)
            throws NodelibraryException
        {
            this.backend.downloadUserUploadedStorage(storageDestinationParentPath);
        }

        @Override
        public synchronized boolean hasUserUploadedStorage() throws NodelibraryException
        {
            return this.backend.hasUserUploadedStorage();
        }

        @Override
        public synchronized List<BackupMetadata> listBackups() throws NodelibraryException
        {
            return this.backend.listBackups();
        }

		private void stopDataClient()
		{
			LOG.trace("Waiting for data client to stop reading");
			this.dataClient.stopAtLatestMessage();
			final long deadline = System.nanoTime() + STOP_TIMEOUT_NANOS;
			while (true)
			{
				final ClusterStorageBinaryDataClient.StopResult result = this.dataClient.stopResult();
				final ClusterStorageBinaryDataClient.StopOutcome outcome = result.outcome();
				if (outcome == ClusterStorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY)
				{
					return;
				}
				final RuntimeException failure = this.dataClient.failure();
				if (failure != null)
				{
					throw new IllegalStateException("Cannot create backup after replication reader failure", failure);
				}
				if (outcome == ClusterStorageBinaryDataClient.StopOutcome.TIMED_OUT ||
					outcome == ClusterStorageBinaryDataClient.StopOutcome.FAILED)
				{
					throw new IllegalStateException("Cannot create backup after replication reader stop " + outcome);
				}
				if (System.nanoTime() >= deadline)
				{
					throw new IllegalStateException("Timed out waiting for replication reader boundary at " +
						this.dataClient.messageInfo() + " (last resolved sequence=" + result.sequence() +
						", position=" + result.position() + ")");
				}
				XThreads.sleep(100);
			}
		}

        private BackupMetadata oldestBackup(final List<BackupMetadata> backups)
        {
            return backups.stream().min(Comparator.comparingLong(BackupMetadata::timestamp)).orElse(null);
        }
    }
}
