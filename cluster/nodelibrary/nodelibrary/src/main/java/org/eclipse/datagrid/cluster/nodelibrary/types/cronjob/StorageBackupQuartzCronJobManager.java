package org.eclipse.datagrid.cluster.nodelibrary.types.cronjob;

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


import org.eclipse.datagrid.cluster.nodelibrary.types.StorageBackupManager;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This manager creates the scheduled full-backup job.
 *
 * <p>The job uses the automatic backup slot and cannot overlap another run.
 * The backup manager handles the reader boundary and restart metadata.</p>
 */
public interface StorageBackupQuartzCronJobManager extends QuartzCronJobManager
{
	static StorageBackupQuartzCronJobManager New(final StorageBackupManager backupManager)
	{
		return new Default(notNull(backupManager));
	}

	/** Creates backup jobs with one shared backup manager. */
	final class Default implements StorageBackupQuartzCronJobManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageBackupQuartzCronJobManager.class);
		private final StorageBackupManager backupManager;

		private Default(final StorageBackupManager backupManager)
		{
			this.backupManager = backupManager;
		}

		@Override
		public Job create()
		{
			LOG.debug("Instancing new storage backup quartz cron job");
			return new StorageBackupQuartzCronJob(this.backupManager);
		}
	}

	/** Requests one automatic storage backup when the trigger fires. */
	@DisallowConcurrentExecution
	final class StorageBackupQuartzCronJob implements Job
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageBackupQuartzCronJob.class);
		private final StorageBackupManager backupManager;

		private StorageBackupQuartzCronJob(final StorageBackupManager backupManager)
		{
			this.backupManager = backupManager;
		}

		@Override
		public void execute(final JobExecutionContext context)
		{
			LOG.info("Issuing full backup");
			this.backupManager.createStorageBackup(false);
		}
	}
}
