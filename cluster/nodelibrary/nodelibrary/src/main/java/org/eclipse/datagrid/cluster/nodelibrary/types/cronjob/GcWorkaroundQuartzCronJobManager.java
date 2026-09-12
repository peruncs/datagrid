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


import org.eclipse.store.storage.types.StorageConnection;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This manager creates the scheduled storage cleanup job.
 *
 * <p>The job asks Store to check its cache and then run garbage collection.
 * Quartz prevents two cleanup jobs from using the same connection at once.</p>
 */
public interface GcWorkaroundQuartzCronJobManager extends QuartzCronJobManager
{
	static GcWorkaroundQuartzCronJobManager New(final StorageConnection connection)
	{
		return new Default(notNull(connection));
	}

	/** Creates cleanup jobs with one shared storage connection. */
	final class Default implements GcWorkaroundQuartzCronJobManager
	{
		private static final Logger LOG = LoggerFactory.getLogger(GcWorkaroundQuartzCronJobManager.class);
		private final StorageConnection connection;

		private Default(final StorageConnection connection)
		{
			this.connection = connection;
		}

		@Override
		public Job create()
		{
			LOG.debug("Instancing new gc workaround quartz cron job");
			return new GcWorkaroundQuartzCronJob(this.connection);
		}
	}

	/** Runs the Store cache check and garbage collection sequence. */
	@DisallowConcurrentExecution
	final class GcWorkaroundQuartzCronJob implements Job
	{
		private static final Logger LOG = LoggerFactory.getLogger(GcWorkaroundQuartzCronJob.class);
		private final StorageConnection connection;

		private GcWorkaroundQuartzCronJob(final StorageConnection connection)
		{
			this.connection = connection;
		}

		@Override
		public void execute(final JobExecutionContext context)
		{
			LOG.info("Issuing GC and CC");
			this.connection.issueFullCacheCheck();
			this.connection.issueFullGarbageCollection();
		}
	}
}
