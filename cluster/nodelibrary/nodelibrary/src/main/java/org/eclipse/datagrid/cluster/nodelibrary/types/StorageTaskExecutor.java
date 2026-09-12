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


import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This executor runs storage maintenance work away from the caller thread.
 *
 * <p>Only one check task may run at a time. A later request while that task is
 * active is ignored, and the next request can start after the previous thread
 * has finished.</p>
 */
public interface StorageTaskExecutor
{
	/** Starts a storage check task. */
	void runChecks();

	/** Reports whether a storage check is running.
	 * @return {@code true} when running
	 */
	boolean isRunningChecks();

	/** Creates a storage task executor.
	 * @param connection Store connection
	 * @return task executor
	 */
	static StorageTaskExecutor New(final StorageConnection connection)
	{
		return new Default(notNull(connection));
	}

	/** Implements the single-flight storage-check state machine. */
	class Abstract implements StorageTaskExecutor
	{
		private static final Logger LOG = LoggerFactory.getLogger(Abstract.class);
		private final StorageConnection connection;

		private Thread checksThread;

		/** Creates the shared executor state.
		 * @param connection Store connection
		 */
		protected Abstract(final StorageConnection connection)
		{
			this.connection = connection;
		}

		@Override
		public void runChecks()
		{
			if (this.checksThread == null || !this.checksThread.isAlive())
			{
				LOG.debug("Issuing new storage checks");
				this.checksThread = new Thread(() ->
				{
					this.connection.issueFullGarbageCollection();
					this.connection.issueFullCacheCheck();
					this.connection.issueFullFileCheck();
				}, "EclipseStore-StorageChecks");
				this.checksThread.start();
			}
		}

		@Override
		public boolean isRunningChecks()
		{
			if (this.checksThread != null && !this.checksThread.isAlive())
			{
				LOG.trace("Cleanup previous storage checks thread");
				this.checksThread = null;
			}
			return this.checksThread != null;
		}
	}

	/** Provides the standard storage-check executor. */
	final class Default extends Abstract implements StorageTaskExecutor
	{
		private Default(final StorageConnection connection)
		{
			super(connection);
		}
	}
}
