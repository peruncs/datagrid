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

import static org.eclipse.serializer.util.X.notNull;

/** Neutral storage + replication readiness gate. */
public interface StorageNodeHealthCheck extends AutoCloseable
{
	/** Reports whether Store and replication are ready.
	 * @return {@code true} when ready
	 * @throws NodelibraryException if readiness cannot be checked
	 */
	boolean isReady() throws NodelibraryException;

	/** Reports whether Store and replication are healthy.
	 * @return {@code true} when healthy
	 */
	boolean isHealthy();

	/** Returns the provider state used by monitoring and readiness diagnostics.
	 * @return provider state
	 */
	default ReplicationHealth.State replicationState()
	{
		return isHealthy() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.STARTING;
	}

	/** Returns the provider's current Archive free-space estimate, or {@code -1}.
	 * @return free bytes
	 */
	default long archiveUsableSpaceBytes()
	{
		return -1L;
	}

	/** Returns the writer's last durable recording position, or {@code -1}.
	 * @return durable position
	 */
	default long writerDurablePosition()
	{
		return -1L;
	}

	/** Returns the writer's last durable sequence, or {@code -1}.
	 * @return durable sequence
	 */
	default long writerDurableSequence()
	{
		return -1L;
	}

	/** Returns the reader's last applied sequence, or {@code -1}.
	 * @return applied sequence
	 */
	default long appliedSequence()
	{
		return -1L;
	}

	@Override
	void close();

	/** Initializes the health checks.
	 * @throws NodelibraryException if initialization fails
	 */
	void init() throws NodelibraryException;

	/** Creates a health check.
	 *
	 * @param storageController Store controller
	 * @param replicationHealth replication health
	 * @return health check
	 */
	static StorageNodeHealthCheck New(
		final StorageController storageController,
		final ReplicationHealth replicationHealth
	)
	{
		return new Default(notNull(storageController), notNull(replicationHealth));
	}

	/** Combines Store readiness with provider health and lifecycle state. */
	final class Default implements StorageNodeHealthCheck
	{
		private final StorageController storageController;
		private final ReplicationHealth replicationHealth;
		private boolean active = true;

		private Default(
			final StorageController storageController,
			final ReplicationHealth replicationHealth
		)
		{
			this.storageController = storageController;
			this.replicationHealth = replicationHealth;
		}

		@Override
		public void init() throws NodelibraryException
		{
			this.replicationHealth.init();
		}

		@Override
		public boolean isHealthy()
		{
			return this.active && this.storageReady() && this.replicationHealth.isHealthy();
		}

		@Override
		public ReplicationHealth.State replicationState()
		{
			return this.active ? this.replicationHealth.state() : ReplicationHealth.State.FAILED;
		}

		@Override public long archiveUsableSpaceBytes() { return this.replicationHealth.archiveUsableSpaceBytes(); }
		@Override public long writerDurablePosition() { return this.replicationHealth.writerDurablePosition(); }
		@Override public long writerDurableSequence() { return this.replicationHealth.writerDurableSequence(); }
		@Override public long appliedSequence() { return this.replicationHealth.appliedSequence(); }

		@Override
		public boolean isReady() throws NodelibraryException
		{
			return this.active && this.storageReady() && this.replicationHealth.isReady();
		}

		private boolean storageReady()
		{
			return this.storageController.isRunning() && !this.storageController.isStartingUp();
		}

		@Override
		public synchronized void close()
		{
			if (!this.active)
			{
				return;
			}
			this.active = false;
			this.replicationHealth.close();
		}
	}
}
