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

public interface ClusterNodeManager extends AutoCloseable
{
	@Override
	void close();

	void startStorageChecks();

	boolean isRunningStorageChecks();

	boolean isReady() throws NodelibraryException;

	boolean isHealthy();

	long readStorageSizeBytes() throws NodelibraryException;

	/** Monitoring hook; nodes without a replication stream return {@code -1}. */
	default long getCurrentMessageIndex() { return -1; }

	/** Monitoring hook; nodes without a replication stream return {@code -1}. */
	default long getLatestMessageIndex() { return -1; }

	/** Monitoring hook for the selected provider. */
	default String getReplicationTransport() { return "none"; }

	/** Monitoring hook for provider lifecycle state. */
	default ReplicationHealth.State getReplicationState()
	{
		return isHealthy() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.STARTING;
	}
}
