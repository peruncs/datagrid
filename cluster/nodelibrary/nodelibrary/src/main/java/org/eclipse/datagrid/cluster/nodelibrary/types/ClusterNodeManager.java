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

/**
 * This manager reports node readiness and starts storage maintenance work.
 *
 * <p>Readiness means the node can serve its role. Health also considers whether
 * its active transport is still functioning. Implementations close their own
 * transport and storage collaborators.</p>
 */
public interface ClusterNodeManager extends AutoCloseable
{
	@Override
	void close();

	/** Starts periodic storage checks. */
	void startStorageChecks();

	/** Reports whether storage checks are running.
	 *
	 * @return {@code true} when checks are running
	 */
	boolean isRunningStorageChecks();

	/** Reports whether the node can serve requests.
	 *
	 * @return {@code true} when the node is ready
	 * @throws NodelibraryException if readiness cannot be determined
	 */
	boolean isReady() throws NodelibraryException;

	/** Reports whether the node and its transport are healthy.
	 *
	 * @return {@code true} when the node is healthy
	 */
	boolean isHealthy();

	/** Reads the current Store size.
	 *
	 * @return storage size in bytes
	 * @throws NodelibraryException if the size cannot be read
	 */
	long readStorageSizeBytes() throws NodelibraryException;

	/** Monitoring hook; nodes without a replication stream return {@code -1}.
	 *
	 * @return current message index
	 */
	default long getCurrentMessageIndex() { return -1; }

	/** Monitoring hook; nodes without a replication stream return {@code -1}.
	 *
	 * @return latest message index
	 */
	default long getLatestMessageIndex() { return -1; }

	/** Monitoring hook for the selected provider.
	 *
	 * @return replication transport name
	 */
	default String getReplicationTransport() { return "none"; }

	/** Monitoring hook for provider lifecycle state.
	 *
	 * @return replication state
	 */
	default ReplicationHealth.State getReplicationState()
	{
		return isHealthy() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.STARTING;
	}

	/** Monitoring hook for the selected provider's Archive capacity.
	 *
	 * @return usable archive space in bytes
	 */
	default long getArchiveUsableSpaceBytes() { return -1L; }

	/** Monitoring hook for the writer's last durable recording position.
	 *
	 * @return durable recording position
	 */
	default long getWriterDurablePosition() { return -1L; }

	/** Monitoring hook for the writer's last durable sequence.
	 *
	 * @return durable sequence
	 */
	default long getWriterDurableSequence() { return -1L; }

	/** Monitoring hook for the reader's last applied sequence.
	 *
	 * @return applied sequence
	 */
	default long getAppliedSequence() { return -1L; }
}
