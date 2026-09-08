package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

/** Transport health independent of a Kafka, Aeron, or other client implementation. */
public interface ReplicationHealth extends AutoCloseable
{
	enum State
	{
		STARTING, REPLAYING, LIVE, DEGRADED_ARCHIVE, RESEED_REQUIRED, FAILED
	}
	/** Returns true only when the node may serve the configured replication role. */
	boolean isReady() throws NodelibraryException;

	/** Returns true when the provider is operating without a fatal condition. */
	boolean isHealthy();

	default State state()
	{
		return isReady() ? State.LIVE : State.STARTING;
	}

	/** Initializes provider-side health probes and counters. */
	void init() throws NodelibraryException;

	@Override
	void close();
}
