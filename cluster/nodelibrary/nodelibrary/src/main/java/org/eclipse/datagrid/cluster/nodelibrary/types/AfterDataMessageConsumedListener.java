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
 * This listener runs after a replicated data message has been applied.
 *
 * <p>The replication cursor is the commit point for follow-up bookkeeping.
 * The listener is also closeable so a reader can release any callback state
 * during shutdown.</p>
 */
public interface AfterDataMessageConsumedListener extends AutoCloseable
{
	/** Records that one replicated message has been applied.
	 *
	 * @param cursor applied replication cursor
	 * @throws NodelibraryException if follow-up bookkeeping fails
	 */
	void onApplied(ReplicationCursor cursor) throws NodelibraryException;

	@Override
	void close();
}
