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

/** Latest-position and provider-readiness contract used by neutral lifecycle code. */
public interface ReplicationPositionProvider extends AutoCloseable
{
	/** Initializes any provider client needed to resolve the current position. */
	void init() throws NodelibraryException;

	/**
	 * Returns the newest position that can be used as a backup/bootstrap boundary.
	 *
	 * @throws UnsupportedOperationException when this role cannot obtain a writer
	 * latest boundary (for example, a reader without a control/status channel)
	 */
	ReplicationCursor latest() throws NodelibraryException;

	default long latestSequence() throws NodelibraryException
	{
		return this.latest().logicalSequence();
	}

	@Override
	void close();
}
