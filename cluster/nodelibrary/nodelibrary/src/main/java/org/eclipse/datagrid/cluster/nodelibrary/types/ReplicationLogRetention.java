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

/** Provider-specific retention hook; unsupported providers retain history and report it explicitly. */
public interface ReplicationLogRetention extends AutoCloseable
{
	/**
	 * Returns whether this transport can safely delete replicated history. A
	 * provider that cannot establish authenticated reader watermarks must return
	 * {@code false}; lifecycle code will retain history and continue backups.
	 *
	 * @return {@code true} when safe retention is supported
	 */
	default boolean isSupported()
	{
		return true;
	}

	/** Deletes only history proven safe by the provider's cursor/watermark rules.
	 *
	 * @param cursor deletion boundary
	 * @throws NodelibraryException if deletion fails
	 */
	void deleteThrough(ReplicationCursor cursor) throws NodelibraryException;

	/**
	 * Records one authenticated reader acknowledgement for a later aggregate
	 * retention request. Providers without reader-watermark support reject this
	 * operation explicitly.
	 *
	 * @param cursor reader watermark
	 */
	default void recordReaderWatermark(final ReplicationCursor cursor)
	{
		throw new UnsupportedOperationException("reader watermarks are unsupported by this transport");
	}

	@Override
	void close();
}
