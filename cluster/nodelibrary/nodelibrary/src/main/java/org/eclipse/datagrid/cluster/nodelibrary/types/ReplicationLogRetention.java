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

import java.util.UUID;

/** Provider-specific retention hook; unsupported providers retain history and report it explicitly. */
public interface ReplicationLogRetention extends AutoCloseable
{
	/** Outcome of one bounded retention maintenance attempt.
	 *
	 * @param status result category
	 * @param position Archive position associated with the attempt
	 * @param detail diagnostic detail, never {@code null}
	 */
	record MaintenanceResult(Status status, long position, String detail)
	{
		/** Retention maintenance outcome. */
		public enum Status
		{
			/** One or more complete leading Archive segments were deleted. */
			DELETED,
			/** No complete segment was eligible for deletion. */
			NOTHING_TO_DELETE,
			/** A stopped recording still has a replay using a segment selected for purge. */
			DEFERRED_ACTIVE_REPLAY
		}

		/** Normalizes the result detail and validates the status. */
		public MaintenanceResult
		{
			if (status == null) throw new NullPointerException("status");
			detail = detail == null ? "" : detail;
		}
	}
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
		 * @return result of the bounded maintenance attempt
		 * @throws NodelibraryException if deletion fails
	 */
	MaintenanceResult deleteThrough(ReplicationCursor cursor) throws NodelibraryException;

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

	/**
	 * Permanently removes a decommissioned reader from the retention quorum.
	 * Implementations must persist the retirement before allowing it to affect
	 * deletion safety.
	 *
	 * @param readerId permanently retired reader identity
	 */
	default void retireReader(final UUID readerId)
	{
		throw new UnsupportedOperationException("reader retirement is unsupported by this transport");
	}

	@Override
	void close();
}
