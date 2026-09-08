package org.eclipse.datagrid.storage.distributed.aeron.checkpoint;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import java.util.UUID;

/**
 * Durable reader checkpoint identity used to resume Archive replay.
 *
 * <p>The tuple fences a cursor to one cluster, node, Store generation, epoch,
 * and recording. A sequence alone is insufficient because a new recording or
 * Store image may reuse sequence numbers. Persistence is provided by the
 * neutral {@code ReplicationCursorStore}; this type is the Aeron provider
 * position value crossing the module boundary.</p>
 */
public record AeronReplicationCursor(
	UUID clusterId,
	UUID nodeId,
	UUID storeGeneration,
	long epoch,
	long recordingId,
	long recordingPosition,
	long sequence
)
{
	public AeronReplicationCursor
	{
		if (clusterId == null || nodeId == null || storeGeneration == null ||
			epoch < 0 || recordingId < 0 || recordingPosition < -1 || sequence < -1)
		{
			throw new IllegalArgumentException("invalid Aeron replication cursor");
		}
	}
}
