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
 * The reader's durable place in one Archive recording.
 *
 * <p>The sequence is paired with the cluster, node, Store image, epoch, and
 * recording identities. A sequence by itself is unsafe after a restart because
 * a new recording may reuse it. The neutral cursor store persists this value;
 * this record carries the Aeron-specific position across the provider
 * boundary.</p>
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
				epoch < 0 || recordingId < 0 || recordingPosition < -1 || sequence < -1 || sequence == Long.MAX_VALUE)
		{
			throw new IllegalArgumentException("invalid Aeron replication cursor");
		}
	}
}
