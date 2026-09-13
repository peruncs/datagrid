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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * The reader's durable place in one Archive recording.
 *
 * <p>The sequence is paired with the cluster, node, Store image, epoch, and
 * recording identities. A sequence by itself is unsafe after a restart because
 * a new recording may reuse it. The neutral cursor store persists this value;
 * this record carries the Aeron-specific position across the provider
 * boundary.</p>
 *
 * @param clusterId replication cluster identity
 * @param nodeId node that produced the cursor; replay may transfer it to another node
 * @param storeGeneration Store image identity
 * @param epoch writer epoch associated with the recording
 * @param recordingId Aeron Archive recording identity
 * @param recordingPosition Archive position at the cursor
 * @param sequence transaction sequence at the cursor
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
	private static final int MAGIC = 0x44474143; // DGAC
	private static final short VERSION = 1;
	private static final int ENCODED_LENGTH = Integer.BYTES + Short.BYTES * 2 + 16 * 3 + Long.BYTES * 4;

	/** Validates the identities and position carried by the durable cursor. */
	public AeronReplicationCursor
	{
		if (clusterId == null || nodeId == null || storeGeneration == null ||
				epoch < 0 || recordingId < 0 || recordingPosition < -1 || sequence < -1 || sequence == Long.MAX_VALUE)
		{
			throw new IllegalArgumentException("invalid Aeron replication cursor");
		}
	}

	/** Encodes the complete provider cursor identity for the neutral cursor store. */
	public byte[] encode()
	{
		return ByteBuffer.allocate(ENCODED_LENGTH).order(ByteOrder.BIG_ENDIAN)
			.putInt(MAGIC).putShort(VERSION).putShort((short)0)
			.putLong(this.clusterId.getMostSignificantBits()).putLong(this.clusterId.getLeastSignificantBits())
			.putLong(this.nodeId.getMostSignificantBits()).putLong(this.nodeId.getLeastSignificantBits())
			.putLong(this.storeGeneration.getMostSignificantBits()).putLong(this.storeGeneration.getLeastSignificantBits())
			.putLong(this.epoch).putLong(this.recordingId).putLong(this.recordingPosition).putLong(this.sequence)
			.array();
	}

	/** Decodes the sole supported Aeron provider-position format. */
	public static AeronReplicationCursor decode(final byte[] encoded)
	{
		if (encoded == null) throw new NullPointerException("encoded");
		if (encoded.length != ENCODED_LENGTH)
			throw new IllegalArgumentException("invalid Aeron cursor encoding length");
		final ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
		if (buffer.getInt() != MAGIC || buffer.getShort() != VERSION || buffer.getShort() != 0)
			throw new IllegalArgumentException("unsupported Aeron cursor format");
		return new AeronReplicationCursor(
			new UUID(buffer.getLong(), buffer.getLong()),
			new UUID(buffer.getLong(), buffer.getLong()),
			new UUID(buffer.getLong(), buffer.getLong()),
			buffer.getLong(), buffer.getLong(), buffer.getLong(), buffer.getLong());
	}
}
