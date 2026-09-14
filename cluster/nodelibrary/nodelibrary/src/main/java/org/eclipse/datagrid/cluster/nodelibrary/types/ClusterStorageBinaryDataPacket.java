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


import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataException;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/**
 * This packet adds a monotonically increasing message index to a storage
 * packet.
 *
 * <p>The packet index identifies a fragment inside one message. The message
 * index identifies the complete message in the cluster stream, so readers can
 * resume after a backup without confusing fragments from different messages.</p>
 */
public interface ClusterStorageBinaryDataPacket extends StorageBinaryDataPacket
{
	/** Returns the complete message index.
	 * @return message index
	 */
	long messageIndex();

	/** Creates a packet with cluster message metadata.
	 *
	 * @param messageType message kind
	 * @param messageLength complete message length
	 * @param packetIndex packet index
	 * @param packetCount packet count
	 * @param messageIndex complete message index
	 * @param buffer packet payload
	 * @return new packet
	 */
	static ClusterStorageBinaryDataPacket New(
            final MessageType messageType,
            final int messageLength,
            final int packetIndex,
            final int packetCount,
            final long messageIndex,
            final ByteBuffer buffer
	)
	{
		if (messageIndex < -1L || messageIndex == Long.MAX_VALUE)
		{
			throw new StorageBinaryDataException("message index must be in [-1, Long.MAX_VALUE)");
		}
		final int validatedPacketIndex = notNegative(packetIndex);
		final int validatedPacketCount = positive(packetCount);
		if (validatedPacketIndex >= validatedPacketCount)
		{
			throw new IllegalArgumentException("packetIndex must be less than packetCount");
		}
		return new ClusterStorageBinaryDataPacketDefault(
			notNull(messageType),
			notNegative(messageLength),
			validatedPacketIndex,
			validatedPacketCount,
			messageIndex,
			notNull(buffer)
		);
	}

}

/** Package-private implementation of a cluster packet. */
final class ClusterStorageBinaryDataPacketDefault implements ClusterStorageBinaryDataPacket
	{
		private final MessageType messageType;
		private final int messageLength;
		private final int packetIndex;
		private final int packetCount;
		private final long messageIndex;
		private final ByteBuffer buffer;

		ClusterStorageBinaryDataPacketDefault(
			final MessageType messageType,
			final int messageLength,
			final int packetIndex,
			final int packetCount,
			final long messageIndex,
			final ByteBuffer buffer
		)
		{
			this.messageType = messageType;
			this.messageLength = messageLength;
			this.packetIndex = packetIndex;
			this.packetCount = packetCount;
			this.messageIndex = messageIndex;
			this.buffer = buffer;
		}

		@Override
		public MessageType messageType()
		{
			return this.messageType;
		}

		@Override
		public int messageLength()
		{
			return this.messageLength;
		}

		@Override
		public int packetIndex()
		{
			return this.packetIndex;
		}

		@Override
		public int packetCount()
		{
			return this.packetCount;
		}

		@Override
		public long messageIndex()
		{
			return this.messageIndex;
		}

		@Override
		public ByteBuffer buffer()
		{
			return this.buffer;
		}
	}
