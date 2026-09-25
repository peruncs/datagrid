package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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


import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/** One ordered chunk of a type-dictionary or Store-binary message. */
	public interface StorageBinaryDataPacket
	{
		/** Returns the message kind.
		 *
		 * @return message kind
		 */
		MessageType messageType();

		/** Returns the complete message length.
		 *
		 * @return complete message length in bytes
		 */
		int messageLength();

		/** Returns the zero-based packet index.
		 *
		 * @return packet index
		 */
		int packetIndex();

		/** Returns the total packet count.
		 *
		 * @return packet count
		 */
		int packetCount();

		/** Returns the transport message index, or {@code -1} when unindexed.
		 *
		 * @return complete message index, or {@code -1}
		 */
		default long messageIndex()
		{
			return -1L;
		}

		/** Returns the borrowed packet payload.
		 *
		 * @return packet payload
		 */
		ByteBuffer buffer();

		/** Creates a packet with validated metadata.
		 *
		 * @param messageType message kind
		 * @param messageLength complete message length
		 * @param packetIndex zero-based packet index
		 * @param packetCount total packet count
		 * @param buffer packet payload
		 * @return new packet
		 */
		static StorageBinaryDataPacket New(
	            final MessageType messageType,
	            final int messageLength,
	            final int packetIndex,
	            final int packetCount,
	            final ByteBuffer buffer
	    )
		{
			return New(messageType, messageLength, packetIndex, packetCount, -1L, buffer);
		}

		/** Creates a packet with validated metadata and a transport message index.
		 *
		 * @param messageType message kind
		 * @param messageLength complete message length
		 * @param packetIndex zero-based packet index
		 * @param packetCount total packet count
		 * @param messageIndex complete message index, or {@code -1}
		 * @param buffer packet payload
		 * @return new packet
		 */
		static StorageBinaryDataPacket New(
			final MessageType messageType,
			final int messageLength,
			final int packetIndex,
			final int packetCount,
			final long messageIndex,
			final ByteBuffer buffer
		)
	{
		if (messageIndex < -1L || messageIndex == Long.MAX_VALUE)
			throw new IllegalArgumentException("message index must be in [-1, Long.MAX_VALUE)");
		final int validatedPacketIndex = notNegative(packetIndex);
		final int validatedPacketCount = positive(packetCount);
		if (validatedPacketIndex >= validatedPacketCount)
		{
			throw new IllegalArgumentException("packetIndex must be less than packetCount");
		}
		return new StorageBinaryDataPacketDefault(
			notNull(messageType),
				notNegative(messageLength),
				validatedPacketIndex,
				validatedPacketCount,
				messageIndex,
				notNull(buffer)
			);
	}

	/** Immutable packet metadata and borrowed payload view.
	 *
	 * @param messageType message kind
	 * @param messageLength complete message length
		 * @param packetIndex zero-based packet index
		 * @param packetCount total packet count
		 * @param messageIndex complete message index, or {@code -1}
		 * @param buffer borrowed packet payload
	 */
	record StorageBinaryDataPacketDefault(
		MessageType messageType,
			int messageLength,
			int packetIndex,
			int packetCount,
			long messageIndex,
			ByteBuffer buffer
		) implements StorageBinaryDataPacket
	{
	}

}
