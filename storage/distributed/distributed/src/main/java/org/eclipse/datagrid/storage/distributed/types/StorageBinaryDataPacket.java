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
		return new StorageBinaryDataPacket.Default(
			notNull(messageType),
			notNegative(messageLength),
			notNegative(packetIndex),
			positive(packetCount),
			notNull(buffer)
		);
	}

	/** Immutable packet metadata and borrowed payload view. */
	class Default implements StorageBinaryDataPacket
	{
		private final MessageType messageType;
		private final int messageLength;
		private final int packetIndex;
		private final int packetCount;
		private final ByteBuffer buffer;

		Default(
			final MessageType messageType,
			final int messageLength,
			final int packetIndex,
			final int packetCount,
			final ByteBuffer buffer
		)
		{
			super();
			this.messageType = messageType;
			this.messageLength = messageLength;
			this.packetIndex = packetIndex;
			this.packetCount = packetCount;
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
		public ByteBuffer buffer()
		{
			return this.buffer;
		}

	}

}
