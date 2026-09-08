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


import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.typing.Disposable;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.util.X.notNull;

public interface StorageBinaryDataMessage extends Disposable
{
	/** Defensive upper bound for a single network message before a transport-specific limit is supplied. */
	int MAX_MESSAGE_LENGTH = 256 * 1024 * 1024;
	public static enum MessageType
	{
		TYPE_DICTIONARY,
		DATA
	}

	public MessageType type();

	public int length();

	public int packetCount();

	public StorageBinaryDataMessage addPacket(StorageBinaryDataPacket packet);

	public boolean isComplete();

	public ByteBuffer data();

	public static StorageBinaryDataMessage New(final StorageBinaryDataPacket initialPacket)
	{
		return new StorageBinaryDataMessage.Default(
			notNull(initialPacket)
		);
	}

	public static class Default implements StorageBinaryDataMessage
	{
		private final MessageType type;
		private final int length;
		private final int packetCount;
		private int receivedPackets = 0;
		private ByteBuffer buffer;

		Default(final StorageBinaryDataPacket initialPacket)
		{
			super();
			this.type = initialPacket.messageType();
			this.length = initialPacket.messageLength();
			this.packetCount = initialPacket.packetCount();
			if (this.length > MAX_MESSAGE_LENGTH)
			{
				throw new IllegalArgumentException("Data message exceeds maximum length " + MAX_MESSAGE_LENGTH);
			}
			if (this.length < 0 || this.packetCount <= 0)
			{
				throw new IllegalArgumentException("invalid data message dimensions");
			}
			this.buffer = XMemory.allocateDirectNative(this.length);
			try
			{
				this.addPacket(initialPacket);
			}
			catch (final RuntimeException | Error failure)
			{
				this.dispose();
				throw failure;
			}
		}

		private void validateForAddition(final StorageBinaryDataPacket packet)
		{
			if (this.isComplete())
			{
				// TODO typed exception
				throw new RuntimeException("Data message already complete");
			}

			final MessageType expectedMessageType = this.type;
			if (packet.messageType() != expectedMessageType)
			{
				// TODO typed exception
				throw new RuntimeException(
					"Invalid packet type, received " + packet.messageType() + ", expected " + expectedMessageType
				);
			}

			final int expectedPacketIndex = this.receivedPackets;
			if (packet.packetIndex() != expectedPacketIndex)
			{
				// TODO typed exception
				throw new RuntimeException(
					"Invalid packet index, received " + packet.packetIndex() + ", expected " + expectedPacketIndex
				);
			}
			if (packet.messageLength() != this.length || packet.packetCount() != this.packetCount)
			{
				throw new IllegalArgumentException("packet dimensions changed within a message");
			}
			final ByteBuffer packetBuffer = packet.buffer();
			if (packetBuffer == null || packetBuffer.remaining() > this.buffer.remaining())
			{
				throw new IllegalArgumentException("packet payload exceeds declared message length");
			}
		}

		private void internalAddPacket(final StorageBinaryDataPacket packet)
		{
			final ByteBuffer source = packet.buffer().duplicate();
			this.buffer.put(source);

			this.receivedPackets++;

			if (this.isComplete())
			{
				if (this.buffer.position() != this.length)
				{
					throw new IllegalArgumentException("packet payload does not fill declared message length");
				}
				this.buffer.flip();
			}
		}

		@Override
		public MessageType type()
		{
			return this.type;
		}

		@Override
		public int length()
		{
			return this.length;
		}

		@Override
		public int packetCount()
		{
			return this.packetCount;
		}

		@Override
		public synchronized StorageBinaryDataMessage addPacket(final StorageBinaryDataPacket packet)
		{
			this.validateForAddition(packet);
			this.internalAddPacket(packet);

			return this;
		}

		@Override
		public synchronized boolean isComplete()
		{
			return this.receivedPackets == this.packetCount;
		}

		@Override
		public synchronized ByteBuffer data()
		{
			if (!this.isComplete())
			{
				// TODO typed exception
				throw new RuntimeException("Data message not complete yet");
			}

			if (this.buffer == null)
			{
				// TODO typed exception
				throw new RuntimeException("Data message already disposed");
			}

			return this.buffer;
		}

		@Override
		public synchronized void dispose()
		{
			final ByteBuffer value = this.buffer;
			this.buffer = null;
			if (value != null) XMemory.deallocateDirectByteBuffer(value);
		}

	}

}
