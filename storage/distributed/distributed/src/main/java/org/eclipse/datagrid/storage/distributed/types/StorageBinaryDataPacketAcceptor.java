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


import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static org.eclipse.serializer.util.X.notNull;

/** Reassembles ordered packets and forwards complete messages to a receiver. */
public interface StorageBinaryDataPacketAcceptor extends Consumer<List<StorageBinaryDataPacket>>
{
	@Override
    void accept(final List<StorageBinaryDataPacket> packet);

	static StorageBinaryDataPacketAcceptor New(final StorageBinaryDataReceiver receiver)
	{
		return new StorageBinaryDataPacketAcceptor.Default(
			notNull(receiver)
		);
	}

	/** Reassembles packets and forwards complete messages to a receiver. */
	class Default implements StorageBinaryDataPacketAcceptor
	{
		private final StorageBinaryDataReceiver receiver;
		private StorageBinaryDataMessage message;

		protected Default(final StorageBinaryDataReceiver receiver)
		{
			super();
			this.receiver = receiver;
		}

		@Override
		public synchronized void accept(final List<StorageBinaryDataPacket> packets)
		{
			final StorageBinaryDataPacketAssembler.Result result =
				StorageBinaryDataPacketAssembler.collect(this.message, packets);
			this.message = result.pending();
			if (!result.completed().isEmpty())
			{
				this.handleCompleteMessages(result.completed());
			}
		}

		private void handleCompleteMessages(final List<StorageBinaryDataMessage> messages)
		{
			// Join similiar messages and hand over to receiver
			try
			{
				StorageBinaryDataPacketAssembler.dispatch(messages, this::send);
			}
			finally
			{
				messages.forEach(StorageBinaryDataMessage::dispose);
			}
		}

		@SuppressWarnings("incomplete-switch")
		private void send(final StorageBinaryDataMessage last, final List<ByteBuffer> buffers)
		{
			switch (last.type())
			{
			case DATA:
			{
				// join all buffers of previous data messages
				this.receiver.receiveData(
					ChunksWrapper.New(
						buffers.toArray(ByteBuffer[]::new)
					)
				);
			}
				break;

			case TYPE_DICTIONARY:
			{
				// type dictionary is always sent completely, so only the last one is relevant
				this.receiver.receiveTypeDictionary(StorageBinaryDataPacketAssembler.decodeTypeDictionary(last.data()));
			}
				break;
			}
		}

	}

}
