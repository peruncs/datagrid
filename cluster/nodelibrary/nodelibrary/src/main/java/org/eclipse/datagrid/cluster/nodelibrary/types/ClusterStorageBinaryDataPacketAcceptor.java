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


import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAssembler;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.typing.Disposable;

import java.nio.ByteBuffer;
import java.util.List;

import static org.eclipse.serializer.util.X.notNull;

/**
 * A {@link StorageBinaryDataPacketAcceptor} that forwards completed messages to
 * the merger and then disposes their packet-owned buffers. The merger copies
 * data it needs for deferred materialization, so ownership remains local to
 * this acceptor. This class will also call
 * {@link #dispose()} on the {@link ClusterStorageBinaryDataMerger}
 */
public interface ClusterStorageBinaryDataPacketAcceptor extends StorageBinaryDataPacketAcceptor, Disposable
{
	default void awaitApplied()
	{
	}
	static ClusterStorageBinaryDataPacketAcceptor New(final ClusterStorageBinaryDataMerger merger)
	{
		return new Default(notNull(merger));
	}

	class Default implements ClusterStorageBinaryDataPacketAcceptor
	{
		private final ClusterStorageBinaryDataMerger merger;
		private StorageBinaryDataMessage message;

		protected Default(final ClusterStorageBinaryDataMerger merger)
		{
			super();
			this.merger = merger;
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
				this.merger.receiveData(ChunksWrapper.New(buffers.toArray(ByteBuffer[]::new)));
			}
				break;

			case TYPE_DICTIONARY:
			{
				// type dictionary is always sent completely, so only the last one is relevant
				this.merger.receiveTypeDictionary(StorageBinaryDataPacketAssembler.decodeTypeDictionary(last.data()));
			}
				break;
			}
		}

		@Override
		public synchronized void dispose()
		{
			final StorageBinaryDataMessage pending = this.message;
			this.message = null;
			if (pending != null)
			{
				pending.dispose();
			}
			this.merger.dispose();
		}

		@Override
		public void awaitApplied()
		{
			this.merger.awaitApplied();
		}
	}
}
