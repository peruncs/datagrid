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
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.typing.Disposable;

import java.nio.ByteBuffer;
import java.util.List;

import static org.eclipse.serializer.util.X.notNull;

/**
 * A {@link StorageBinaryDataPacketAcceptor} that forwards completed messages to
 * the merger and then disposes their packet-owned buffers. The merger copies
 * data it needs for deferred materialization, so ownership remains local to
 * this acceptor. Complete-binary delivery is a borrowed, synchronous call:
 * implementations must consume or copy the supplied {@link Binary} before
 * returning and must not retain it. This class will also call
 * {@link #dispose()} on the {@link ClusterStorageBinaryDataMerger}
 */
public interface ClusterStorageBinaryDataPacketAcceptor extends StorageBinaryDataPacketAcceptor, Disposable
{
	/** Returns a failure reported by the asynchronous merger, or {@code null}. */
	default RuntimeException failure()
	{
		return null;
	}

	default void awaitApplied()
	{
	}

	/**
	 * Accepts a complete binary without rebuilding packets. Aeron readers use
	 * this boundary because their assembler has already validated and reassembled
	 * the transaction. The binary is borrowed for the duration of this call;
	 * packet transports continue to use {@link #accept(List)}.
	 */
	default void acceptData(final Binary data)
	{
		throw new UnsupportedOperationException("complete-binary delivery is not supported");
	}

	/**
	 * Accepts a complete binary and may take ownership of its direct buffers.
	 * Returning {@code true} transfers release responsibility to the acceptor.
	 */
	default boolean acceptDataOwned(final Binary data)
	{
		this.acceptData(data);
		return false;
	}

	/** Accepts a type dictionary already decoded by the transport. */
	default void acceptTypeDictionary(final String dictionary)
	{
		throw new UnsupportedOperationException("decoded dictionary delivery is not supported");
	}
	static ClusterStorageBinaryDataPacketAcceptor New(final ClusterStorageBinaryDataMerger merger)
	{
		return new Default(notNull(merger));
	}

	/** Reassembles packets and forwards complete messages to the merger. */
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

		@Override
		public RuntimeException failure()
		{
			return this.merger.failure();
		}

		@Override
		public void acceptData(final Binary data)
		{
			this.merger.receiveData(data);
		}

		@Override
		public boolean acceptDataOwned(final Binary data)
		{
			return this.merger.receiveDataOwned(data);
		}

		@Override
		public void acceptTypeDictionary(final String dictionary)
		{
			this.merger.receiveTypeDictionary(dictionary);
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
