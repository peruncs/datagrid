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


import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;

import static org.eclipse.serializer.util.X.notNull;

/**
 * Cluster-aware extension of the transport-neutral binary distributor.
 *
 * <p>The message index and ignore flag are lifecycle controls, not transport
 * details. {@link Caching} preserves a type dictionary until the next data
 * message and is shared by Kafka and Aeron providers.</p>
 */
public interface ClusterStorageBinaryDataDistributor extends StorageBinaryDataDistributor
{
	static ClusterStorageBinaryDataDistributor NoOp()
	{
		return new ClusterStorageBinaryDataDistributor()
		{
			private long index = -1;
			private boolean ignored;
			public void messageIndex(final long value) { this.index = value; }
			public long messageIndex() { return this.index; }
			public void ignoreDistribution(final boolean value) { this.ignored = value; }
			public boolean ignoreDistribution() { return this.ignored; }
			public void distributeTypeDictionary(final String value) { }
			public void distributeData(final Binary value) { }
			public void dispose() { }
		};
	}

	void messageIndex(long index);

	long messageIndex();

	void ignoreDistribution(boolean ignore);

	boolean ignoreDistribution();

	/**
	 * Queues a complete dictionary for the next data transaction, regardless of
	 * which Store thread performs that transaction. This is used after writer
	 * restart to re-establish the reader schema before new binaries arrive.
	 */
	default void queueTypeDictionaryForNextTransaction(final String typeDictionaryData)
	{
		this.distributeTypeDictionary(typeDictionaryData);
	}

	static ClusterStorageBinaryDataDistributor Caching(final ClusterStorageBinaryDataDistributor delegate)
	{
		return new Caching(notNull(delegate));
	}

	final class Caching implements ClusterStorageBinaryDataDistributor
	{
		private final ClusterStorageBinaryDataDistributor delegate;
		private final ThreadLocal<String> typeDictionaryData = new ThreadLocal<>();
		private volatile String queuedTypeDictionary;

		private Caching(final ClusterStorageBinaryDataDistributor delegate)
		{
			this.delegate = delegate;
		}

		@Override
		public synchronized void distributeData(final Binary data)
		{
			final String dictionary = this.typeDictionaryData.get();
			if (dictionary != null)
			{
				try
				{
					this.delegate.distributeTypeDictionary(dictionary);
				}
				finally
				{
					this.typeDictionaryData.remove();
				}
			}
			this.delegate.distributeData(data);
		}

		@Override
		public synchronized void distributeTypeDictionary(final String typeDictionaryData)
		{
			if (typeDictionaryData == null)
			{
				this.typeDictionaryData.remove();
			}
			else
			{
				this.typeDictionaryData.set(typeDictionaryData);
			}
		}

		@Override
		public synchronized void queueTypeDictionaryForNextTransaction(final String typeDictionaryData)
		{
			this.queuedTypeDictionary = typeDictionaryData;
		}

		@Override
		public synchronized String consumeTypeDictionary()
		{
			final String value = this.typeDictionaryData.get();
			this.typeDictionaryData.remove();
			if (value != null) return value;
			final String queued = this.queuedTypeDictionary;
			this.queuedTypeDictionary = null;
			return queued;
		}

		@Override
		public void messageIndex(final long index)
		{
			this.delegate.messageIndex(index);
		}

		@Override
		public long messageIndex()
		{
			return this.delegate.messageIndex();
		}

		@Override
		public boolean ignoreDistribution()
		{
			return this.delegate.ignoreDistribution();
		}

		@Override
		public void ignoreDistribution(final boolean ignore)
		{
			this.delegate.ignoreDistribution(ignore);
		}

		@Override
		public void dispose()
		{
			this.typeDictionaryData.remove();
			this.queuedTypeDictionary = null;
			this.delegate.dispose();
		}
	}
}
