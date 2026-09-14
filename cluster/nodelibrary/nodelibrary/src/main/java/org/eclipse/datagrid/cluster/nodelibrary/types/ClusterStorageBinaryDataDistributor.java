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

import java.util.concurrent.atomic.AtomicReference;

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
	/** Creates a distributor that ignores all transport work.
	 * @return neutral distributor
	 */
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

	/** Sets the next message index.
	 * @param index message index
	 */
	void messageIndex(long index);

	/** Returns the current message index.
	 * @return message index
	 */
	long messageIndex();

	/** Sets whether distribution is ignored.
	 * @param ignore whether to ignore distribution
	 */
	void ignoreDistribution(boolean ignore);

	/** Reports whether distribution is ignored.
	 * @return {@code true} when ignored
	 */
	boolean ignoreDistribution();

	/** Returns a terminal distribution failure, or {@code null} while healthy.
	 * @return terminal failure, or {@code null}
	 */
	default RuntimeException failure()
	{
		return null;
	}

	/**
	 * Queues a complete dictionary for the next data transaction, regardless of
	 * which Store thread performs that transaction. This is used after writer
	 * restart to re-establish the reader schema before new binaries arrive.
	 *
	 * @param typeDictionaryData assembled type dictionary
	 */
	default void queueTypeDictionaryForNextTransaction(final String typeDictionaryData)
	{
		this.distributeTypeDictionary(typeDictionaryData);
	}

	/** Creates a distributor that keeps dictionary data beside its next binary.
	 *
	 * @param delegate destination distributor
	 * @return caching distributor
	 */
	static ClusterStorageBinaryDataDistributor Caching(final ClusterStorageBinaryDataDistributor delegate)
	{
		return new Caching(notNull(delegate));
	}

	/** Keeps dictionary data adjacent to the transaction that needs it. */
	final class Caching implements ClusterStorageBinaryDataDistributor
	{
		private final ClusterStorageBinaryDataDistributor delegate;
		private final ThreadLocal<String> typeDictionaryData = new ThreadLocal<>();
		private final AtomicReference<String> queuedTypeDictionary = new AtomicReference<>();

		private Caching(final ClusterStorageBinaryDataDistributor delegate)
		{
			this.delegate = delegate;
		}

		@Override
		public void distributeData(final Binary data)
		{
			final String dictionary = this.typeDictionaryData.get();
			this.typeDictionaryData.remove();
			if (dictionary != null)
			{
				this.delegate.distributeTypeDictionary(dictionary);
			}
			this.delegate.distributeData(data);
		}

		@Override
		public void distributeTypeDictionary(final String typeDictionaryData)
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
		public void queueTypeDictionaryForNextTransaction(final String typeDictionaryData)
		{
			/* A node may have accumulated an incremental dictionary while startup
			 * distribution was disabled.  The restart snapshot is authoritative and
			 * must replace that stale thread-bound value, otherwise consumeTypeDictionary
			 * would return the incremental fragment and the queued full dictionary would
			 * never reach the next replicated transaction. */
			this.typeDictionaryData.remove();
			this.queuedTypeDictionary.set(typeDictionaryData);
		}

		@Override
		public String consumeTypeDictionary()
		{
			final String queued = this.queuedTypeDictionary.getAndSet(null);
			if (queued != null)
			{
				/* A full restart snapshot supersedes any incremental dictionary staged
				 * on the calling thread while startup distribution was disabled. */
				this.typeDictionaryData.remove();
				return queued;
			}
			final String value = this.typeDictionaryData.get();
			this.typeDictionaryData.remove();
			return value;
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
		public RuntimeException failure()
		{
			return this.delegate.failure();
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
			this.queuedTypeDictionary.set(null);
			this.delegate.dispose();
		}
	}
}
