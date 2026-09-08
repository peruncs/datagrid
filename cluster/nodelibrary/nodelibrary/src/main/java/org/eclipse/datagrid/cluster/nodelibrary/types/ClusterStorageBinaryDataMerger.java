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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.datagrid.storage.distributed.types.ObjectMaterializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataImporter;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMerger;
import org.eclipse.serializer.collections.types.XEnum;
import org.eclipse.serializer.concurrency.XThreads;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.typing.Disposable;
import org.eclipse.serializer.util.X;
import org.eclipse.serializer.util.logging.Logging;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/**
 * Applies committed Store binary data on a reader node.
 *
 * <p>Incoming buffers are imported into the local Store immediately and then
 * coalesced for object-graph updates on a bounded single-thread executor.
 * Providers must call this merger only after their transport-specific commit
 * validation has completed.</p>
 */
public interface ClusterStorageBinaryDataMerger extends StorageBinaryDataMerger, Disposable
{
	static ClusterStorageBinaryDataMerger New(
		final BinaryPersistenceFoundation<?> foundation,
		final StorageConnection storage,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler,
		final long cachingTimeoutMs,
		final long cachedBinaryLimit
	)
	{
		return new Default(
			notNull(foundation),
			notNull(storage),
			notNull(objectGraphUpdateHandler),
			notNegative(cachingTimeoutMs),
			positive(cachedBinaryLimit)
		);
	}

	interface Defaults
	{
		static long cachingTimeoutMs()
		{
			return 10_000L;
		}

		static long cachingLimit()
		{
			return 50L;
		}
	}

	class Default implements ClusterStorageBinaryDataMerger
	{
		private static final Logger LOG = Logging.getLogger(ClusterStorageBinaryDataMerger.class);

		private final ExecutorService executor = Executors.newSingleThreadExecutor();
		private final ConcurrentLinkedQueue<ByteBuffer> cachedData = new ConcurrentLinkedQueue<>();
		private final Object applyLock = new Object();

		private final BinaryPersistenceFoundation<?> foundation;
		private final StorageConnection storage;
		private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
		private final long cachingTimeoutMs;
		private final long cacheLimit;
		private volatile boolean disposed;

		private Future<?> updateFuture = CompletableFuture.completedFuture(null);

		private Default(
			final BinaryPersistenceFoundation<?> foundation,
			final StorageConnection storage,
			final ObjectGraphUpdateHandler objectGraphUpdateHandler,
			final long cachingTimeoutMs,
			final long cacheLimit
		)
		{
			this.foundation = foundation;
			this.storage = storage;
			this.objectGraphUpdateHandler = objectGraphUpdateHandler;
			this.cachingTimeoutMs = cachingTimeoutMs;
			this.cacheLimit = cacheLimit;
		}

		@Override
		public synchronized void receiveData(final Binary data)
		{
			if (this.disposed)
			{
				return;
			}
			final ByteBuffer[] ownedBuffers = StorageBinaryDataImporter.importOwned(this.storage, data.buffers());

			this.cachedData.addAll(Arrays.asList(ownedBuffers));

			if (this.updateFuture.isDone())
			{
				this.updateFuture = this.executor.submit(() ->
				{
					try
					{
						XThreads.sleep(this.cachingTimeoutMs);
						this.applyDataSafely();
					}
					catch (final Throwable t)
					{
						if (Thread.currentThread().isInterrupted())
						{
							this.releaseCachedData();
							return;
						}
						GlobalErrorHandling.handleFatalError(t);
					}
				});
			}

			if (this.cachedData.size() > this.cacheLimit)
			{
				while (!this.updateFuture.isDone())
				{
					try
					{
						this.updateFuture.get(500, TimeUnit.MILLISECONDS);
					}
					catch (final InterruptedException e)
					{
						LOG.debug("Interrupted while waiting for import data task", e);
						Thread.currentThread().interrupt();
					}
					catch (final ExecutionException e)
					{
						// does not happen as any throwables are handled
					}
					catch (final TimeoutException e)
					{
						// no-op
					}
				}
			}
		}

		private void applyData()
		{
			final XEnum<ByteBuffer> data = X.Enum();
			final AtomicBoolean released = new AtomicBoolean();
			final Runnable release = () ->
			{
				if (released.compareAndSet(false, true))
				{
					for (final ByteBuffer buffer : data)
					{
						XMemory.deallocateDirectByteBuffer(buffer);
					}
				}
			};

			ByteBuffer next;
			while ((next = this.cachedData.poll()) != null)
			{
				data.add(next);
			}

			try
			{
				this.objectGraphUpdateHandler.objectGraphUpdateAvailable(() ->
				{
					final ObjectMaterializer materializer = new ObjectMaterializer(this.storage.persistenceManager());

					final BinaryEntityRawDataIterator iterator = BinaryEntityRawDataIterator.New();
					try
					{
						for (final ByteBuffer buffer : data)
						{
							final long address = XMemory.getDirectByteBufferAddress(buffer);
							iterator.iterateEntityRawData(address, address + buffer.limit(), materializer);
						}
						materializer.materialize();
					}
					finally
					{
						release.run();
					}
				});
			}
			catch (final RuntimeException | Error failure)
			{
				release.run();
				throw failure;
			}
		}

		private void applyDataSafely()
		{
			synchronized (this.applyLock)
			{
				if (!this.cachedData.isEmpty())
				{
					this.applyData();
				}
			}
		}

		private void releaseCachedData()
		{
			synchronized (this.applyLock)
			{
				ByteBuffer buffer;
				while ((buffer = this.cachedData.poll()) != null)
				{
					XMemory.deallocateDirectByteBuffer(buffer);
				}
			}
		}

		@Override
		public synchronized void receiveTypeDictionary(final String typeDictionaryData)
		{
			if (this.disposed) return;
			final PersistenceTypeDictionary remoteTypeDictionary = BinaryPersistence.Foundation()
				.setClassLoaderProvider(this.foundation.getClassLoaderProvider())
				.setFieldEvaluatorPersister(this.foundation.getFieldEvaluatorPersistable())
				.setTypeDictionaryLoader(() -> typeDictionaryData)
				.getTypeDictionaryProvider()
				.provideTypeDictionary();
			final PersistenceTypeDictionary localTypeDictionary = this.storage.persistenceManager().typeDictionary();

			remoteTypeDictionary.iterateAllTypeDefinitions(remoteType ->
			{
				final PersistenceTypeDefinition localType = localTypeDictionary.lookupTypeById(remoteType.typeId());
				if (localType == null)
				{
					LOG.debug("New type: {}", remoteType.typeName());
					this.foundation.getTypeHandlerManager().ensureTypeHandler(remoteType);

				}
				else if (!PersistenceTypeDescription.equalStructure(localType, remoteType))
				{
					throw new RuntimeException(localType + " <> " + remoteType);
				}
			});
		}

		@Override
		public synchronized void dispose()
		{
			if (this.disposed) return;
			this.disposed = true;
			this.executor.shutdown();
			try
			{
				// if any external processes like Kubernetes shuts us down, it will wait for the externally set
				// grace period and then kill the process. But any other case we will await the task orderly like this.
				if (!this.executor.awaitTermination(30, TimeUnit.SECONDS))
				{
					LOG.warn("Timed out waiting for storage graph updates; interrupting remaining work");
					this.executor.shutdownNow();
				}
			}
			catch (final InterruptedException e)
			{
				this.executor.shutdownNow();
				Thread.currentThread().interrupt();
				throw new NodelibraryException(e);
			}
			finally
			{
				if (this.executor.isTerminated()) this.releaseCachedData();
			}
		}

		@Override
		public synchronized void awaitApplied()
		{
			/*
			 * The normal merger deliberately delays materialization to coalesce updates.
			 * A replication cursor/ACK, however, is a durability boundary: waiting for
			 * the delayed task would add the full cache timeout to every Aeron commit.
			 * Drain immediately under the same lock used by the worker. The delayed
			 * worker is deliberately not interrupted: it may already be inside Store
			 * materialization, and interrupting it can leave the object graph half-applied.
			 */
			this.applyDataSafely();
			final Future<?> pending = this.updateFuture;
			if (pending.isDone())
			{
				try
				{
					pending.get();
				}
				catch (final InterruptedException e)
				{
					Thread.currentThread().interrupt();
					throw new NodelibraryException(e);
				}
				catch (final ExecutionException e)
				{
					throw new NodelibraryException("Failed to materialize imported Store data", e.getCause());
				}
			}
		}
	}

	/** Wait until the latest accepted import has completed object-graph materialization. */
	default void awaitApplied()
	{
	}
}
