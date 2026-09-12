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
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataImporter;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMaterializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMerger;
import org.eclipse.serializer.collections.types.XEnum;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
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
	/** Returns an asynchronous materialization failure, or {@code null} while healthy.
	 * @return terminal failure, or {@code null}
	 */
	default RuntimeException failure()
	{
		return null;
	}

	/** Creates a merger with bounded deferred materialization.
	 *
	 * @param foundation persistence foundation
	 * @param storage Store connection
	 * @param objectGraphUpdateHandler graph update handler
	 * @param cachingTimeoutMs maximum wait for a cached batch
	 * @param cachedBinaryLimit maximum cached binary count
	 * @return binary merger
	 */
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

	/** Supplies conservative defaults for deferred object-graph application. */
	interface Defaults
	{
		/** Returns the default cache timeout in milliseconds.
		 * @return timeout in milliseconds
		 */
		static long cachingTimeoutMs()
		{
			return 10_000L;
		}

		/** Returns the default cached binary count.
		 * @return cached binary limit
		 */
		static long cachingLimit()
		{
			return 50L;
		}
	}

	/** Applies imported data on one bounded worker and reports failures. */
	class Default implements ClusterStorageBinaryDataMerger
	{
		private static final Logger LOG = Logging.getLogger(ClusterStorageBinaryDataMerger.class);

		private final ExecutorService executor = Executors.newSingleThreadExecutor();
		private final ConcurrentLinkedQueue<ByteBuffer> cachedData = new ConcurrentLinkedQueue<>();
		private final Object applyLock = new Object();
		private final Object flushMonitor = new Object();
		private volatile boolean flushRequested;

		private final BinaryPersistenceFoundation<?> foundation;
		private final StorageConnection storage;
		private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
		private final long cachingTimeoutMs;
		private final long cacheLimit;
		private volatile boolean disposed;
		private volatile RuntimeException failure;

		private Future<?> updateFuture = CompletableFuture.completedFuture(null);

		/** Aeron transfers its assembled direct buffers before this callback starts.
		 *
		 * @return {@code true} because this merger releases the transferred buffers
		 */
		@Override
		public boolean canReceiveDataOwned()
		{
			return true;
		}

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
			if (this.failure != null)
			{
				throw new IllegalStateException("Storage binary merger has failed", this.failure);
			}
			if (this.disposed)
			{
				/* A disposed receiver must not acknowledge data. Returning normally
				 * would let the Aeron assembler advance its cursor even though the Store
				 * binary was discarded. */
				throw new IllegalStateException("Storage binary merger is disposed");
			}
			final ByteBuffer[] sourceBuffers = org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataChunker
				.buffers(data).toArray(ByteBuffer[]::new);
			final ByteBuffer[] ownedBuffers = StorageBinaryDataImporter.importOwned(this.storage, sourceBuffers);
			/* scheduleMaterialization owns cleanup on every rejection.  Releasing here
			 * as well would double-free buffers when the worker has already drained its
			 * queue after a terminal failure. */
			this.scheduleMaterialization(ownedBuffers);
		}

		/** Imports Aeron-owned direct buffers without a second native allocation. */
		@Override
		public synchronized boolean receiveDataOwned(final Binary data)
		{
			final ByteBuffer[] buffers = org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataChunker
				.buffers(org.eclipse.serializer.util.X.notNull(data)).toArray(ByteBuffer[]::new);
			if (this.failure != null)
			{
				StorageBinaryDataImporter.release(buffers);
				throw new IllegalStateException("Storage binary merger has failed", this.failure);
			}
			if (this.disposed)
			{
				StorageBinaryDataImporter.release(buffers);
				throw new IllegalStateException("Storage binary merger is disposed");
			}
			if (!StorageBinaryDataImporter.importDirect(this.storage, buffers))
			{
				try
				{
					this.receiveData(data);
					return true;
				}
				finally
				{
					StorageBinaryDataImporter.release(buffers);
				}
			}
			/* scheduleMaterialization now owns the buffers, including any cleanup when
			 * executor submission or backpressure fails after queue admission. */
			this.scheduleMaterialization(buffers);
			return true;
		}

		private void scheduleMaterialization(final ByteBuffer[] ownedBuffers)
		{
			if (this.failure != null)
			{
				StorageBinaryDataImporter.release(ownedBuffers);
				throw new IllegalStateException("Storage binary merger has failed", this.failure);
			}
			if (this.disposed)
			{
				StorageBinaryDataImporter.release(ownedBuffers);
				throw new IllegalStateException("Storage binary merger is disposed");
			}

			boolean queued = false;
			try
			{
				synchronized (this.applyLock)
				{
					/* The worker can fail between the entry check above and this
					 * ownership hand-off.  Reject before enqueueing so a caller never
					 * loses the native buffers into a dead queue. */
					if (this.failure != null)
					{
						throw new IllegalStateException("Storage binary merger has failed", this.failure);
					}
					if (this.disposed)
					{
						throw new IllegalStateException("Storage binary merger is disposed");
					}
					this.cachedData.addAll(Arrays.asList(ownedBuffers));
					queued = true;
					if (this.updateFuture.isDone())
					{
						try
						{
							this.updateFuture = this.executor.submit(() ->
							{
								try
								{
									this.awaitFlushRequestOrTimeout();
									this.applyDataSafely();
								}
								catch (final Throwable t)
								{
									if (this.disposed && Thread.currentThread().isInterrupted())
									{
										this.releaseCachedData();
										return;
									}
									this.failure = t instanceof RuntimeException runtime
										? runtime
										: new IllegalStateException("Storage binary merger failed", t);
									LOG.error("Storage binary merger failed", this.failure);
									this.releaseCachedData();
									throw this.failure;
								}
							});
						}
						catch (final RuntimeException | Error failure)
						{
							/* Submission happens under applyLock, so the worker cannot have
							 * removed these newly queued buffers yet. */
							for (final ByteBuffer buffer : ownedBuffers)
							{
								this.cachedData.removeIf(candidate -> candidate == buffer);
							}
							queued = false;
							throw failure;
						}
					}
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
							Thread.currentThread().interrupt();
							/* A Kafka reader can be interrupted while disposal is waiting for this
							 * backpressure loop. Do not continue calling Future.get() with the
							 * interrupt flag set: that creates a hot loop and prevents shutdown. */
							throw new IllegalStateException("Interrupted while waiting for import data task", e);
						}
						catch (final ExecutionException e)
						{
							// The worker records its terminal failure before completing exceptionally.
							final RuntimeException mergerFailure = this.failure;
							if (mergerFailure != null)
							{
								throw new IllegalStateException("Storage binary merger has failed", mergerFailure);
							}
							throw new IllegalStateException("Storage binary merger task failed", e.getCause());
						}
						catch (final TimeoutException e)
						{
							// no-op
						}
					}
				}
			}
			catch (final RuntimeException | Error failure)
			{
				if (!queued)
				{
					StorageBinaryDataImporter.release(ownedBuffers);
				}
				/* Once queued, the worker or releaseCachedData owns the buffers.  Releasing
				 * them here would race applyDataSafely and double-deallocate native memory. */
				throw failure;
			}
		}

		@Override
		public RuntimeException failure()
		{
			return this.failure;
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
				try
				{
					StorageBinaryDataMaterializer.materialize(this.storage, data.toArray(ByteBuffer.class));
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

		private void awaitFlushRequestOrTimeout()
		{
			if (this.cachingTimeoutMs <= 0L) return;
			synchronized (this.flushMonitor)
			{
				if (this.flushRequested)
				{
					this.flushRequested = false;
					return;
				}
				try
				{
					this.flushMonitor.wait(this.cachingTimeoutMs);
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Storage graph update worker was interrupted", interrupted);
				}
				this.flushRequested = false;
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
			if (this.failure != null)
			{
				throw new IllegalStateException("Storage binary merger has failed", this.failure);
			}
			if (this.disposed)
			{
				throw new IllegalStateException("Storage binary merger is disposed");
			}
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
			/* A timeout is retryable: the worker may still own native buffers.  Do
			 * not let the first failed attempt make every later cleanup a no-op. */
			if (this.disposed && this.executor.isTerminated()) return;
			this.disposed = true;
			this.executor.shutdown();
			boolean terminated = false;
			try
			{
				// if any external processes like Kubernetes shuts us down, it will wait for the externally set
				// grace period and then kill the process. But any other case we will await the task orderly like this.
				terminated = this.executor.awaitTermination(30, TimeUnit.SECONDS);
				if (!terminated)
				{
					LOG.warn("Timed out waiting for storage graph updates; interrupting remaining work");
					this.executor.shutdownNow();
					terminated = this.executor.awaitTermination(5, TimeUnit.SECONDS);
					if (!terminated)
					{
						throw new IllegalStateException(
							"Storage graph update worker did not terminate; native buffers remain owned by it");
					}
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
				if (terminated || this.executor.isTerminated()) this.releaseCachedData();
			}
		}

		@Override
		public synchronized void awaitApplied()
		{
			if (this.failure != null)
			{
				/* A worker can fail between receiveDataOwned() and this boundary.
				 * Release buffers accepted after the worker's failure cleanup so the
				 * assembler's ownership transfer cannot turn into a native leak. */
				this.releaseCachedData();
				throw new IllegalStateException("Storage binary merger has failed", this.failure);
			}
			/*
			 * The normal merger deliberately delays materialization to coalesce updates.
			 * A replication cursor/ACK, however, is a durability boundary: waiting for
			 * the delayed task would add the full cache timeout to every Aeron commit.
			 * Drain immediately under the same lock used by the worker. The delayed
			 * worker is deliberately not interrupted: it may already be inside Store
			 * materialization, and interrupting it can leave the object graph half-applied.
			 */
			/* Wake a worker that is in its coalescing delay.  The flag avoids a lost
			 * notification when the worker is between checking the flag and entering
			 * wait(). */
			synchronized (this.flushMonitor)
			{
				this.flushRequested = true;
				this.flushMonitor.notifyAll();
			}
			this.applyDataSafely();
			final Future<?> pending = this.updateFuture;
			if (!pending.isDone())
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
					final RuntimeException mergerFailure = this.failure;
					if (mergerFailure != null)
					{
						throw new IllegalStateException("Storage binary merger has failed", mergerFailure);
					}
					throw new NodelibraryException("Failed to materialize imported Store data", e.getCause());
				}
			}
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
