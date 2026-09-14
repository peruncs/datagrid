package org.eclipse.datagrid.cluster.nodelibrary.kafka;

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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.Crc32c;
import org.eclipse.datagrid.storage.distributed.types.ReplicationRetry;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataChunker;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

/**
 * This distributor publishes cluster storage packets to one Kafka partition.
 *
 * <p>The stable partition key keeps packet order intact. The synchronous and
 * asynchronous variants share failure tracking and disposal rules, but differ
 * in whether the caller waits for Kafka work to finish.</p>
 */
public interface ClusterStorageBinaryDataDistributorKafka extends ClusterStorageBinaryDataDistributor
{
	/** Stable key used to keep every packet for a stream on one Kafka partition. */
	String PARTITION_KEY = "eclipse-datagrid-replication";
	/** Creates a synchronous Kafka distributor.
	 * @param topicName Kafka topic
	 * @param kafkaPropertiesProvider Kafka properties provider
	 * @return synchronous distributor
	 */
	static ClusterStorageBinaryDataDistributorKafka Sync(
		final String topicName,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		return new ClusterStorageBinaryDataDistributorKafka.Sync(notEmpty(topicName), notNull(kafkaPropertiesProvider));
	}

	/** Creates an asynchronous Kafka distributor.
	 * @param topicName Kafka topic
	 * @param kafkaPropertiesProvider Kafka properties provider
	 * @return asynchronous distributor
	 */
	static ClusterStorageBinaryDataDistributorKafka Async(
		final String topicName,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		return new ClusterStorageBinaryDataDistributorKafka.Async(
			notEmpty(topicName),
			notNull(kafkaPropertiesProvider)
		);
	}

	/** Shared Kafka producer, ordering, failure, and disposal behavior. */
	abstract class Abstract implements ClusterStorageBinaryDataDistributorKafka
	{
		private static final System.Logger LOG =
			System.getLogger(ClusterStorageBinaryDataDistributorKafka.class.getName());

		private final String topicName;
		private final KafkaProducer<String, byte[]> producer;
		private final AtomicLong messageIndex = new AtomicLong(-1L);
		private final AtomicLong droppedAfterFailure = new AtomicLong();
		private final AtomicBoolean ignoreDistribution = new AtomicBoolean();
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private final Object lifecycleMonitor = new Object();
		private int activeActions;
		private volatile boolean disposed;
		private volatile boolean disposing;

		/** Creates the shared Kafka distributor state.
		 * @param topicName Kafka topic
		 * @param kafkaPropertiesProvider Kafka properties provider
		 */
		protected Abstract(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			this.topicName = topicName;

			final Properties properties = new Properties();
			properties.putAll(kafkaPropertiesProvider.provide());
			properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
			properties.putIfAbsent(COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
			/* Bound producer construction and acknowledgement separately; the
			 * Future timeout alone does not bound partitionsFor() or send(). */
			properties.putIfAbsent(MAX_BLOCK_MS_CONFIG, "30000");
			properties.putIfAbsent(DELIVERY_TIMEOUT_MS_CONFIG, "35000");
			KafkaProducer<String, byte[]> created = null;
			try
			{
				created = new KafkaProducer<>(properties);
				final int partitionCount = created.partitionsFor(topicName).size();
				if (partitionCount != 1)
				{
					throw new IllegalArgumentException(
						"Kafka replication topic must have exactly one partition; found " + partitionCount
					);
				}
				this.producer = created;
			}
			catch (final RuntimeException | Error failure)
			{
				if (created != null)
				{
					try { created.close(); }
					catch (final RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
				}
				throw failure;
			}
		}

		/** Runs or submits one distribution action.
		 * @param action distribution action
		 */
		protected abstract void execute(Runnable action);

		/** Returns whether a previously failed action may be dropped from a queue.
		 * @return {@code true} for an asynchronous queue-backed distributor
		 */
		protected abstract boolean asynchronous();

		private void distribute(final MessageType messageType, final Binary data)
		{
			this.distribute(messageType, data, null);
		}

		private void distribute(
			final MessageType messageType,
			final Binary data,
			final Runnable cleanup
		)
		{
			Objects.requireNonNull(data, "data");
			synchronized (this.lifecycleMonitor)
			{
				if (this.disposed || this.disposing)
				{
					if (cleanup != null) cleanup.run();
					throw new IllegalStateException(this.disposed ? "Kafka distributor is disposed" : "Kafka distributor is stopping");
				}
				if (this.failure.get() != null)
				{
					if (cleanup != null) cleanup.run();
					throw new IllegalStateException("Kafka distributor has failed", this.failure.get());
				}
				if (this.ignoreDistribution.get())
				{
					LOG.log(System.Logger.Level.DEBUG, "Ignoring distribution for data of type " + messageType);
					if (cleanup != null) cleanup.run();
					return;
				}
				this.activeActions++;
			}
			final List<StorageBinaryDataChunker.Chunk> chunks;
			try
			{
				/* Store owns and reuses Binary buffers after this method returns. Packetize
				 * on the caller thread so an asynchronous worker owns byte arrays rather
				 * than borrowed Serializer buffers. */
				chunks = StorageBinaryDataChunker.chunk(
					data, KafkaHeaderCodec.maxPacketSize());
			}
			catch (final RuntimeException | Error failure)
			{
				this.completeAction();
				throw failure;
			}
			finally
			{
				if (cleanup != null) cleanup.run();
			}
			final AtomicBoolean completed = new AtomicBoolean();
			try
			{
				this.execute(() ->
				{
					try
					{
						this.tryExecuteDistribution(messageType, chunks);
					}
					finally
					{
						if (completed.compareAndSet(false, true)) this.completeAction();
					}
				});
			}
			catch (final RuntimeException | Error submissionFailure)
			{
				if (completed.compareAndSet(false, true)) this.completeAction();
				throw submissionFailure;
			}
		}

		private void completeAction()
		{
			synchronized (this.lifecycleMonitor)
			{
				if (--this.activeActions == 0) this.lifecycleMonitor.notifyAll();
			}
		}

		private void tryExecuteDistribution(final MessageType messageType,
			final List<StorageBinaryDataChunker.Chunk> chunks)
		{
			if (this.failure.get() != null)
			{
				if (!this.asynchronous())
					throw new IllegalStateException("Kafka distributor has failed", this.failure.get());
				final long dropped = this.droppedAfterFailure.incrementAndGet();
				LOG.log(System.Logger.Level.WARNING,
					"Dropping queued Kafka distribution " + dropped + " after terminal failure");
				return;
			}
			try
			{
				this.executeDistribution(messageType, chunks);
			}
			catch (final RuntimeException | Error failure)
			{
				this.recordFailure(failure);
				LOG.log(System.Logger.Level.ERROR, "Kafka distribution failed", failure);
				/* Synchronous callers must observe the same failure that is retained for
				 * asynchronous health checks. Async.execute catches this rethrow at its
				 * executor boundary; Sync.execute lets it reach distributeData(). */
				throw failure;
			}
		}

		/** Records the first terminal distribution failure.
		 * @param failure terminal failure
		 */
		protected final void recordFailure(final Throwable failure)
		{
			final RuntimeException normalized = failure instanceof RuntimeException runtime
				? runtime
				: new IllegalStateException("Kafka distribution failed", failure);
			this.failure.compareAndSet(null, normalized);
		}

		/** Claims disposal ownership before an asynchronous worker is drained. */
		protected final void beginDisposal()
		{
			synchronized (this.lifecycleMonitor)
			{
				if (this.disposed) return;
				if (this.disposing) throw new IllegalStateException("Kafka distributor disposal is already in progress");
				this.disposing = true;
			}
		}

		/** Reopens admission after a bounded disposal attempt could not stop the worker. */
		protected final void cancelDisposal()
		{
			synchronized (this.lifecycleMonitor)
			{
				this.disposing = false;
				this.disposed = false;
			}
		}

		/** Releases the disposal claim after the producer has been handled. */
		protected final void finishDisposal()
		{
			synchronized (this.lifecycleMonitor) { this.disposing = false; }
		}

		private void awaitActions()
		{
			final long deadline = ReplicationRetry.deadlineNanos(TimeUnit.SECONDS.toNanos(30L));
			synchronized (this.lifecycleMonitor)
			{
				while (this.activeActions != 0)
				{
					final long remaining = ReplicationRetry.remainingNanos(deadline);
					if (remaining <= 0L)
					{
						throw new IllegalStateException("Kafka distributor has in-flight sends after disposal timeout");
					}
					try
					{
						TimeUnit.NANOSECONDS.timedWait(this.lifecycleMonitor, remaining);
					}
					catch (final InterruptedException interrupted)
					{
						Thread.currentThread().interrupt();
						throw new IllegalStateException("Interrupted while waiting for Kafka sends", interrupted);
					}
				}
			}
		}

		private void executeDistribution(final MessageType messageType, final List<StorageBinaryDataChunker.Chunk> chunks)
		{
			final long messageIndex = this.messageIndex.incrementAndGet();
			final var checksum = Crc32c.accumulator();
			for (final StorageBinaryDataChunker.Chunk chunk : chunks)
			{
				final byte[] payload = chunk.bytes();
				checksum.update(payload, 0, payload.length);
			}
			final int messageCrc32c = (int)checksum.getValue();
			for (final StorageBinaryDataChunker.Chunk chunk : chunks)
			{
				if (this.failure.get() != null)
				{
					throw new IllegalStateException("Kafka distributor has failed", this.failure.get());
				}
				final var kafkaRecord = new ProducerRecord<>(
					this.topicName, 0, PARTITION_KEY, chunk.bytes());

				KafkaHeaderCodec.addPacketHeaders(
					kafkaRecord.headers(),
					messageType,
					chunk.messageLength(),
					chunk.index(),
					chunk.count(),
					messageIndex,
					messageCrc32c
				);

				if (messageIndex % 10_000 == 0)
				{
					LOG.log(System.Logger.Level.DEBUG, "Sending kafka packet at message index " + messageIndex);
				}

				try
				{
					this.producer.send(kafkaRecord).get(30L, TimeUnit.SECONDS);
				}
			catch (final InterruptedException e)
			{
				Thread.currentThread().interrupt();
				throw new RuntimeException("Interrupted while sending the Kafka record", e);
			}
			catch (final ExecutionException e)
			{
				final Throwable cause = e.getCause();
				if (cause instanceof Error error) throw error;
				if (cause instanceof RuntimeException runtime) throw runtime;
				throw new IllegalStateException("Kafka record send failed", cause);
			}
			catch (final TimeoutException e)
			{
				throw new IllegalStateException("Timed out waiting for Kafka record acknowledgement", e);
			}

			}
		}

		@Override
		public void distributeData(final Binary data)
		{
			this.distribute(MessageType.DATA, data);
		}

		@Override
		public void distributeTypeDictionary(final String typeDictionaryData)
		{
			final java.nio.ByteBuffer buffer = XMemory.toDirectByteBuffer(
				KafkaHeaderCodec.serializeString(typeDictionaryData));
			final Runnable cleanup = releaseOnce(buffer);
			try
			{
				this.distribute(MessageType.TYPE_DICTIONARY, ChunksWrapper.New(buffer), cleanup);
			}
			catch (final RuntimeException | Error failure)
			{
				cleanup.run();
				throw failure;
			}
		}

		private static Runnable releaseOnce(final java.nio.ByteBuffer buffer)
		{
			final AtomicBoolean released = new AtomicBoolean();
			return () ->
			{
				if (released.compareAndSet(false, true)) XMemory.deallocateDirectByteBuffer(buffer);
			};
		}

		/** Set the next packet index; callers must do this while the distributor is quiescent. */
		@Override
		public void messageIndex(final long index)
		{
			if (index < -1L || index == Long.MAX_VALUE)
			{
				throw new IllegalArgumentException("message index must be in [-1, Long.MAX_VALUE)");
			}
			synchronized (this.lifecycleMonitor)
			{
				if (this.activeActions != 0)
				{
					throw new IllegalStateException("Kafka message index can only change while the distributor is quiescent");
				}
				if (this.disposed || this.disposing)
				{
					throw new IllegalStateException("Kafka distributor is not accepting index changes");
				}
				LOG.log(System.Logger.Level.INFO, "Setting distributor message index to " + index);
				this.messageIndex.set(index);
			}
		}

		/** Returns the latest packet index; concurrent writes may advance it immediately. */
		@Override
		public long messageIndex()
		{
			return this.messageIndex.get();
		}

		/** Returns the terminal send failure, or {@code null} while healthy. */
		public RuntimeException failure()
		{
			return this.failure.get();
		}

		/**
		 * Returns whether the producer has completed disposal.
		 *
		 * @return {@code true} after the producer has completed disposal
		 */
		protected final boolean isDisposed()
		{
			return this.disposed;
		}

		@Override
		public boolean ignoreDistribution()
		{
			return this.ignoreDistribution.get();
		}

		@Override
		public void ignoreDistribution(final boolean ignore)
		{
			this.ignoreDistribution.set(ignore);
		}

		@Override
		public void dispose()
		{
			this.beginDisposal();
			if (this.isDisposed()) return;
			this.disposeClaimed();
		}

		/** Completes a disposal claim after any asynchronous worker has stopped. */
		protected final void disposeClaimed()
		{
			if (this.isDisposed()) return;
			LOG.log(System.Logger.Level.DEBUG, "Disposing data distributor");
			try
			{
				this.awaitActions();
				synchronized (this.lifecycleMonitor) { this.disposed = true; }
				this.producer.close(java.time.Duration.ofSeconds(5L));
			}
			catch (final RuntimeException | Error failure)
			{
				/* A failed close must remain retryable; otherwise a transient broker or
				 * interrupt leaves the producer permanently owned but unreachable. */
				this.cancelDisposal();
				throw failure;
			}
			this.finishDisposal();
		}
	}

	/** Sends records on the calling thread; distribution blocks until Kafka acknowledges them. */
	class Sync extends Abstract
	{
		private Sync(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			super(topicName, kafkaPropertiesProvider);
		}

		@Override
		protected void execute(final Runnable action)
		{
			action.run();
		}

		@Override
		protected boolean asynchronous() { return false; }
	}

	/** Sends packets on a dedicated executor without blocking the caller. */
		class Async extends Abstract
		{
		private static final int MAX_QUEUED_ACTIONS = 1;
		private final ThreadPoolExecutor executor;

		private Async(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			super(topicName, kafkaPropertiesProvider);
			this.executor = new ThreadPoolExecutor(
				1, 1, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(MAX_QUEUED_ACTIONS),
				this::createThread,
				new ThreadPoolExecutor.AbortPolicy());
		}

		private Thread createThread(final Runnable runnable)
		{
			final Thread thread = new Thread(runnable);
			thread.setName("Eclipse-Datagrid-StorageDistributor-Kafka");
			thread.setDaemon(true);
			return thread;
		}

		@Override
		protected void execute(final Runnable action)
		{
			/* Keep the single worker alive after a failed task.  The terminal failure is
			 * retained by tryExecuteDistribution; allowing the exception to escape the
			 * executor would kill the worker and make queued tasks disappear without the
			 * explicit dropped-after-failure diagnostic. */
			final Runnable guarded = () ->
			{
				try
				{
					action.run();
				}
				catch (final Throwable failure)
				{
					recordFailure(failure);
					if (failure instanceof Error error) throw error;
				}
			};
			try
			{
				this.executor.execute(guarded);
			}
			catch (final RejectedExecutionException rejected)
			{
				if (this.executor.isShutdown()) throw rejected;
				try
				{
					if (!this.executor.getQueue().offer(guarded, 30L, TimeUnit.SECONDS))
						throw new RejectedExecutionException("Kafka distributor queue remained full", rejected);
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Interrupted while waiting for Kafka distributor capacity", interrupted);
				}
			}
		}

		@Override
		protected boolean asynchronous() { return true; }

		@Override
		public void dispose()
		{
			this.beginDisposal();
			if (this.isDisposed()) return;
			if (!this.executor.isShutdown())
			{
				this.executor.shutdown();
			}
			try
			{
					if (!this.executor.awaitTermination(30, TimeUnit.SECONDS))
					{
						this.executor.shutdownNow();
						if (!this.executor.awaitTermination(5, TimeUnit.SECONDS))
						{
							this.cancelDisposal();
							throw new IllegalStateException(
								"Kafka distributor worker did not stop; producer remains open for retry");
						}
					}
				}
				catch (final InterruptedException interrupted)
				{
					this.executor.shutdownNow();
					Thread.currentThread().interrupt();
					this.cancelDisposal();
					throw new IllegalStateException(
						"Interrupted while stopping Kafka distributor; producer remains open for retry", interrupted);
				}
			this.disposeClaimed();
		}
	}
}
