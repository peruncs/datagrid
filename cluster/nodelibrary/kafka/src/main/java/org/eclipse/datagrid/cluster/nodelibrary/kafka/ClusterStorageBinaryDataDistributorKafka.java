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
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataChunker;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
		private static final Logger LOG = LoggerFactory.getLogger(ClusterStorageBinaryDataDistributorKafka.class);

		private final String topicName;
		private final KafkaProducer<String, byte[]> producer;
		private final AtomicLong messageIndex = new AtomicLong(-1L);
		private final AtomicLong droppedAfterFailure = new AtomicLong();
		private final AtomicBoolean ignoreDistribution = new AtomicBoolean();
		private volatile RuntimeException failure;
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
			properties.setProperty(COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
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

		private void distribute(final MessageType messageType, final Binary data)
		{
			this.distribute(messageType, data, null);
		}

		private void distribute(final MessageType messageType, final Binary data, final Runnable cleanup)
		{
			Objects.requireNonNull(data, "data");
			if (this.disposed || this.disposing)
			{
				if (cleanup != null) cleanup.run();
				throw new IllegalStateException(this.disposed ? "Kafka distributor is disposed" : "Kafka distributor is stopping");
			}
			if (this.failure != null)
			{
				if (cleanup != null) cleanup.run();
				throw new IllegalStateException("Kafka distributor has failed", this.failure);
			}
			if (this.ignoreDistribution.get())
			{
				LOG.trace("Ignoring distribution for data of type {}", messageType);
				if (cleanup != null) cleanup.run();
				return;
			}
			try
			{
				this.execute(() ->
				{
					try
					{
						this.tryExecuteDistribution(messageType, data);
					}
					finally
					{
						if (cleanup != null) cleanup.run();
					}
				});
			}
			catch (final RuntimeException | Error submissionFailure)
			{
				if (cleanup != null) cleanup.run();
				this.recordFailure(submissionFailure);
				throw submissionFailure;
			}
		}

		private void tryExecuteDistribution(final MessageType messageType, final Binary data)
		{
			if (this.failure != null)
			{
				final long dropped = this.droppedAfterFailure.incrementAndGet();
				LOG.warn("Dropping queued Kafka distribution {} after terminal failure", dropped);
				return;
			}
			try
			{
				this.executeDistribution(messageType, data);
			}
			catch (final Throwable t)
			{
				this.recordFailure(t);
				LOG.error("Kafka distribution failed", t);
				/* Synchronous callers must observe the same failure that is retained for
				 * asynchronous health checks.  Async.execute catches this rethrow at its
				 * executor boundary; Sync.execute lets it reach distributeData(). */
				if (t instanceof Error error) throw error;
				if (t instanceof RuntimeException runtime) throw runtime;
				throw new IllegalStateException("Kafka distribution failed", t);
			}
		}

		/** Records the first terminal distribution failure.
		 * @param failure terminal failure
		 */
		protected final synchronized void recordFailure(final Throwable failure)
		{
			if (this.failure == null)
			{
				this.failure = failure instanceof RuntimeException runtime
					? runtime
					: new IllegalStateException("Kafka distribution failed", failure);
			}
		}

		/** Claims disposal ownership before an asynchronous worker is drained. */
		protected final synchronized void beginDisposal()
		{
			if (this.disposed) return;
			if (this.disposing) throw new IllegalStateException("Kafka distributor disposal is already in progress");
			this.disposing = true;
		}

		/** Reopens admission after a bounded disposal attempt could not stop the worker. */
		protected final synchronized void cancelDisposal()
		{
			this.disposing = false;
			this.disposed = false;
		}

		/** Releases the disposal claim after the producer has been handled. */
		protected final synchronized void finishDisposal()
		{
			this.disposing = false;
		}

		private void executeDistribution(final MessageType messageType, final Binary data) throws InterruptedException
		{
			StorageBinaryDataChunker.forEach(data, ClusterStorageBinaryDistributedKafka.maxPacketSize(), chunk ->
			{
				if (this.failure != null)
				{
					throw new IllegalStateException("Kafka distributor has failed", this.failure);
				}
				final var kafkaRecord = new ProducerRecord<String, byte[]>(
					this.topicName, PARTITION_KEY, chunk.bytes());

				final long messageIndex = this.messageIndex.incrementAndGet();

				ClusterStorageBinaryDistributedKafka.addPacketHeaders(
					kafkaRecord.headers(),
					messageType,
					chunk.messageLength(),
					chunk.index(),
					chunk.count(),
					messageIndex
				);

				if (LOG.isDebugEnabled() && messageIndex % 10_000 == 0)
				{
					LOG.debug("Sending kafka packet at message index {}", messageIndex);
				}

				try
				{
					this.producer.send(kafkaRecord).get();
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

			});
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
				ClusterStorageBinaryDistributedKafka.serializeString(typeDictionaryData));
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
			LOG.info("Setting distributor message index to {}", index);
			this.messageIndex.set(index);
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
			return this.failure;
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
		public synchronized void dispose()
		{
			if (this.disposed) return;
			if (!this.disposing) this.disposing = true;
			this.disposed = true;
			LOG.trace("Disposing data distributor");
			try
			{
				this.producer.close();
			}
			catch (final RuntimeException | Error failure)
			{
				/* A failed close must remain retryable; otherwise a transient broker or
				 * interrupt leaves the producer permanently owned but unreachable. */
				this.disposed = false;
				this.disposing = false;
				throw failure;
			}
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
	}

	/** Sends packets on a dedicated executor without blocking the caller. */
	class Async extends Abstract
	{
		private final ExecutorService executor;

		private Async(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			super(topicName, kafkaPropertiesProvider);
			this.executor = Executors.newSingleThreadExecutor(this::createThread);
		}

		private Thread createThread(final Runnable runnable)
		{
			final Thread thread = new Thread(runnable);
			thread.setName("Eclipse-Datagrid-StorageDistributor-Kafka");
			return thread;
		}

		@Override
		protected void execute(final Runnable action)
		{
			/* Keep the single worker alive after a failed task.  The terminal failure is
			 * retained by tryExecuteDistribution; allowing the exception to escape the
			 * executor would kill the worker and make queued tasks disappear without the
			 * explicit dropped-after-failure diagnostic. */
			this.executor.execute(() ->
			{
				try
				{
					action.run();
				}
				catch (final Throwable failure)
				{
					recordFailure(failure);
				}
			});
		}

		@Override
		public synchronized void dispose()
		{
			this.beginDisposal();
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
			try
			{
				super.dispose();
			}
			finally
			{
				this.finishDisposal();
			}
		}
	}
}
