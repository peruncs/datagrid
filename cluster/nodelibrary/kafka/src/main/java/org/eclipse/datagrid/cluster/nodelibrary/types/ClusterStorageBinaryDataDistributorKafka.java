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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataChunker;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public interface ClusterStorageBinaryDataDistributorKafka extends ClusterStorageBinaryDataDistributor
{
	/** Stable key used to keep every packet for a stream on one Kafka partition. */
	String PARTITION_KEY = "eclipse-datagrid-replication";
	static ClusterStorageBinaryDataDistributorKafka Sync(
		final String topicName,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		return new ClusterStorageBinaryDataDistributorKafka.Sync(notEmpty(topicName), notNull(kafkaPropertiesProvider));
	}

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

	abstract class Abstract implements ClusterStorageBinaryDataDistributorKafka
	{
		private static final Logger LOG = LoggerFactory.getLogger(ClusterStorageBinaryDataDistributorKafka.class);

		private final String topicName;
		private final KafkaProducer<String, byte[]> producer;
		private final AtomicLong messageIndex = new AtomicLong(Long.MIN_VALUE);
		private final AtomicLong droppedAfterFailure = new AtomicLong();
		private final AtomicBoolean ignoreDistribution = new AtomicBoolean();
		private volatile RuntimeException failure;

		protected Abstract(final String topicName, final KafkaPropertiesProvider kafkaPropertiesProvider)
		{
			this.topicName = topicName;

			final Properties properties = kafkaPropertiesProvider.provide();
			properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
			properties.setProperty(COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
			this.producer = new KafkaProducer<>(properties);
			final int partitionCount = this.producer.partitionsFor(topicName).size();
			if (partitionCount != 1)
			{
				this.producer.close();
				throw new IllegalArgumentException(
					"Kafka replication topic must have exactly one partition; found " + partitionCount
				);
			}
		}

		protected abstract void execute(Runnable action);

		private void distribute(final MessageType messageType, final Binary data)
		{
			if (this.failure != null) throw new IllegalStateException("Kafka distributor has failed", this.failure);
			if (this.ignoreDistribution.get())
			{
				LOG.trace("Ignoring distribution for data of type {}", messageType);
				return;
			}
			this.execute(() -> this.tryExecuteDistribution(messageType, data));
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
			}
		}

		private synchronized void recordFailure(final Throwable failure)
		{
			if (this.failure == null)
			{
				this.failure = failure instanceof RuntimeException runtime
					? runtime
					: new IllegalStateException("Kafka distribution failed", failure);
			}
		}

		private void executeDistribution(final MessageType messageType, final Binary data) throws InterruptedException
		{
			StorageBinaryDataChunker.forEach(data, ClusterStorageBinaryDistributedKafka.maxPacketSize(), chunk ->
			{
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
					// Kafka only throws RuntimeException's
					throw (RuntimeException)e.getCause();
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
			this.distribute(
				MessageType.TYPE_DICTIONARY,
				ChunksWrapper.New(
					XMemory.toDirectByteBuffer(ClusterStorageBinaryDistributedKafka.serializeString(typeDictionaryData))
				)
			);
		}

		/** Set the next packet index; callers must do this while the distributor is quiescent. */
		@Override
		public void messageIndex(final long index)
		{
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
			LOG.trace("Disposing data distributor");
			this.producer.close();
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
			// Async submission is drained by dispose(); tryExecuteDistribution owns
			// failure capture so queued actions use the same terminal-state policy.
			this.executor.execute(action);
		}

		@Override
		public synchronized void dispose()
		{
			if (!this.executor.isShutdown())
			{
				this.executor.shutdown();
			}
			try
			{
				if (!this.executor.awaitTermination(30, TimeUnit.SECONDS))
				{
					this.executor.shutdownNow();
					this.executor.awaitTermination(5, TimeUnit.SECONDS);
				}
			}
			catch (final InterruptedException interrupted)
			{
				this.executor.shutdownNow();
				Thread.currentThread().interrupt();
			}
			super.dispose();
		}
	}
}
