package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataChunker;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

/**
 * This distributor publishes storage packets to Kafka.
 *
 * <p>Every packet uses one stable partition key, so Kafka keeps the packet
 * order required to rebuild a transaction. The asynchronous variant may
 * return before Kafka finishes; callers must observe its failure state and
 * dispose the distributor during shutdown.</p>
 */
public interface StorageBinaryDataDistributorKafka
	extends
		StorageBinaryDataDistributor
{
	/** Stable key used to keep every packet for a stream on one Kafka partition. */
	String PARTITION_KEY = "eclipse-datagrid-replication";
	static StorageBinaryDataDistributorKafka Sync(
            final Properties kafkaProperties,
            final String topicName
    )
	{
		return new StorageBinaryDataDistributorKafka.Sync(
			notNull(kafkaProperties),
			notEmpty(topicName)
		);
	}

	static StorageBinaryDataDistributorKafka Async(
            final Properties kafkaProperties,
            final String topicName
    )
	{
		return new StorageBinaryDataDistributorKafka.Async(
			notNull(kafkaProperties),
			notEmpty(topicName)
		);
	}

	/** Shared producer, failure, and disposal behavior for both modes. */
	abstract class Abstract implements StorageBinaryDataDistributorKafka
	{
		protected static final System.Logger LOG = System.getLogger(StorageBinaryDataDistributorKafka.class.getName());
		private final Properties kafkaProperties;
		private final String topicName;
		private KafkaProducer<String, byte[]> kafkaProducer;
		private volatile RuntimeException failure;
		protected final AtomicLong droppedAfterFailure = new AtomicLong();
		private volatile boolean disposed;
		private volatile boolean disposing;

		Abstract(
			final Properties kafkaProperties,
			final String topicName
		)
		{
			super();
			this.kafkaProperties = kafkaProperties;
			this.topicName = topicName;
		}

		protected abstract void execute(Runnable action);

		private synchronized KafkaProducer<String, byte[]> ensureProducer()
		{
			return this.kafkaProducer != null
				? this.kafkaProducer
				: (this.kafkaProducer = this.createProducer());
		}

		private KafkaProducer<String, byte[]> createProducer()
		{
			final Properties properties = new Properties();
			properties.putAll(this.kafkaProperties);
			properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

			KafkaProducer<String, byte[]> producer = null;
			try
			{
				producer = new KafkaProducer<>(properties);
				final int partitionCount = producer.partitionsFor(this.topicName).size();
				if (partitionCount != 1)
				{
					throw new IllegalArgumentException(
						"Kafka replication topic must have exactly one partition; found " + partitionCount
					);
				}
				return producer;
			}
			catch (final RuntimeException | Error failure)
			{
				if (producer != null)
				{
					try { producer.close(); }
					catch (final RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
				}
				throw failure;
			}
		}

		private void distribute(
			final MessageType messageType,
			final Binary data
		)
		{
			this.distribute(messageType, data, null);
		}

		private void distribute(
			final MessageType messageType,
			final Binary data,
			final Runnable cleanup
		)
		{
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
			try
			{
				this.execute(() ->
				{
					try
					{
						this.executeDistribution(messageType, data);
					}
					finally
					{
						if (cleanup != null) cleanup.run();
					}
				});
			}
			catch (final RuntimeException | Error failure)
			{
				if (cleanup != null) cleanup.run();
				/* Sync execution and executor submission failures must establish the same
				 * terminal state; otherwise a later write can silently retry a broken log. */
				this.recordFailure(failure);
				throw failure;
			}
		}

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

		public final RuntimeException failure()
		{
			return this.failure;
		}

		private void executeDistribution(
			final MessageType messageType,
			final Binary data
		)
		{
			if (this.failure != null)
			{
				final long dropped = this.droppedAfterFailure.incrementAndGet();
				LOG.log(System.Logger.Level.WARNING,
					"Dropping queued Kafka distribution " + dropped + " after terminal failure");
				return;
			}
			final KafkaProducer<String, byte[]> producer = this.ensureProducer();
			StorageBinaryDataChunker.forEach(data, StorageBinaryDistributedKafka.maxPacketSize(), chunk ->
			{
				if (this.failure != null)
				{
					throw new IllegalStateException("Kafka distributor has failed", this.failure);
				}
				final ProducerRecord<String, byte[]> record = new ProducerRecord<>(
					this.topicName, PARTITION_KEY, chunk.bytes());
				StorageBinaryDistributedKafka.addPacketHeaders(
					record.headers(),
					messageType,
					chunk.messageLength(),
					chunk.index(),
					chunk.count()
				);

				try
				{
					producer.send(record).get();
				}
					catch (final InterruptedException e)
					{
						Thread.currentThread().interrupt();
						throw new InterruptException("Interrupted while sending the Kafka record", e);
					}
				catch (final ExecutionException e)
				{
					final var cause = e.getCause();
					if (cause instanceof final Error error)
					{
						throw error;
					}
					if (cause instanceof final RuntimeException rte)
					{
						throw rte;
					}
					else
					{
						throw new IllegalStateException("Kafka record send failed", cause);
					}
				}

			});
		}

		@Override
		public void distributeData(final Binary data)
		{
			this.distribute(
				MessageType.DATA,
				data
			);
		}

		@Override
		public void distributeTypeDictionary(final String typeDictionaryData)
		{
			final java.nio.ByteBuffer buffer = XMemory.toDirectByteBuffer(
				StorageBinaryDistributedKafka.serialize(typeDictionaryData));
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

		@Override
		public synchronized void dispose()
		{
			if (this.disposed) return;
			if (!this.disposing) this.disposing = true;
			this.disposed = true;
			if (this.kafkaProducer != null)
			{
				try
				{
					this.kafkaProducer.close();
				}
				catch (final RuntimeException | Error failure)
				{
					this.disposed = false;
					this.disposing = false;
					throw failure;
				}
			}
			this.disposing = false;

		}

	}

	/** Sends records on the calling thread; distribution blocks until Kafka acknowledges them. */
	class Sync extends Abstract
	{
		Sync(
			final Properties kafkaProperties,
			final String topicName
		)
		{
			super(kafkaProperties, topicName);
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

		Async(
			final Properties kafkaProperties,
			final String topicName
		)
		{
			super(kafkaProperties, topicName);
			this.executor = Executors.newSingleThreadExecutor(this::createThread);
		}

		private Thread createThread(final Runnable runnable)
		{
			final Thread thread = new Thread(runnable);
			thread.setName("StorageDistributor-Kafka");
			return thread;
		}

		@Override
		protected void execute(final Runnable action)
		{
			// The wrapper records failures because the base implementation submits
			// arbitrary actions; the cluster distributor performs this capture in its
			// own tryExecuteDistribution method.
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
		public void dispose()
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
