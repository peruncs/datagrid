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

import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

public interface StorageBinaryDataDistributorKafka
	extends
	StorageBinaryDataDistributor
{
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

	abstract class Abstract implements StorageBinaryDataDistributorKafka
	{
		private final Properties kafkaProperties;
		private final String topicName;
		private KafkaProducer<String, byte[]> kafkaProducer;

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

			return new KafkaProducer<>(properties);
		}

		private void distribute(
			final MessageType messageType,
			final Binary data
		)
		{
			this.execute(() -> this.executeDistribution(messageType, data));
		}

		private void executeDistribution(
			final MessageType messageType,
			final Binary data
		)
		{
			final KafkaProducer<String, byte[]> producer = this.ensureProducer();
			StorageBinaryDataChunker.forEach(data, StorageBinaryDistributedKafka.maxPacketSize(), chunk ->
			{
				final ProducerRecord<String, byte[]> record = new ProducerRecord<>(this.topicName, chunk.bytes());
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
					throw new InterruptException("Interrupted while sending the Kafka record", e);
				}
				catch (final ExecutionException e)
				{
					final var cause = e.getCause();
					if (cause instanceof final RuntimeException rte)
					{
						throw rte;
					}
					else
					{
						throw new RuntimeException(cause);
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
			this.distribute(
				MessageType.TYPE_DICTIONARY,
				ChunksWrapper.New(
					XMemory.toDirectByteBuffer(
						StorageBinaryDistributedKafka.serialize(typeDictionaryData)
					)
				)
			);
		}

		@Override
		public synchronized void dispose()
		{
			if (this.kafkaProducer != null)
			{
				this.kafkaProducer.close();
			}

		}

	}

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
			this.executor.execute(action);
		}

		@Override
		public void dispose()
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
