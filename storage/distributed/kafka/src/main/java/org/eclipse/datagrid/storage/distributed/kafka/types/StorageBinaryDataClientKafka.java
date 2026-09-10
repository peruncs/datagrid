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


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacketAcceptor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

public interface StorageBinaryDataClientKafka extends StorageBinaryDataClient
{
	/** Returns the terminal consumer failure, or {@code null} while healthy. */
	RuntimeException failure();

	static StorageBinaryDataClientKafka New(
		final Properties kafkaProperties,
		final String topicName,
		final String clientId,
		final StorageBinaryDataReceiver receiver
	)
	{
		return New(
			kafkaProperties,
			topicName,
			clientId,
			StorageBinaryDataPacketAcceptor.New(receiver)
		);
	}

	static StorageBinaryDataClientKafka New(
		final Properties kafkaProperties,
		final String topicName,
		final String clientId,
		final StorageBinaryDataPacketAcceptor packetAcceptor
	)
	{
		return new StorageBinaryDataClientKafka.Default(
			notNull(kafkaProperties),
			notEmpty(topicName),
			notEmpty(clientId),
			notNull(packetAcceptor)
		);
	}

	class Default implements StorageBinaryDataClientKafka
	{
		private final Properties kafkaProperties;
		private final String topicName;
		private final String clientId;
		private final StorageBinaryDataPacketAcceptor packetAcceptor;
		private final AtomicBoolean active = new AtomicBoolean();
		private volatile Thread thread;
		private volatile KafkaConsumer<String, byte[]> consumer;
		private volatile boolean disposed;
		private volatile RuntimeException failure;

		Default(
			final Properties kafkaProperties,
			final String topicName,
			final String clientId,
			final StorageBinaryDataPacketAcceptor packetAcceptor
		)
		{
			super();
			this.kafkaProperties = kafkaProperties;
			this.topicName = topicName;
			this.clientId = clientId;
			this.packetAcceptor = packetAcceptor;
		}

		@Override
		public synchronized void start()
		{
			if (this.disposed) throw new IllegalStateException("Kafka client is disposed");
			if (this.failure != null) throw new IllegalStateException("Kafka client has failed", this.failure);
			if (this.active.getAndSet(true)) return;

			this.thread = new Thread(this::run, "datagrid-kafka-reader");
			this.thread.setDaemon(true);
			this.thread.start();
		}

		private void run()
		{
			final Properties properties = new Properties();
			properties.putAll(this.kafkaProperties);
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.clientId);
			properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
			try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties))
			{
				this.consumer = consumer;
				final int partitionCount = consumer.partitionsFor(this.topicName).size();
				if (partitionCount != 1)
				{
					throw new IllegalStateException(
						"Kafka replication topic must have exactly one partition; found " + partitionCount
					);
				}
				consumer.subscribe(Collections.singletonList(this.topicName));
				while (this.active.get())
				{
					this.consume(consumer.poll(Duration.ofMillis(250L)));
					consumer.commitSync();
				}
			}
			catch (final Throwable failure)
			{
				if (this.active.get())
				{
					this.failure = failure instanceof RuntimeException runtime
						? runtime
						: new IllegalStateException("Kafka client stopped unexpectedly", failure);
				}
			}
			finally
			{
				this.consumer = null;
				this.active.set(false);
			}
		}

		@Override
		public RuntimeException failure()
		{
			return this.failure;
		}

		private void consume(final ConsumerRecords<String, byte[]> records)
		{
			final List<StorageBinaryDataPacket> packets = new ArrayList<>();
			for (final ConsumerRecord<String, byte[]> record : records)
			{
				packets.add(this.createDataPacket(record));
			}
			if (!packets.isEmpty())
			{
				this.packetAcceptor.accept(packets);
			}
		}

		private StorageBinaryDataPacket createDataPacket(final ConsumerRecord<String, byte[]> record)
		{
			final Headers headers = record.headers();
			return StorageBinaryDataPacket.New(
				StorageBinaryDistributedKafka.messageType(headers),
				StorageBinaryDistributedKafka.messageLength(headers),
				StorageBinaryDistributedKafka.packetIndex(headers),
				StorageBinaryDistributedKafka.packetCount(headers),
				ByteBuffer.wrap(record.value())
			);
		}

		/**
		 * Wakes the Kafka poll before joining the reader. A timeout leaves the
		 * packet acceptor open so the caller can retry disposal safely.
		 */
		@Override
		public synchronized void dispose()
		{
			if (this.disposed) return;
			this.disposed = true;
			this.active.set(false);
			final Thread current = this.thread;
			if (current != null && current != Thread.currentThread())
			{
				final KafkaConsumer<String, byte[]> consumer = this.consumer;
				if (consumer != null)
				{
					consumer.wakeup();
				}
				current.interrupt();
				try
				{
					current.join(5_000L);
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
				}
				if (current.isAlive())
				{
					this.disposed = false;
					throw new IllegalStateException("Kafka client reader did not stop before disposal timeout");
				}
			}
			/* Keep the thread reference until the join succeeds. A timed-out disposal
			 * is retryable and must still be able to join the same reader. */
			this.thread = null;
		}

	}

}
