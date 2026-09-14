package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.storage.distributed.types.*;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.serializer.typing.Disposable;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

/** Consumes validated neutral Store packets from one Kafka partition. */
public interface StorageBinaryDataClientKafka extends StorageBinaryDataClient
{
	/** Returns the terminal consumer failure, or {@code null} while healthy.
	 * @return terminal consumer failure, or {@code null}
	 */
	RuntimeException failure();

	/** Creates a client that forwards complete messages to a receiver.
	 * @param kafkaProperties Kafka consumer properties
	 * @param topicName single-partition replication topic
	 * @param clientId consumer group and client identity
	 * @param receiver destination receiver
	 * @return Kafka storage-data client
	 */
	static StorageBinaryDataClientKafka New(
		final Properties kafkaProperties,
		final String topicName,
		final String clientId,
		final StorageBinaryDataReceiver receiver)
	{
		return New(kafkaProperties, topicName, clientId, StorageBinaryDataPacketAcceptor.New(receiver));
	}

	/** Creates a client that forwards complete messages to an acceptor.
	 * @param kafkaProperties Kafka consumer properties
	 * @param topicName single-partition replication topic
	 * @param clientId consumer group and client identity
	 * @param packetAcceptor packet destination
	 * @return Kafka storage-data client
	 */
	static StorageBinaryDataClientKafka New(
		final Properties kafkaProperties,
		final String topicName,
		final String clientId,
		final StorageBinaryDataPacketAcceptor packetAcceptor)
	{
		return new Default(notNull(kafkaProperties), notEmpty(topicName), notEmpty(clientId), notNull(packetAcceptor));
	}

	/** Polls Kafka on one thread and forwards complete packet groups. */
	final class Default implements StorageBinaryDataClientKafka
	{
		private static final System.Logger LOG =
			System.getLogger(StorageBinaryDataClientKafka.class.getName());
		private static final Duration POLL_TIMEOUT = Duration.ofMillis(250L);
		private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(30L);
		private static final Duration PARTITION_LOOKUP_TIMEOUT = Duration.ofSeconds(30L);

		private final Properties kafkaProperties;
		private final String topicName;
		private final String clientId;
		private final StorageBinaryDataPacketAcceptor packetAcceptor;
		private final AtomicBoolean active = new AtomicBoolean();
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private final List<StorageBinaryDataPacket> pendingPackets = new ArrayList<>();
		private long pendingMessageIndex = Long.MIN_VALUE;
		private long lastMessageIndex = -1L;
		private int pendingMessageLength;
		private int pendingPacketCount;
		private MessageType pendingMessageType;
		private int pendingCrc32c;
		private java.util.zip.CRC32C pendingChecksum;
		private volatile Thread thread;
		private volatile KafkaConsumer<String, byte[]> consumer;
		private volatile boolean disposed;
		private boolean packetAcceptorDisposed;

		Default(
			final Properties kafkaProperties,
			final String topicName,
			final String clientId,
			final StorageBinaryDataPacketAcceptor packetAcceptor)
		{
			this.kafkaProperties = new Properties();
			this.kafkaProperties.putAll(kafkaProperties);
			this.topicName = topicName;
			this.clientId = clientId;
			this.packetAcceptor = packetAcceptor;
		}

		@Override
		public synchronized void start()
		{
			if (this.disposed) throw new IllegalStateException("Kafka client is disposed");
			if (this.failure.get() != null) throw new IllegalStateException("Kafka client has failed", this.failure.get());
			if (this.active.get()) return;
			final Thread existing = this.thread;
			if (existing != null && existing.isAlive())
				throw new IllegalStateException("Kafka client reader is still stopping");
			this.active.set(true);
			try
			{
				this.thread = new Thread(this::run, "datagrid-legacy-kafka-reader");
				this.thread.setDaemon(true);
				this.thread.start();
			}
			catch (final RuntimeException | Error failure)
			{
				this.active.set(false);
				this.thread = null;
				throw failure;
			}
		}

		private void run()
		{
			final Properties properties = new Properties();
			properties.putAll(this.kafkaProperties);
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.clientId);
			properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			properties.put(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
			properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
			properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
			try (final KafkaConsumer<String, byte[]> kafka = new KafkaConsumer<>(properties))
			{
				this.consumer = kafka;
				final List<TopicPartition> partitions = kafka.partitionsFor(this.topicName, PARTITION_LOOKUP_TIMEOUT)
					.stream().map(info -> new TopicPartition(info.topic(), info.partition())).toList();
					if (partitions.size() != 1)
						throw new IllegalStateException("Kafka replication topic must have exactly one partition; found " + partitions.size());
					/* This client is deliberately a single-partition replication reader. Group
					 * rebalances can otherwise move it to a different assignment while a
					 * transaction is being assembled. */
					kafka.assign(partitions);
				while (this.active.get())
				{
					if (this.consume(kafka.poll(POLL_TIMEOUT))) kafka.commitSync(COMMIT_TIMEOUT);
				}
			}
			catch (final Throwable failure)
			{
				if (this.active.get()) this.recordFailure(failure);
			}
			finally
			{
				this.consumer = null;
				this.active.set(false);
			}
		}

		private void recordFailure(final Throwable throwable)
		{
			this.failure.compareAndSet(null, throwable instanceof RuntimeException runtime
				? runtime : new IllegalStateException("Kafka client stopped unexpectedly", throwable));
			LOG.log(System.Logger.Level.ERROR, "Kafka client stopped unexpectedly", throwable);
		}

		private boolean consume(final ConsumerRecords<String, byte[]> records)
		{
			boolean sawRecord = false;
			for (final ConsumerRecord<String, byte[]> record : records)
			{
				sawRecord = true;
				if (record.value() == null) continue;
				if (record.value().length == 0)
					throw new IllegalStateException("empty Kafka replication record is not a packet");
				this.acceptPacket(record);
			}
			/* Never commit past an incomplete packet group. Tombstones are safe to commit
			 * only when there is no pending message before them. */
			return sawRecord && this.pendingPackets.isEmpty();
		}

		private void acceptPacket(final ConsumerRecord<String, byte[]> record)
		{
			final Headers headers = record.headers();
			final int length = StorageBinaryDistributedKafka.messageLength(headers);
			final int index = StorageBinaryDistributedKafka.packetIndex(headers);
			final int count = StorageBinaryDistributedKafka.packetCount(headers);
			final long messageIndex = StorageBinaryDistributedKafka.messageIndex(headers);
			StorageBinaryDistributedKafka.validateMetadata(length, index, count, messageIndex);
			final int expectedCount = (length + StorageBinaryDistributedKafka.maxPacketSize() - 1) /
				StorageBinaryDistributedKafka.maxPacketSize();
			if (count != expectedCount || record.value().length > StorageBinaryDistributedKafka.maxPacketSize())
				throw new IllegalStateException("invalid Kafka replication packet dimensions");

			if (index == 0)
			{
				if (!this.pendingPackets.isEmpty())
					throw new IllegalStateException("Kafka replication message started before the previous message completed");
				if (messageIndex >= 0 && messageIndex <= this.lastMessageIndex)
					throw new IllegalStateException("Kafka replication message index is not increasing: " + messageIndex);
				this.pendingMessageIndex = messageIndex;
				this.pendingMessageLength = length;
				this.pendingPacketCount = count;
				this.pendingMessageType = StorageBinaryDistributedKafka.messageType(headers);
				this.pendingCrc32c = StorageBinaryDistributedKafka.messageCrc32c(headers);
				this.pendingChecksum = Crc32c.accumulator();
			}
			else
			{
				if (this.pendingPackets.isEmpty() || messageIndex != this.pendingMessageIndex ||
					length != this.pendingMessageLength || count != this.pendingPacketCount ||
					StorageBinaryDistributedKafka.messageType(headers) != this.pendingMessageType ||
					StorageBinaryDistributedKafka.messageCrc32c(headers) != this.pendingCrc32c)
					throw new IllegalStateException("Kafka replication packet metadata changed within a message");
			}
			if (index != this.pendingPackets.size())
				throw new IllegalStateException("Kafka replication packet index is not contiguous: " + index);
			this.pendingChecksum.update(record.value(), 0, record.value().length);
			this.pendingPackets.add(StorageBinaryDataPacket.New(
				this.pendingMessageType, length, index, count, ByteBuffer.wrap(record.value())));
			if (this.pendingPackets.size() == this.pendingPacketCount)
			{
				int bytes = 0;
				for (final StorageBinaryDataPacket packet : this.pendingPackets) bytes += packet.buffer().remaining();
				if (bytes != this.pendingMessageLength || (int)this.pendingChecksum.getValue() != this.pendingCrc32c)
					throw new IllegalStateException("Kafka replication message length or checksum mismatch");
				final List<StorageBinaryDataPacket> complete = List.copyOf(this.pendingPackets);
				this.packetAcceptor.accept(complete);
				if (!this.packetAcceptor.isAtMessageBoundary())
					throw new IllegalStateException("packet acceptor retained an incomplete complete message");
				if (this.pendingMessageIndex >= 0) this.lastMessageIndex = this.pendingMessageIndex;
				this.pendingPackets.clear();
				this.pendingMessageIndex = Long.MIN_VALUE;
			}
		}

		@Override
		public RuntimeException failure() { return this.failure.get(); }

		/** Wakes and joins the polling thread before disposing the acceptor. */
		@Override
		public synchronized void dispose()
		{
			if (this.disposed && this.packetAcceptorDisposed) return;
			this.disposed = true;
			this.active.set(false);
			final Thread current = this.thread;
			if (current == Thread.currentThread())
			{
				this.disposed = false;
				throw new IllegalStateException("Kafka client cannot dispose itself from its polling thread");
			}
			if (current != null)
			{
				final KafkaConsumer<String, byte[]> kafka = this.consumer;
				if (kafka != null) kafka.wakeup();
				try { current.join(5_000L); }
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
					this.disposed = false;
					throw new IllegalStateException("Interrupted while stopping Kafka client", interrupted);
				}
				if (current.isAlive())
				{
					this.disposed = false;
					throw new IllegalStateException("Kafka client reader did not stop before disposal timeout");
				}
			}
			this.thread = null;
			if (!this.packetAcceptorDisposed)
			{
				if (this.packetAcceptor instanceof Disposable disposable) disposable.dispose();
				this.packetAcceptorDisposed = true;
			}
		}
	}
}
