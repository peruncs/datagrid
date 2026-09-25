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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.storage.distributed.types.ReplicationRetry;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;

/** Reads the durable logical message index from a legacy Kafka replication topic. */
final class KafkaLatestMessageIndex
{
	private static final Duration POLL_TIMEOUT = Duration.ofMillis(250L);
	private static final Duration PARTITION_LOOKUP_TIMEOUT = Duration.ofSeconds(30L);
	private static final Duration OFFSET_LOOKUP_TIMEOUT = Duration.ofSeconds(10L);
	private static final long LOOKUP_TIMEOUT_NANOS = Duration.ofSeconds(10L).toNanos();
	private static final long MAX_RECORDS_PER_SCAN = 1_024L;

	private KafkaLatestMessageIndex()
	{
	}

	static long read(final Properties kafkaProperties, final String topicName)
	{
		final Properties properties = new Properties();
		properties.putAll(kafkaProperties);
		final String identity = "datagrid-legacy-kafka-index-" + UUID.randomUUID();
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, identity);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, identity);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
		properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

		final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
		RuntimeException runtimeFailure = null;
		Error errorFailure = null;
		try
		{
			final List<TopicPartition> partitions = consumer.partitionsFor(topicName, PARTITION_LOOKUP_TIMEOUT)
				.stream()
				.map(info -> new TopicPartition(info.topic(), info.partition()))
				.toList();
			if (partitions.size() != 1)
			{
				throw new IllegalStateException(
					"Kafka replication topic must have exactly one partition; found " + partitions.size());
			}
			consumer.assign(partitions);
			consumer.seekToEnd(partitions);
			final TopicPartition partition = partitions.get(0);
			final long endOffset = consumer.position(partition);
			final long beginningOffset = consumer.beginningOffsets(partitions, OFFSET_LOOKUP_TIMEOUT).get(partition);
			final long deadline = ReplicationRetry.deadlineNanos(LOOKUP_TIMEOUT_NANOS);
			long scanEnd = endOffset;
			while (scanEnd > beginningOffset && !ReplicationRetry.expired(deadline))
			{
				final long scanStart = Math.max(beginningOffset, scanEnd - MAX_RECORDS_PER_SCAN);
				consumer.seek(partition, scanStart);
				long latest = -1L;
				while (consumer.position(partition) < scanEnd && !ReplicationRetry.expired(deadline))
				{
					for (final ConsumerRecord<String, byte[]> record : consumer.poll(POLL_TIMEOUT))
					{
						if (record.offset() >= scanEnd || record.value() == null) continue;
						final long sequence = sequence(record);
						if (sequence >= 0L && sequence > latest) latest = sequence;
					}
				}
				if (latest >= 0L) return latest;
				scanEnd = scanStart;
			}
			return -1L;
		}
		catch (final RuntimeException failure)
		{
			runtimeFailure = failure;
			throw failure;
		}
		catch (final Error failure)
		{
			errorFailure = failure;
			throw failure;
		}
		finally
		{
			try
			{
				consumer.close(CloseOptions.timeout(Duration.ofSeconds(5L)));
			}
			catch (final RuntimeException | Error closeFailure)
			{
				if (runtimeFailure != null) runtimeFailure.addSuppressed(closeFailure);
				else if (errorFailure != null) errorFailure.addSuppressed(closeFailure);
				else throw closeFailure;
			}
		}
	}

	/**
	 * Returns the greatest indexed record in one backward-scan result.
	 * Tombstones are ignored; malformed live records fail closed because accepting
	 * a possibly corrupt tail could make the next writer reuse an index.
	 */
	static long highestSequence(final Iterable<ConsumerRecord<String, byte[]>> records)
	{
		long latest = -1L;
		for (final ConsumerRecord<String, byte[]> record : records)
		{
			if (record.value() == null) continue;
			final long sequence = sequence(record);
			if (sequence >= 0L && sequence > latest) latest = sequence;
		}
		return latest;
	}

	private static long sequence(final ConsumerRecord<String, byte[]> record)
	{
			if (record.value().length == 0 || record.value().length > StorageBinaryDistributedKafka.maxPacketSize())
				throw malformed(record, "invalid packet payload length");
			final var headers = record.headers();
			final long sequence;
			try
			{
				final int messageLength = StorageBinaryDistributedKafka.messageLength(headers);
				final int packetIndex = StorageBinaryDistributedKafka.packetIndex(headers);
				final int packetCount = StorageBinaryDistributedKafka.packetCount(headers);
				sequence = StorageBinaryDistributedKafka.messageIndex(headers);
				StorageBinaryDistributedKafka.messageType(headers);
				StorageBinaryDistributedKafka.messageCrc32c(headers);
				StorageBinaryDistributedKafka.validateMetadata(messageLength, packetIndex, packetCount, sequence);
			}
			catch (final IllegalArgumentException malformed)
			{
				throw malformed(record, malformed.getMessage(), malformed);
			}
			return sequence;
		}

	private static IllegalStateException malformed(final ConsumerRecord<String, byte[]> record, final String message)
	{
		return malformed(record, message, null);
	}

	private static IllegalStateException malformed(
		final ConsumerRecord<String, byte[]> record, final String message, final Throwable cause)
	{
		return new IllegalStateException("Malformed Kafka replication record at offset " + record.offset() + ": " + message, cause);
	}
}
