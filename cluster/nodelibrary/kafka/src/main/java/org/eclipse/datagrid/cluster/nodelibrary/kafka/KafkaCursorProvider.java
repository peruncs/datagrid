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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationCursor;
import org.eclipse.datagrid.storage.distributed.types.ReplicationRetry;
import org.eclipse.serializer.collections.EqHashTable;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.eclipse.serializer.util.X.notNull;

/**
 * Looks up the latest published replication sequence and Kafka position.
 */
public class KafkaCursorProvider implements AutoCloseable
{
	/** Creates a provider for one Kafka topic.
	 *
	 * @param topic Kafka topic
	 * @param groupInstanceId stable base identity used to isolate this lookup consumer group
	 * @param kafkaPropertiesProvider Kafka properties provider
	 * @return cursor provider
	 */
	public static KafkaCursorProvider New(
		final String topic,
		final String groupInstanceId,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		return new KafkaCursorProvider(
			notNull(topic),
			notNull(groupInstanceId),
			notNull(kafkaPropertiesProvider)
		);
	}

	private static final System.Logger LOG = System.getLogger(KafkaCursorProvider.class.getName());
	private static final long PARTITION_ASSIGNMENT_TIMEOUT_NANOS = Duration.ofSeconds(10L).toNanos();
	private static final Duration POLL_TIMEOUT = Duration.ofMillis(250L);
	private static final long LATEST_LOOKUP_TIMEOUT_NANOS = Duration.ofSeconds(10L).toNanos();

	private volatile KafkaConsumer<String, byte[]> kafka;
	private final KafkaPropertiesProvider kafkaPropertiesProvider;
	private final String topic;
	private boolean initialized;

	private KafkaCursorProvider(
		final String topic,
		final String groupInstanceId,
		final KafkaPropertiesProvider kafkaPropertiesProvider
	)
	{
		this.topic = topic;
		this.kafkaPropertiesProvider = kafkaPropertiesProvider;
		this.kafka = this.createKafkaConsumer(groupInstanceId);
	}

	private KafkaConsumer<String, byte[]> createKafkaConsumer(final String groupInstanceId)
	{
		final var properties = this.kafkaPropertiesProvider.provide();
		/* The lookup consumer must not share the replication reader group. Its
		 * group is a distinct role-specific namespace. */
		properties.setProperty(GROUP_ID_CONFIG, groupInstanceId + "-lookup");
		properties.setProperty(CLIENT_ID_CONFIG, groupInstanceId);
		properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
		properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
		return new KafkaConsumer<>(properties);
	}

	@Override
	public synchronized void close() throws KafkaException
	{
		LOG.log(System.Logger.Level.DEBUG, "Closing KafkaCursorProvider");
		final KafkaConsumer<String, byte[]> consumer = this.kafka;
		this.kafka = null;
		this.initialized = false;
		if (consumer != null) consumer.close(CloseOptions.timeout(Duration.ofSeconds(5L)));
	}

	/** Subscribes and waits for the topic assignment.
	 * @throws KafkaException if initialization fails
	 */
	public synchronized void init() throws KafkaException
	{
		if (this.initialized) return;
		LOG.log(System.Logger.Level.DEBUG, "Initializing KafkaCursorProvider. Subscribing to topic " + this.topic);
		final KafkaConsumer<String, byte[]> consumer = this.consumer();
		try
		{
			consumer.subscribe(Collections.singleton(this.topic));

			LOG.log(System.Logger.Level.DEBUG, "Polling consumer until we have partitions assigned.");
			final long deadline = ReplicationRetry.deadlineNanos(PARTITION_ASSIGNMENT_TIMEOUT_NANOS);
			while (consumer.assignment().isEmpty())
			{
				if (ReplicationRetry.expired(deadline))
				{
					throw new RuntimeException("Timed out waiting for topic partition assignment");
				}
				consumer.poll(POLL_TIMEOUT);
			}
			if (consumer.assignment().size() != 1)
			{
					throw new IllegalStateException(
						"Kafka replication topic must have exactly one partition; found " + consumer.assignment().size()
					);
			}
			this.initialized = true;
		}
		catch (final RuntimeException failure)
		{
			this.initialized = false;
			if (this.kafka == consumer) this.kafka = null;
			try { consumer.close(CloseOptions.timeout(Duration.ofSeconds(5L))); }
			catch (final RuntimeException closeFailure) { failure.addSuppressed(closeFailure); }
			throw failure;
		}
	}

	/**
	 * Seeks the provider's assigned consumer to the latest message and returns
	 * the greatest replication sequence carried by the available records.
	 *
	 * @return latest sequence, or {@code -1} when the topic has no indexed
	 * replication record yet
	 */
	public synchronized long provideLatestSequence() throws KafkaException
	{
		final KafkaConsumer<String, byte[]> consumer = this.consumer();
		long lastSequence = -1L;
		consumer.poll(POLL_TIMEOUT);
		final var partitions = consumer.assignment();
		if (partitions.size() != 1) throw new IllegalStateException("Kafka replication topic must have exactly one partition");
		final TopicPartition partition = partitions.iterator().next();
		consumer.seekToEnd(partitions);
		long candidate = consumer.position(partition) - 1L;
		final long deadline = ReplicationRetry.deadlineNanos(LATEST_LOOKUP_TIMEOUT_NANOS);
		while (candidate >= 0L && !ReplicationRetry.expired(deadline))
		{
			consumer.seek(partition, candidate);
			for (final var rec : consumer.poll(POLL_TIMEOUT))
			{
				final long sequence;
				try
				{
					sequence = KafkaHeaderCodec.messageIndex(rec.headers());
				}
				catch (final IllegalArgumentException malformed)
				{
					throw new KafkaException(
						"Malformed Kafka replication record at offset " + rec.offset(), malformed);
				}
				if (sequence >= 0L && sequence > lastSequence) lastSequence = sequence;
			}
			if (lastSequence >= 0L) return lastSequence;
			candidate--;
		}

		return lastSequence;
	}

	/** Returns the latest Kafka replication cursor.
	 * @return latest replication cursor
	 */
	public synchronized ReplicationCursor provideLatestCursor()
	{
		final var sequence = this.provideLatestSequence();
		final KafkaConsumer<String, byte[]> consumer = this.consumer();
		/* The lookup scan seeks backwards. A latest cursor must start at the live
		 * tail, not at the offset of the record that happened to carry the header. */
		consumer.seekToEnd(consumer.assignment());

		final EqHashTable<TopicPartition, Long> kafkaOffsets = EqHashTable.New();
		for (final var partition : consumer.assignment())
		{
			final long offset = consumer.position(partition);
			kafkaOffsets.put(partition, offset);
		}

		return new ReplicationCursor("kafka", null, sequence, KafkaCursorCodec.encode(kafkaOffsets.immure()));
	}

	private KafkaConsumer<String, byte[]> consumer()
	{
		final KafkaConsumer<String, byte[]> consumer = this.kafka;
		if (consumer == null) throw new IllegalStateException("Kafka cursor provider is closed or failed to initialize");
		return consumer;
	}
}
