package org.eclipse.datagrid.cache.clustered.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Kafka
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.datagrid.cache.clustered.types.TimestampsRegionUpdateMessage;
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.SerializerFoundation;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Broker-free lifecycle and configuration tests for the Kafka provider. */
class KafkaClusteredCacheMessageComProviderTest
{
	@Test
	void providerOwnsOneReceiverAndRejectsConflictingConfiguration()
	{
		final Serializer<byte[]> serializer = Serializer.Bytes(SerializerFoundation.New());
		final ClusteredCacheMessageAcceptor acceptor = new ClusteredCacheMessageAcceptor(null);
		final Map<String, Object> properties = new HashMap<>();
		properties.put(KafkaClusteredConfigurationPropertyNames.TOPIC, "timestamps");
		properties.put(KafkaClusteredConfigurationPropertyNames.GROUP_ID, "node-a");
		final KafkaClusteredCacheMessageComProvider provider = new KafkaClusteredCacheMessageComProvider();

		final ClusteredCacheMessageReceiver first = provider.provideMessageReceiver(properties, serializer, acceptor);
		assertSame(first, provider.provideMessageReceiver(properties, serializer, acceptor));

		final Map<String, Object> conflicting = new HashMap<>(properties);
		conflicting.put(KafkaClusteredConfigurationPropertyNames.GROUP_ID, "node-b");
		assertThrows(IllegalArgumentException.class,
			() -> provider.provideMessageReceiver(conflicting, serializer, acceptor));
	}

	@Test
	void providerOwnsOneSenderAndRejectsConflictingSerializer()
	{
		final Serializer<byte[]> serializer = Serializer.Bytes(SerializerFoundation.New());
		final Map<String, Object> properties = new HashMap<>();
		properties.put(KafkaClusteredConfigurationPropertyNames.TOPIC, "timestamps");
		properties.put(KafkaClusteredConfigurationPropertyNames.KAFKA_CONFIG_PREFIX + "bootstrap.servers",
			"localhost:9092");
		final KafkaClusteredCacheMessageComProvider provider = new KafkaClusteredCacheMessageComProvider();

		final ClusteredCacheMessageSender<Object, Object> first =
			provider.provideUpdateTimestampsCacheMessageSender(properties, serializer);
		assertSame(first, provider.provideUpdateTimestampsCacheMessageSender(properties, serializer));
		assertThrows(IllegalArgumentException.class,
			() -> provider.provideUpdateTimestampsCacheMessageSender(properties,
				Serializer.Bytes(SerializerFoundation.New())));
		first.dispose();
	}

	@Test
	void kafkaPartitionKeyKeepsOneTimestampTableOrdered()
	{
		final TimestampsRegionUpdateMessage message =
			new TimestampsRegionUpdateMessage("query-cache", "entity-table", 42L);
		assertEquals("query-cache\0entity-table", KafkaClusteredCacheMessageSender.partitionKey(message));
	}

	@Test
	void timestampNamesRejectThePartitionKeySeparator()
	{
		assertThrows(IllegalArgumentException.class,
			() -> new TimestampsRegionUpdateMessage("query\0cache", "table", 1L));
		assertThrows(IllegalArgumentException.class,
			() -> new TimestampsRegionUpdateMessage("query-cache", "table\0name", 1L));
	}

	@Test
	void inferredGroupsAreIsolatedPerProviderButStableProviderIdsAreReusable()
	{
		final Map<String, Object> properties = new HashMap<>();
		properties.put(KafkaClusteredConfigurationPropertyNames.TOPIC, "timestamps");
		final KafkaClusteredCacheMessageComProvider first = new KafkaClusteredCacheMessageComProvider();
		final KafkaClusteredCacheMessageComProvider second = new KafkaClusteredCacheMessageComProvider();

		assertNotEquals(first.groupId(properties, "timestamps"), second.groupId(properties, "timestamps"));

		properties.put(KafkaClusteredConfigurationPropertyNames.PROVIDER_ID, "timestamps-provider");
		assertEquals(first.groupId(properties, "timestamps"), second.groupId(properties, "timestamps"));
	}

	@Test
	void malformedKafkaRecordStopsTheBatchBeforeOffsetCommit()
	{
		final Serializer<byte[]> serializer = Serializer.Bytes(SerializerFoundation.New());
        final KafkaClusteredCacheMessageReceiver receiver = new KafkaClusteredCacheMessageReceiver(
            new Properties(), "timestamps", "node-a", "client-a",
            new ClusteredCacheMessageAcceptor(null), serializer, 1024);
		final TopicPartition partition = new TopicPartition("timestamps", 0);
		final ConsumerRecord<String, byte[]> malformed =
			new ConsumerRecord<>("timestamps", 0, 0L, null, new byte[]{1, 2, 3});
		final ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(
			Map.of(partition, List.of(malformed)), Map.of());
		assertThrows(IllegalStateException.class, () -> receiver.consume(records));
	}
}
