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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageBinaryDistributedKafkaTest
{
	@Test
	void roundTripsValidatedMetadata()
	{
		final var headers = new RecordHeaders();
		StorageBinaryDistributedKafka.addPacketHeaders(headers, MessageType.DATA,
			2_000_000, 1, 2, 73L, 0x12345678);

		assertEquals(MessageType.DATA, StorageBinaryDistributedKafka.messageType(headers));
		assertEquals(2_000_000, StorageBinaryDistributedKafka.messageLength(headers));
		assertEquals(1, StorageBinaryDistributedKafka.packetIndex(headers));
		assertEquals(2, StorageBinaryDistributedKafka.packetCount(headers));
		assertEquals(73L, StorageBinaryDistributedKafka.messageIndex(headers));
		assertEquals(0x12345678, StorageBinaryDistributedKafka.messageCrc32c(headers));
		assertArrayEquals(new byte[]{0, 0, 0, 1}, StorageBinaryDistributedKafka.serialize(1));
		assertEquals(0x0102030405060708L,
			StorageBinaryDistributedKafka.deserializeLong(StorageBinaryDistributedKafka.serializeLong(0x0102030405060708L)));
	}

	@Test
	void rejectsDuplicateDecisionHeaders()
	{
		final var headers = new RecordHeaders();
		StorageBinaryDistributedKafka.addPacketHeaders(headers, MessageType.DATA, 1, 0, 1, 0L, 0);
		headers.add(StorageBinaryDistributedKafka.keyPacketIndex(), StorageBinaryDistributedKafka.serialize(0));

		assertThrows(IllegalArgumentException.class,
			() -> StorageBinaryDistributedKafka.packetIndex(headers));
	}

	@Test
	void rejectsInvalidMetadata()
	{
		assertThrows(IllegalArgumentException.class,
			() -> StorageBinaryDistributedKafka.validateMetadata(1, 1, 1, 0L));
		assertThrows(IllegalArgumentException.class,
			() -> StorageBinaryDistributedKafka.validateMetadata(0, 0, 1, 0L));
	}

	@Test
	void findsTheGreatestIndexedRecordAndIgnoresTombstones()
	{
		final var first = new ConsumerRecord<String, byte[]>("replication", 0, 4L, "key", new byte[]{1});
		StorageBinaryDistributedKafka.addPacketHeaders(first.headers(), MessageType.DATA, 1, 0, 1, 4L, 0);
		final var last = new ConsumerRecord<String, byte[]>("replication", 0, 5L, "key", new byte[]{1});
		StorageBinaryDistributedKafka.addPacketHeaders(last.headers(), MessageType.DATA, 1, 0, 1, 9L, 0);
		final var tombstone = new ConsumerRecord<String, byte[]>("replication", 0, 6L, "key", null);

		assertEquals(9L, KafkaLatestMessageIndex.highestSequence(List.of(first, last, tombstone)));
	}

	@Test
	void rejectsMalformedLiveTailRecord()
	{
		final var malformed = new ConsumerRecord<String, byte[]>("replication", 0, 7L, "key", new byte[]{1});

		assertThrows(IllegalStateException.class,
			() -> KafkaLatestMessageIndex.highestSequence(List.of(malformed)));
	}
}
