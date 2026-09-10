package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
 * %%
 * Copyright (C) 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.collections.EqHashTable;
import org.eclipse.serializer.collections.types.XImmutableMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the single-partition Kafka cursor wire contract without a broker. */
class KafkaCursorCodecTest
{
	@Test
	void roundTripsTheConfiguredTopicPartition()
	{
		final EqHashTable<TopicPartition, Long> offsets = EqHashTable.New();
		offsets.put(new TopicPartition("events", 0), 42L);
		final MessageInfo info = MessageInfo.New(7L, "kafka", null, KafkaCursorCodec.encode(offsets.immure()));

		final XImmutableMap<TopicPartition, Long> decoded = KafkaCursorCodec.decode(info, "events");

		assertEquals(1, decoded.size());
		decoded.forEach(entry ->
		{
			assertEquals(new TopicPartition("events", 0), entry.key());
			assertEquals(42L, entry.value());
		});
	}

	@Test
	void rejectsWrongTopicAndPartition()
	{
		assertRejected("other,0,1\n", "events");
		assertRejected("events,1,1\n", "events");
	}

	@Test
	void rejectsDuplicateAndMalformedRows()
	{
		assertRejected("events,0,1\nevents,0,2\n", "events");
		assertRejected("events,not-a-partition,1\n", "events");
		assertRejected("events,0,not-an-offset\n", "events");
	}

	private static void assertRejected(final String cursor, final String expectedTopic)
	{
		final MessageInfo info = MessageInfo.New(0L, "kafka", null, cursor.getBytes(java.nio.charset.StandardCharsets.UTF_8));
		assertThrows(NodelibraryException.class, () -> KafkaCursorCodec.decode(info, expectedTopic));
	}
}
