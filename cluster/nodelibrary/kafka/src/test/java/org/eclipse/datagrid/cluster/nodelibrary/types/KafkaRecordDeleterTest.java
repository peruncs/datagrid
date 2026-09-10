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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.serializer.collections.EqHashTable;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

/** Verifies retention fencing before any Kafka admin operation is attempted. */
class KafkaRecordDeleterTest
{
	@Test
	void rejectsMultiplePartitionsBeforeCallingKafka()
	{
		final AdminClient admin = mock(AdminClient.class);
		final KafkaRecordDeleter deleter = KafkaRecordDeleter.New(admin, "events");
		final EqHashTable<TopicPartition, Long> offsets = EqHashTable.New();
		offsets.put(new TopicPartition("events", 0), 1L);
		offsets.put(new TopicPartition("events", 1), 1L);

		assertThrows(NodelibraryException.class, () -> deleter.deleteUntilOffsets(offsets.immure()));
		verifyNoInteractions(admin);
	}

	@Test
	void rejectsWrongTopicOrPartitionBeforeCallingKafka()
	{
		final AdminClient admin = mock(AdminClient.class);
		final KafkaRecordDeleter deleter = KafkaRecordDeleter.New(admin, "events");
		final EqHashTable<TopicPartition, Long> offsets = EqHashTable.New();
		offsets.put(new TopicPartition("other", 0), 1L);

		assertThrows(NodelibraryException.class, () -> deleter.deleteUntilOffsets(offsets.immure()));
		verifyNoInteractions(admin);
	}

	@Test
	void mapsDeleteTimeoutToNodelibraryException()
	{
		final AdminClient admin = mock(AdminClient.class);
		final DeleteRecordsResult result = mock(DeleteRecordsResult.class);
		final var future = mock(org.apache.kafka.common.KafkaFuture.class);
		when(admin.deleteRecords(anyMap())).thenReturn(result);
		when(result.all()).thenReturn(future);
		try
		{
			when(future.get(10, TimeUnit.MINUTES)).thenThrow(new TimeoutException("test timeout"));
		}
		catch (final Exception impossible)
		{
			throw new AssertionError(impossible);
		}

		final EqHashTable<TopicPartition, Long> offsets = EqHashTable.New();
		offsets.put(new TopicPartition("events", 0), 1L);
		assertThrows(NodelibraryException.class,
			() -> KafkaRecordDeleter.New(admin, "events").deleteUntilOffsets(offsets.immure()));
	}
}
