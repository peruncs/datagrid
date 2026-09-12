package org.eclipse.datagrid.cluster.nodelibrary.kafka;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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

import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.types.MessageInfo;
import org.eclipse.serializer.collections.EqHashTable;
import org.eclipse.serializer.collections.types.XImmutableMap;

import java.nio.charset.StandardCharsets;

/** Strict codec for the single-partition Kafka replication cursor. */
final class KafkaCursorCodec
{
	private KafkaCursorCodec() { }

	static XImmutableMap<TopicPartition, Long> decode(final MessageInfo info) throws NodelibraryException
	{
		return decode(info, null);
	}

	static XImmutableMap<TopicPartition, Long> decode(
		final MessageInfo info,
		final String expectedTopic
	) throws NodelibraryException
	{
		final EqHashTable<TopicPartition, Long> offsets = EqHashTable.New();
		final String text = new String(info.providerPosition(), StandardCharsets.UTF_8).trim();
		if (text.isEmpty())
		{
			return offsets.immure();
		}
		for (final String row : text.split("\\n"))
		{
			final String[] columns = row.split(",", -1);
			if (columns.length != 3)
			{
				throw new NodelibraryException("Invalid Kafka cursor row: " + row);
			}
			final TopicPartition partition;
			final long offset;
			try
			{
				partition = new TopicPartition(columns[0], Integer.parseInt(columns[1]));
				offset = Long.parseLong(columns[2]);
			}
			catch (final NumberFormatException failure)
			{
				throw new NodelibraryException("Invalid Kafka cursor row: " + row, failure);
			}
			if (expectedTopic != null && (!expectedTopic.equals(partition.topic()) || partition.partition() != 0))
			{
				throw new NodelibraryException(
					"Kafka cursor must contain only " + expectedTopic + " partition 0; found " + partition
				);
			}
			if (offsets.get(partition) != null)
			{
				throw new NodelibraryException("Duplicate Kafka partition: " + partition);
			}
			offsets.put(partition, offset);
		}
		if (expectedTopic != null && offsets.size() > 1)
		{
			throw new NodelibraryException("Kafka cursor must contain exactly one partition");
		}
		return offsets.immure();
	}

	static byte[] encode(final XImmutableMap<TopicPartition, Long> offsets)
	{
		final StringBuilder result = new StringBuilder();
		offsets.forEach(entry -> result.append(entry.key().topic()).append(',')
			.append(entry.key().partition()).append(',').append(entry.value()).append('\n'));
		return result.toString().getBytes(StandardCharsets.UTF_8);
	}
}
