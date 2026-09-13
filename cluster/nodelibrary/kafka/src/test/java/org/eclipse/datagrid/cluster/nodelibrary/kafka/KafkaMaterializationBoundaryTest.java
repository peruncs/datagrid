/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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
package org.eclipse.datagrid.cluster.nodelibrary.kafka;

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageBinaryDataPacketAcceptor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies the Kafka acknowledgement order used by replay. */
class KafkaMaterializationBoundaryTest
{
	@Test
	void materializationIsAwaitedAfterAccept()
	{
		final List<String> events = new ArrayList<>();
		final ClusterStorageBinaryDataPacketAcceptor acceptor = new ClusterStorageBinaryDataPacketAcceptor()
		{
			@Override
			public void accept(final List<StorageBinaryDataPacket> packets)
			{
				events.add("accept");
			}

			@Override
			public void awaitApplied()
			{
				events.add("awaitApplied");
			}

			@Override
			public void dispose()
			{
			}
		};

		KafkaClusterStorageBinaryDataClient.acceptAndAwait(acceptor, List.of());

		assertEquals(List.of("accept", "awaitApplied"), events);
	}
}
