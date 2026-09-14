package org.eclipse.datagrid.cluster.nodelibrary.kafka;

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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the canonical Kafka replication wire format. */
class KafkaWireCompatibilityTest
{
	@Test
	void headersRoundTripThroughCanonicalCodec()
	{
		final var headers = new RecordHeaders();
		KafkaHeaderCodec.addPacketHeaders(headers, MessageType.DATA, 6_123_456, 2, 7, 42L, 0x12345678);

		assertEquals(MessageType.DATA, KafkaHeaderCodec.messageType(headers));
		assertEquals(6_123_456, KafkaHeaderCodec.messageLength(headers));
		assertEquals(2, KafkaHeaderCodec.packetIndex(headers));
		assertEquals(7, KafkaHeaderCodec.packetCount(headers));
		assertEquals(42L, KafkaHeaderCodec.messageIndex(headers));
		assertEquals(0x12345678, KafkaHeaderCodec.messageCrc32c(headers));
	}

	@Test
	void legacyHeadersWithoutWireVersionAreRejected()
	{
		final var headers = new RecordHeaders();
		headers.add(KafkaHeaderCodec.keyMessageLength(), "9876".getBytes(java.nio.charset.StandardCharsets.UTF_8));
		assertThrows(IllegalArgumentException.class,
			() -> KafkaHeaderCodec.messageLength(headers));
	}

	@Test
	void packetCountMustMatchMessageLength()
	{
		assertThrows(IllegalArgumentException.class,
			() -> KafkaHeaderCodec.addPacketHeaders(
				new RecordHeaders(), MessageType.DATA, 1_000_001, 0, 1, 0L, 0));
	}
}
