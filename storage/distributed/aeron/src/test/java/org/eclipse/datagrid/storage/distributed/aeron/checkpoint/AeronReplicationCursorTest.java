package org.eclipse.datagrid.storage.distributed.aeron.checkpoint;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies that a reader cursor cannot be mistaken for another recording. */
class AeronReplicationCursorTest
{
	/** Verifies preservation of the self-describing replay boundary. */
	@Test
	void preservesTheSelfDescribingReplayBoundary()
	{
		final UUID clusterId = UUID.randomUUID();
		final UUID nodeId = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		final AeronReplicationCursor cursor = new AeronReplicationCursor(
			clusterId, nodeId, generation, 7, 42, 4096, 13);

		assertEquals(clusterId, cursor.clusterId());
		assertEquals(nodeId, cursor.nodeId());
		assertEquals(generation, cursor.storeGeneration());
		assertEquals(7, cursor.epoch());
		assertEquals(42, cursor.recordingId());
		assertEquals(4096, cursor.recordingPosition());
		assertEquals(13, cursor.sequence());
		assertEquals(cursor, AeronReplicationCursor.decode(cursor.encode()));
	}

	@Test
	void rejectsLegacyAndUnknownProviderPositionFormats()
	{
		assertThrows(IllegalArgumentException.class,
			() -> AeronReplicationCursor.decode(new byte[Long.BYTES * 2]));
		final AeronReplicationCursor cursor = new AeronReplicationCursor(
			UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 1, 2, 3, 4);
		final byte[] encoded = cursor.encode();
		encoded[0] ^= 1;
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationCursor.decode(encoded));
	}

	@Test
	void rejectsCorruptedReplayPositionOrSequence()
	{
		final AeronReplicationCursor cursor = new AeronReplicationCursor(
			UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 1, 2, 3, 4);
		final byte[] encoded = cursor.encode();
		encoded[encoded.length - Integer.BYTES - Long.BYTES] ^= 1;
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationCursor.decode(encoded));
	}

	/** Verifies rejection of invalid replay identity and positions. */
	@Test
	void rejectsInvalidReplayIdentityAndPositions()
	{
		final UUID id = UUID.randomUUID();
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCursor(
			null, id, id, 0, 1, 0, 0));
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCursor(
			id, id, id, -1, 1, 0, 0));
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCursor(
			id, id, id, 0, -1, 0, 0));
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCursor(
			id, id, id, 0, 1, -2, 0));
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCursor(
			id, id, id, 0, 1, 0, -2));
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCursor(
			id, id, id, 0, 1, 0, Long.MAX_VALUE));
	}
}
