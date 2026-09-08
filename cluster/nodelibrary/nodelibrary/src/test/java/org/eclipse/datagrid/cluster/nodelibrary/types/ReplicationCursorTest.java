package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReplicationCursorTest
{
	@Test
	void providerPositionIsDefensiveAndIdentityIsValueBased()
	{
		final byte[] position = { 1, 2, 3 };
		final UUID generation = UUID.randomUUID();
		final ReplicationCursor cursor = new ReplicationCursor("aeron", generation, 8, position);
		position[0] = 9;
		assertArrayEquals(new byte[] { 1, 2, 3 }, cursor.providerPosition());
		final byte[] returned = cursor.providerPosition();
		returned[1] = 9;
		assertArrayEquals(new byte[] { 1, 2, 3 }, cursor.providerPosition());
	}

	@Test
	void rejectsInvalidSequenceAndTransport()
	{
		assertThrows(IllegalArgumentException.class,
			() -> new ReplicationCursor("", null, -1, new byte[0]));
		assertThrows(IllegalArgumentException.class,
			() -> new ReplicationCursor("aeron", null, -2, new byte[0]));
	}
}
