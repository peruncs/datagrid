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

import java.nio.file.Files;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReplicationCursorStoreTest
{
	@Test
	void roundTripsOpaqueProviderPositionAtomically() throws Exception
	{
		final var path = Files.createTempFile("datagrid-replication", ".cursor");
		final var expected = new ReplicationCursor("aeron", UUID.randomUUID(), 17, new byte[] { 4, 5, 6 });
		ReplicationCursorStore.write(path, expected);
		assertEquals(expected, ReplicationCursorStore.read(path));
		Files.deleteIfExists(path);
	}

	@Test
	void detectsCorruptCursorBeforeUsingProviderBytes() throws Exception
	{
		final var path = Files.createTempFile("datagrid-replication", ".cursor");
		ReplicationCursorStore.write(path, new ReplicationCursor("kafka", null, 3, new byte[] { 1 }));
		final byte[] bytes = Files.readAllBytes(path);
		bytes[10] ^= 1;
		Files.write(path, bytes);
		assertThrows(java.io.IOException.class, () -> ReplicationCursorStore.read(path));
		Files.deleteIfExists(path);
	}
}
