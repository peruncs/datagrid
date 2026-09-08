package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StorageBinaryDataChunkerTest
{
	@Test
	void chunksAcrossSourceBuffersWithoutChangingTheirPositions()
	{
		final ByteBuffer first = XMemory.toDirectByteBuffer(new byte[] {1, 2});
		final ByteBuffer second = XMemory.toDirectByteBuffer(new byte[] {3, 4, 5});
		final var binary = ChunksWrapper.New(first, second);

		final var chunks = StorageBinaryDataChunker.chunk(binary, 2);

		assertEquals(3, chunks.size());
		assertArrayEquals(new byte[] {1, 2}, chunks.get(0).bytes());
		assertArrayEquals(new byte[] {3, 4}, chunks.get(1).bytes());
		assertArrayEquals(new byte[] {5}, chunks.get(2).bytes());
		assertEquals(0, first.position());
		assertEquals(0, second.position());
		assertEquals(3, chunks.get(0).count());
		assertEquals(5, chunks.get(0).messageLength());
	}

	@Test
	void emptyBinaryProducesNoPackets()
	{
		final var binary = ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[0]));
		assertEquals(0, StorageBinaryDataChunker.chunk(binary, 128).size());
	}

	@Test
	void streamingForEachDoesNotMaterializeASecondChunkList()
	{
		final ByteBuffer source = XMemory.toDirectByteBuffer(new byte[] { 1, 2, 3 });
		final var binary = ChunksWrapper.New(source);
		final var chunks = new ArrayList<StorageBinaryDataChunker.Chunk>();
		StorageBinaryDataChunker.forEach(binary, 2, chunks::add);
		assertEquals(2, chunks.size());
		assertArrayEquals(new byte[] { 1, 2 }, chunks.get(0).bytes());
		assertArrayEquals(new byte[] { 3 }, chunks.get(1).bytes());
		assertEquals(0, source.position());
	}
}
