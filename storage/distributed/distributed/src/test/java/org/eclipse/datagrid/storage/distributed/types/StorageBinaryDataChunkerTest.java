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
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Tests storage binary data chunker behavior. */
class StorageBinaryDataChunkerTest
{
	/** Verifies chunks across source buffers without changing their positions. */
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

	/** Verifies empty binary produces no packets. */
	@Test
	void emptyBinaryProducesNoPackets()
	{
		final var binary = ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[0]));
		assertEquals(0, StorageBinaryDataChunker.chunk(binary, 128).size());
	}

	/** Verifies that streaming iteration does not materialize a second chunk list. */
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

	/** Verifies channel-partitioned Store binaries are flattened in channel order. */
	@Test
	void chunksAllSerializerChannelsInOrder()
	{
		final ByteBuffer first = XMemory.toDirectByteBuffer(new byte[] { 1, 2 });
		final ByteBuffer second = XMemory.toDirectByteBuffer(new byte[] { 3, 4 });
		final Binary binary = mock(Binary.class);
		when(binary.buffers()).thenReturn(new ByteBuffer[] { first });
		doAnswer(invocation ->
		{
			final java.util.function.Consumer<? super Binary> consumer = invocation.getArgument(0);
			consumer.accept(ChunksWrapper.New(first));
			consumer.accept(ChunksWrapper.New(second));
			return null;
		}).when(binary).iterateChannelChunks(any());

		final var chunks = StorageBinaryDataChunker.chunk(binary, 3);

		assertEquals(2, chunks.size());
		assertArrayEquals(new byte[] { 1, 2, 3 }, chunks.get(0).bytes());
		assertArrayEquals(new byte[] { 4 }, chunks.get(1).bytes());
		assertEquals(0, first.position());
		assertEquals(0, second.position());
	}
}
