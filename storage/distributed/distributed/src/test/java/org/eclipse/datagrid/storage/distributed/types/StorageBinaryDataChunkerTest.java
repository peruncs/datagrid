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
import org.eclipse.serializer.persistence.binary.types.ChunksBuffer;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.util.BufferSizeProviderIncremental;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
		final ChunksBuffer[] channels = new ChunksBuffer[2];
		final var bufferSize = BufferSizeProviderIncremental.New(32);
		channels[0] = ChunksBuffer.New(channels, bufferSize);
		channels[1] = ChunksBuffer.New(channels, bufferSize);
		channels[0].store_bytes(1L, 1L, new byte[] { 1, 2 });
		channels[1].store_bytes(1L, 2L, new byte[] { 3, 4 });
		channels[0].complete();
		channels[1].complete();
		try
		{
			final var chunks = StorageBinaryDataChunker.chunk(channels[0], 3);
			final var expected = new ArrayList<Byte>(channels[0].buffers()[0].remaining()
				+ channels[1].buffers()[0].remaining());
			for (final ChunksBuffer channel : channels)
			{
				final ByteBuffer buffer = channel.buffers()[0].duplicate();
				while (buffer.hasRemaining()) expected.add(buffer.get());
			}
			final var actual = new ArrayList<Byte>();
			for (final var chunk : chunks)
				for (final byte value : chunk.bytes()) actual.add(value);
			assertEquals(expected, actual);
		}
		finally
		{
			channels[0].clear();
			channels[1].clear();
		}
	}

	/** The receiver fast path must collect exactly one array without a list copy. */
	@Test
	void bufferArrayPreservesAllChannelBuffers()
	{
		final ByteBuffer first = XMemory.toDirectByteBuffer(new byte[] { 1 });
		final ByteBuffer second = XMemory.toDirectByteBuffer(new byte[] { 2 });
		final var binary = ChunksWrapper.New(first, second);
		final ByteBuffer[] buffers = StorageBinaryDataChunker.bufferArray(binary);
		assertEquals(2, buffers.length);
		assertEquals(first, buffers[0]);
		assertEquals(second, buffers[1]);
		assertEquals(0, first.position());
		assertEquals(0, second.position());
	}

	/** Import views normalize Serializer's position-as-length representation without mutating it. */
	@Test
	void importArrayNormalizesChunksWrapperBuffers()
	{
		final ByteBuffer source = XMemory.allocateDirectNative(8);
		try
		{
			source.put(new byte[] { 1, 2, 3 });
			final var binary = ChunksWrapper.New(source);
			final ByteBuffer[] normalized = StorageBinaryDataChunker.importArray(binary);
			assertEquals(1, normalized.length);
			assertEquals(0, normalized[0].position());
			assertEquals(3, normalized[0].remaining());
			assertEquals(3, source.position());
		}
		finally
		{
			XMemory.deallocateDirectByteBuffer(source);
		}
	}
}
