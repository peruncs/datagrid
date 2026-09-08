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

import org.eclipse.serializer.persistence.binary.types.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.eclipse.serializer.util.X.notNull;

/** Splits a Store binary into transport-sized packets without mutating its buffers. */
public final class StorageBinaryDataChunker
{
	private StorageBinaryDataChunker()
	{
	}

	/** One packet and its position in the source binary. */
	public record Chunk(byte[] bytes, int index, int count, int messageLength)
	{
	}

	/**
	 * Copies a binary into packets no larger than {@code maxPacketSize}.
	 *
	 * @param data source Store binary
	 * @param maxPacketSize maximum packet payload size, in bytes
	 * @return packets in source order
	 */
	public static List<Chunk> chunk(final Binary data, final int maxPacketSize)
	{
		notNull(data);
		if (maxPacketSize <= 0)
		{
			throw new IllegalArgumentException("maxPacketSize must be positive");
		}

		final List<Chunk> chunks = new ArrayList<>();
		visit(data, maxPacketSize, chunks::add);
		return chunks;
	}

	private static void visit(final Binary data, final int maxPacketSize, final Consumer<Chunk> consumer)
	{
		final ByteBuffer[] source = data.buffers();
		long messageLength = 0;
		for (final ByteBuffer buffer : source)
		{
			messageLength += buffer.remaining();
			if (messageLength > Integer.MAX_VALUE)
			{
				throw new IllegalArgumentException("binary is too large for transport metadata");
			}
		}
		final int length = (int)messageLength;
		if (length == 0) return;

		final int count = (int)((messageLength + maxPacketSize - 1L) / maxPacketSize);
		final ByteBuffer[] buffers = new ByteBuffer[source.length];
		for (int i = 0; i < source.length; i++)
		{
			buffers[i] = source[i].duplicate();
		}

		int remaining = length;
		int currentBuffer = 0;
		for (int index = 0; remaining > 0; index++)
		{
			final byte[] packet = new byte[Math.min(remaining, maxPacketSize)];
			int packetOffset = 0;
			while (packetOffset < packet.length)
			{
				final ByteBuffer buffer = buffers[currentBuffer];
				final int copied = Math.min(packet.length - packetOffset, buffer.remaining());
				buffer.get(packet, packetOffset, copied);
				packetOffset += copied;
				remaining -= copied;
				if (!buffer.hasRemaining())
				{
					currentBuffer++;
				}
			}
			consumer.accept(new Chunk(packet, index, count, length));
		}
	}

	/**
	 * Visits transport chunks in source order. This form keeps distributor implementations
	 * from duplicating packet traversal and preserves the source binary's buffer positions.
	 *
	 * @param data source Store binary
	 * @param maxPacketSize maximum packet payload size, in bytes
	 * @param consumer callback for each chunk
	 */
	public static void forEach(
		final Binary data,
		final int maxPacketSize,
		final Consumer<Chunk> consumer
	)
	{
		notNull(consumer);
		notNull(data);
		if (maxPacketSize <= 0) throw new IllegalArgumentException("maxPacketSize must be positive");
		visit(data, maxPacketSize, consumer);
	}

	/** Collects channel buffers in order without advancing their positions. */
	public static List<ByteBuffer> buffers(final Binary data)
	{
		notNull(data);
		final List<ByteBuffer> buffers = new ArrayList<>();
		data.iterateChannelChunks(chunk -> java.util.Collections.addAll(buffers, chunk.buffers()));
		return buffers;
	}
}
