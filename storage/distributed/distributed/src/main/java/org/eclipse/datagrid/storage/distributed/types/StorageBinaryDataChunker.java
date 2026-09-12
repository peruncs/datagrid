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

	/** One packet and its position in the source binary.
	 *
	 * @param bytes packet payload
	 * @param index zero-based packet index
	 * @param count total packet count
	 * @param messageLength total message length
	 */
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
		/* Binary.buffers() exposes only the first Serializer channel.  A Store
		 * transaction can span several channels, so traverse every channel in
		 * Serializer order. We count first, then stream the same views directly
		 * into packets; no flattened buffer array or payload copy is retained. */
		final long messageLength = totalLength(data);
		final int length = (int)messageLength;
		if (length == 0) return;

		final int count = (int)((messageLength + maxPacketSize - 1L) / maxPacketSize);
		final PacketWriter writer = new PacketWriter(maxPacketSize, count, length, consumer);
		data.iterateChannelChunks(channel ->
		{
			if (channel == null) throw new IllegalStateException("binary contains a null channel");
			for (final ByteBuffer source : channel.buffers())
			{
				if (source == null) throw new IllegalStateException("binary contains a null channel buffer");
				writer.copy(source);
			}
		});
		writer.finish();
	}

	private static long totalLength(final Binary data)
	{
		final long[] total = {0L};
		data.iterateChannelChunks(channel ->
		{
			if (channel == null) throw new IllegalStateException("binary contains a null channel");
			for (final ByteBuffer buffer : channel.buffers())
			{
				if (buffer == null) throw new IllegalStateException("binary contains a null channel buffer");
				total[0] = Math.addExact(total[0], buffer.remaining());
				if (total[0] > Integer.MAX_VALUE)
				{
					throw new IllegalArgumentException("binary is too large for transport metadata");
				}
			}
		});
		return total[0];
	}

	/** Streams source channels into fixed-size packet payloads. */
	private static final class PacketWriter
	{
		private final int maxPacketSize;
		private final int count;
		private final int messageLength;
		private final Consumer<Chunk> consumer;
		private byte[] packet;
		private int packetOffset;
		private int index;
		private int copied;

		PacketWriter(final int maxPacketSize, final int count, final int messageLength,
			final Consumer<Chunk> consumer)
		{
			this.maxPacketSize = maxPacketSize;
			this.count = count;
			this.messageLength = messageLength;
			this.consumer = consumer;
		}

		void copy(final ByteBuffer source)
		{
			final ByteBuffer buffer = source.duplicate();
			while (buffer.hasRemaining())
			{
				if (this.copied >= this.messageLength)
				{
					throw new IllegalStateException("binary grew while chunking");
				}
				if (this.packet == null)
				{
					this.packet = new byte[Math.min(this.maxPacketSize, this.messageLength - this.copied)];
				}
				final int amount = Math.min(buffer.remaining(), this.packet.length - this.packetOffset);
				buffer.get(this.packet, this.packetOffset, amount);
				this.packetOffset += amount;
				this.copied += amount;
				if (this.packetOffset == this.packet.length)
				{
					this.consumer.accept(new Chunk(this.packet, this.index++, this.count, this.messageLength));
					this.packet = null;
					this.packetOffset = 0;
				}
			}
		}

		void finish()
		{
			if (this.packet != null)
			{
				this.consumer.accept(new Chunk(this.packet, this.index++, this.count, this.messageLength));
			}
			if (this.copied != this.messageLength || this.index != this.count)
			{
				throw new IllegalStateException("binary changed while chunking");
			}
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

	/** Collects channel buffers in order without advancing their positions.
	 *
	 * @param data source Store binary
	 * @return source buffers in channel order
	 */
	public static List<ByteBuffer> buffers(final Binary data)
	{
		notNull(data);
		final List<ByteBuffer> buffers = new ArrayList<>();
		data.iterateChannelChunks(chunk ->
		{
			if (chunk == null) throw new IllegalStateException("binary contains a null channel");
			for (final ByteBuffer buffer : chunk.buffers())
			{
				if (buffer == null) throw new IllegalStateException("binary contains a null channel buffer");
				buffers.add(buffer);
			}
		});
		return buffers;
	}
}
