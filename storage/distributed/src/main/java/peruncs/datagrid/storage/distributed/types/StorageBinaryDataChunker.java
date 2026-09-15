package peruncs.datagrid.storage.distributed.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

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

	/**
	 * One transport packet and its position in the source binary.
	 *
	 * <p>The payload is owned by the chunk and is not copied by the accessor.
	 * Consumers must treat it as read-only and must not retain or mutate it after
	 * their packet operation completes. This avoids a second copy at every Aeron
	 * hand-off.</p>
	 *
	 * @param bytes packet payload owned by this chunk
	 * @param index zero-based packet index
	 * @param count packet count for the complete message
		 * @param messageLength complete message length in bytes
		 */
		public record Chunk(byte[] bytes, int index, int count, int messageLength)
	{
		/** Validates packet ownership and positional metadata at construction time. */
		public Chunk
		{
			if (bytes == null || bytes.length == 0)
				throw new IllegalArgumentException("chunk payload must not be empty");
			if (index < 0 || count <= 0 || index >= count || messageLength <= 0)
				throw new IllegalArgumentException("invalid chunk metadata");
		}
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
		if (maxPacketSize <= 0) throw new IllegalArgumentException("maxPacketSize must be positive");
		final List<Chunk> chunks = new ArrayList<>();
		visit(data, maxPacketSize, chunks::add);
		return chunks;
	}

	/**
	 * Visits transport chunks in source order. The packet count is derived from
	 * the immutable binary length, so this method does not buffer the complete
	 * message before invoking the consumer.
	 *
	 * @param data source Store binary
	 * @param maxPacketSize maximum packet payload size, in bytes
	 * @param consumer callback for each chunk
	 */
	public static void forEach(final Binary data, final int maxPacketSize, final Consumer<Chunk> consumer)
	{
		notNull(consumer);
		notNull(data);
		if (maxPacketSize <= 0) throw new IllegalArgumentException("maxPacketSize must be positive");
		visit(data, maxPacketSize, consumer);
	}

	private static void visit(final Binary data, final int maxPacketSize, final Consumer<Chunk> consumer)
	{
		final long totalLength = totalLength(data);
		if (totalLength < 0L || totalLength > StorageBinaryDataMessage.MAX_MESSAGE_LENGTH)
		{
			throw new IllegalArgumentException("binary is too large for transport metadata");
		}
		final long packetCount = totalLength == 0L ? 0L : ((totalLength - 1L) / maxPacketSize) + 1L;
		if (packetCount > StorageBinaryDataMessage.MAX_PACKET_COUNT)
		{
			throw new IllegalArgumentException("binary has too many transport packets");
		}
		final PacketWriter writer = new PacketWriter(maxPacketSize, (int)totalLength,
			(int)packetCount, consumer);
		data.iterateChannelChunks(channel ->
		{
			if (channel == null) throw new StorageBinaryDataException("binary contains a null channel");
			for (final ByteBuffer source : channel.buffers())
			{
				if (source == null) throw new StorageBinaryDataException("binary contains a null channel buffer");
				writer.copy(source);
			}
		});
		writer.finish(totalLength);
	}

	private static long totalLength(final Binary data)
	{
		final long[] length = {0L};
		data.iterateChannelChunks(channel ->
		{
			if (channel == null) throw new StorageBinaryDataException("binary contains a null channel");
			for (final ByteBuffer buffer : channel.buffers())
			{
				if (buffer == null) throw new StorageBinaryDataException("binary contains a null channel buffer");
				try
				{
					length[0] = Math.addExact(length[0], buffer.remaining());
				}
				catch (final ArithmeticException e)
				{
					throw new StorageBinaryDataException("binary length overflow", e);
				}
			}
		});
		return length[0];
	}

	/** Streams source channels into fixed-size packet payloads. */
	private static final class PacketWriter
	{
		private final int maxPacketSize;
		private final int messageLength;
		private final int packetCount;
		private final Consumer<Chunk> consumer;
		private byte[] packet;
		private int packetOffset;
		private int packetIndex;
		private long copied;

		PacketWriter(final int maxPacketSize, final int messageLength, final int packetCount,
			final Consumer<Chunk> consumer)
		{
			this.maxPacketSize = maxPacketSize;
			this.messageLength = messageLength;
			this.packetCount = packetCount;
			this.consumer = consumer;
		}

		void copy(final ByteBuffer source)
		{
			final ByteBuffer buffer = source.duplicate();
			while (buffer.hasRemaining())
			{
				if (this.packet == null) this.packet = new byte[this.maxPacketSize];
				final int amount = Math.min(buffer.remaining(), this.packet.length - this.packetOffset);
				buffer.get(this.packet, this.packetOffset, amount);
				this.packetOffset += amount;
				this.copied += amount;
				if (this.packetOffset == this.packet.length) this.emit(this.packet);
			}
		}

		void finish(final long expectedLength)
		{
		if (this.packetOffset > 0)
		{
			final byte[] payload = new byte[this.packetOffset];
			System.arraycopy(this.packet, 0, payload, 0, this.packetOffset);
			this.emit(payload);
		}
			if (this.copied != expectedLength || this.packetIndex != this.packetCount)
			{
				throw new StorageBinaryDataException("binary changed while it was being chunked");
			}
		}

		private void emit(final byte[] payload)
		{
			this.consumer.accept(new Chunk(payload, this.packetIndex++, this.packetCount, this.messageLength));
			this.packet = null;
			this.packetOffset = 0;
		}
	}

	/** Collects duplicate source views in channel order.
	 * @param data source Store binary
	 * @return duplicate views in channel order
	 */
	public static List<ByteBuffer> buffers(final Binary data)
	{
		notNull(data);
		final List<ByteBuffer> buffers = new ArrayList<>();
		data.iterateChannelChunks(chunk ->
		{
			if (chunk == null) throw new StorageBinaryDataException("binary contains a null channel");
			for (final ByteBuffer buffer : chunk.buffers())
			{
				if (buffer == null) throw new StorageBinaryDataException("binary contains a null channel buffer");
				buffers.add(buffer.duplicate());
			}
		});
		return buffers;
	}

	/** Returns duplicate source views in channel order.
	 * @param data source Store binary
	 * @return duplicate views in channel order
	 */
	public static ByteBuffer[] bufferArray(final Binary data)
	{
		return buffers(data).toArray(ByteBuffer[]::new);
	}

	/**
	 * Creates a {@link ChunksWrapper} from borrowed packet buffers without
	 * changing those buffers. The wrapper format stores each logical length in
	 * the buffer position, while packet data exposes it as the remaining range.
	 *
	 * @param buffers borrowed direct packet buffers
	 * @return read-only wrapper over duplicate buffer views
	 */
	public static ChunksWrapper wrap(final Iterable<ByteBuffer> buffers)
	{
		notNull(buffers);
		final List<ByteBuffer> normalized = new ArrayList<>();
		for (final ByteBuffer source : buffers)
		{
			if (source == null || !source.isDirect())
			{
				throw new StorageBinaryDataException("packet payload must be a direct buffer");
			}
			final ByteBuffer duplicate = source.asReadOnlyBuffer();
			duplicate.position(duplicate.limit());
			normalized.add(duplicate);
		}
		return ChunksWrapper.New(normalized.toArray(ByteBuffer[]::new));
	}

	/**
	 * Returns the original direct buffers of an owned binary and normalizes them
	 * in place for Store import. This method is only for a caller that has
	 * already taken ownership of the binary and therefore may transfer release
	 * responsibility for the returned buffers.
	 *
	 * @param data owned binary
	 * @return original direct buffers, positioned at zero
	 */
	public static ByteBuffer[] ownedArray(final Binary data)
	{
		notNull(data);
		final List<ByteBuffer> buffers = new ArrayList<>();
		final List<Integer> logicalLengths = new ArrayList<>();
		data.iterateChannelChunks(channel ->
		{
			if (channel == null) throw new StorageBinaryDataException("binary contains a null channel");
			for (final ByteBuffer buffer : channel.buffers())
			{
				if (buffer == null || !buffer.isDirect())
				{
					throw new StorageBinaryDataException("owned binary contains a non-direct buffer");
				}
				final int logicalLength = data instanceof ChunksWrapper ? buffer.position() : buffer.remaining();
				if (logicalLength < 0 || logicalLength > buffer.capacity())
				{
					throw new StorageBinaryDataException("owned binary contains an invalid buffer length");
				}
				buffers.add(buffer);
				logicalLengths.add(logicalLength);
			}
		});
		for (int index = 0; index < buffers.size(); index++)
		{
			final ByteBuffer buffer = buffers.get(index);
			buffer.clear();
			buffer.limit(logicalLengths.get(index));
		}
		return buffers.toArray(ByteBuffer[]::new);
	}

	/**
	 * Returns import-ready duplicate views with position zero. Serializer's
	 * {@link ChunksWrapper} stores its logical length in the source position;
	 * ordinary binaries expose the remaining bytes instead.
	 *
	 * @param data source Store binary
	 * @return import-ready duplicate views
	 */
	public static ByteBuffer[] importArray(final Binary data)
	{
		notNull(data);
		final ByteBuffer[] source = bufferArray(data);
		final ByteBuffer[] result = new ByteBuffer[source.length];
		for (int index = 0; index < source.length; index++)
		{
			final ByteBuffer buffer = source[index];
			if (data instanceof ChunksWrapper)
			{
				final int logicalLength = buffer.position();
				if (logicalLength < 0 || logicalLength > buffer.capacity())
				{
					throw new StorageBinaryDataException("invalid wrapped binary buffer length");
				}
				buffer.clear();
				buffer.limit(logicalLength);
			}
			result[index] = buffer.slice();
		}
		return result;
	}
}
