package peruncs.datagrid.cluster.nodelibrary.replication;

import peruncs.datagrid.storage.distributed.types.AtomicFileStore;
import peruncs.datagrid.storage.distributed.types.Crc32c;
import org.eclipse.serializer.io.XIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.UUID;

/** Atomic cursor file used by lifecycle implementations. */
public final class ReplicationCursorStore
{
	private static final int MAGIC = 0x44474352; // DGCR
	private static final short VERSION = 1;
	private static final int MAX_TRANSPORT_BYTES = 256;
	private static final int MAX_CURSOR_BYTES = 1 << 20;
	private static final int MAGIC_BYTES = Integer.BYTES;
	private static final int VERSION_BYTES = Short.BYTES;
	private static final int FLAGS_BYTES = Short.BYTES;
	private static final int TRANSPORT_LENGTH_BYTES = Integer.BYTES;
	private static final int GENERATION_BYTES = Long.BYTES * 2;
	private static final int SEQUENCE_BYTES = Long.BYTES;
	private static final int POSITION_LENGTH_BYTES = Integer.BYTES;
	private static final int CRC_BYTES = Integer.BYTES;
	private static final int FIXED_BYTES = MAGIC_BYTES + VERSION_BYTES + FLAGS_BYTES
		+ TRANSPORT_LENGTH_BYTES + GENERATION_BYTES + SEQUENCE_BYTES + POSITION_LENGTH_BYTES + CRC_BYTES;
	private static final UUID NULL_GENERATION = new UUID(0L, 0L);

	private ReplicationCursorStore() { }

	/** Writes a CRC-protected cursor using a forced temporary file and replace.
	 *
	 * @param path cursor path
	 * @param cursor cursor to store
	 * @throws IOException if the cursor cannot be stored
	 */
	public static void write(final Path path, final ReplicationCursor cursor) throws IOException
	{
		final ByteBuffer encoded = ByteBuffer.wrap(encode(cursor));
		AtomicFileStore.write(path, channel -> XIO.appendAll(channel, new ByteBuffer[] { encoded }),
			AtomicFileStore.PHASE_CURSOR);
	}

	/** Reads and validates a persisted cursor, rejecting truncation and bit-rot.
	 *
	 * @param path cursor path
	 * @return stored cursor
	 * @throws IOException if the cursor is missing or invalid
	 */
	public static ReplicationCursor read(final Path path) throws IOException
	{
		try (SeekableByteChannel channel = Files.newByteChannel(path,
			Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)))
		{
			final long size = channel.size();
			if (size < 0L || size > MAX_CURSOR_BYTES)
			{
				throw new IOException("replication cursor is too large");
			}
			final ByteBuffer bytes = ByteBuffer.allocate((int)size);
			while (bytes.hasRemaining())
			{
				if (channel.read(bytes) < 0) throw new IOException("truncated replication cursor");
			}
			return decode(bytes.array());
		}
	}

	/** Encodes the cursor format used both by cursor files and backup manifests. */
	public static byte[] encode(final ReplicationCursor cursor) throws IOException
	{
		final byte[] transport = cursor.transport().getBytes(StandardCharsets.UTF_8);
		if (transport.length > MAX_TRANSPORT_BYTES) throw new IOException("transport name is too long");
		final byte[] position = cursor.providerPosition();
		if (position.length > Integer.MAX_VALUE - FIXED_BYTES - MAX_TRANSPORT_BYTES)
		{
			throw new IOException("cursor position is too large");
		}
		final int length = FIXED_BYTES + transport.length + position.length;
		if (length > MAX_CURSOR_BYTES) throw new IOException("cursor is too large");
		final ByteBuffer encoded = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN)
			.putInt(MAGIC).putShort(VERSION).putShort((short)0).putInt(transport.length)
			.put(transport).putLong(cursor.storeGeneration() == null ? 0 : cursor.storeGeneration().getMostSignificantBits())
			.putLong(cursor.storeGeneration() == null ? 0 : cursor.storeGeneration().getLeastSignificantBits())
			.putLong(cursor.logicalSequence()).putInt(position.length).put(position);
		encoded.putInt(Crc32c.compute(encoded.array(), 0, encoded.position()));
		return encoded.array();
	}

	/** Decodes and validates a cursor file or backup manifest. */
	public static ReplicationCursor decode(final byte[] bytes) throws IOException
	{
		if (bytes.length < FIXED_BYTES)
		{
			throw new IOException("truncated replication cursor");
		}
		final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
		final int expected = buffer.getInt(bytes.length - CRC_BYTES);
		if (expected != Crc32c.compute(bytes, 0, bytes.length - CRC_BYTES)) throw new IOException("cursor CRC32C mismatch");
		if (buffer.getInt() != MAGIC || buffer.getShort() != VERSION) throw new IOException("unknown cursor format");
		buffer.getShort();
		final int transportLength = buffer.getInt();
		if (transportLength < 1 || transportLength > MAX_TRANSPORT_BYTES || transportLength > buffer.remaining())
		{
			throw new IOException("invalid transport length");
		}
		final byte[] transport = new byte[transportLength];
		buffer.get(transport);
		final UUID generation = new UUID(buffer.getLong(), buffer.getLong());
		final long sequence = buffer.getLong();
		final int positionLength = buffer.getInt();
		if (positionLength < 0 || positionLength > buffer.remaining() - CRC_BYTES)
		{
			throw new IOException("invalid provider position length");
		}
		final byte[] position = new byte[positionLength];
		buffer.get(position);
		if (buffer.position() != bytes.length - CRC_BYTES)
		{
			throw new IOException("replication cursor contains trailing bytes");
		}
		try
		{
			return new ReplicationCursor(new String(transport, StandardCharsets.UTF_8),
				generation.equals(NULL_GENERATION) ? null : generation, sequence, position);
		}
		catch (final IllegalArgumentException invalidCursor)
		{
			throw new IOException("invalid replication cursor values", invalidCursor);
		}
	}

}
