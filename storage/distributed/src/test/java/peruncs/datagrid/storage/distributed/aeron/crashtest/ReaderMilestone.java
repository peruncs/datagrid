package peruncs.datagrid.storage.distributed.aeron.crashtest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * Fixed CRC-protected barrier value exchanged by the reader crash child.
 *
 * <p>Unlike the writer milestone, this record includes the Archive position.
 * Reader cells use that position to prove that the cursor boundary and the
 * assembled transaction refer to the same terminal frame; the two schemas are
 * intentionally not interchangeable.</p>
 */
record ReaderMilestone(String point, long sequence, long position)
{
	private static final int MAGIC = 0x4447524D;
	private static final short VERSION = 1;
	private static final String[] POINTS = {
		"REPLAY_BEFORE_FIRST_IMPORT", "DURING_STORE_IMPORT", "DURING_STORE_IMPORT_FAILURE",
		"AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE", "DURING_CURSOR_FILE_WRITE",
		"AFTER_CURSOR_TEMP_WRITE_BEFORE_RENAME", "AFTER_CURSOR_RENAME_BEFORE_DIRECTORY_SYNC"
	};
	private static final int BYTES = Integer.BYTES + Short.BYTES + Short.BYTES + Long.BYTES * 3 + Integer.BYTES;

	static void write(final Path path, final String point, final long sequence, final long position) throws IOException
	{
		final ByteBuffer value = ByteBuffer.allocate(BYTES).order(ByteOrder.BIG_ENDIAN)
			.putInt(MAGIC).putShort(VERSION).putShort((short)code(point))
			.putLong(sequence).putLong(position).putLong(System.nanoTime());
		final byte[] bytes = value.array();
		final CRC32C crc = new CRC32C();
		crc.update(bytes, 0, BYTES - Integer.BYTES);
		value.putInt((int)crc.getValue()).flip();
		final Path absolute = path.toAbsolutePath();
		final Path parent = absolute.getParent();
		if (parent == null) throw new IOException("Reader milestone path has no parent: " + path);
		Files.createDirectories(parent);
		final Path temporary = Files.createTempFile(parent, absolute.getFileName() + ".tmp-", null);
		try
		{
			try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE))
			{
				while (value.hasRemaining())
				{
					if (channel.write(value) == 0) throw new IOException("Reader milestone write made no progress");
				}
				channel.force(true);
			}
			Files.move(temporary, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ))
			{
				directory.force(true);
			}
		}
		finally
		{
			Files.deleteIfExists(temporary);
		}
	}

	/** Returns whether this reader-process schema can encode the named point. */
	static boolean supports(final String point)
	{
		for (final String supported : POINTS) if (supported.equals(point)) return true;
		return false;
	}

	static ReaderMilestone read(final Path path) throws IOException
	{
		final byte[] bytes = Files.readAllBytes(path);
		if (bytes.length != BYTES) throw new IOException("invalid reader milestone length=" + bytes.length);
		final ByteBuffer value = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
		final int magic = value.getInt();
		final short version = value.getShort();
		final int point = Short.toUnsignedInt(value.getShort());
		final long sequence = value.getLong();
		final long position = value.getLong();
		value.getLong();
		final int actual = value.getInt();
		final CRC32C crc = new CRC32C();
		crc.update(bytes, 0, BYTES - Integer.BYTES);
		if (magic != MAGIC || version != VERSION || actual != (int)crc.getValue())
		{
			throw new IOException("invalid reader milestone header or CRC");
		}
		if (point >= POINTS.length) throw new IOException("unknown reader milestone point=" + point);
		return new ReaderMilestone(POINTS[point], sequence, position);
	}

	private static int code(final String point)
	{
		for (int i = 0; i < POINTS.length; i++) if (POINTS[i].equals(point)) return i;
		throw new IllegalArgumentException("unknown reader crash point " + point);
	}
}
