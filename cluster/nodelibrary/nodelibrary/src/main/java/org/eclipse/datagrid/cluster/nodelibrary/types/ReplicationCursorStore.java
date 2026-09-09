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

import org.eclipse.datagrid.storage.distributed.types.AtomicFileStore;
import org.eclipse.datagrid.storage.distributed.types.Crc32c;
import org.eclipse.serializer.io.XIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/** Atomic transport-neutral cursor file used by lifecycle implementations. */
public final class ReplicationCursorStore
{
	private static final int MAGIC = 0x44474352; // DGCR
	private static final short VERSION = 1;
	private static final int MAX_TRANSPORT_BYTES = 256;

	private ReplicationCursorStore() { }

	/** Writes a CRC-protected cursor using a forced temporary file and replace. */
	public static void write(final Path path, final ReplicationCursor cursor) throws IOException
	{
		final byte[] transport = cursor.transport().getBytes(StandardCharsets.UTF_8);
		if (transport.length > MAX_TRANSPORT_BYTES) throw new IOException("transport name is too long");
		final byte[] position = cursor.providerPosition();
		if (position.length > Integer.MAX_VALUE - 64) throw new IOException("cursor position is too large");
		final int length = 4 + 2 + 2 + 4 + transport.length + 16 + 8 + 4 + position.length + 4;
		final ByteBuffer encoded = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN)
			.putInt(MAGIC).putShort(VERSION).putShort((short)0).putInt(transport.length)
			.put(transport).putLong(cursor.storeGeneration() == null ? 0 : cursor.storeGeneration().getMostSignificantBits())
			.putLong(cursor.storeGeneration() == null ? 0 : cursor.storeGeneration().getLeastSignificantBits())
			.putLong(cursor.logicalSequence()).putInt(position.length).put(position);
		encoded.putInt(Crc32c.compute(encoded.array(), 0, encoded.position())).flip();
		AtomicFileStore.write(path, channel -> XIO.appendAll(channel, new ByteBuffer[] { encoded }),
			AtomicFileStore.PHASE_CURSOR);
	}

	/** Reads and validates a persisted cursor, rejecting truncation and bit-rot. */
	public static ReplicationCursor read(final Path path) throws IOException
	{
		final byte[] bytes = Files.readAllBytes(path);
		if (bytes.length < 4 + 2 + 2 + 4 + 16 + 8 + 4 + 4)
		{
			throw new IOException("truncated replication cursor");
		}
		final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
		final int expected = buffer.getInt(bytes.length - Integer.BYTES);
		if (expected != Crc32c.compute(bytes, 0, bytes.length - Integer.BYTES)) throw new IOException("cursor CRC32C mismatch");
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
		if (positionLength < 0 || positionLength > buffer.remaining() - Integer.BYTES)
		{
			throw new IOException("invalid provider position length");
		}
		final byte[] position = new byte[positionLength];
		buffer.get(position);
		return new ReplicationCursor(new String(transport, StandardCharsets.UTF_8),
			generation.equals(new UUID(0, 0)) ? null : generation, sequence, position);
	}

}
