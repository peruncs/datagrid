package org.eclipse.datagrid.storage.distributed.aeron.checkpoint;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Persists restart records without coupling them to the wire format.
 *
 * <p>A new record is forced to a temporary file before it replaces the old
 * one. Reads validate the complete record and its checksum. A failed write
 * therefore leaves the previous restart boundary available.</p>
 */
public final class AeronReplicationCheckpointStore
{
	private static final ThreadLocal<ByteBuffer> ENCODE_BUFFER = ThreadLocal.withInitial(
		() -> ByteBuffer.allocate(AeronReplicationCheckpoint.ENCODED_BYTES).order(ByteOrder.BIG_ENDIAN));

	private AeronReplicationCheckpointStore() { }

	/**
	 * Replaces {@code path} only after the complete record is on disk.
	 *
	 * @param path checkpoint file
	 * @param checkpoint record to persist
	 * @throws IOException if the record cannot be written or forced to disk
	 */
	public static void write(final Path path, final AeronReplicationCheckpoint checkpoint) throws IOException
	{
		final ByteBuffer encoded = encode(checkpoint);
		AtomicFileStore.write(path, channel ->
		{
			while (encoded.hasRemaining())
			{
				if (channel.write(encoded) == 0) throw new IOException("Aeron checkpoint write made no progress");
			}
		},
			AtomicFileStore.PHASE_CHECKPOINT);
	}

	/**
	 * Reads a record and rejects a torn, corrupt, or incompatible file.
	 *
	 * @param path checkpoint file
	 * @return validated checkpoint
	 * @throws IOException if the file is missing, truncated, or invalid
	 */
	public static AeronReplicationCheckpoint read(final Path path) throws IOException
	{
		final long size = Files.size(path);
		if (size != AeronReplicationCheckpoint.ENCODED_BYTES)
		{
			throw new IOException("invalid Aeron checkpoint length=" + size);
		}
		final byte[] bytes = Files.readAllBytes(path);
		if (bytes.length != AeronReplicationCheckpoint.ENCODED_BYTES)
		{
			throw new IOException("invalid Aeron checkpoint length=" + bytes.length);
		}
		final int expected = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
			.getInt(AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES);
		if (expected != Crc32c.compute(bytes, 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES))
		{
			throw new IOException("Aeron checkpoint CRC32C mismatch");
		}
		try
		{
			final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
			if (buffer.getInt() != AeronReplicationCheckpoint.MAGIC ||
				buffer.getShort() != AeronReplicationCheckpoint.VERSION)
			{
				throw new IOException("unknown Aeron checkpoint format");
			}
			final var recordType = AeronReplicationCheckpoint.RecordType.from(Byte.toUnsignedInt(buffer.get()));
			final var mode = AeronReplicationCheckpoint.DurabilityMode.from(Byte.toUnsignedInt(buffer.get()));
			final var state = AeronReplicationCheckpoint.State.from(Byte.toUnsignedInt(buffer.get()));
			buffer.getShort();
			buffer.get();
			return new AeronReplicationCheckpoint(recordType, mode, state,
				readUuid(buffer), readUuid(buffer), readUuid(buffer),
				buffer.getLong(), buffer.getLong(), buffer.getLong(), buffer.getLong(),
				buffer.getInt(), buffer.getInt(), buffer.getInt());
		}
		catch (final RuntimeException e)
		{
			throw new IOException("invalid Aeron checkpoint fields", e);
		}
	}

	private static ByteBuffer encode(final AeronReplicationCheckpoint checkpoint)
	{
		final ByteBuffer buffer = ENCODE_BUFFER.get();
		buffer.clear();
		buffer.putInt(AeronReplicationCheckpoint.MAGIC)
			.putShort(AeronReplicationCheckpoint.VERSION)
			.put((byte)checkpoint.recordTypeCode())
			.put((byte)checkpoint.durabilityModeCode())
			.put((byte)checkpoint.stateCode())
			.putShort((short)0).put((byte)0);
		putUuid(buffer, checkpoint.clusterId());
		putUuid(buffer, checkpoint.nodeId());
		putUuid(buffer, checkpoint.storeGeneration());
		buffer.putLong(checkpoint.recordingId()).putLong(checkpoint.writerEpoch())
			.putLong(checkpoint.transactionSequence()).putLong(checkpoint.recordingPosition())
			.putInt(checkpoint.dataLength()).putInt(checkpoint.dataChunkCount())
			.putInt(checkpoint.resolutionCrc32c())
			.putInt(Crc32c.compute(buffer.array(), 0, AeronReplicationCheckpoint.ENCODED_BYTES - Integer.BYTES));
		return buffer.flip();
	}

	private static UUID readUuid(final ByteBuffer buffer)
	{
		return new UUID(buffer.getLong(), buffer.getLong());
	}

	private static void putUuid(final ByteBuffer buffer, final UUID value)
	{
		buffer.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits());
	}

}
