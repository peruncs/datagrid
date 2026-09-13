package org.eclipse.datagrid.cache.clustered.aeron.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Aeron
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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * Fixed framing for one clustered-cache invalidation frame.
 *
 * <p>A frame is a big-endian magic, a version, the sender identity (a 16-byte
 * UUID), a per-sender monotonic sequence, the serialized payload length, and
 * the serialized payload. The sender identity lets a node ignore frames it
 * published itself; comparing two 64-bit words is allocation-free and
 * constant-time, so a hostile sender id cannot trigger a per-byte comparison
 * loop. The sequence lets a receiver detect a missed burst of invalidations.</p>
 *
 * <p>The payload length is validated before any allocation, so a hostile or
 * corrupt length cannot trigger an unbounded allocation. The channel is
 * assumed to be on an isolated network; the frame carries no authentication,
 * matching the Kafka adapter.</p>
 */
final class AeronClusteredCacheMessageCodec
{
	/** Wire version; bump when the framing changes. */
	static final int VERSION = 1;
	/** Magic "DGAC" identifying a DataGrid clustered-cache frame. */
	private static final int MAGIC = 0x44474143;
	/** Header bytes: magic, version, sender id, sequence, payload length. */
	static final int HEADER_LENGTH = Integer.BYTES * 3 + Long.BYTES * 3;
	private static final int MAGIC_OFFSET = 0;
	private static final int VERSION_OFFSET = Integer.BYTES;
	private static final int SENDER_ID_OFFSET = Integer.BYTES * 2;
	private static final int SEQUENCE_OFFSET = SENDER_ID_OFFSET + Long.BYTES * 2;
	private static final int PAYLOAD_LENGTH_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
	private static final int PAYLOAD_OFFSET = HEADER_LENGTH;

	private AeronClusteredCacheMessageCodec()
	{
	}

	/**
	 * Encodes one frame into the supplied buffer at offset zero.
	 *
	 * @param buffer target buffer with room for the frame
	 * @param senderId sender identity bytes
	 * @param sequence per-sender monotonic sequence of this frame
	 * @param payload serialized invalidation payload
	 * @return encoded frame length
	 */
	static int encode(final MutableDirectBuffer buffer, final byte[] senderId, final long sequence,
		final byte[] payload)
	{
		if (senderId == null || senderId.length != Long.BYTES * 2)
		{
			throw new IllegalArgumentException("sender id must be exactly 16 bytes");
		}
		buffer.putInt(MAGIC_OFFSET, MAGIC, ByteOrder.BIG_ENDIAN);
		buffer.putInt(VERSION_OFFSET, VERSION, ByteOrder.BIG_ENDIAN);
		buffer.putBytes(SENDER_ID_OFFSET, senderId, 0, Long.BYTES);
		buffer.putBytes(SENDER_ID_OFFSET + Long.BYTES, senderId, Long.BYTES, Long.BYTES);
		buffer.putLong(SEQUENCE_OFFSET, sequence, ByteOrder.BIG_ENDIAN);
		buffer.putInt(PAYLOAD_LENGTH_OFFSET, payload.length, ByteOrder.BIG_ENDIAN);
		buffer.putBytes(PAYLOAD_OFFSET, payload, 0, payload.length);
		return HEADER_LENGTH + payload.length;
	}

	/**
	 * Validates the frame header and reports whether it carries the expected
	 * sender identity without copying the sender-id bytes or looping per byte.
	 * A malformed or truncated frame simply does not match.
	 *
	 * @param buffer source buffer
	 * @param offset frame offset
	 * @param length frame length
	 * @param expectedSenderId sender identity to compare against
	 * @return {@code true} when the frame is well formed and carries the expected identity
	 */
	static boolean senderIdMatches(final DirectBuffer buffer, final int offset, final int length,
		final byte[] expectedSenderId)
	{
		if (length < HEADER_LENGTH ||
			buffer.getInt(offset + MAGIC_OFFSET, ByteOrder.BIG_ENDIAN) != MAGIC ||
			buffer.getInt(offset + VERSION_OFFSET, ByteOrder.BIG_ENDIAN) != VERSION)
		{
			return false;
		}
		return buffer.getLong(offset + SENDER_ID_OFFSET, ByteOrder.BIG_ENDIAN) == readLong(expectedSenderId, 0) &&
			buffer.getLong(offset + SENDER_ID_OFFSET + Long.BYTES, ByteOrder.BIG_ENDIAN) ==
				readLong(expectedSenderId, Long.BYTES);
	}

	/** Identity of one frame sender, used as a gap-tracking key. */
	record SenderId(long mostSignificantBits, long leastSignificantBits)
	{
	}

	/**
	 * Returns the sender identity of a frame whose header has already been
	 * validated by {@link #senderIdMatches}.
	 *
	 * @param buffer source buffer
	 * @param offset frame offset
	 * @return sender identity
	 */
	static SenderId senderIdOf(final DirectBuffer buffer, final int offset)
	{
		return new SenderId(
			buffer.getLong(offset + SENDER_ID_OFFSET, ByteOrder.BIG_ENDIAN),
			buffer.getLong(offset + SENDER_ID_OFFSET + Long.BYTES, ByteOrder.BIG_ENDIAN));
	}

	/**
	 * Returns the identity of a 16-byte sender id byte array.
	 *
	 * @param senderId sender identity bytes
	 * @return sender identity
	 */
	static SenderId senderIdOf(final byte[] senderId)
	{
		if (senderId == null || senderId.length != Long.BYTES * 2)
		{
			throw new IllegalArgumentException("sender id must be exactly 16 bytes");
		}
		return new SenderId(readLong(senderId, 0), readLong(senderId, Long.BYTES));
	}

	/**
	 * Validates the header length and returns the per-sender sequence of a
	 * frame. The magic and version must already have been validated by
	 * {@link #senderIdMatches}.
	 *
	 * @param buffer source buffer
	 * @param offset frame offset
	 * @param length frame length
	 * @return per-sender sequence
	 * @throws IllegalArgumentException when the frame is shorter than its header
	 */
	static long sequenceOf(final DirectBuffer buffer, final int offset, final int length)
	{
		if (length < HEADER_LENGTH)
		{
			throw new IllegalArgumentException(
				"Aeron clustered-cache frame of " + length + " bytes is shorter than its header");
		}
		return buffer.getLong(offset + SEQUENCE_OFFSET, ByteOrder.BIG_ENDIAN);
	}

	/**
	 * Validates the complete frame and returns a copy of its payload.
	 *
	 * @param buffer source buffer
	 * @param offset frame offset
	 * @param length frame length
	 * @param maxPayloadBytes maximum accepted payload size
	 * @return copied payload bytes
	 * @throws IllegalArgumentException when the frame is malformed or oversized
	 */
	static byte[] decodePayload(final DirectBuffer buffer, final int offset, final int length,
		final int maxPayloadBytes)
	{
		if (length < HEADER_LENGTH)
		{
			throw new IllegalArgumentException(
				"Aeron clustered-cache frame of " + length + " bytes is shorter than its header");
		}
		if (buffer.getInt(offset + MAGIC_OFFSET, ByteOrder.BIG_ENDIAN) != MAGIC)
		{
			throw new IllegalArgumentException("unknown Aeron clustered-cache frame magic");
		}
		if (buffer.getInt(offset + VERSION_OFFSET, ByteOrder.BIG_ENDIAN) != VERSION)
		{
			throw new IllegalArgumentException("unsupported Aeron clustered-cache frame version");
		}
		final int payloadLength = buffer.getInt(offset + PAYLOAD_LENGTH_OFFSET, ByteOrder.BIG_ENDIAN);
		if (payloadLength < 0 || payloadLength > maxPayloadBytes)
		{
			throw new IllegalArgumentException(
				"Invalid Aeron clustered-cache payload length: " + payloadLength);
		}
		if (HEADER_LENGTH + payloadLength != length)
		{
			throw new IllegalArgumentException(
				"Aeron clustered-cache frame length does not match its payload");
		}

		final byte[] payload = new byte[payloadLength];
		buffer.getBytes(offset + PAYLOAD_OFFSET, payload, 0, payloadLength);
		return payload;
	}

	/** Reads a big-endian long without allocating a buffer. */
	private static long readLong(final byte[] bytes, final int offset)
	{
		return ((long)bytes[offset] & 0xffL) << 56 |
			((long)bytes[offset + 1] & 0xffL) << 48 |
			((long)bytes[offset + 2] & 0xffL) << 40 |
			((long)bytes[offset + 3] & 0xffL) << 32 |
			((long)bytes[offset + 4] & 0xffL) << 24 |
			((long)bytes[offset + 5] & 0xffL) << 16 |
			((long)bytes[offset + 6] & 0xffL) << 8 |
			((long)bytes[offset + 7] & 0xffL);
	}
}