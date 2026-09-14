package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.eclipse.datagrid.storage.distributed.types.ReplicationLimits;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Fixed-width, validated Kafka metadata for neutral storage packets. */
public final class StorageBinaryDistributedKafka
{
	private static final int WIRE_VERSION = 1;
	private static final String WIRE_VERSION_KEY = "wire-version";
	private static final String MESSAGE_TYPE_KEY = "message-type";
	private static final String MESSAGE_LENGTH_KEY = "message-length";
	private static final String MESSAGE_INDEX_KEY = "message-index";
	private static final String PACKET_COUNT_KEY = "packet-count";
	private static final String PACKET_INDEX_KEY = "packet-index";
	private static final String MESSAGE_CRC32C_KEY = "message-crc32c";
	private static final int MAX_PACKET_SIZE = 1_000_000;

	private StorageBinaryDistributedKafka()
	{
	}

	/** Returns the message-type header name.
	 * @return message-type header name
	 */
	public static String keyMessageType() { return MESSAGE_TYPE_KEY; }
	/** Returns the message-length header name.
	 * @return message-length header name
	 */
	public static String keyMessageLength() { return MESSAGE_LENGTH_KEY; }
	/** Returns the message-index header name.
	 * @return message-index header name
	 */
	public static String keyMessageIndex() { return MESSAGE_INDEX_KEY; }
	/** Returns the packet-count header name.
	 * @return packet-count header name
	 */
	public static String keyPacketCount() { return PACKET_COUNT_KEY; }
	/** Returns the packet-index header name.
	 * @return packet-index header name
	 */
	public static String keyPacketIndex() { return PACKET_INDEX_KEY; }
	/** Returns the message checksum header name.
	 * @return message checksum header name
	 */
	public static String keyMessageCrc32c() { return MESSAGE_CRC32C_KEY; }
	/** Returns the maximum packet payload size.
	 * @return maximum packet payload size in bytes
	 */
	public static int maxPacketSize() { return MAX_PACKET_SIZE; }
	/** Returns the UTF-8 header charset.
	 * @return UTF-8 charset
	 */
	public static Charset charset() { return StandardCharsets.UTF_8; }

	/** Adds complete metadata for one packet.
	 * @param headers Kafka headers to mutate
	 * @param messageType message kind
	 * @param messageLength complete message length
	 * @param packetIndex zero-based packet index
	 * @param packetCount complete packet count
	 * @param messageIndex logical message index
	 * @param messageCrc32c CRC32C of the complete message
	 */
	public static void addPacketHeaders(
		final Headers headers,
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount,
		final long messageIndex,
		final int messageCrc32c
	)
	{
		validateMetadata(messageLength, packetIndex, packetCount, messageIndex);
		Objects.requireNonNull(headers, "headers");
		Objects.requireNonNull(messageType, "messageType");
		headers.add(WIRE_VERSION_KEY, serialize(WIRE_VERSION));
		headers.add(MESSAGE_TYPE_KEY, serialize(messageType.name()));
		headers.add(MESSAGE_LENGTH_KEY, serialize(messageLength));
		headers.add(PACKET_INDEX_KEY, serialize(packetIndex));
		headers.add(PACKET_COUNT_KEY, serialize(packetCount));
		headers.add(MESSAGE_INDEX_KEY, serializeLong(messageIndex));
		headers.add(MESSAGE_CRC32C_KEY, serialize(messageCrc32c));
	}

	/** Reads and validates the message kind.
	 * @param headers Kafka headers
	 * @return message kind
	 */
	public static MessageType messageType(final Headers headers)
	{
		try
		{
			return MessageType.valueOf(deserializeString(required(headers, MESSAGE_TYPE_KEY)));
		}
		catch (final IllegalArgumentException failure)
		{
			throw new IllegalArgumentException("invalid Kafka replication message-type header", failure);
		}
	}

	/** Reads the declared complete message length.
	 * @param headers Kafka headers
	 * @return message length in bytes
	 */
	public static int messageLength(final Headers headers)
	{
		return deserializeInt(requiredValidated(headers, MESSAGE_LENGTH_KEY));
	}

	/** Reads the zero-based packet index.
	 * @param headers Kafka headers
	 * @return zero-based packet index
	 */
	public static int packetIndex(final Headers headers)
	{
		return deserializeInt(requiredValidated(headers, PACKET_INDEX_KEY));
	}

	/** Reads the packet count.
	 * @param headers Kafka headers
	 * @return packet count
	 */
	public static int packetCount(final Headers headers)
	{
		return deserializeInt(requiredValidated(headers, PACKET_COUNT_KEY));
	}

	/** Reads the logical message index, or {@code -1} for an unindexed stream.
	 * @param headers Kafka headers
	 * @return logical message index
	 */
	public static long messageIndex(final Headers headers)
	{
		return deserializeLong(requiredValidated(headers, MESSAGE_INDEX_KEY));
	}

	/** Reads the CRC32C of the complete message payload.
	 * @param headers Kafka headers
	 * @return message CRC32C
	 */
	public static int messageCrc32c(final Headers headers)
	{
		return deserializeInt(requiredValidated(headers, MESSAGE_CRC32C_KEY));
	}

	/** Validates metadata independently of Kafka header encoding.
	 * @param messageLength complete message length
	 * @param packetIndex zero-based packet index
	 * @param packetCount complete packet count
	 * @param messageIndex logical message index, or {@code -1}
	 */
	public static void validateMetadata(
		final int messageLength,
		final int packetIndex,
		final int packetCount,
		final long messageIndex
	)
	{
		if (messageLength <= 0 || messageLength > ReplicationLimits.MAX_MESSAGE_BYTES)
			throw new IllegalArgumentException("invalid replication message length: " + messageLength);
		if (packetCount <= 0 || packetCount > ReplicationLimits.MAX_PACKET_COUNT)
			throw new IllegalArgumentException("invalid replication packet count: " + packetCount);
		if (packetIndex < 0 || packetIndex >= packetCount)
			throw new IllegalArgumentException("invalid replication packet index: " + packetIndex);
		if (messageIndex < -1L || messageIndex == Long.MAX_VALUE)
			throw new IllegalArgumentException("invalid replication message index: " + messageIndex);
		final int expectedPacketCount = (messageLength + MAX_PACKET_SIZE - 1) / MAX_PACKET_SIZE;
		if (packetCount != expectedPacketCount)
			throw new IllegalArgumentException("replication packet count does not match message length");
	}

	/** Encodes a UTF-8 header string.
	 * @param value string value
	 * @return UTF-8 bytes
	 */
	public static byte[] serialize(final String value)
	{
		return Objects.requireNonNull(value, "value").getBytes(charset());
	}

	/** Decodes a UTF-8 header string.
	 * @param bytes UTF-8 bytes
	 * @return decoded string
	 */
	public static String deserializeString(final byte[] bytes)
	{
		if (bytes == null) throw new IllegalArgumentException("missing Kafka replication header value");
		return new String(bytes, charset());
	}

	/** Encodes a fixed-width big-endian integer.
	 * @param value integer value
	 * @return four encoded bytes
	 */
	public static byte[] serialize(final int value)
	{
		return new byte[]{(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value};
	}

	/** Decodes a fixed-width big-endian integer.
	 * @param bytes four encoded bytes
	 * @return decoded integer
	 */
	public static int deserializeInt(final byte[] bytes)
	{
		if (bytes == null || bytes.length != Integer.BYTES)
			throw new IllegalArgumentException("Kafka integer header must contain four bytes");
		return (bytes[0] & 0xff) << 24 | (bytes[1] & 0xff) << 16 |
			(bytes[2] & 0xff) << 8 | (bytes[3] & 0xff);
	}

	/** Encodes a fixed-width big-endian long.
	 * @param value long value
	 * @return eight encoded bytes
	 */
	public static byte[] serializeLong(final long value)
	{
		return new byte[]{
			(byte)(value >>> 56), (byte)(value >>> 48), (byte)(value >>> 40), (byte)(value >>> 32),
			(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value
		};
	}

	/** Decodes a fixed-width big-endian long.
	 * @param bytes eight encoded bytes
	 * @return decoded long
	 */
	public static long deserializeLong(final byte[] bytes)
	{
		if (bytes == null || bytes.length != Long.BYTES)
			throw new IllegalArgumentException("Kafka long header must contain eight bytes");
		return ((long)(bytes[0] & 0xff) << 56) | ((long)(bytes[1] & 0xff) << 48) |
			((long)(bytes[2] & 0xff) << 40) | ((long)(bytes[3] & 0xff) << 32) |
			((long)(bytes[4] & 0xff) << 24) | ((long)(bytes[5] & 0xff) << 16) |
			((long)(bytes[6] & 0xff) << 8) | (long)(bytes[7] & 0xff);
	}

	private static byte[] requiredValidated(final Headers headers, final String key)
	{
		validateWireVersion(headers);
		return required(headers, key);
	}

	private static void validateWireVersion(final Headers headers)
	{
		if (deserializeInt(required(headers, WIRE_VERSION_KEY)) != WIRE_VERSION)
			throw new IllegalArgumentException("unsupported Kafka replication wire version");
	}

	private static byte[] required(final Headers headers, final String key)
	{
		if (headers == null) throw new IllegalArgumentException("Kafka headers are required");
		Header found = null;
		for (final Header header : headers.headers(key))
		{
			if (found != null) throw new IllegalArgumentException("duplicate Kafka replication header: " + key);
			found = header;
		}
		if (found == null || found.value() == null)
			throw new IllegalArgumentException("missing Kafka replication header: " + key);
		return found.value();
	}
}
