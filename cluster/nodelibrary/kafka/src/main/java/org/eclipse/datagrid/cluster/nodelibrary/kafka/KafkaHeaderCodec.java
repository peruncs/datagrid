package org.eclipse.datagrid.cluster.nodelibrary.kafka;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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

import java.nio.charset.StandardCharsets;

/** Canonical fixed-width Kafka header codec for replicated storage packets. */
final class KafkaHeaderCodec
{
	static final int WIRE_VERSION = 1;
	private static final String WIRE_VERSION_KEY = "wire-version";
	private static final String MESSAGE_TYPE_KEY = "message-type";
	private static final String MESSAGE_LENGTH_KEY = "message-length";
	private static final String MESSAGE_INDEX_KEY = "message-index";
	private static final String PACKET_COUNT_KEY = "packet-count";
	private static final String PACKET_INDEX_KEY = "packet-index";
	private static final String MESSAGE_CRC32C_KEY = "message-crc32c";
	private static final int MAX_PACKET_SIZE = 1_000_000;

	private KafkaHeaderCodec() { }

	static String keyMessageLength() { return MESSAGE_LENGTH_KEY; }
	static String keyMessageIndex() { return MESSAGE_INDEX_KEY; }
	static int maxPacketSize() { return MAX_PACKET_SIZE; }

	static void addPacketHeaders(
		final Headers headers,
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount,
		final long messageIndex,
		final int messageCrc32c
	)
	{
		if (headers == null) throw new IllegalArgumentException("Kafka headers are required");
		validateMetadata(messageLength, packetIndex, packetCount, messageIndex);
		headers.add(WIRE_VERSION_KEY, serializeInt(WIRE_VERSION));
		headers.add(MESSAGE_TYPE_KEY, serializeString(messageType.name()));
		headers.add(MESSAGE_LENGTH_KEY, serializeInt(messageLength));
		headers.add(PACKET_INDEX_KEY, serializeInt(packetIndex));
		headers.add(PACKET_COUNT_KEY, serializeInt(packetCount));
		headers.add(MESSAGE_INDEX_KEY, serializeLong(messageIndex));
		headers.add(MESSAGE_CRC32C_KEY, serializeInt(messageCrc32c));
	}

	static MessageType messageType(final Headers headers)
	{
		validateWireVersion(headers);
		try
		{
			return MessageType.valueOf(deserializeString(required(headers, MESSAGE_TYPE_KEY)));
		}
		catch (final IllegalArgumentException failure)
		{
			throw new IllegalArgumentException("invalid Kafka replication message-type header", failure);
		}
	}

	static int messageLength(final Headers headers)
	{
		validateWireVersion(headers);
		return deserializeInt(required(headers, MESSAGE_LENGTH_KEY));
	}

	static int packetIndex(final Headers headers)
	{
		validateWireVersion(headers);
		return deserializeInt(required(headers, PACKET_INDEX_KEY));
	}

	static int packetCount(final Headers headers)
	{
		validateWireVersion(headers);
		return deserializeInt(required(headers, PACKET_COUNT_KEY));
	}

	static long messageIndex(final Headers headers)
	{
		validateWireVersion(headers);
		return deserializeLong(required(headers, MESSAGE_INDEX_KEY));
	}

	static int messageCrc32c(final Headers headers)
	{
		validateWireVersion(headers);
		return deserializeInt(required(headers, MESSAGE_CRC32C_KEY));
	}

	static void validateMetadata(
		final int messageLength,
		final int packetIndex,
		final int packetCount,
		final long messageIndex
	)
	{
		if (messageLength <= 0 || messageLength > ReplicationLimits.MAX_MESSAGE_BYTES)
			throw new IllegalArgumentException("invalid Kafka replication message length: " + messageLength);
		if (packetCount <= 0 || packetCount > ReplicationLimits.MAX_PACKET_COUNT)
			throw new IllegalArgumentException("invalid Kafka replication packet count: " + packetCount);
		if (packetIndex < 0 || packetIndex >= packetCount)
			throw new IllegalArgumentException("invalid Kafka replication packet index: " + packetIndex);
		if (messageIndex < -1L || messageIndex == Long.MAX_VALUE)
			throw new IllegalArgumentException("invalid Kafka replication message index: " + messageIndex);
		final int expectedPacketCount = (messageLength + MAX_PACKET_SIZE - 1) / MAX_PACKET_SIZE;
		if (packetCount != expectedPacketCount)
			throw new IllegalArgumentException("Kafka replication packet count does not match message length");
	}

	static byte[] serializeString(final String value)
	{
		if (value == null) throw new IllegalArgumentException("Kafka header string is required");
		return value.getBytes(StandardCharsets.UTF_8);
	}

	static String deserializeString(final byte[] bytes)
	{
		if (bytes == null) throw new IllegalArgumentException("missing Kafka replication header value");
		return new String(bytes, StandardCharsets.UTF_8);
	}

	static byte[] serializeInt(final int value)
	{
		return new byte[]
		{
			(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value
		};
	}

	static int deserializeInt(final byte[] bytes)
	{
		if (bytes == null || bytes.length != Integer.BYTES)
			throw new IllegalArgumentException("Kafka integer header must contain four bytes");
		return (bytes[0] & 0xff) << 24 | (bytes[1] & 0xff) << 16 |
			(bytes[2] & 0xff) << 8 | (bytes[3] & 0xff);
	}

	static byte[] serializeLong(final long value)
	{
		return new byte[]
		{
			(byte)(value >>> 56), (byte)(value >>> 48), (byte)(value >>> 40), (byte)(value >>> 32),
			(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value
		};
	}

	static long deserializeLong(final byte[] bytes)
	{
		if (bytes == null || bytes.length != Long.BYTES)
			throw new IllegalArgumentException("Kafka long header must contain eight bytes");
		return ((long)(bytes[0] & 0xff) << 56) | ((long)(bytes[1] & 0xff) << 48) |
			((long)(bytes[2] & 0xff) << 40) | ((long)(bytes[3] & 0xff) << 32) |
			((long)(bytes[4] & 0xff) << 24) | ((long)(bytes[5] & 0xff) << 16) |
			((long)(bytes[6] & 0xff) << 8) | (long)(bytes[7] & 0xff);
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
			if (found != null)
			{
				throw new IllegalArgumentException("duplicate Kafka replication header: " + key);
			}
			found = header;
		}
		if (found == null || found.value() == null)
			throw new IllegalArgumentException("missing Kafka replication header: " + key);
		return found.value();
	}
}
