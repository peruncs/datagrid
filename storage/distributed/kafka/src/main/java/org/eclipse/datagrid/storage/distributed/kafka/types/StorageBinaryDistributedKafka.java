package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */


import org.apache.kafka.common.header.Headers;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** Kafka header codec for the transport-neutral storage distributor. */
public final class StorageBinaryDistributedKafka
{
	/** Returns the message-type header name.
	 * @return header name
	 */
	public static String keyMessageType()
	{
		return "message-type";
	}

	/** Returns the message-length header name.
	 * @return header name
	 */
	public static String keyMessageLength()
	{
		return "message-length";
	}

	/** Returns the packet-count header name.
	 * @return header name
	 */
	public static String keyPacketCount()
	{
		return "packet-count";
	}

	/** Returns the packet-index header name.
	 * @return header name
	 */
	public static String keyPacketIndex()
	{
		return "packet-index";
	}

	/** Returns the maximum Kafka packet payload size.
	 * @return payload size in bytes
	 */
	public static int maxPacketSize()
	{
		return 1_000_000;
	}

	/** Returns the header charset.
	 * @return UTF-8 charset
	 */
	public static Charset charset()
	{
		return StandardCharsets.UTF_8;
	}

	/** Adds packet metadata headers.
	 *
	 * @param headers target headers
	 * @param messageType message kind
	 * @param messageLength complete message length
	 * @param packetIndex packet index
	 * @param packetCount packet count
	 */
	public static void addPacketHeaders(
		final Headers headers,
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount
	)
	{
		headers.add(keyMessageType(), serialize(messageType.name()));
		headers.add(keyMessageLength(), serialize(messageLength));
		headers.add(keyPacketIndex(), serialize(packetIndex));
		headers.add(keyPacketCount(), serialize(packetCount));
	}

	/** Reads the message kind from headers.
	 * @param headers Kafka headers
	 * @return message kind
	 */
	public static MessageType messageType(final Headers headers)
	{
		return MessageType.valueOf(deserializeString(headers.lastHeader(keyMessageType()).value()));
	}

	/** Reads the message length from headers.
	 * @param headers Kafka headers
	 * @return message length
	 */
	public static int messageLength(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyMessageLength()).value());
	}

	/** Reads the packet index from headers.
	 * @param headers Kafka headers
	 * @return packet index
	 */
	public static int packetIndex(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketIndex()).value());
	}

	/** Reads the packet count from headers.
	 * @param headers Kafka headers
	 * @return packet count
	 */
	public static int packetCount(final Headers headers)
	{
		return deserializeInt(headers.lastHeader(keyPacketCount()).value());
	}

	/** Encodes a header string.
	 * @param value string value
	 * @return UTF-8 bytes
	 */
	public static byte[] serialize(final String value)
	{
		return value.getBytes(charset());
	}

	/** Decodes a header string.
	 * @param bytes UTF-8 bytes
	 * @return decoded string
	 */
	public static String deserializeString(final byte[] bytes)
	{
		return new String(bytes, charset());
	}

	/** Encodes an integer header value.
	 * @param value integer value
	 * @return encoded bytes
	 */
	public static byte[] serialize(final int value)
	{
		return new byte[] {
			(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value
		};
	}

	/** Decodes an integer header value.
	 * @param bytes encoded integer
	 * @return integer value
	 */
	public static int deserializeInt(final byte[] bytes)
	{
		int value = 0;
		for (final byte b : bytes)
		{
			value <<= 8;
			value |= b & 0xFF;
		}
		return value;
	}

	private StorageBinaryDistributedKafka()
	{
		throw new UnsupportedOperationException();
	}
}
