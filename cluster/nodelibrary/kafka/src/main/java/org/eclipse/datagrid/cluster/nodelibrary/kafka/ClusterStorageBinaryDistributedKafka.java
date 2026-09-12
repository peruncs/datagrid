package org.eclipse.datagrid.cluster.nodelibrary.kafka;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

/** Kafka header codec for the cluster nodelibrary packet stream. */
public final class ClusterStorageBinaryDistributedKafka
{
	/**
	 * Returns the message-type header name.
	 *
	 * @return header name
	 */
	public static String keyMessageType()
	{
		return "message-type";
	}

	/**
	 * Returns the message-length header name.
	 *
	 * @return header name
	 */
	public static String keyMessageLength()
	{
		return "message-length";
	}

	/**
	 * Returns the packet-count header name.
	 *
	 * @return header name
	 */
	public static String keyPacketCount()
	{
		return "packet-count";
	}

	/**
	 * Returns the message-index header name.
	 *
	 * @return header name
	 */
	public static String keyMessageIndex()
	{
		return "messageIndex";
	}

	/**
	 * Returns the packet-index header name.
	 *
	 * @return header name
	 */
	public static String keyPacketIndex()
	{
		return "packet-index";
	}

	/**
	 * Returns the maximum packet payload size.
	 *
	 * @return payload size in bytes
	 */
	public static int maxPacketSize()
	{
		return 1_000_000;
	}

	/**
	 * Returns the header charset.
	 *
	 * @return UTF-8 charset
	 */
	public static Charset charset()
	{
		return StandardCharsets.UTF_8;
	}

	/** Adds packet metadata headers.
	 * @param headers target headers
	 * @param messageType message kind
	 * @param messageLength message length
	 * @param packetIndex packet index
	 * @param packetCount packet count
	 * @param messageIndex message index
	 */
	public static void addPacketHeaders(
		final Headers headers,
		final MessageType messageType,
		final int messageLength,
		final int packetIndex,
		final int packetCount,
		final long messageIndex
	)
	{
		headers.add(keyMessageType(), serializeString(messageType.name()));
		headers.add(keyMessageLength(), serializeInt(messageLength));
		headers.add(keyPacketIndex(), serializeInt(packetIndex));
		headers.add(keyPacketCount(), serializeInt(packetCount));
		headers.add(keyMessageIndex(), serializeLong(messageIndex));
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

	/** Reads the message index from headers.
	 * @param headers Kafka headers
	 * @return message index
	 */
	public static long messageIndex(final Headers headers)
	{
		return deserializeLong(headers.lastHeader(keyMessageIndex()).value());
	}

	/** Encodes a string header.
	 * @param value string value
	 * @return UTF-8 bytes
	 */
	public static byte[] serializeString(final String value)
	{
		return value.getBytes(charset());
	}

	/** Decodes a string header.
	 * @param bytes UTF-8 bytes
	 * @return decoded string
	 */
	public static String deserializeString(final byte[] bytes)
	{
		return new String(bytes, charset());
	}

	/** Encodes an integer header.
	 * @param value integer value
	 * @return encoded bytes
	 */
	public static byte[] serializeInt(final int value)
	{
		return Integer.toString(value).getBytes(charset());
	}

	/** Decodes an integer header.
	 * @param bytes encoded integer
	 * @return integer value
	 */
	public static int deserializeInt(final byte[] bytes)
	{
		return Integer.parseInt(deserializeString(bytes));
	}

	/** Encodes a long header.
	 * @param value long value
	 * @return encoded bytes
	 */
	public static byte[] serializeLong(final long value)
	{
		return Long.toString(value).getBytes(charset());
	}

	/** Decodes a long header.
	 * @param bytes encoded long
	 * @return long value
	 */
	public static long deserializeLong(final byte[] bytes)
	{
		return Long.parseLong(deserializeString(bytes));
	}

	private ClusterStorageBinaryDistributedKafka()
	{
		throw new UnsupportedOperationException();
	}
}
