package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

import org.eclipse.serializer.memory.XMemory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/** Shared packet reassembly and message grouping for all DataGrid transports. */
public final class StorageBinaryDataPacketAssembler
{
	private StorageBinaryDataPacketAssembler()
	{
	}

	/** Result of consuming a packet batch, including a possibly incomplete message. */
	public record Result(StorageBinaryDataMessage pending, List<StorageBinaryDataMessage> completed)
	{
	}

	/**
	 * Adds packets to the pending message and extracts every newly completed message.
	 *
	 * @param pending message carried over from the preceding packet batch
	 * @param packets packets received in order
	 * @return pending partial message and completed messages
	 */
	public static Result collect(
		final StorageBinaryDataMessage pending,
		final List<StorageBinaryDataPacket> packets
	)
	{
		StorageBinaryDataMessage current = pending;
		final List<StorageBinaryDataMessage> completed = new ArrayList<>();
		try
		{
			for (final StorageBinaryDataPacket packet : packets)
			{
				if (current == null)
				{
					current = StorageBinaryDataMessage.New(packet);
				}
				else
				{
					current.addPacket(packet);
				}
				if (current.isComplete())
				{
					completed.add(current);
					current = null;
				}
			}
		}
		catch (final RuntimeException | Error failure)
		{
			if (current != null)
			{
				current.dispose();
			}
			completed.forEach(StorageBinaryDataMessage::dispose);
			throw failure;
		}
		return new Result(current, completed);
	}

	/**
	 * Groups adjacent completed messages by type and sends each group once.
	 *
	 * @param messages completed messages in stream order
	 * @param sender callback receiving the final message and its buffers; the
	 * callback must consume the list before returning because the assembler
	 * reuses it for the next type group
	 */
	public static void dispatch(
		final List<StorageBinaryDataMessage> messages,
		final BiConsumer<StorageBinaryDataMessage, List<ByteBuffer>> sender
	)
	{
		StorageBinaryDataMessage last = null;
		final List<ByteBuffer> buffers = new ArrayList<>();
		for (final StorageBinaryDataMessage message : messages)
		{
			if (last != null && last.type() != message.type())
			{
				sender.accept(last, buffers);
				buffers.clear();
			}
			buffers.add(message.data());
			last = message;
		}
		if (last != null)
		{
			sender.accept(last, buffers);
		}
	}

	/** Decodes a completed type-dictionary message without changing its buffer position. */
	public static String decodeTypeDictionary(final ByteBuffer data)
	{
		return new String(XMemory.toArray(data.duplicate()), StandardCharsets.UTF_8);
	}
}
