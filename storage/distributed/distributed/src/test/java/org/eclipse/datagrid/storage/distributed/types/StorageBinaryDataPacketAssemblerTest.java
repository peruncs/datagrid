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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType.DATA;
import static org.junit.jupiter.api.Assertions.*;

class StorageBinaryDataPacketAssemblerTest
{
	@Test
	void carriesIncompleteMessageAcrossBatchesAndCompletesInOrder()
	{
		final StorageBinaryDataPacket first = packet(0, 2, new byte[] {1, 2});
		final StorageBinaryDataPacket second = packet(1, 2, new byte[] {3});

		final var partial = StorageBinaryDataPacketAssembler.collect(null, List.of(first));
		assertEquals(0, partial.completed().size());

		final var complete = StorageBinaryDataPacketAssembler.collect(partial.pending(), List.of(second));
		assertNull(complete.pending());
		assertEquals(1, complete.completed().size());
		final ByteBuffer data = complete.completed().get(0).data();
		assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), data);
		complete.completed().forEach(StorageBinaryDataMessage::dispose);
	}

	@Test
	void dispatchesAdjacentMessagesAsOneBufferGroupPerType()
	{
		final var first = message(DATA, new byte[] {1});
		final var second = message(DATA, new byte[] {2});
		final var dictionary = message(StorageBinaryDataMessage.MessageType.TYPE_DICTIONARY, new byte[] {3});

		final var groups = new java.util.ArrayList<String>();
		StorageBinaryDataPacketAssembler.dispatch(List.of(first, second, dictionary), (last, buffers) ->
			groups.add(last.type() + ":" + buffers.size()));

		assertEquals(List.of("DATA:2", "TYPE_DICTIONARY:1"), groups);
		first.dispose();
		second.dispose();
		dictionary.dispose();
	}

	@Test
	void rejectsOversizedMessageBeforeNativeAllocation()
	{
		assertThrows(IllegalArgumentException.class, () -> StorageBinaryDataMessage.New(
			StorageBinaryDataPacket.New(DATA, StorageBinaryDataMessage.MAX_MESSAGE_LENGTH + 1,
				0, 1, ByteBuffer.allocate(0))));
	}

	private static StorageBinaryDataPacket packet(final int index, final int count, final byte[] bytes)
	{
		return StorageBinaryDataPacket.New(DATA, 3, index, count, ByteBuffer.wrap(bytes));
	}

	private static StorageBinaryDataMessage message(
		final StorageBinaryDataMessage.MessageType type,
		final byte[] bytes
	)
	{
		return StorageBinaryDataMessage.New(
			StorageBinaryDataPacket.New(type, bytes.length, 0, 1, ByteBuffer.wrap(bytes))
		);
	}
}
