package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
 * %%
 * Copyright (C) 2026 MicroStream Software
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies the packet boundary used by the logical-message commit policy. */
class StorageBinaryDataPacketAcceptorTest
{
	@Test
	void reportsIncompleteMessageUntilItsLastPacketIsAccepted()
	{
		final StorageBinaryDataPacketAcceptor acceptor = StorageBinaryDataPacketAcceptor.New(
			new StorageBinaryDataReceiver()
			{
				@Override public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary data) { }
				@Override public void receiveTypeDictionary(final String data) { }
			});

		assertTrue(acceptor.isAtMessageBoundary());
		acceptor.accept(List.of(packet(0, 2, new byte[] {1})));
		assertFalse(acceptor.isAtMessageBoundary());
		acceptor.accept(List.of(packet(1, 2, new byte[] {2})));
		assertTrue(acceptor.isAtMessageBoundary());
	}

	@Test
	void disposeDiscardsRetainedMessage()
	{
		final StorageBinaryDataPacketAcceptor acceptor = StorageBinaryDataPacketAcceptor.New(
			new StorageBinaryDataReceiver()
			{
				@Override public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary data) { }
				@Override public void receiveTypeDictionary(final String data) { }
			});

		acceptor.accept(List.of(packet(0, 2, new byte[] {1})));
		assertFalse(acceptor.isAtMessageBoundary());
		acceptor.dispose();
		assertTrue(acceptor.isAtMessageBoundary());
		acceptor.dispose();
	}

	private static StorageBinaryDataPacket packet(final int index, final int count, final byte[] bytes)
	{
		return StorageBinaryDataPacket.New(DATA, 2, index, count, ByteBuffer.wrap(bytes));
	}
}
