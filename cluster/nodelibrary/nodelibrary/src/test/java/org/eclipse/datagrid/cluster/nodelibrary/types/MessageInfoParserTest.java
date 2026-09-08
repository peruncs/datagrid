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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MessageInfoParserTest
{
	@Test
	void parsesVersionedCursorText()
	{
		final UUID generation = UUID.randomUUID();
		final byte[] position = { 4, 5, 6 };
		final MessageInfo info = MessageInfoParser.New().parseMessageInfo(
			"41\naeron\n" + generation + "\n" + Base64.getEncoder().encodeToString(position) + "\n"
		);
		assertEquals(41, info.messageIndex());
		assertEquals("aeron", info.transport());
		assertEquals(generation, info.storeGeneration());
		assertArrayEquals(position, info.providerPosition());
	}

	@Test
	void keepsLegacyProviderRowsOpaque()
	{
		final MessageInfo info = MessageInfoParser.New().parseMessageInfo("7\ntopic,0,19\n");
		assertEquals(7, info.messageIndex());
		assertEquals("kafka", info.transport());
		assertEquals("topic,0,19", new String(info.providerPosition()));
	}

	@Test
	void keepsAllLegacyKafkaPartitions()
	{
		final MessageInfo info = MessageInfoParser.New().parseMessageInfo(
			"7\ntopic,0,19\ntopic,1,23\n");
		assertEquals("topic,0,19\ntopic,1,23", new String(info.providerPosition()));
	}

	@Test
	void rejectsTrailingCursorFields()
	{
		assertThrows(NodelibraryException.class, () -> MessageInfoParser.New().parseMessageInfo(
			"7\naeron\n\n\nunexpected\n"));
	}
}
