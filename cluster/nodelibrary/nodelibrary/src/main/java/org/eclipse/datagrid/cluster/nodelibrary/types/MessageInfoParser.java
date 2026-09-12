package org.eclipse.datagrid.cluster.nodelibrary.types;

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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

import java.util.Base64;
import java.util.UUID;

/** Parses the transport-neutral persisted replication cursor format. */
public interface MessageInfoParser
{
	/** Creates the default parser.
	 * @return message information parser
	 */
	static MessageInfoParser New() { return new Default(); }

	/** Parses persisted message information.
	 *
	 * @param offsetFileContent persisted cursor text
	 * @return parsed message information
	 * @throws NodelibraryException if the text is invalid
	 */
	MessageInfo parseMessageInfo(String offsetFileContent) throws NodelibraryException;

	/** Parses both the current cursor format and the older Kafka form. */
	final class Default implements MessageInfoParser
	{
		/** Creates the default parser implementation. */
		Default()
		{
		}

		@Override
		public MessageInfo parseMessageInfo(final String content) throws NodelibraryException
		{
			if (content == null || content.isBlank()) throw new NodelibraryException("Empty replication cursor");
			final String[] rows = content.trim().split("\\R");
			try
			{
				final long index = Long.parseLong(rows[0]);
				if (rows.length == 1) return MessageInfo.New(index);
				if (rows.length >= 4 && !rows[1].contains(","))
				{
					if (rows.length != 4) throw new IllegalArgumentException("trailing replication cursor fields");
					final UUID generation = rows[2].isBlank() ? null : UUID.fromString(rows[2]);
					return MessageInfo.New(index, rows[1], generation, Base64.getDecoder().decode(rows[3]));
				}
				final String legacyPosition = String.join("\n", java.util.Arrays.copyOfRange(rows, 1, rows.length));
				return MessageInfo.New(index, "kafka", null, legacyPosition.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			}
			catch (final RuntimeException failure)
			{
				throw new NodelibraryException("Failed to parse replication cursor", failure);
			}
		}
	}
}
