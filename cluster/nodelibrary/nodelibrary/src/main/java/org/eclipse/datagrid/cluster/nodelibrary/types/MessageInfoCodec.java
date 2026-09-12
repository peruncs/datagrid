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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Shared versioned text codec for legacy offsets and backup replication manifests. */
public final class MessageInfoCodec
{
	private MessageInfoCodec() { }

	/** Serializes message information as a text manifest.
	 *
	 * @param info message information
	 * @return serialized manifest
	 */
	public static String serialize(final MessageInfo info)
	{
		return info.messageIndex() + "\n"
			+ info.transport() + "\n"
			+ (info.storeGeneration() == null ? "" : info.storeGeneration()) + "\n"
			+ Base64.getEncoder().encodeToString(info.providerPosition()) + "\n";
	}

	/** Serializes message information as UTF-8 bytes.
	 *
	 * @param info message information
	 * @return serialized bytes
	 */
	public static byte[] serializeBytes(final MessageInfo info)
	{
		return serialize(info).getBytes(StandardCharsets.UTF_8);
	}
}
