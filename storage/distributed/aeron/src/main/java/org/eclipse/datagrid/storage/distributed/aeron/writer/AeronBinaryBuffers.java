package org.eclipse.datagrid.storage.distributed.aeron.writer;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import org.eclipse.serializer.persistence.binary.types.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapts Serializer's multi-channel binary view to the ordered buffer stream
 * used by the Aeron wire publisher.
 *
 * <p>{@link Binary#buffers()} is the buffers for one channel when the Store
 * uses channel partitioning. Replication must include every channel in channel
 * order, otherwise a reader can materialise a transaction that is only a
 * prefix of the local Store commit.</p>
 */
final class AeronBinaryBuffers
{
	private AeronBinaryBuffers()
	{
	}

	/** Returns all completed channel buffers without changing their positions. */
	static ByteBuffer[] collect(final Binary data)
	{
		if (data == null) throw new NullPointerException("data");
		final List<ByteBuffer> buffers = new ArrayList<>();
		data.iterateChannelChunks(channel ->
		{
			for (final ByteBuffer buffer : channel.buffers())
			{
				if (buffer == null)
				{
					throw new IllegalStateException("Serializer returned a null channel buffer");
				}
				buffers.add(buffer);
			}
		});
		return buffers.toArray(ByteBuffer[]::new);
	}
}
