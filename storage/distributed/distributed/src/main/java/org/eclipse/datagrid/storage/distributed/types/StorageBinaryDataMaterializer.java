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

import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.memory.XMemory.getDirectByteBufferAddress;

/** Applies imported native Store binary buffers to the object graph. */
public final class StorageBinaryDataMaterializer
{
	private StorageBinaryDataMaterializer()
	{
	}

	/**
	 * Materializes all entities in the supplied buffers.
	 *
	 * @param storage Store connection owning the persistence manager
	 * @param buffers direct buffers imported into {@code storage}
	 */
	public static void materialize(final StorageConnection storage, final ByteBuffer[] buffers)
	{
		if (storage == null || buffers == null) throw new NullPointerException("storage and buffers");
		final ObjectMaterializer materializer = new ObjectMaterializer(storage.persistenceManager());
		final BinaryEntityRawDataIterator iterator = BinaryEntityRawDataIterator.New();
		for (final ByteBuffer buffer : buffers)
		{
			if (buffer == null || !buffer.isDirect() || buffer.position() != 0)
			{
				throw new StorageBinaryDataException("materializer requires direct buffers at position zero");
			}
			if (buffer.limit() == 0) continue;
			final long address = getDirectByteBufferAddress(buffer);
			iterator.iterateEntityRawData(address, address + buffer.limit(), materializer);
		}
		materializer.materialize();
	}
}
