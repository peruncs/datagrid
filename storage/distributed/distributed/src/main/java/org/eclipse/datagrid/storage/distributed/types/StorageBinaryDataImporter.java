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
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.util.X.notNull;

/** Copies incoming Store binary buffers into owned native memory and imports them. */
public final class StorageBinaryDataImporter
{
	private StorageBinaryDataImporter()
	{
	}

	/**
	 * Imports a snapshot of the source buffers and returns the native buffers that
	 * remain owned by the caller for subsequent object-graph materialization.
	 * Source positions are never changed.
	 *
	 * @param storage destination Store connection
	 * @param sourceBuffers source buffers
	 * @return imported native buffers, owned by the caller
	 */
	public static ByteBuffer[] importOwned(
		final StorageConnection storage,
		final ByteBuffer[] sourceBuffers
	)
	{
		notNull(storage);
		notNull(sourceBuffers);
		final ByteBuffer[] ownedBuffers = new ByteBuffer[sourceBuffers.length];
		try
		{
			for (int i = 0; i < sourceBuffers.length; i++)
			{
				final ByteBuffer source = notNull(sourceBuffers[i]).duplicate();
				if (!source.hasRemaining())
				{
					ownedBuffers[i] = ByteBuffer.allocateDirect(0);
					continue;
				}
				final ByteBuffer owned = XMemory.allocateDirectNative(source.remaining());
				ownedBuffers[i] = owned;
				owned.put(source).flip();
			}
			/* Storage.importData consumes the supplied views synchronously and does not
			 * retain them. Reset the owned buffers afterwards because their positions are
			 * needed by the deferred materializer. */
			storage.importData(org.eclipse.serializer.util.X.Enum(ownedBuffers));
			for (final ByteBuffer owned : ownedBuffers) owned.position(0);
			return ownedBuffers;
		}
		catch (final RuntimeException | Error failure)
		{
			release(ownedBuffers);
			throw failure;
		}
	}

	/**
	 * Imports already-direct buffers without allocating a second native copy.
	 *
	 * @param storage destination Store connection
	 * @param buffers buffers offered by the transport
	 * @return {@code true} when all buffers were direct and ownership was imported
	 */
	public static boolean importDirect(final StorageConnection storage, final ByteBuffer[] buffers)
	{
		notNull(storage);
		notNull(buffers);
		for (final ByteBuffer buffer : buffers)
		{
			if (buffer == null || !buffer.isDirect()) return false;
		}
		try
		{
			storage.importData(org.eclipse.serializer.util.X.Enum(buffers));
			for (final ByteBuffer buffer : buffers) buffer.position(0);
			return true;
		}
		catch (final RuntimeException | Error failure)
		{
			release(buffers);
			throw failure;
		}
	}

	/** Releases native buffers returned by {@link #importOwned(StorageConnection, ByteBuffer[])}. */
	public static void release(final ByteBuffer[] buffers)
	{
		if (buffers == null) return;
		for (final ByteBuffer buffer : buffers)
		{
			if (buffer != null) XMemory.deallocateDirectByteBuffer(buffer);
		}
	}
}
