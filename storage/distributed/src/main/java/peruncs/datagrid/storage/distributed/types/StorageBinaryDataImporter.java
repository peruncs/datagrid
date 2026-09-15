package peruncs.datagrid.storage.distributed.types;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.util.X.notNull;

/** Copies incoming Store binary buffers into owned native memory and imports them. */
public final class StorageBinaryDataImporter
{
	private static final ByteBuffer EMPTY_DIRECT_BUFFER = ByteBuffer.allocateDirect(0);

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
		return importAndReset(storage, copyBuffers(sourceBuffers));
	}

	private static ByteBuffer[] importAndReset(final StorageConnection storage, final ByteBuffer[] importedBuffers)
	{
		try
		{
			/* Storage.importData consumes the supplied views synchronously and does not
			 * retain them. Reset the owned buffers afterwards because their positions are
			 * needed by the deferred materializer. */
			storage.importData(org.eclipse.serializer.util.X.Enum(importedBuffers));
			for (final ByteBuffer imported : importedBuffers) imported.position(0);
			return importedBuffers;
		}
		catch (final RuntimeException | Error failure)
		{
			release(importedBuffers);
			throw failure;
		}
	}

	private static ByteBuffer[] copyBuffers(final ByteBuffer[] sourceBuffers)
	{
		final ByteBuffer[] ownedBuffers = new ByteBuffer[sourceBuffers.length];
		try
		{
			for (int i = 0; i < sourceBuffers.length; i++)
			{
				final ByteBuffer source = notNull(sourceBuffers[i]).duplicate();
				if (source.position() != 0)
				{
					throw new IllegalArgumentException(
						"import buffers must be normalized to position zero; got " + source.position());
				}
				final int sourceLength = source.remaining();
				if (sourceLength == 0)
				{
					ownedBuffers[i] = EMPTY_DIRECT_BUFFER.duplicate();
					continue;
				}
				final ByteBuffer owned = XMemory.allocateDirectNative(sourceLength);
				ownedBuffers[i] = owned;
				owned.put(source).flip();
			}
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
			if (buffer.position() != 0)
			{
				throw new IllegalArgumentException("import buffers must be normalized to position zero");
			}
		}
		try
		{
			importAndReset(storage, buffers);
			return true;
		}
		catch (final RuntimeException | Error failure)
		{
			release(buffers);
			throw failure;
		}
	}

	/** Releases native buffers returned by {@link #importOwned(StorageConnection, ByteBuffer[])}.
	 *
	 * @param buffers buffers to release
	 */
	public static void release(final ByteBuffer[] buffers)
	{
		if (buffers == null) return;
		RuntimeException failure = null;
		for (final ByteBuffer buffer : buffers)
		{
			if (buffer != null && buffer.capacity() > 0)
			{
				try
				{
					XMemory.deallocateDirectByteBuffer(buffer);
				}
				catch (final RuntimeException cleanupFailure)
				{
					if (failure == null) failure = cleanupFailure;
					else failure.addSuppressed(cleanupFailure);
				}
			}
		}
		if (failure != null) throw failure;
	}
}
