package peruncs.datagrid.storage.distributed.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.exceptions.PersistenceExceptionTransfer;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

import static org.eclipse.serializer.util.X.notNull;

/**
 * Persistence target decorator that mirrors each accepted Store binary to the
 * Aeron distributor.
 */
public interface StorageBinaryTargetDistributing extends PersistenceTarget<Binary>
{
	/** Creates a target that mirrors local writes to a distributor.
	 *
	 * @param delegate local persistence target
	 * @param distributor destination for committed binaries
	 * @return distributing target
	 */
	static StorageBinaryTargetDistributing New(
            final PersistenceTarget<Binary> delegate,
            final StorageBinaryDataDistributor distributor
    )
	{
		return new StorageBinaryTargetDistributing.Default(
			notNull(delegate),
			notNull(distributor)
		);
	}

	/** Delegates a local Store write before publishing the same binary. */
	class Default implements StorageBinaryTargetDistributing
	{
		private final PersistenceTarget<Binary> delegate;
		private final StorageBinaryDataDistributor distributor;

		Default(
			final PersistenceTarget<Binary> delegate,
			final StorageBinaryDataDistributor distributor
		)
		{
			super();
			this.delegate = delegate;
			this.distributor = distributor;
		}

		@Override
		public void write(final Binary data) throws PersistenceExceptionTransfer
		{
			data.iterateChannelChunks(Binary::mark);
			try
			{
				this.delegate.write(data);
			}
			finally
			{
				/* A failed local write must not leave channel positions marked. The
				 * same Binary instance is commonly retried by the Store thread. */
				data.iterateChannelChunks(Binary::reset);
			}

			this.distributor.distributeData(data);
		}

		@Override
		public boolean isWritable()
		{
			return this.delegate.isWritable();
		}

	}

}
