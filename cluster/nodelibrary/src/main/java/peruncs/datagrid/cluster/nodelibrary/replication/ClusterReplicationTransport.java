package peruncs.datagrid.cluster.nodelibrary.replication;

import peruncs.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import peruncs.datagrid.storage.distributed.types.StorageBinaryTargetDistributing;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

import java.util.function.UnaryOperator;

/**
 * Aeron replication transport for one Data Grid cluster instance.
 *
 * <p>The transport supplies the distributor, reader, position, health, and
 * retention implementations; {@link #noOp()} covers nodes with replication
 * disabled.</p>
 */
public interface ClusterReplicationTransport extends AutoCloseable
{
	/** Returns the stable provider id, {@code aeron} or {@code none}.
	 * @return provider id
	 */
	String id();

	/**
	 * Creates a writer-side binary distributor for the named logical stream.
	 * Implementations may reject direct data publication when local Store
		 * acceptance must be coordinated; use {@link #persistenceTargetFactory(String,
		 * StorageBinaryDataDistributor)} for that transaction boundary.
		 *
		 * @param streamName logical stream name
		 * @param asynchronous whether publication may be asynchronous
		 * @return binary distributor
		 */
	ClusterStorageBinaryDataDistributor distributor(String streamName, boolean asynchronous);

		/** Creates a reader-side client starting at the supplied durable cursor.
		 *
		 * @param packetAcceptor destination for received packets
		 * @param streamName logical stream name
		 * @param cursorListener callback after data is applied
		 * @param startingCursor durable starting cursor
		 * @param commitPosition whether reader positions are committed
		 * @return binary data client
		 */
	ClusterStorageBinaryDataClient client(
		ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
		String streamName,
		AfterDataMessageConsumedListener cursorListener,
		ReplicationCursor startingCursor,
		boolean commitPosition
	);

		/** Returns the provider's latest published position used for backup/bootstrap.
		 *
		 * @param streamName logical stream name
		 * @return position provider
		 */
	ReplicationPositionProvider positionProvider(String streamName);

		/** Returns a provider-specific, safe log-retention controller.
		 *
		 * @return retention controller
		 */
	ReplicationLogRetention retention();

		/** Creates health state independent of any provider client implementation.
		 *
		 * @param storage storage readiness view
		 * @param client reader client
		 * @return health view
		 */
	ReplicationHealth health(
		StorageControllerAdapter storage,
		ClusterStorageBinaryDataClient client
	);

	/** Creates a target wrapper for coordinated publication.
	 *
	 * @param streamName logical stream name
	 * @param distributor binary distributor
	 * @return target factory
	 */
	default UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
		final String streamName,
		final StorageBinaryDataDistributor distributor
	)
	{
		return delegate -> StorageBinaryTargetDistributing.New(delegate, distributor);
	}

	@Override
	void close();

	/** Small view that avoids making the transport depend on Store internals. */
	interface StorageControllerAdapter
	{
		/** Reports whether local storage is ready.
		 * @return {@code true} when ready
		 */
		boolean isReady();
	}

	/** Creates a transport that performs no replication.
	 * @return disabled transport
	 */
	static ClusterReplicationTransport noOp()
	{
		return new ClusterReplicationTransport()
		{
			private final ReplicationCursor cursor = new ReplicationCursor("none", null, -1, new byte[0]);

			@Override
			public String id() { return "none"; }

			@Override
			public ClusterStorageBinaryDataDistributor distributor(final String streamName, final boolean asynchronous)
			{
				return ClusterStorageBinaryDataDistributor.NoOp();
			}

			@Override
			public ClusterStorageBinaryDataClient client(
				final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
				final String streamName,
				final AfterDataMessageConsumedListener cursorListener,
				final ReplicationCursor startingCursor,
				final boolean commitPosition
			)
			{
				return ClusterStorageBinaryDataClient.NoOp(startingCursor);
			}

			@Override
			public ReplicationPositionProvider positionProvider(final String streamName)
			{
				return new ReplicationPositionProvider()
				{
					public void init() { }
					public ReplicationCursor latest() { return cursor; }
					public void close() { }
				};
			}

			@Override
			public ReplicationLogRetention retention()
			{
				return new ReplicationLogRetention()
				{
					public MaintenanceResult deleteThrough(final ReplicationCursor ignored)
					{
						return new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, -1, "no replication log");
					}
					public void close() { }
				};
			}

			@Override
			public ReplicationHealth health(
				final StorageControllerAdapter storage,
				final ClusterStorageBinaryDataClient client
			)
			{
				return new ReplicationHealth()
				{
					public boolean isReady() { return storage.isReady(); }
					public boolean isHealthy() { return storage.isReady(); }
					public void init() { }
					public void close() { }
				};
			}

			@Override
			public void close() { }
		};
	}
}
