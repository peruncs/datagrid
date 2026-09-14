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

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryTargetDistributing;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

import java.util.function.UnaryOperator;

/**
 * Selected replication provider for one Data Grid cluster instance.
 *
 * <p>The lifecycle owns only this SPI. Provider modules supply the concrete
 * distributor, reader, position, health, and retention implementations through
 * {@link ClusterReplicationTransportProvider}; Kafka and Aeron are therefore
 * interchangeable without adding either client library to this artifact.</p>
 */
public interface ClusterReplicationTransport extends AutoCloseable
{
		/** Returns the stable provider id, for example {@code kafka}, {@code aeron}, or {@code none}.
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

	/** Small neutral view that avoids making the SPI depend on Store internals. */
	interface StorageControllerAdapter
	{
		/** Reports whether local storage is ready.
		 * @return {@code true} when ready
		 */
		boolean isReady();
	}

	/** Creates a transport that performs no replication.
	 * @return neutral transport
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
