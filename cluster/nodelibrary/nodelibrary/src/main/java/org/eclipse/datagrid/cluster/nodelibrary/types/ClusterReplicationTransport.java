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
	/** Returns the stable provider id, for example {@code kafka}, {@code aeron}, or {@code none}. */
	String id();

	/**
	 * Creates a writer-side binary distributor for the named logical stream.
	 * Implementations may reject direct data publication when local Store
	 * acceptance must be coordinated; use {@link #persistenceTargetFactory(String,
	 * StorageBinaryDataDistributor)} for that transaction boundary.
	 */
	ClusterStorageBinaryDataDistributor distributor(String streamName, boolean asynchronous);

	/** Creates a reader-side client starting at the supplied durable cursor. */
	ClusterStorageBinaryDataClient client(
		ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
		String streamName,
		AfterDataMessageConsumedListener cursorListener,
		ReplicationCursor startingCursor,
		boolean commitPosition
	);

	/** Returns the provider's latest published position used for backup/bootstrap. */
	ReplicationPositionProvider positionProvider(String streamName);

	/** Returns a provider-specific, safe log-retention controller. */
	ReplicationLogRetention retention();

	/** Creates health state independent of any provider client implementation. */
	ReplicationHealth health(
		StorageControllerAdapter storage,
		ClusterStorageBinaryDataClient client
	);

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
		boolean isReady();
	}

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
				return ClusterStorageBinaryDataClient.NoOp(startingCursor, cursorListener);
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
					public void deleteThrough(final ReplicationCursor ignored) { }
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
