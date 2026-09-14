package org.eclipse.datagrid.cluster.nodelibrary.kafka;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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
import org.eclipse.datagrid.cluster.nodelibrary.types.*;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Kafka implementation behind the neutral nodelibrary SPI. */
public final class KafkaClusterReplicationTransportProvider
	implements ClusterReplicationTransportProvider
{
	/** Creates the Kafka transport provider. */
	public KafkaClusterReplicationTransportProvider()
	{
	}

	@Override
	public String id()
	{
		return "kafka";
	}

	@Override
	public ClusterReplicationTransport create(final NodelibraryPropertiesProvider properties)
	{
		final String role = properties.replicationRole();
		final boolean writerRole;
		if ("writer".equalsIgnoreCase(role))
		{
			writerRole = true;
		}
		else if ("reader".equalsIgnoreCase(role) || "backup-reader".equalsIgnoreCase(role))
		{
			writerRole = false;
		}
		else
		{
			throw new IllegalArgumentException("Unsupported Kafka replication role: " + role);
		}
		final KafkaPropertiesProvider kafkaProperties = KafkaPropertiesProvider.ConfigDirectory();
		kafkaProperties.init();
		final String readerIdentity = readerIdentity(properties);
		return new Transport(properties.replicationStreamName(), kafkaProperties, writerRole, readerIdentity);
	}

	/** Resolves a group identity that survives replacement of the reader JVM. */
	static String readerIdentity(final NodelibraryPropertiesProvider properties)
	{
		return readerIdentity(properties.myPodName(), properties.replicationNodeIdentity(), properties.isProdMode());
	}

	static String readerIdentity(final String podName, final String nodeId, final boolean production)
	{
		final String identity = podName != null && !podName.isBlank() ? podName.trim()
			: nodeId != null && !nodeId.isBlank() ? nodeId.trim() : null;
		if (identity != null)
		{
			if (!identity.matches("[A-Za-z0-9._-]+"))
			{
				throw new IllegalArgumentException(
					"Kafka reader identity may contain only letters, digits, '.', '_' and '-'");
			}
			return identity;
		}
		if (production)
		{
			throw new IllegalArgumentException(
				"Kafka readers require MY_POD_NAME or ECLIPSE_DATAGRID_NODE_ID in production");
		}
		return UUID.randomUUID().toString();
	}

	/** Owns the Kafka replication resources for one node. */
	private static final class Transport implements ClusterReplicationTransport
	{
		private final String topic;
		private final KafkaPropertiesProvider properties;
		private final boolean writerRole;
		private final String readerIdentity;
		private volatile ClusterStorageBinaryDataDistributor distributor;
		private volatile ClusterStorageBinaryDataClient client;
		private volatile ReplicationPositionProvider positionProvider;
		private volatile ReplicationLogRetention retention;
		private volatile boolean closed;

		private Transport(final String topic, final KafkaPropertiesProvider properties, final boolean writerRole,
			final String readerIdentity)
		{
			if (topic == null || topic.isBlank())
			{
				throw new IllegalArgumentException("Kafka replication stream must not be blank");
			}
			this.topic = topic;
			this.properties = properties;
			this.writerRole = writerRole;
			this.readerIdentity = readerIdentity;
		}

		@Override
		public String id() { return "kafka"; }

		@Override
		public synchronized ClusterStorageBinaryDataDistributor distributor(
				final String streamName,
				final boolean asynchronous
		) {
			this.ensureOpen();
			if (!this.writerRole)
			{
				throw new IllegalStateException("Kafka reader cannot create a replication distributor");
			}
			if (!this.topic.equals(streamName))
			{
				throw new IllegalArgumentException("Kafka transport is configured for topic " + this.topic);
			}
			if (this.distributor != null) return this.distributor;
			final long initialMessageIndex;
			try
			{
				/* Kafka is the durable sequence source for this adapter. Resolve its tail
				 * before creating a producer so a writer restart cannot reuse an index that
				 * is already present in the topic. An empty topic deliberately returns -1. */
				final ReplicationPositionProvider position = this.positionProvider(streamName);
				position.init();
				initialMessageIndex = position.latestSequence();
			}
			catch (final NodelibraryException failure)
			{
				throw new IllegalStateException("Cannot resolve the Kafka writer sequence before startup", failure);
			}
			this.distributor = asynchronous
					? ClusterStorageBinaryDataDistributor.Caching(
					ClusterStorageBinaryDataDistributorKafka.Async(streamName, this.properties)
				)
					: ClusterStorageBinaryDataDistributor.Caching(
					ClusterStorageBinaryDataDistributorKafka.Sync(streamName, this.properties)
				);
			this.distributor.messageIndex(initialMessageIndex);
			return this.distributor;
		}

		@Override
		public synchronized ClusterStorageBinaryDataClient client(
				final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
				final String streamName,
				final AfterDataMessageConsumedListener cursorListener,
				final ReplicationCursor startingCursor,
				final boolean commitPosition
		) {
			this.ensureOpen();
			if (!this.topic.equals(streamName))
			{
				throw new IllegalArgumentException("Kafka transport is configured for topic " + this.topic);
			}
			if (this.writerRole)
			{
				return ClusterStorageBinaryDataClient.NoOp(startingCursor);
			}
			if (this.client != null)
			{
				this.client.dispose();
				this.client = null;
			}
			final ReplicationCursor cursorValue = startingCursor == null
				? new ReplicationCursor("kafka", null, -1, new byte[0])
				: startingCursor;
			final String groupId = streamName + "-" + this.readerIdentity;
			this.client = KafkaClusterStorageBinaryDataClient.New(
				packetAcceptor, streamName, groupId, cursorListener, cursorValue, this.properties, commitPosition
			);
			return this.client;
		}

		@Override
		public synchronized ReplicationPositionProvider positionProvider(final String streamName) {
			this.ensureOpen();
			if (!this.topic.equals(streamName))
			{
				throw new IllegalArgumentException("Kafka transport is configured for topic " + this.topic);
			}
			if (this.positionProvider != null) return this.positionProvider;
			final KafkaCursorProvider provider = KafkaCursorProvider.New(
					streamName, streamName + "-position-" + this.readerIdentity, this.properties
				);
			this.positionProvider = new ReplicationPositionProvider() {
				@Override
				public void init() {
					provider.init();
				}

				@Override
				public ReplicationCursor latest() {
					return provider.provideLatestCursor();
				}

				@Override
				public void close() {
					provider.close();
				}
			};
			return this.positionProvider;
		}

		@Override
		public synchronized ReplicationLogRetention retention() {
			this.ensureOpen();
			if (this.retention != null) return this.retention;
			/* Kafka deletion needs the minimum committed position of every active
			 * reader. This transport has no authenticated reader registry, so expose
			 * an explicit unsupported policy and preserve the topic history. */
			this.retention = new ReplicationLogRetention()
			{
				@Override
				public boolean isSupported() { return false; }

				@Override
				public MaintenanceResult deleteThrough(final ReplicationCursor ignored)
				{
					return new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, -1L,
						"Kafka reader-watermark registry is not configured");
				}

				@Override
				public void close() { }
			};
			return this.retention;
		}

		@Override
		public ReplicationHealth health(
				final StorageControllerAdapter storage,
				final ClusterStorageBinaryDataClient client
		) {
			final ReplicationPositionProvider latest = this.writerRole ? null : this.positionProvider(this.topic);
			return new ReplicationHealth() {
				private volatile boolean active = true;
				private volatile long latestSequence = -1L;
				private volatile RuntimeException latestFailure;
				private boolean latestRefreshStarted;
				private final ScheduledExecutorService latestRefresh =
					Executors.newSingleThreadScheduledExecutor(runnable ->
					{
						final Thread thread = new Thread(runnable, "datagrid-kafka-latest-reader");
						thread.setDaemon(true);
						return thread;
					});

				@Override
				public synchronized void init() throws NodelibraryException {
					if (latest == null) return;
					latest.init();
					this.latestSequence = latest.latestSequence();
					if (!this.latestRefreshStarted)
					{
						this.latestRefreshStarted = true;
						this.latestRefresh.scheduleWithFixedDelay(this::refreshLatest, 1L, 1L, TimeUnit.SECONDS);
					}
				}

				private void refreshLatest()
				{
					if (!this.active) return;
					try
					{
						final long candidate = latest.latestSequence();
						this.latestSequence = Math.max(this.latestSequence, candidate);
						this.latestFailure = null;
					}
					catch (final Throwable failure)
					{
						this.latestFailure = failure instanceof RuntimeException runtime
							? runtime : new IllegalStateException("Kafka latest-position refresh failed", failure);
					}
				}

				@Override
				public boolean isReady() throws NodelibraryException {
					if (!this.active || !storage.isReady()) return false;
					if (Transport.this.writerRole)
					{
						final ClusterStorageBinaryDataDistributor current = Transport.this.distributor;
						return current == null || current.failure() == null;
					}
					final ClusterStorageBinaryDataClient current = Transport.this.client;
					/* Readiness probes must not perform Kafka metadata or tail scans. The
					 * explicit init() call owns that bounded blocking operation. */
					return this.latestFailure == null && current != null && current.failure() == null && current.isRunning() &&
						this.latestSequence >= 0L &&
						current.cursor().logicalSequence() >= this.latestSequence;
				}

				@Override
				public boolean isHealthy() {
					final ClusterStorageBinaryDataClient current = Transport.this.client;
					return this.active && storage.isReady() &&
						(Transport.this.writerRole && (Transport.this.distributor == null ||
							Transport.this.distributor.failure() == null) ||
							(this.latestFailure == null && current != null && current.failure() == null && current.isRunning()));
				}

				@Override
				public State state()
				{
					if (!this.active) return State.FAILED;
					final ClusterStorageBinaryDataClient current = Transport.this.client;
					if (this.latestFailure != null || (current != null && current.failure() != null) ||
						(Transport.this.writerRole && Transport.this.distributor != null &&
							Transport.this.distributor.failure() != null))
					{
						return State.FAILED;
					}
					if (this.isReady()) return State.LIVE;
					return current != null && current.isRunning() ? State.REPLAYING : State.STARTING;
				}

				@Override
				public long appliedSequence()
				{
					final ClusterStorageBinaryDataClient current = Transport.this.client;
					return current == null ? -1L : current.cursor().logicalSequence();
				}

				@Override
				public void close() {
					this.active = false;
					this.latestRefresh.shutdownNow();
					/* The position provider is transport-owned and is closed by Transport.close().
					 * Keeping it alive here allows health probes to be recreated safely. */
				}
			};
		}

			@Override
			public synchronized void close() {
				if (this.closed) return;
				Throwable failure = null;
			try { if (this.client != null) this.client.dispose(); }
			catch (final Throwable closeFailure) { failure = closeFailure; }
			try { if (this.distributor != null) this.distributor.dispose(); }
			catch (final Throwable closeFailure)
			{
				if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
			}
			try { if (this.positionProvider != null) this.positionProvider.close(); }
			catch (final Throwable closeFailure)
			{
				if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
			}
			try { if (this.retention != null) this.retention.close(); }
			catch (final Throwable closeFailure)
			{
				if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
			}
			if (failure instanceof Error error) throw error;
			if (failure instanceof RuntimeException runtime) throw runtime;
			if (failure != null) throw new IllegalStateException("failed to close Kafka transport", failure);
			this.closed = true;
		}

		private void ensureOpen()
		{
			if (this.closed) throw new IllegalStateException("Kafka transport is closed");
		}
	}
}
