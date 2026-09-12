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

import org.apache.kafka.clients.admin.AdminClient;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.types.*;

import java.util.UUID;

/** Kafka implementation behind the neutral nodelibrary SPI. */
public final class KafkaClusterReplicationTransportProvider
	implements ClusterReplicationTransportProvider
{
	private static final String FALLBACK_READER_IDENTITY = UUID.randomUUID().toString();

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
		final String configuredReaderIdentity = properties.myPodName();
		final String readerIdentity = configuredReaderIdentity == null || configuredReaderIdentity.isBlank()
			? FALLBACK_READER_IDENTITY
			: configuredReaderIdentity.trim();
		return new Transport(properties.replicationStreamName(), kafkaProperties, writerRole, readerIdentity);
	}

	/** Owns the Kafka replication resources for one node. */
	private static final class Transport implements ClusterReplicationTransport
	{
		private final String topic;
		private final KafkaPropertiesProvider properties;
		private final boolean writerRole;
		private final String readerIdentity;
		private ClusterStorageBinaryDataDistributor distributor;
		private ClusterStorageBinaryDataClient client;
		private ReplicationPositionProvider positionProvider;
		private ReplicationLogRetention retention;
		private boolean closed;

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
			this.distributor = asynchronous
					? ClusterStorageBinaryDataDistributor.Caching(
					ClusterStorageBinaryDataDistributorKafka.Async(streamName, this.properties)
				)
					: ClusterStorageBinaryDataDistributor.Caching(
					ClusterStorageBinaryDataDistributorKafka.Sync(streamName, this.properties)
				);
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
				return ClusterStorageBinaryDataClient.NoOp(startingCursor, cursorListener);
			}
			if (this.client != null)
			{
				this.client.dispose();
			}
			final ReplicationCursor cursorValue = startingCursor == null
				? new ReplicationCursor("kafka", null, -1, new byte[0])
				: startingCursor;
			final MessageInfo cursor = MessageInfo.New(
					cursorValue.logicalSequence(), cursorValue.transport(), cursorValue.storeGeneration(),
					cursorValue.providerPosition()
			);
			final String groupId = streamName + "-" + this.readerIdentity;
			this.client = KafkaClusterStorageBinaryDataClient.New(
					packetAcceptor, streamName, groupId, cursorListener, cursor, this.properties, commitPosition
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
			final KafkaMessageInfoProvider provider = KafkaMessageInfoProvider.New(
					streamName, streamName + "-position", this.properties
				);
			this.positionProvider = new ReplicationPositionProvider() {
				@Override
				public void init() {
					provider.init();
				}

				@Override
				public ReplicationCursor latest() {
					final MessageInfo info = provider.provideLatestMessageInfo();
					return new ReplicationCursor(
							info.transport(), info.storeGeneration(), info.messageIndex(), info.providerPosition()
					);
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
			final AdminClient admin = AdminClient.create(this.properties.provide());
			this.retention = KafkaRecordDeleter.New(admin, this.topic);
			return this.retention;
		}

		@Override
		public ReplicationHealth health(
				final StorageControllerAdapter storage,
				final ClusterStorageBinaryDataClient client
		) {
			final ReplicationPositionProvider latest = this.positionProvider(this.topic);
			return new ReplicationHealth() {
				private volatile boolean active = true;

				@Override
				public void init() throws NodelibraryException {
					latest.init();
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
					return current != null && current.failure() == null && current.isRunning() &&
						current.messageInfo().messageIndex() >= latest.latestSequence() - 10;
				}

			@Override
				public boolean isHealthy() {
					final ClusterStorageBinaryDataClient current = Transport.this.client;
					return this.active && storage.isReady() &&
						(Transport.this.writerRole && (Transport.this.distributor == null ||
							Transport.this.distributor.failure() == null) ||
							(current != null && current.failure() == null && current.isRunning()));
				}

				@Override
				public void close() {
					this.active = false;
					/* The position provider is transport-owned and is closed by Transport.close().
					 * Keeping it alive here allows health probes to be recreated safely. */
				}
			};
		}

			@Override
			public synchronized void close() {
				if (this.closed) return;
				RuntimeException failure = null;
			try { if (this.client != null) this.client.dispose(); }
			catch (final RuntimeException closeFailure) { failure = closeFailure; }
			try { if (this.distributor != null) this.distributor.dispose(); }
			catch (final RuntimeException closeFailure)
			{
				if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
			}
			try { if (this.positionProvider != null) this.positionProvider.close(); }
			catch (final RuntimeException closeFailure)
			{
				if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
			}
			try { if (this.retention != null) this.retention.close(); }
			catch (final RuntimeException closeFailure)
			{
				if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
			}
			if (failure != null) throw new IllegalStateException("failed to close Kafka transport", failure);
			this.closed = true;
		}

		private void ensureOpen()
		{
			if (this.closed) throw new IllegalStateException("Kafka transport is closed");
		}
	}
}
