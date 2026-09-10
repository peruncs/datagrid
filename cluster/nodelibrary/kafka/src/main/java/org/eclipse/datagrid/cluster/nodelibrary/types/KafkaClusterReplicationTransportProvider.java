package org.eclipse.datagrid.cluster.nodelibrary.types;

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

	private static final class Transport implements ClusterReplicationTransport
	{
		private final String topic;
		private final KafkaPropertiesProvider properties;
		private final boolean writerRole;
		private final String readerIdentity;

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
		public ClusterStorageBinaryDataDistributor distributor(
				final String streamName,
				final boolean asynchronous
		) {
			return asynchronous
					? ClusterStorageBinaryDataDistributor.Caching(
					ClusterStorageBinaryDataDistributorKafka.Async(streamName, this.properties)
			)
					: ClusterStorageBinaryDataDistributor.Caching(
					ClusterStorageBinaryDataDistributorKafka.Sync(streamName, this.properties)
			);
		}

		@Override
		public ClusterStorageBinaryDataClient client(
				final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
				final String streamName,
				final AfterDataMessageConsumedListener cursorListener,
				final ReplicationCursor startingCursor,
				final boolean commitPosition
		) {
			final ReplicationCursor cursorValue = startingCursor == null
				? new ReplicationCursor("kafka", null, -1, new byte[0])
				: startingCursor;
			final MessageInfo cursor = MessageInfo.New(
					cursorValue.logicalSequence(), cursorValue.transport(), cursorValue.storeGeneration(),
					cursorValue.providerPosition()
			);
			final String groupId = streamName + "-" + this.readerIdentity;
			return KafkaClusterStorageBinaryDataClient.New(
					packetAcceptor, streamName, groupId, cursorListener, cursor, this.properties, commitPosition
			);
		}

		@Override
		public ReplicationPositionProvider positionProvider(final String streamName) {
			final KafkaMessageInfoProvider provider = KafkaMessageInfoProvider.New(
					streamName, streamName + "-position", this.properties
			);
			return new ReplicationPositionProvider() {
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
		}

		@Override
		public ReplicationLogRetention retention() {
			final AdminClient admin = AdminClient.create(this.properties.provide());
			return KafkaRecordDeleter.New(admin, this.topic);
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
					return this.active && storage.isReady() &&
						   (Transport.this.writerRole ||
							   (client.failure() == null && client.isRunning())) &&
						   client.messageInfo().messageIndex() >= latest.latestSequence() - 10;
				}

				@Override
				public boolean isHealthy() {
					return this.active && storage.isReady() &&
						   (Transport.this.writerRole ||
							   (client.failure() == null && client.isRunning()));
				}

				@Override
				public void close() {
					this.active = false;
					latest.close();
				}
			};
		}

		@Override
		public void close() {
		}
	}
}
