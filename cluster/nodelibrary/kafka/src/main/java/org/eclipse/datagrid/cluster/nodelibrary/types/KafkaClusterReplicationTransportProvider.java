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

/** Kafka implementation behind the neutral nodelibrary SPI. */
public final class KafkaClusterReplicationTransportProvider
	implements ClusterReplicationTransportProvider
{
	@Override
	public String id()
	{
		return "kafka";
	}

	@Override
	public ClusterReplicationTransport create(final NodelibraryPropertiesProvider properties)
	{
		final KafkaPropertiesProvider kafkaProperties = KafkaPropertiesProvider.ConfigDirectory();
		kafkaProperties.init();
		return new Transport(properties.replicationStreamName(), kafkaProperties);
	}

    private record Transport(String topic, KafkaPropertiesProvider properties) implements ClusterReplicationTransport {
        private Transport {
            if (topic == null || topic.isBlank()) {
                throw new IllegalArgumentException("Kafka replication stream must not be blank");
            }
        }

        @Override
        public String id() {
            return "kafka";
        }

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
            final MessageInfo cursor = MessageInfo.New(
                    startingCursor.logicalSequence(), startingCursor.transport(), startingCursor.storeGeneration(),
                    startingCursor.providerPosition()
            );
            final String groupId = streamName + "-" + System.getenv().getOrDefault("MY_POD_NAME", "reader");
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
            return KafkaRecordDeleter.New(admin);
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
                           client.messageInfo().messageIndex() >= latest.latestSequence() - 10;
                }

                @Override
                public boolean isHealthy() {
                    return this.active && storage.isReady();
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
