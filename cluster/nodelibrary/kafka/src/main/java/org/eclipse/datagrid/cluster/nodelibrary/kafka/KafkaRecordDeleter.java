package org.eclipse.datagrid.cluster.nodelibrary.kafka;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.types.MessageInfo;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationCursor;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationLogRetention;
import org.eclipse.serializer.collections.types.XImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

/** Deletes Kafka history only after the provider has validated its cursor boundary. */
public interface KafkaRecordDeleter extends ReplicationLogRetention
{
	/** Creates a deleter that discovers the topic from the admin client.
	 * @param kafkaAdminClient Kafka admin client
	 * @return record deleter
	 */
    static KafkaRecordDeleter New(final AdminClient kafkaAdminClient)
    {
        return new Default(notNull(kafkaAdminClient), null);
    }

	/** Creates a deleter for one topic.
	 * @param kafkaAdminClient Kafka admin client
	 * @param topicName Kafka topic
	 * @return record deleter
	 */
	static KafkaRecordDeleter New(final AdminClient kafkaAdminClient, final String topicName)
    {
        return new Default(notNull(kafkaAdminClient), notEmpty(topicName));
    }

	/** Deletes records through the supplied offsets.
	 * @param partitionOffsets partition end offsets
	 * @throws NodelibraryException if deletion fails
	 */
	void deleteUntilOffsets(XImmutableMap<TopicPartition, Long> partitionOffsets) throws NodelibraryException;

    @Override
	default MaintenanceResult deleteThrough(final ReplicationCursor cursor) throws NodelibraryException
    {
        this.deleteUntilOffsets(KafkaCursorCodec.decode(
            MessageInfo.New(cursor.logicalSequence(), cursor.transport(), cursor.storeGeneration(), cursor.providerPosition())
        ));
		return new MaintenanceResult(MaintenanceResult.Status.DELETED, cursor.logicalSequence(),
			"Kafka records deleted through cursor");
    }

    @Override
    default void close()
    {
    }

    /** Uses Kafka administration APIs to delete records before a cursor. */
    class Default implements KafkaRecordDeleter
    {
        private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordDeleter.class);

        private final AdminClient kafkaAdminClient;
        private final String topicName;

        private Default(final AdminClient kafkaAdminClient, final String topicName)
        {
            this.kafkaAdminClient = kafkaAdminClient;
            this.topicName = topicName;
        }

        @Override
		public MaintenanceResult deleteThrough(final ReplicationCursor cursor) throws NodelibraryException
        {
            final MessageInfo info = MessageInfo.New(
                cursor.logicalSequence(), cursor.transport(), cursor.storeGeneration(), cursor.providerPosition());
            this.deleteUntilOffsets(KafkaCursorCodec.decode(info, this.topicName));
			return new MaintenanceResult(MaintenanceResult.Status.DELETED, cursor.logicalSequence(),
				"Kafka records deleted through cursor");
        }

        @Override
        public void deleteUntilOffsets(final XImmutableMap<TopicPartition, Long> partitionOffsets)
            throws NodelibraryException
        {
            LOG.trace("Deleting old unused Kafka records.");

            if (partitionOffsets.size() != 1)
            {
                throw new NodelibraryException(
                    "Kafka replication retention requires exactly one partition; found " + partitionOffsets.size()
                );
            }
            if (this.topicName != null)
            {
                final TopicPartition[] partitionHolder = new TopicPartition[1];
                partitionOffsets.forEach(entry -> partitionHolder[0] = entry.key());
                final TopicPartition partition = partitionHolder[0];
                if (!this.topicName.equals(partition.topic()) || partition.partition() != 0)
                {
                    throw new NodelibraryException(
                        "Kafka replication retention cursor must target " + this.topicName + " partition 0"
                    );
                }
            }

            final var partitionRecordsToDelete = new HashMap<TopicPartition, RecordsToDelete>();
            partitionOffsets.forEach(entry ->
            {
                LOG.debug("Deleting to offset {} in partition {}", entry.value(), entry.key().partition());
                final var partition = entry.key();
                final var recordsToDelete = RecordsToDelete.beforeOffset(entry.value());
                partitionRecordsToDelete.put(partition, recordsToDelete);
            });
            final var deleteResult = this.kafkaAdminClient.deleteRecords(partitionRecordsToDelete);
            try
            {
                deleteResult.all().get(10, TimeUnit.MINUTES);
            }
			catch (final InterruptedException e)
			{
				Thread.currentThread().interrupt();
				throw new NodelibraryException(e);
			}
			catch (final ExecutionException e)
			{
				throw new NodelibraryException(e);
			}
			catch (final TimeoutException e)
			{
				LOG.warn("Timed out waiting for old records to be deleted.", e);
				throw new NodelibraryException(e);
			}
        }

        @Override
        public void close()
        {
            this.kafkaAdminClient.close();
        }
    }
}
