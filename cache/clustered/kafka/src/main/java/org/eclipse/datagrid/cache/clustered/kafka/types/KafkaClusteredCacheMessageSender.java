package org.eclipse.datagrid.cache.clustered.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Kafka
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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.datagrid.cache.clustered.types.TimestampsRegionUpdateMessage;
import org.eclipse.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;

import static org.eclipse.serializer.util.X.notNull;

/**
 * Publishes cache timestamp changes to a Kafka topic.
 *
 * <p>The sender is synchronous: it waits for each Kafka send to complete so a
 * local cache event cannot outrun the invalidation it represents, and it
 * throws {@link CacheEntryListenerException} when the send fails. The sender
 * closes the producer when the cache region is released. This class is the
 * Kafka counterpart of the Aeron sender and is not part of the public API; the
 * {@link KafkaClusteredCacheMessageComProvider} is the only entry point.</p>
 */
abstract class KafkaClusteredCacheMessageSender implements ClusteredCacheMessageSender<Object, Object>
{
    /** Record header that carries the sender identity used to ignore a node's own updates. */
    static final String SENDER_ID_HEADER = "Sender-Id";

    /** Creates a sender for the timestamp cache.
     *
     * @param producer Kafka producer
     * @param topicName Kafka topic for cache updates
     * @param clientId identifier used to ignore this client's own updates
     * @param serializer message serializer
     * @return timestamp update sender
     */
    static ClusteredCacheMessageSender<Object, Object> UpdateTimestamps(
        final KafkaProducer<String, byte[]> producer,
        final String topicName,
        final String clientId,
        final Serializer<byte[]> serializer
    )
    {
        return new UpdateTimestamps(
            notNull(producer),
            notNull(topicName),
            notNull(clientId),
            notNull(serializer)
        );
    }

    private static final Logger logger = LoggerFactory.getLogger(KafkaClusteredCacheMessageSender.class);
    private final KafkaProducer<String, byte[]> producer;
    private final String topicName;
    /* The serialized identity is fixed per sender; serializing it once avoids
     * an allocation and a serializer pass on every invalidation. */
    private final byte[] clientIdBytes;
    private final Serializer<byte[]> serializer;
    private volatile boolean disposed;

    /** Creates the shared sender state.
     *
     * @param producer Kafka producer
     * @param topicName Kafka topic for cache updates
     * @param clientId identifier used to ignore this client's own updates
     * @param serializer message serializer
     */
    private KafkaClusteredCacheMessageSender(
        final KafkaProducer<String, byte[]> producer,
        final String topicName, final String clientId,
        final Serializer<byte[]> serializer
    )
    {
        this.producer = producer;
        this.topicName = topicName;
        this.clientIdBytes = serializer.serialize(clientId);
        this.serializer = serializer;
    }

    /** Converts one cache event into a cluster update message.
     *
     * @param event cache event
     * @return message for the event
     */
    protected abstract TimestampsRegionUpdateMessage createMessage(CacheEntryEvent<?, ?> event);

    /** Publishes each event in order.
     *
     * @param cacheEntryEvents cache events to publish
     * @throws CacheEntryListenerException if serialization or publishing fails
     */
    protected void handleEvents(final Iterable<CacheEntryEvent<?, ?>> cacheEntryEvents)
        throws CacheEntryListenerException
    {
        for (final var event : cacheEntryEvents)
        {
            final var cacheName = event.getSource().getName();
            final var key = event.getKey();
            logger.debug("Sending cache message cache={}, key={}", cacheName, key);

            final byte[] message;
            try
            {
                message = this.serializer.serialize(this.createMessage(event));
            }
            catch (final Exception e)
            {
                throw new CacheEntryListenerException("Failed to serialize message for cache=" + cacheName, e);
            }

            final var record = new ProducerRecord<String, byte[]>(this.topicName, message);
            record.headers().add(SENDER_ID_HEADER, this.clientIdBytes);

            this.sendRecord(record, cacheName);
        }
    }

    private void sendRecord(final ProducerRecord<String, byte[]> record, final String cacheName)
        throws CacheEntryListenerException
    {
        final var future = this.producer.send(record);
        try
        {
            future.get();
        }
        catch (final Exception e)
        {
            if (e instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            throw new CacheEntryListenerException("Kafka send failed for cache=" + cacheName, e);
        }
    }

    @Override
    public void dispose()
    {
        if (this.disposed)
        {
            return;
        }
        this.disposed = true;
        try
        {
            this.producer.close();
        }
        catch (final RuntimeException e)
        {
            logger.error("Failed to close Kafka producer.", e);
        }
    }

    /** Converts timestamp cache events into cluster update messages. */
    private static final class UpdateTimestamps extends KafkaClusteredCacheMessageSender
        implements CacheEntryUpdatedListener<Object, Object>, CacheEntryCreatedListener<Object, Object>
    {
        /** Creates a timestamp update sender.
         *
         * @param producer Kafka producer
         * @param topicName Kafka topic for cache updates
         * @param clientId identifier used to ignore this client's own updates
         * @param serializer message serializer
         */
        private UpdateTimestamps(
            final KafkaProducer<String, byte[]> producer,
            final String topicName,
            final String clientId,
            final Serializer<byte[]> serializer
        )
        {
            super(producer, topicName, clientId, serializer);
        }

        @Override
        public void onCreated(final Iterable<CacheEntryEvent<?, ?>> cacheEntryEvents)
            throws CacheEntryListenerException
        {
            this.handleEvents(cacheEntryEvents);
        }

        @Override
        public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> cacheEntryEvents)
            throws CacheEntryListenerException
        {
            this.handleEvents(cacheEntryEvents);
        }

        @Override
        protected TimestampsRegionUpdateMessage createMessage(final CacheEntryEvent<?, ?> event)
        {
            return TimestampsRegionUpdateMessage.fromEvent(event);
        }
    }
}
