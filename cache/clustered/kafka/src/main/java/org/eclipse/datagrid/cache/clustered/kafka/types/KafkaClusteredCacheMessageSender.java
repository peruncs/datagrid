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

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.concurrent.TimeUnit;

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
            notNull(serializer),
            1 << 20,
            30_000L
        );
    }

    /** Creates a sender with explicit payload and send-time limits.
     *
     * @param producer Kafka producer
     * @param topicName Kafka topic for cache updates
     * @param clientId identifier used to ignore this client's own updates
     * @param serializer message serializer
     * @param maxPayloadBytes maximum serialized payload accepted for publication
     * @param sendTimeoutMillis maximum wait for one Kafka acknowledgement
     * @return timestamp update sender
     */
    static ClusteredCacheMessageSender<Object, Object> UpdateTimestamps(
        final KafkaProducer<String, byte[]> producer,
        final String topicName,
        final String clientId,
        final Serializer<byte[]> serializer,
        final int maxPayloadBytes,
        final long sendTimeoutMillis
    )
    {
        if (maxPayloadBytes <= 0) throw new IllegalArgumentException("maxPayloadBytes must be positive");
        if (sendTimeoutMillis <= 0L) throw new IllegalArgumentException("sendTimeoutMillis must be positive");
        return new UpdateTimestamps(
            notNull(producer), notNull(topicName), notNull(clientId), notNull(serializer),
            maxPayloadBytes, sendTimeoutMillis);
    }

    private static final System.Logger LOGGER =
        System.getLogger(KafkaClusteredCacheMessageSender.class.getName());
    private final KafkaProducer<String, byte[]> producer;
    private final String topicName;
    /* The serialized identity is fixed per sender; serializing it once avoids
     * an allocation and a serializer pass on every invalidation. */
    private final byte[] clientIdBytes;
    private final Serializer<byte[]> serializer;
    private final int maxPayloadBytes;
    private final long sendTimeoutMillis;
    private final Object lifecycleMonitor = new Object();
    private volatile boolean disposed;
    private boolean closing;
    private int inFlight;

    /** Creates the shared sender state.
     *
     * @param producer Kafka producer
     * @param topicName Kafka topic for cache updates
     * @param clientId identifier used to ignore this client's own updates
     * @param serializer message serializer
     * @param maxPayloadBytes maximum serialized payload accepted for publication
     * @param sendTimeoutMillis maximum wait for one Kafka acknowledgement
     */
    private KafkaClusteredCacheMessageSender(
        final KafkaProducer<String, byte[]> producer,
        final String topicName, final String clientId,
        final Serializer<byte[]> serializer,
        final int maxPayloadBytes,
        final long sendTimeoutMillis
    )
    {
        this.producer = producer;
        this.topicName = topicName;
        this.clientIdBytes = java.util.Objects.requireNonNull(
            serializer.serialize(clientId), "serializer returned a null client identity").clone();
        this.serializer = serializer;
        this.maxPayloadBytes = maxPayloadBytes;
        this.sendTimeoutMillis = sendTimeoutMillis;
    }

    /** Returns whether this sender has completed its terminal disposal. */
    boolean isDisposed()
    {
        return this.disposed;
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
            LOGGER.log(System.Logger.Level.DEBUG,
                "Sending cache message cache=" + cacheName + ", key=" + key);

            final TimestampsRegionUpdateMessage messageObject;
            final byte[] message;
            try
            {
                messageObject = this.createMessage(event);
                message = java.util.Objects.requireNonNull(
                    this.serializer.serialize(messageObject),
                    "serializer returned a null clustered-cache payload");
                if (message.length > this.maxPayloadBytes)
                {
                    throw new IllegalArgumentException(
                        "serialized clustered-cache payload exceeds " + this.maxPayloadBytes + " bytes");
                }
            }
            catch (final Exception e)
            {
                throw new CacheEntryListenerException("Failed to serialize message for cache=" + cacheName, e);
            }

            final String partitionKey = partitionKey(messageObject);
            final var record = new ProducerRecord<>(this.topicName, partitionKey, message);
            record.headers().add(SENDER_ID_HEADER, this.clientIdBytes);

            this.sendRecord(record, cacheName);
        }
    }

    /**
     * Returns the stable Kafka partition key for one timestamp table. The NUL
     * separator cannot occur in a valid Kafka topic/cache name (the provider
     * rejects it for the topic and the message record validates its names), so
     * the mapping is unambiguous while avoiding a per-record hashing object.
     */
    static String partitionKey(final TimestampsRegionUpdateMessage message)
    {
        return message.cacheName() + '\0' + message.tableName();
    }

    private void sendRecord(final ProducerRecord<String, byte[]> record, final String cacheName)
        throws CacheEntryListenerException
    {
        synchronized (this.lifecycleMonitor)
        {
            if (this.disposed || this.closing)
            {
                throw new CacheEntryListenerException(
                    "Kafka clustered-cache sender is closing or disposed");
            }
            this.inFlight++;
        }
        try
        {
            final var future = this.producer.send(record);
            future.get(this.sendTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        catch (final Exception e)
        {
            if (e instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            throw new CacheEntryListenerException("Kafka send failed for cache=" + cacheName, e);
        }
        finally
        {
            synchronized (this.lifecycleMonitor)
            {
                this.inFlight--;
                if (this.inFlight == 0)
                {
                    this.lifecycleMonitor.notifyAll();
                }
            }
        }
    }

    @Override
    public void dispose()
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        synchronized (this.lifecycleMonitor)
        {
            while (this.closing && !this.disposed)
            {
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0L)
                {
                    throw new IllegalStateException(
                        "Kafka clustered-cache sender disposal is already in progress");
                }
                try
                {
                    TimeUnit.NANOSECONDS.timedWait(this.lifecycleMonitor, remaining);
                }
                catch (final InterruptedException failure)
                {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                        "interrupted while waiting for Kafka clustered-cache sender disposal", failure);
                }
            }
            if (this.disposed) return;
            this.closing = true;
            try
            {
                while (this.inFlight != 0)
                {
                    final long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L)
                    {
                        throw new IllegalStateException(
                            "Kafka clustered-cache sender did not stop before disposal timeout");
                    }
                    TimeUnit.NANOSECONDS.timedWait(this.lifecycleMonitor, remaining);
                }
            }
            catch (final InterruptedException failure)
            {
                this.closing = false;
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while closing Kafka clustered-cache sender", failure);
            }
            catch (final RuntimeException | Error failure)
            {
                this.closing = false;
                this.lifecycleMonitor.notifyAll();
                LOGGER.log(System.Logger.Level.ERROR,
                    "Failed to wait for Kafka clustered-cache sends to finish.", failure);
                throw failure;
            }
            try
            {
                this.producer.close(java.time.Duration.ofSeconds(5L));
                this.disposed = true;
                this.closing = false;
                this.lifecycleMonitor.notifyAll();
            }
            catch (final RuntimeException | Error failure)
            {
                this.closing = false;
                this.lifecycleMonitor.notifyAll();
                LOGGER.log(System.Logger.Level.ERROR, "Failed to close Kafka producer.", failure);
                throw failure;
            }
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
         * @param maxPayloadBytes maximum serialized payload accepted for publication
         */
        private UpdateTimestamps(
            final KafkaProducer<String, byte[]> producer,
            final String topicName,
            final String clientId,
            final Serializer<byte[]> serializer,
            final int maxPayloadBytes,
            final long sendTimeoutMillis
        )
        {
            super(producer, topicName, clientId, serializer, maxPayloadBytes, sendTimeoutMillis);
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
