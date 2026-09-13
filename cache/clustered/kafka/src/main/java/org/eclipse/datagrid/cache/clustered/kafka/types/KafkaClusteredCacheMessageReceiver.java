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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * This receiver consumes clustered-cache timestamps from Kafka.
 *
 * <p>It uses one consumer group per node, ignores records written by its own
 * client, and commits offsets after the batch has been accepted. Disposal
 * interrupts the polling thread and waits briefly for it to finish.</p>
 *
 * <p>Unlike the Aeron receiver, Kafka retains the topic, so a node that was
 * down replays missed invalidations from its committed offset after restart —
 * but only when a stable {@code group-id} is configured. Without one, the
 * consumer group is derived from the random client id, so a restarted node
 * starts a fresh group at the latest offset and does not replay. A malformed
 * or oversized record is logged and skipped; a terminal consumer failure is
 * logged loudly and reported through {@link #isRunning()} and
 * {@link #failure()} instead of silently stopping the node.</p>
 *
 * <p>A receiver is single-use, like the Aeron receiver: after {@link #dispose()}
 * it cannot be started again.</p>
 *
 * <p>Observability: {@link #isRunning()} is the programmatic health surface,
 * and received/self-skipped/malformed counters are reported in the dispose
 * debug log; a terminal consumer failure is logged at error level.</p>
 */
final class KafkaClusteredCacheMessageReceiver implements ClusteredCacheMessageReceiver
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaClusteredCacheMessageReceiver.class);
    private static final String ROLE_NAME = "eclipse-datagrid-cache-invalidation-kafka";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1L);
    private final Properties kafkaProperties;
    private final String topicName;
    private final String groupId;
    private final String clientId;
    private final byte[] clientIdBytes;
    private final ClusteredCacheMessageAcceptor messageAcceptor;
    private final Serializer<byte[]> serializer;
    private final int maxPayloadBytes;
    private final LongAdder received = new LongAdder();
    private final LongAdder selfSkipped = new LongAdder();
    private final LongAdder malformed = new LongAdder();

    private boolean started = false;
    private final AtomicBoolean active = new AtomicBoolean(false);
    private volatile boolean running;
    private volatile boolean disposed;
    private volatile RuntimeException failure;
    private Thread thread;

    /** Creates a receiver for one Kafka topic and client.
     *
     * @param kafkaProperties Kafka consumer properties
     * @param topicName topic that carries cache updates
     * @param groupId consumer group id; a stable value makes a restarted node replay
     * @param clientId client identifier used to ignore this client's own updates
     * @param messageAcceptor target for accepted cache updates
     * @param serializer message serializer
     * @param maxPayloadBytes maximum accepted serialized payload size
     */
    KafkaClusteredCacheMessageReceiver(
        final Properties kafkaProperties,
        final String topicName,
        final String groupId,
        final String clientId,
        final ClusteredCacheMessageAcceptor messageAcceptor,
        final Serializer<byte[]> serializer,
        final int maxPayloadBytes
    )
    {
        this.kafkaProperties = kafkaProperties;
        this.topicName = topicName;
        this.groupId = groupId;
        this.clientId = clientId;
        this.clientIdBytes = serializer.serialize(clientId);
        this.messageAcceptor = messageAcceptor;
        this.serializer = serializer;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    @Override
    public synchronized void start() throws IllegalStateException
    {
        if (this.disposed)
        {
            throw new IllegalStateException("Kafka clustered-cache receiver is disposed");
        }
        if (this.started)
        {
            throw new IllegalStateException("Kafka clustered-cache receiver is already started or terminally stopped");
        }

        this.started = true;
        this.running = true;
        this.active.set(true);
        this.thread = new Thread(this::run, ROLE_NAME);
        this.thread.setDaemon(true);
        this.thread.start();
    }

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public RuntimeException failure()
    {
        return this.failure;
    }

    private void run()
    {
        final Properties properties = new Properties();
        properties.putAll(this.kafkaProperties);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try
        {
            try (final var consumer = new KafkaConsumer<String, byte[]>(properties))
            {
                consumer.subscribe(Collections.singleton(this.topicName));
                while (this.active.get())
                {
                    final ConsumerRecords<String, byte[]> records;

                    try
                    {
                        records = consumer.poll(POLL_TIMEOUT);
                    }
                    catch (final InterruptException e)
                    {
                        // clear interrupt flag so Kafka can properly close the consumer
                        final var ignored = Thread.interrupted();
                        continue;
                    }

                    this.consume(records);

                    try
                    {
                        consumer.commitSync();
                    }
                    catch (final InterruptException e)
                    {
                        // clear interrupt flag so Kafka can properly close the consumer
                        final var ignored = Thread.interrupted();
                    }
                }
            }
        }
        catch (final Throwable failure)
        {
            /* A terminal failure (for example a consumer that cannot join the
             * group) must not be silent. The Aeron receiver reports every agent
             * failure through its error handler; log the same terminal event here
             * so a node serving stale query-cache timestamps is observable. */
            logger.error("Kafka clustered-cache receiver stopped", failure);
            if (failure instanceof final RuntimeException runtime)
            {
                this.failure = runtime;
            }
            if (failure instanceof final Error error)
            {
                /* Fatal errors must not be swallowed by the logging path. */
                throw error;
            }
        }
        finally
        {
            this.running = false;
        }
    }

    /** Applies one polled batch. */
    private void consume(final ConsumerRecords<String, byte[]> records)
    {
        for (final var record : records)
        {
            final byte[] value;
            try
            {
                final var senderHeader = record.headers().lastHeader(
                    KafkaClusteredCacheMessageSender.SENDER_ID_HEADER);
                if (senderHeader == null)
                {
                    this.malformed.increment();
                    logger.error("Discarding clustered-cache record without a {} header",
                        KafkaClusteredCacheMessageSender.SENDER_ID_HEADER);
                    continue;
                }
                /* The header carries the serialized client id; compare the raw
                 * bytes so no String is allocated for every record. */
                if (Arrays.equals(this.clientIdBytes, senderHeader.value()))
                {
                    this.selfSkipped.increment();
                    continue;
                }
                value = record.value();
                if (value == null || value.length > this.maxPayloadBytes)
                {
                    this.malformed.increment();
                    logger.error("Discarding clustered-cache record with {} payload bytes exceeding the limit of {}",
                        value == null ? 0 : value.length, this.maxPayloadBytes);
                    continue;
                }
            }
            catch (final RuntimeException failure)
            {
                this.malformed.increment();
                logger.error("Discarding unreadable Kafka clustered-cache record", failure);
                continue;
            }

            try
            {
                this.messageAcceptor.accept(this.serializer.deserialize(value));
                this.received.increment();
            }
            catch (final RuntimeException failure)
            {
                /* The record was well formed; applying it failed. Keep it out of
                 * the malformed counter, matching the Aeron receiver. */
                logger.error("Failed to apply Kafka clustered-cache invalidation", failure);
            }
        }
    }

    @Override
    public void dispose()
    {
        final Thread worker;
        synchronized (this)
        {
            if (this.disposed)
            {
                return;
            }
            this.disposed = true;
            this.running = false;
            if (!this.active.compareAndSet(true, false))
            {
                return;
            }
            worker = this.thread;
            this.thread = null;
        }

        /* Join outside the monitor, mirroring the Aeron receiver, so a
         * concurrent start() is not blocked while the worker stops. */
        if (worker != null)
        {
            worker.interrupt();

            try
            {
                worker.join(1_000L);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }

            if (worker.isAlive())
            {
                logger.warn("Failed to wait for receiver thread to finish.");
            }
        }
        logger.debug("Disposed Kafka clustered-cache receiver: received={}, selfSkipped={}, malformed={}",
            this.received.sum(), this.selfSkipped.sum(), this.malformed.sum());
    }
}
