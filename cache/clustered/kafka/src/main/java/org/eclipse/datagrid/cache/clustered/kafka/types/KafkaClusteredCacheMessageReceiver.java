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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.serializer.Serializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * This receiver consumes clustered-cache timestamps from Kafka.
 *
 * <p>It uses one consumer group per provider instance, ignores records written
 * by its own client, and commits offsets after the batch has been accepted.
 * Disposal interrupts the polling thread and waits briefly for it to finish.</p>
 *
 * <p>Unlike the Aeron receiver, Kafka retains the topic, so a node that was
 * down replays missed invalidations from its committed offset after restart —
 * when a stable {@code group-id} is configured or can be derived from node and
 * provider identity. New groups start at {@code earliest}, allowing a node with a
 * persisted local cache to reconcile against retained invalidations. A malformed
 * or oversized record stops the receiver before its offset is committed; a
 * terminal consumer failure is logged loudly and reported through
 * {@link #isRunning()} and {@link #failure()} instead of silently stopping the
 * node. The offending record remains replayable after operator remediation.</p>
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
    private static final System.Logger LOGGER =
        System.getLogger(KafkaClusteredCacheMessageReceiver.class.getName());
    private static final String ROLE_NAME = "eclipse-datagrid-cache-invalidation-kafka";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1L);
    private final Properties kafkaProperties;
    private final String topicName;
    private final String groupId;
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
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private volatile KafkaConsumer<String, byte[]> consumer;
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
        this.kafkaProperties = Objects.requireNonNull(kafkaProperties, "kafkaProperties");
        this.topicName = Objects.requireNonNull(topicName, "topicName");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.clientIdBytes = Objects.requireNonNull(
            Objects.requireNonNull(serializer, "serializer").serialize(
                Objects.requireNonNull(clientId, "clientId")),
            "serializer returned a null client identity").clone();
        this.messageAcceptor = Objects.requireNonNull(messageAcceptor, "messageAcceptor");
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
        return this.failure.get();
    }

    private void run()
    {
        try
        {
            final Properties properties = new Properties();
            properties.putAll(this.kafkaProperties);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            /* A new node must reconcile retained invalidations before serving cache
             * reads. Stable groups resume from their committed offset; earliest is
             * the safe bootstrap policy for a group with no committed offset. */
            final Object configuredReset = properties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            if (configuredReset == null)
            {
                properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
            else if (!"earliest".equals(configuredReset) && !"latest".equals(configuredReset))
            {
                throw new IllegalArgumentException(
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + " must be earliest or latest: " + configuredReset);
            }
            /* Acknowledgements are committed only after every record in the poll
             * has been applied. Kafka's default auto-commit can advance the group
             * while the acceptor is still running and lose an invalidation after a
             * crash. */
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
            this.consumer = consumer;
            try (consumer)
            {
                consumer.subscribe(Collections.singleton(this.topicName));
                while (this.active.get())
                {
                    final ConsumerRecords<String, byte[]> records;

                    try
                    {
                        records = consumer.poll(POLL_TIMEOUT);
                    }
                    catch (final WakeupException | InterruptException e)
                    {
                        if (!this.active.get())
                        {
                            break;
                        }
                        throw e;
                    }

                    this.consume(records);

                    try
                    {
                        consumer.commitSync(Duration.ofSeconds(30L));
                    }
                    catch (final InterruptException e)
                    {
                        if (!this.active.get())
                        {
                            break;
                        }
                        throw e;
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
            if (!this.disposed || this.active.get())
            {
                LOGGER.log(System.Logger.Level.ERROR, "Kafka clustered-cache receiver stopped", failure);
                if (failure instanceof final RuntimeException runtime)
                {
                    this.failure.compareAndSet(null, runtime);
                }
                else
                {
                    this.failure.compareAndSet(null,
                        new IllegalStateException("Kafka clustered-cache receiver failed", failure));
                }
            }
            if (failure instanceof final Error error)
            {
                /* Fatal errors must not be swallowed by the logging path. */
                throw error;
            }
        }
        finally
        {
            this.consumer = null;
            this.running = false;
            this.active.set(false);
        }
    }

    /** Applies one polled batch; package-private for broker-free regression tests. */
    void consume(final ConsumerRecords<String, byte[]> records)
    {
        for (final var record : records)
        {
            final byte[] value;
            try
            {
                final var senderHeader = record.headers().lastHeader(
                    KafkaClusteredCacheMessageSender.SENDER_ID_HEADER);
                if (senderHeader == null || senderHeader.value() == null)
                {
                    throw new IllegalStateException(
                        "Kafka clustered-cache record is missing a valid " +
                            KafkaClusteredCacheMessageSender.SENDER_ID_HEADER + " header");
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
                    throw new IllegalStateException(
                        "Kafka clustered-cache record payload of " + (value == null ? 0 : value.length) +
                            " bytes exceeds the limit of " + this.maxPayloadBytes);
                }
            }
            catch (final RuntimeException failure)
            {
                this.malformed.increment();
                throw new IllegalStateException("Kafka clustered-cache record is malformed", failure);
            }

            try
            {
                this.messageAcceptor.accept(this.serializer.deserialize(value));
                this.received.increment();
            }
            catch (final RuntimeException failure)
            {
                /* A well-formed invalidation that cannot be applied leaves the
                 * local timestamps cache stale. Do not commit this batch or
                 * continue polling as if the node were healthy. */
                throw new IllegalStateException(
                    "Failed to apply Kafka clustered-cache invalidation", failure);
            }
        }
    }

    @Override
    public void dispose()
    {
        final Thread worker;
        final KafkaConsumer<String, byte[]> currentConsumer;
        synchronized (this)
        {
            if (this.disposed && this.thread == null)
            {
                return;
            }
            this.disposed = true;
            this.running = false;
            this.active.set(false);
            worker = this.thread;
            currentConsumer = this.consumer;
        }

        /* Join outside the monitor, mirroring the Aeron receiver, so a
         * concurrent start() is not blocked while the worker stops. */
        if (worker != null)
        {
            if (currentConsumer != null)
            {
                currentConsumer.wakeup();
            }
            worker.interrupt();

            try
            {
                worker.join(5_000L);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while closing Kafka clustered-cache receiver", e);
            }

            if (worker.isAlive())
            {
                LOGGER.log(System.Logger.Level.WARNING,
                    "Kafka clustered-cache receiver did not stop; resources remain owned for retry");
                throw new IllegalStateException(
                    "Kafka clustered-cache receiver did not stop before disposal timeout");
            }
            synchronized (this)
            {
                if (this.thread == worker)
                {
                    this.thread = null;
                }
            }
        }
        LOGGER.log(System.Logger.Level.DEBUG,
            "Disposed Kafka clustered-cache receiver: received=" + this.received.sum() +
                ", selfSkipped=" + this.selfSkipped.sum() + ", malformed=" + this.malformed.sum());
    }

    /** Returns whether this receiver has completed terminal disposal. */
    boolean isDisposed()
    {
        return this.disposed && this.thread == null;
    }
}
