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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.datagrid.cache.clustered.types.*;
import org.eclipse.serializer.Serializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import static org.eclipse.datagrid.cache.clustered.kafka.types.KafkaClusteredConfigurationPropertyNames.KAFKA_CONSUMER_CONFIG_PREFIX;
import static org.eclipse.datagrid.cache.clustered.kafka.types.KafkaClusteredConfigurationPropertyNames.KAFKA_PRODUCER_CONFIG_PREFIX;

/**
 * This provider builds the Kafka sender and receiver for clustered cache
 * invalidation.
 *
 * <p>It creates one producer, one sender, and one generated client id for the
 * provider instance. Producer and consumer settings are read from their
 * separate configuration prefixes so the two clients cannot accidentally
 * share a role-specific setting. An explicit {@code group-id} is used as-is.
 * Otherwise the group contains a stable node identity and a provider-instance
 * identity. Configure {@code provider-id} when the group must remain stable
 * across provider recreation or when one node hosts more than one provider;
 * without it, each provider instance gets its own group and therefore cannot
 * steal another provider's invalidations.</p>
 *
 * <p>Failure latency differs from the Aeron adapter: the sender waits at most
 * the configured send timeout for each Kafka send to complete, while the Aeron
 * sender waits a configurable offer timeout. Both fail the local cache
 * operation, but after different delays.</p>
 */
public class KafkaClusteredCacheMessageComProvider implements ClusteredCacheMessageComProvider
{
    private static final System.Logger LOGGER =
        System.getLogger(KafkaClusteredCacheMessageComProvider.class.getName());
    private static final int DEFAULT_MAX_PAYLOAD_BYTES = 1 << 20;
    private static final long DEFAULT_SEND_TIMEOUT_MILLIS = 30_000L;
    private String clientId;
    private final String providerInstanceId = UUID.randomUUID().toString();
    private String configuredTopic;
    private KafkaProducer<String, byte[]> producer;
    private ClusteredCacheMessageSender<Object, Object> sender;
    private Serializer<byte[]> senderSerializer;
    private int senderMaxPayloadBytes;
    private long senderSendTimeoutMillis;
    private ClusteredCacheMessageReceiver receiver;
    private Serializer<byte[]> receiverSerializer;
    private ClusteredCacheMessageAcceptor receiverAcceptor;
    private String receiverGroupId;
    private int receiverMaxPayloadBytes;

    /** Creates a provider with no Kafka clients yet.
     *
     * <p>The clients are created when the cache region asks for them.</p>
     */
    public KafkaClusteredCacheMessageComProvider()
    {
    }

    @Override
    public synchronized ClusteredCacheMessageSender<Object, Object> provideUpdateTimestampsCacheMessageSender(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer
    )
    {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(serializer, "serializer");
        final var topicName = this.getTopicName(properties);
        if (this.sender != null)
        {
            if (this.sender instanceof final KafkaClusteredCacheMessageSender kafkaSender && kafkaSender.isDisposed())
            {
                throw new IllegalStateException(
                    "Kafka clustered-cache sender is single-use and has been disposed; create a new provider");
            }
            final int maxPayloadBytes = this.maxPayloadBytes(properties);
            final long sendTimeoutMillis = this.sendTimeoutMillis(properties);
            if (this.senderSerializer != serializer || this.senderMaxPayloadBytes != maxPayloadBytes ||
                this.senderSendTimeoutMillis != sendTimeoutMillis)
            {
                throw new IllegalArgumentException(
                    "Kafka clustered-cache provider already owns a sender with a different serializer");
            }
            return this.sender;
        }
        final var producer = this.ensureProducer(properties);
        final var clientId = this.ensureClientId();
        final int maxPayloadBytes = this.maxPayloadBytes(properties);
        final long sendTimeoutMillis = this.sendTimeoutMillis(properties);
        try
        {
            this.sender = KafkaClusteredCacheMessageSender.UpdateTimestamps(
                producer, topicName, clientId, serializer, maxPayloadBytes, sendTimeoutMillis);
        }
        catch (final RuntimeException | Error failure)
        {
            try
            {
                producer.close();
                this.producer = null;
            }
            catch (final RuntimeException | Error closeFailure)
            {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
        this.senderSerializer = serializer;
        this.senderMaxPayloadBytes = maxPayloadBytes;
        this.senderSendTimeoutMillis = sendTimeoutMillis;
        return this.sender;
    }

    @Override
    public synchronized ClusteredCacheMessageReceiver provideMessageReceiver(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer,
        final ClusteredCacheMessageAcceptor messageAcceptor
    )
    {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(serializer, "serializer");
        Objects.requireNonNull(messageAcceptor, "messageAcceptor");
        final var topicName = this.getTopicName(properties);
        final var clientId = this.ensureClientId();
        final var groupId = this.groupId(properties, topicName);
        final int maxPayloadBytes = this.maxPayloadBytes(properties);
        if (this.receiver != null)
        {
            if (this.receiver instanceof final KafkaClusteredCacheMessageReceiver kafkaReceiver && kafkaReceiver.isDisposed())
            {
                throw new IllegalStateException(
                    "Kafka clustered-cache receiver is single-use and has been disposed; create a new provider");
            }
            if (this.receiverSerializer != serializer || this.receiverAcceptor != messageAcceptor ||
                !this.receiverGroupId.equals(groupId) || this.receiverMaxPayloadBytes != maxPayloadBytes)
            {
                throw new IllegalArgumentException(
                    "Kafka clustered-cache provider already owns a receiver with different configuration");
            }
            return this.receiver;
        }
        final var kafkaProperties = this.readKafkaConfigProperties(properties, KAFKA_CONSUMER_CONFIG_PREFIX);
        final ClusteredCacheMessageReceiver created = new KafkaClusteredCacheMessageReceiver(
            kafkaProperties,
            topicName,
            groupId,
            clientId,
            messageAcceptor,
            serializer,
            maxPayloadBytes
        );
        this.receiverSerializer = serializer;
        this.receiverAcceptor = messageAcceptor;
        this.receiverGroupId = groupId;
        this.receiverMaxPayloadBytes = maxPayloadBytes;
        this.receiver = created;
        return created;
    }

    private int maxPayloadBytes(@SuppressWarnings("rawtypes") final Map properties)
    {
        return ClusteredCachePropertyParsers.intProperty(properties,
            KafkaClusteredConfigurationPropertyNames.MAX_PAYLOAD_BYTES, DEFAULT_MAX_PAYLOAD_BYTES, 1);
    }

    private long sendTimeoutMillis(@SuppressWarnings("rawtypes") final Map properties)
    {
        return ClusteredCachePropertyParsers.longProperty(
            properties, KafkaClusteredConfigurationPropertyNames.SEND_TIMEOUT_MILLIS,
            DEFAULT_SEND_TIMEOUT_MILLIS, 1L);
    }

    private String ensureClientId()
    {
        if (this.clientId == null)
        {
            this.clientId = UUID.randomUUID().toString();
        }
        return this.clientId;
    }

    /**
     * Resolves a consumer group that remains stable for one node across cache
     * provider instances and JVM restarts when an explicit group id or stable
     * provider id is configured. An explicit group id always wins; otherwise
     * the configured node id, Kubernetes pod name, or hostname is used. The
     * provider identity is always part of an inferred group so two provider
     * instances on one node cannot consume one another's invalidations.
     */
    String groupId(@SuppressWarnings("rawtypes") final Map properties, final String topicName)
    {
        final String configured = ClusteredCachePropertyParsers.stringProperty(
            properties, KafkaClusteredConfigurationPropertyNames.GROUP_ID, null);
        if (configured != null && !configured.isBlank())
        {
            return configured;
        }
        final String configuredNode = ClusteredCachePropertyParsers.stringProperty(
            properties, KafkaClusteredConfigurationPropertyNames.NODE_ID, null);
        final String node = configuredNode == null || configuredNode.isBlank()
            ? stableNodeIdentity() : configuredNode.trim();
        final String provider = ClusteredCachePropertyParsers.stringProperty(
            properties, KafkaClusteredConfigurationPropertyNames.PROVIDER_ID, null);
        if (provider != null && !provider.isBlank())
        {
            return "eclipse-datagrid-cache-" + topicName + "-" + node + "-" + provider.trim();
        }
        return "eclipse-datagrid-cache-" + topicName + "-" + node + "-" + this.providerInstanceId;
    }

    private static String stableNodeIdentity()
    {
        final String pod = System.getenv("MY_POD_NAME");
        if (pod != null && !pod.isBlank()) return pod.trim();
        final String configured = System.getProperty("eclipse.datagrid.node-id");
        if (configured != null && !configured.isBlank()) return configured.trim();
        try
        {
            final String host = InetAddress.getLocalHost().getHostName();
            if (host != null && !host.isBlank()) return host.trim();
        }
        catch (final UnknownHostException | SecurityException failure)
        {
            LOGGER.log(System.Logger.Level.WARNING,
                "Unable to resolve a stable host name for the Kafka cache group id", failure);
        }
        throw new IllegalArgumentException("No stable Kafka cache node identity found; configure " +
            KafkaClusteredConfigurationPropertyNames.GROUP_ID + " or " +
            KafkaClusteredConfigurationPropertyNames.NODE_ID);
    }

    private KafkaProducer<String, byte[]> ensureProducer(@SuppressWarnings("rawtypes") final Map properties)
    {
        if (this.producer == null)
        {
            final var kafkaProperties = this.readKafkaConfigProperties(
                properties,
                KAFKA_PRODUCER_CONFIG_PREFIX
            );
            /* A deterministic key keeps all updates for one timestamp table in
             * one partition.  Null keys use Kafka's round-robin partitioner and
             * can reorder successive invalidations for the same table. */
            kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName()
            );
            kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
            kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            final KafkaProducer<String, byte[]> created;
            try
            {
                created = new KafkaProducer<>(kafkaProperties);
            }
            catch (final RuntimeException | Error failure)
            {
                this.producer = null;
                throw failure;
            }
            this.producer = created;
        }
        return this.producer;
    }

    private String getTopicName(@SuppressWarnings("rawtypes") final Map properties)
    {
        final String topicName = ClusteredCachePropertyParsers.stringProperty(properties,
            KafkaClusteredConfigurationPropertyNames.TOPIC, "es-cache-invalidation");
        if (topicName.indexOf('\0') >= 0)
        {
            throw new IllegalArgumentException(
                KafkaClusteredConfigurationPropertyNames.TOPIC + " must not contain the NUL separator: " + topicName);
        }
        if (this.configuredTopic == null)
        {
            this.configuredTopic = topicName;
        }
        else if (!this.configuredTopic.equals(topicName))
        {
            throw new IllegalArgumentException(
                "Conflicting " + KafkaClusteredConfigurationPropertyNames.TOPIC +
                    ": the provider is already bound to " + this.configuredTopic + ", requested " + topicName);
        }
        return topicName;
    }

    private Properties readKafkaConfigProperties(
        @SuppressWarnings("rawtypes") final Map rawProperties,
        final String specificPrefix
    )
    {
        final Properties kafkaProperties = new Properties();
        for (final var rawKey : rawProperties.keySet())
        {
            if (rawKey instanceof final String prefixedKey)
            {
                // skip the key if we are a producer and it's a consumer key and vice versa
                if (specificPrefix.equals(KAFKA_CONSUMER_CONFIG_PREFIX)
                    && prefixedKey.startsWith(KAFKA_PRODUCER_CONFIG_PREFIX)
                    || specificPrefix.equals(KAFKA_PRODUCER_CONFIG_PREFIX)
                    && prefixedKey.startsWith(KAFKA_CONSUMER_CONFIG_PREFIX))
                {
                    LOGGER.log(System.Logger.Level.TRACE,
                        "Ignoring Kafka config with key=" + prefixedKey);
                    continue;
                }

                final var key = this.removePrefixIfConfigKey(prefixedKey, specificPrefix);
                if (key != null)
                {
                    final var value = rawProperties.get(rawKey);
                    LOGGER.log(System.Logger.Level.TRACE,
                        "Found Kafka config with key=" + key + ", value=" + value);
                    kafkaProperties.put(key, value);
                }
            }
        }
        return kafkaProperties;
    }

    private String removePrefixIfConfigKey(final String key, final String specificPrefix)
    {
        final var prefixes = new String[] {
            specificPrefix,
            KafkaClusteredConfigurationPropertyNames.KAFKA_CONFIG_PREFIX
        };
        for (final var prefix : prefixes)
        {
            if (key.startsWith(prefix))
            {
                return key.substring(prefix.length());
            }
        }
        return null;
    }
}
