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
import org.apache.kafka.common.serialization.VoidSerializer;
import org.eclipse.datagrid.cache.clustered.types.*;
import org.eclipse.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.eclipse.datagrid.cache.clustered.kafka.types.KafkaClusteredConfigurationPropertyNames.KAFKA_CONSUMER_CONFIG_PREFIX;
import static org.eclipse.datagrid.cache.clustered.kafka.types.KafkaClusteredConfigurationPropertyNames.KAFKA_PRODUCER_CONFIG_PREFIX;

/**
 * This provider builds the Kafka sender and receiver for clustered cache
 * invalidation.
 *
 * <p>It creates one producer and one generated client id for the provider
 * instance. Producer and consumer settings are read from their separate
 * configuration prefixes so the two clients cannot accidentally share a
 * role-specific setting.</p>
 *
 * <p>Failure latency differs from the Aeron adapter: the sender waits for
 * Kafka's {@code delivery.timeout.ms} (default 120 s) for a send to complete,
 * while the Aeron sender waits a configurable offer timeout (default 5 s).
 * Both fail the local cache operation, but after different delays.</p>
 */
public class KafkaClusteredCacheMessageComProvider implements ClusteredCacheMessageComProvider
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaClusteredCacheMessageComProvider.class);
    private static final int DEFAULT_MAX_PAYLOAD_BYTES = 1 << 20;

    private String clientId;
    private String configuredTopic;
    private KafkaProducer<String, byte[]> producer;

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
        final var producer = this.ensureProducer(properties);
        final var topicName = this.getTopicName(properties);
        final var clientId = this.ensureClientId();
        return KafkaClusteredCacheMessageSender.UpdateTimestamps(producer, topicName, clientId, serializer);
    }

    @Override
    public synchronized ClusteredCacheMessageReceiver provideMessageReceiver(
        @SuppressWarnings("rawtypes") final Map properties,
        final Serializer<byte[]> serializer,
        final ClusteredCacheMessageAcceptor messageAcceptor
    )
    {
        final var kafkaProperties = this.readKafkaConfigProperties(
            properties,
            KAFKA_CONSUMER_CONFIG_PREFIX
        );
        final var topicName = this.getTopicName(properties);
        final var clientId = this.ensureClientId();
        final var groupId = ClusteredCachePropertyParsers.stringProperty(properties,
            KafkaClusteredConfigurationPropertyNames.GROUP_ID, clientId);
        return new KafkaClusteredCacheMessageReceiver(
            kafkaProperties,
            topicName,
            groupId,
            clientId,
            messageAcceptor,
            serializer,
            this.maxPayloadBytes(properties)
        );
    }

    private int maxPayloadBytes(@SuppressWarnings("rawtypes") final Map properties)
    {
        return ClusteredCachePropertyParsers.intProperty(properties,
            KafkaClusteredConfigurationPropertyNames.MAX_PAYLOAD_BYTES, DEFAULT_MAX_PAYLOAD_BYTES, 1);
    }

    private String ensureClientId()
    {
        if (this.clientId == null)
        {
            this.clientId = UUID.randomUUID().toString();
        }
        return this.clientId;
    }

    private KafkaProducer<String, byte[]> ensureProducer(@SuppressWarnings("rawtypes") final Map properties)
    {
        if (this.producer == null)
        {
            final var kafkaProperties = this.readKafkaConfigProperties(
                properties,
                KAFKA_PRODUCER_CONFIG_PREFIX
            );
            kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class.getName());
            kafkaProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName()
            );
            this.producer = new KafkaProducer<>(kafkaProperties);
        }
        return this.producer;
    }

    private String getTopicName(@SuppressWarnings("rawtypes") final Map properties)
    {
        final String topicName = ClusteredCachePropertyParsers.stringProperty(properties,
            KafkaClusteredConfigurationPropertyNames.TOPIC, "es-cache-invalidation");
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
                    logger.trace("Ignoring Kafka config with key={}", prefixedKey);
                    continue;
                }

                final var key = this.removePrefixIfConfigKey(prefixedKey, specificPrefix);
                if (key != null)
                {
                    final var value = rawProperties.get(rawKey);
                    logger.trace("Found Kafka config with key={}, value={}", key, value);
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
