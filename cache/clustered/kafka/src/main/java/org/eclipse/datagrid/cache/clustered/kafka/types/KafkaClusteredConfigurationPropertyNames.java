package org.eclipse.datagrid.cache.clustered.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
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

import org.eclipse.datagrid.cache.clustered.types.ClusteredConfigurationPropertyNames;

/** Names of the Kafka settings used by clustered-cache messages. */
public interface KafkaClusteredConfigurationPropertyNames
{
    /** Prefix shared by Kafka clustered-cache properties. */
    String PREFIX = ClusteredConfigurationPropertyNames.PREFIX + "kafka.";

    /** Property containing the invalidation topic name. */
    String TOPIC = PREFIX + "topic";
    /** Property fixing the consumer group id; a stable value makes a restarted node replay missed invalidations from its committed offset. */
    String GROUP_ID = PREFIX + "group-id";
    /** Prefix for settings shared by the Kafka producer and consumer. */
    String KAFKA_CONFIG_PREFIX = PREFIX + "config.";

    /** Prefix for producer-only Kafka settings. */
    String KAFKA_PRODUCER_CONFIG_PREFIX = KAFKA_CONFIG_PREFIX + "producer.";
    /** Prefix for consumer-only Kafka settings. */
    String KAFKA_CONSUMER_CONFIG_PREFIX = KAFKA_CONFIG_PREFIX + "consumer.";

    /** Property bounding the accepted serialized payload size; defaults to 1 MiB. */
    String MAX_PAYLOAD_BYTES = PREFIX + "max-payload-bytes";
}
