/*-
 * #%L
 * Eclipse Data Grid Clustered Cache Kafka
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
/**
 * This package carries clustered cache timestamps through Kafka.
 *
 * <p>The producer publishes invalidations synchronously and fails the local
 * cache operation when a send fails. The consumer applies accepted updates to
 * the neutral clustered-cache contract, ignores messages sent by its own
 * client, and logs and skips an unreadable record. Kafka settings stay in this
 * adapter package, and cache semantics remain in the sibling transport-neutral
 * package. The Aeron adapter is a drop-in alternative with the same contract.</p>
 *
 * <p>Replay and security: Kafka retains the topic, so a restarted node replays
 * missed invalidations from its committed offset — but only when a stable
 * {@code group-id} is configured; without one the consumer group is derived
 * from a random client id and the node starts at the latest offset. The broker
 * is expected to enforce topic ACLs; this adapter configures none. See
 * {@link KafkaClusteredConfigurationPropertyNames} for the settings.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cache.clustered.kafka.types;
