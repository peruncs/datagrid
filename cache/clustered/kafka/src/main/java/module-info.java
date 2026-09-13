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
/**
 * This module carries clustered cache invalidations through Kafka.
 *
 * <p>It supplies the Kafka implementation selected through the neutral
 * {@code ClusteredCacheMessageComProvider}. A consumer ignores records written
 * by its own client identity, and Kafka settings stay inside this module.</p>
 *
 * <p>Cache invalidation is an N-writer/N-reader broadcast (the Store
 * 1-writer/N-reader constraint does not apply). Kafka retains the topic, so a
 * restarted node replays missed invalidations from its committed offset when a
 * stable {@code group-id} is configured; the broker is expected to enforce
 * topic ACLs, as this adapter configures none.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cache.clustered.kafka
{
	requires org.eclipse.datagrid.cache.clustered;
	requires org.eclipse.serializer;
	requires org.eclipse.serializer.base;
	requires cache.api;
	requires kafka.clients;
	requires org.slf4j;

	exports org.eclipse.datagrid.cache.clustered.kafka.types;
}
