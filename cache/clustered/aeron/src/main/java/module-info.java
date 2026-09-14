/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Aeron
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
 * This module carries clustered cache invalidations over Aeron.
 *
 * <p>The neutral clustered-cache module defines the timestamp invalidation
 * contract. This module supplies the Aeron implementation selected through
 * {@code ClusteredCacheMessageComProvider}. It is a drop-in alternative to the
 * Kafka adapter: the sender is synchronous and fails the local operation when
 * the invalidation cannot be published, while the receiver fails closed on
 * malformed frames or when a valid invalidation cannot be applied. It is
 * independent of the Store replication transport.</p>
 *
 * <p>Cache invalidation is an N-writer/N-reader broadcast (the Store
 * 1-writer/N-reader constraint does not apply). The default channel is
 * single-host {@code aeron:ipc}; multi-host deployments must configure dynamic
 * MDC UDP. The stream is volatile (invalidations are lost while a receiver is
 * down), self-suppression uses a per-provider sender id or a configured
 * {@code node-id}, and the channel is assumed to be on an isolated network —
 * the frames carry no authentication.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cache.clustered.aeron
{
	requires org.eclipse.datagrid.cache.clustered;
	requires org.eclipse.serializer;
	requires cache.api;
	requires io.aeron.client;
	requires io.aeron.driver;
	requires org.agrona;

	exports org.eclipse.datagrid.cache.clustered.aeron.types;
}
