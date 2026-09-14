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
 * This package carries clustered cache invalidations through Aeron.
 *
 * <p>Each node publishes its timestamp updates on one Aeron publication and
 * consumes every other node's updates from one subscription. A receiver
 * ignores frames written by its own sender identity.</p>
 *
 * <p>The transport is a drop-in alternative to the Kafka adapter: the sender
 * is synchronous and fails the local cache operation when it cannot publish,
 * so cache semantics do not change with the transport. At the receiver,
 * malformed or undeserializable frames, application failures, and sender
 * sequence gaps stop delivery and are exposed through failure state;
 * continuing after any of these conditions would leave a volatile broadcast
 * consumer permanently stale.</p>
 *
 * <p>Topology and identity: cache invalidation is an N-writer/N-reader
 * broadcast, not the Store replication 1-writer/N-reader topology. The default
 * channel {@code aeron:ipc} is single-host; multi-host deployments must
 * configure a UDP channel with {@code control-mode=dynamic}. Self-suppression
 * uses a 16-byte sender id per provider, or a configured {@code node-id}
 * shared by every provider of one node.</p>
 *
 * <p>Loss and security: the Aeron stream is volatile, so invalidations are
 * lost while a receiver is down or over a missed burst; a receiver fails
 * closed when it detects that loss. The channel is assumed to sit on an isolated network; like
 * the Kafka adapter the frames carry no authentication, and the provider
 * rejects wildcard, loopback, and non-MDC UDP channels to reduce the exposure.
 * See {@link AeronClusteredConfigurationPropertyNames} for the settings.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cache.clustered.aeron.types;
