package org.eclipse.datagrid.cache.clustered.types;

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

import org.eclipse.serializer.Serializer;

import java.util.Map;

/**
 * This provider creates the sender and receiver used by one clustered cache.
 *
 * <p>The cache factory supplies the Hibernate properties and shared serializer.
 * Implementations decide how messages travel, but they must return resources
 * that remain valid until the cache region factory releases them.</p>
 *
 * <p>Because Hibernate resolves the provider from a configured class name, an
 * implementation must expose a public no-argument constructor and read its
 * configuration from the supplied properties. A sender and receiver from the
 * same provider share one node identity so a node ignores its own
 * invalidations.</p>
 *
 * <p>The provider is deliberately not parameterized by cache key and value
 * types: it produces senders typed for the timestamp region
 * ({@link ClusteredCacheMessageSender}<code>&lt;Object, Object&gt;</code>),
 * which the cache region factory consumes.</p>
 */
public interface ClusteredCacheMessageComProvider
{
	/** Creates the sender for timestamp cache events.
	 *
	 * @param properties cache configuration properties
	 * @param serializer serializer shared by the sender and receiver
	 * @return sender for timestamp cache events
	 */
	ClusteredCacheMessageSender<Object, Object> provideUpdateTimestampsCacheMessageSender(
        @SuppressWarnings("rawtypes") Map properties,
        Serializer<byte[]> serializer
	);

	/** Creates the receiver that passes remote messages to the acceptor.
	 *
	 * @param properties cache configuration properties
	 * @param serializer serializer shared by the sender and receiver
	 * @param messageAcceptor target for received messages
	 * @return receiver for remote cache messages
	 */
	ClusteredCacheMessageReceiver provideMessageReceiver(
        @SuppressWarnings("rawtypes") Map properties,
        Serializer<byte[]> serializer,
        ClusteredCacheMessageAcceptor messageAcceptor
    );
}