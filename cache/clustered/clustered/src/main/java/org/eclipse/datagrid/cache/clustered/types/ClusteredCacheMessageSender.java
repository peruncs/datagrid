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

import org.eclipse.serializer.typing.Disposable;

import javax.cache.event.CacheEntryListener;

/**
 * This listener sends local cache changes to the cluster.
 *
 * <p>The sender is synchronous: a listener callback must not return until the
 * invalidation has been accepted by the transport, and it must throw
 * {@link javax.cache.event.CacheEntryListenerException} when it cannot publish.
 * Every transport therefore fails the local cache operation rather than
 * silently dropping an invalidation. Kafka and Aeron implement this same
 * contract.</p>
 *
 * <p>The sender is also disposable because it owns the transport resource used
 * to publish those changes. The cache configuration releases it when the
 * cache is closed.</p>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public interface ClusteredCacheMessageSender<K, V> extends CacheEntryListener<K, V>, Disposable
{
}
