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

/**
 * This message carries the newest timestamp known for one cache table.
 *
 * <p>Receivers compare the timestamp with their local value and never move it
 * backwards. The cache and table names identify the local entry to update.</p>
 *
 * @param cacheName local cache name
 * @param tableName timestamp table key
 * @param timestamp newest timestamp observed by the sender
 */
public record TimestampsRegionUpdateMessage(String cacheName, String tableName, long timestamp)
{
}
