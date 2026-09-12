/*-
 * #%L
 * Eclipse Data Grid Clustered Cache
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
 * This package connects clustered cache invalidation to a neutral message
 * provider.
 *
 * <p>A sender publishes timestamp updates. A receiver applies an update only
 * when it is newer than the timestamp already held by the local cache. An
 * unknown cache is ignored because a node may receive an update before that
 * cache is opened. Listener configuration owns the sender and releases it
 * when the configuration is disposed.</p>
 */
package org.eclipse.datagrid.cache.clustered.types;
