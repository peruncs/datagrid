/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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
 * This package coordinates the transport-neutral cluster lifecycle.
 *
 * <p>Node services use these contracts to move from construction to running,
 * draining, and closed states. A provider owns its transport resources, while
 * the node owns start and stop order. Kafka and Aeron implementations are
 * optional sibling modules discovered through {@link java.util.ServiceLoader};
 * this package intentionally contains neither client library.</p>
 *
 * <p>Callers must finish a node's write and replication work before closing its
 * storage. Cursors and backup callbacks are valid only while their owning node
 * remains active.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cluster.nodelibrary.types;
