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
 * Transport-neutral cluster lifecycle and replication SPI.
 *
 * <p>The package coordinates Data Grid storage, cursors, health, retention,
 * and provider selection. Kafka and Aeron implementations are installed as
 * optional sibling modules discovered through {@link java.util.ServiceLoader};
 * this package intentionally contains neither client library.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cluster.nodelibrary.types;
