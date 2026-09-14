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
 * This package contains extension points for node storage providers.
 *
 * <p>A provider returns the storage manager used by one node. The caller owns
 * that manager's lifecycle and must close it after replication and backup work
 * has stopped. Providers should keep transport-specific details behind this
 * small boundary.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cluster.nodelibrary.spi;
