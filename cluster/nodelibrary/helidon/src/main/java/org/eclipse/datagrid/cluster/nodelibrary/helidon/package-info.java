/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Helidon Provider
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
 * This package adapts the neutral cluster lifecycle to Helidon.
 *
 * <p>The controller owns the Helidon server and the provider supplies the
 * node root and cluster services used by its handlers. Start the controller
 * after the node is ready, and close it before the node's storage is closed.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cluster.nodelibrary.helidon;
