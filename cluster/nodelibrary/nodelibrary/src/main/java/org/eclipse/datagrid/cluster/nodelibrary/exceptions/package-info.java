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
 * This package defines failures at the node and storage boundaries.
 *
 * <p>HTTP-facing failures carry a status and optional headers so an adapter
 * can build a response without knowing internal implementation classes. The
 * remaining exceptions preserve the original cause and are intended for the
 * node-level error handler.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cluster.nodelibrary.exceptions;
