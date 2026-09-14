/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Micronaut Provider
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
 * This package contains Micronaut-facing factory types.
 *
 * <p>Micronaut creates these objects and supplies their collaborators. The
 * factories keep one neutral cluster foundation, storage manager, and update
 * handler graph for the application context. They must not create a second
 * node graph outside that context.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;
