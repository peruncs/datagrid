/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Spring Boot Provider
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
 * This package adapts the neutral cluster lifecycle to Spring Boot.
 *
 * <p>Spring creates the application components and the controller owns the
 * embedded server. Start the controller after the node is ready, and close it
 * before the node's storage is closed. The provider exposes framework wiring;
 * cluster behavior remains in the neutral nodelibrary.</p>
 */
package org.eclipse.datagrid.cluster.nodelibrary.springboot;
