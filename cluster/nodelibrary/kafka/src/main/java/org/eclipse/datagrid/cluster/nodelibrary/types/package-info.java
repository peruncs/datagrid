/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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
 * Kafka implementation of the transport-neutral cluster nodelibrary SPI.
 *
 * <p>Kafka resources are owned by the provider/client instances and are closed
 * by their lifecycle methods. Aeron-specific types must not leak into this
 * provider or the neutral nodelibrary contracts.</p>
 */
package org.eclipse.datagrid.cluster.nodelibrary.types;
