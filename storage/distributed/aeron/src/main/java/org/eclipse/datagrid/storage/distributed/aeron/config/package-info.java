/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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
 * This package defines the settings that shape an Aeron storage stream.
 *
 * <p>Configuration values describe endpoints, stream identity, and frame
 * limits. Members that share a stream must use compatible values. The records
 * are immutable after construction so a running reader and writer see one
 * stable configuration.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.storage.distributed.aeron.config;
