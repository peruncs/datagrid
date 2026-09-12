/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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
 * This package defines the provider-neutral persistence replication contract.
 *
 * <p>These APIs carry Eclipse Store binary data and lifecycle callbacks without
 * depending on Kafka, Aeron, or another messaging implementation. Writers
 * publish transaction boundaries; readers apply them in order; importers own
 * binary buffers until materialization completes.</p>
 *
 * <p>Implementations must not expose a mutable transport buffer after the
 * callback that consumes it returns.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.storage.distributed.types;
