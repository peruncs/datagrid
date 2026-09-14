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
 * This package keeps the durable state needed to resume Aeron replication.
 *
 * <p>A checkpoint names the stream position and recording that a node has
 * accepted. A reader or writer may reuse a recording only after its identity
 * and generation match the checkpoint. A mismatch starts a new safe path
 * instead of silently appending to unrelated data.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.storage.distributed.aeron.checkpoint;
