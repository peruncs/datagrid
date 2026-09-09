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
 * Restart state for Aeron replication.
 *
 * <p>A writer checkpoint is the last terminal transaction that the Archive
 * confirmed. A reader cursor identifies the exact recording and Store image
 * from which replay may continue. Keeping both identities in the record lets
 * startup reject stale or mixed state instead of guessing.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.checkpoint;
