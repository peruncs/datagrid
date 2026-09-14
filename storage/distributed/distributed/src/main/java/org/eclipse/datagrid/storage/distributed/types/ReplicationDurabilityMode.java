package org.eclipse.datagrid.storage.distributed.types;

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
 * Ordering contract between a local Store enqueue and a replication log.
 * Store 5.x exposes enqueue acceptance, not a durable-completion callback.
 */
public enum ReplicationDurabilityMode
{
	/** Record the prepared transaction before accepting the local Store enqueue. */
	ARCHIVE_FIRST,
	/** Accept the local Store enqueue before recording the transaction; recovery is conservative. */
	ENQUEUE_THEN_ARCHIVE
}
