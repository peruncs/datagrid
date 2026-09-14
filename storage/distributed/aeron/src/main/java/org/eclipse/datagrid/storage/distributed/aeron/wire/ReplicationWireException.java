package org.eclipse.datagrid.storage.distributed.aeron.wire;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
 * %%
 * Copyright (C) 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataException;

/** Signals malformed or corrupted bytes received from an Aeron peer. */
public final class ReplicationWireException extends StorageBinaryDataException
{
	/** Creates a wire failure with a diagnostic message. */
	public ReplicationWireException(final String message)
	{
		super(message);
	}

	/** Creates a wire failure with its underlying cause. */
	public ReplicationWireException(final String message, final Throwable cause)
	{
		super(message, cause);
	}
}
