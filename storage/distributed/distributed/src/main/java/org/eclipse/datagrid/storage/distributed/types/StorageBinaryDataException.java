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

/** Signals an invalid or unusable assembled Store replication message. */
public class StorageBinaryDataException extends IllegalStateException
{
	private static final long serialVersionUID = 1L;

	/** Creates an exception with a message.
	 *
	 * @param message diagnostic message
	 */
	public StorageBinaryDataException(final String message)
	{
		super(message);
	}

	/** Creates an exception with an underlying wire or storage failure.
	 *
	 * @param message diagnostic message
	 * @param cause underlying failure
	 */
	public StorageBinaryDataException(final String message, final Throwable cause)
	{
		super(message, cause);
	}
}
