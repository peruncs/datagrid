package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

/** Reports that a node has reached its configured storage limit. */
public class StorageLimitReachedException extends NodelibraryException
{
	/** Creates an exception without a message. */
	public StorageLimitReachedException()
	{
		super();
	}

	/** Creates an exception with a message.
	 * @param message error message
	 */
	public StorageLimitReachedException(final String message)
	{
		super(message);
	}

	/** Creates an exception with a cause.
	 * @param cause underlying cause
	 */
	public StorageLimitReachedException(final Throwable cause)
	{
		super(cause);
	}

	/** Creates an exception with a message and cause.
	 * @param message error message
	 * @param cause underlying cause
	 */
	public StorageLimitReachedException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	/** Creates an exception with full throwable settings.
	 * @param message error message
	 * @param cause underlying cause
	 * @param enableSuppression whether suppression is enabled
	 * @param writableStackTrace whether the stack trace may be written
	 */
	public StorageLimitReachedException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
