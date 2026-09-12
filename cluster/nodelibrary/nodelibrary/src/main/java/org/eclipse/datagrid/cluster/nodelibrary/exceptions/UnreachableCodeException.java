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

/** Reports an invariant failure that should not be reachable in normal use. */
public class UnreachableCodeException extends NodelibraryException
{
	/** Creates an exception without a message. */
	public UnreachableCodeException()
	{
		super();
	}

	/** Creates an exception with a message.
	 * @param message error message
	 */
	public UnreachableCodeException(final String message)
	{
		super(message);
	}

	/** Creates an exception with a cause.
	 * @param cause underlying cause
	 */
	public UnreachableCodeException(final Throwable cause)
	{
		super(cause);
	}

	/** Creates an exception with a message and cause.
	 * @param message error message
	 * @param cause underlying cause
	 */
	public UnreachableCodeException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	/** Creates an exception with full throwable settings.
	 * @param message error message
	 * @param cause underlying cause
	 * @param enableSuppression whether suppression is enabled
	 * @param writableStackTrace whether the stack trace may be written
	 */
	public UnreachableCodeException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
