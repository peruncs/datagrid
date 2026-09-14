package org.eclipse.datagrid.cluster.nodelibrary.exceptions;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

/** Reports that a remote type definition conflicts with the local definition. */
public final class IncompatibleTypeDictionaryException extends NodelibraryException
{
	/** Creates an exception with a diagnostic message.
	 *
	 * @param message diagnostic message
	 */
	public IncompatibleTypeDictionaryException(final String message)
	{
		super(message);
	}
}
