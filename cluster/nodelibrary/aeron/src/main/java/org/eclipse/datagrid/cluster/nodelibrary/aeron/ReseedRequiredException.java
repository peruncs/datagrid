package org.eclipse.datagrid.cluster.nodelibrary.aeron;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

/** Typed fail-closed signal for recovery evidence that cannot be reconciled. */
final class ReseedRequiredException extends IllegalStateException
{
	ReseedRequiredException(final String message)
	{
		super("RESEED_REQUIRED: " + message);
	}

	ReseedRequiredException(final String message, final Throwable cause)
	{
		super("RESEED_REQUIRED: " + message, cause);
	}
}
