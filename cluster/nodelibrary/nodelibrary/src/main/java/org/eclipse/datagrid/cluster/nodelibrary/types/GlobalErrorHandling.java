package org.eclipse.datagrid.cluster.nodelibrary.types;

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


import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reports an unrecoverable node error without terminating the hosting JVM.
 *
 * <p>A nodelibrary is embedded in an application and must not call
 * {@code System.exit}.  The application supervisor decides whether a fatal
 * node error warrants process termination.</p>
 */
public final class GlobalErrorHandling
{
	private static final Logger LOG = LoggerFactory.getLogger(GlobalErrorHandling.class);

	/** Handles an error that makes the node unsafe to continue.
	 *
	 * @param t fatal error
	 * <p>This method never returns. It rethrows errors and runtime exceptions and
	 * wraps checked failures in a {@link NodelibraryException}.</p>
	 */
	public static void handleFatalError(final Throwable t)
	{
		try
		{
			LOG.error("Shutting down application due to fatal error", t);
		}
		catch (final Throwable ignored)
		{
			// ignore any failures here
		}

		if (t instanceof Error error)
		{
			throw error;
		}
		if (t instanceof RuntimeException runtime)
		{
			throw runtime;
		}
		throw new NodelibraryException("Fatal node error", t);
	}

	private GlobalErrorHandling()
	{
	}
}
