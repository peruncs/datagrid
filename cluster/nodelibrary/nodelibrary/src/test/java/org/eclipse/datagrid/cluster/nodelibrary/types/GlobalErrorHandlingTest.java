package org.eclipse.datagrid.cluster.nodelibrary.types;

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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies fatal node errors remain under the embedding application's control. */
class GlobalErrorHandlingTest
{
	@Test
	void rethrowsRuntimeFailuresWithoutTerminatingTheJvm()
	{
		final RuntimeException failure = new IllegalStateException("boom");
		assertSame(failure, assertThrows(RuntimeException.class,
			() -> GlobalErrorHandling.handleFatalError(failure)));
	}

	@Test
	void wrapsCheckedFailuresWithoutTerminatingTheJvm()
	{
		final Exception failure = new Exception("boom");
		final NodelibraryException reported = assertThrows(NodelibraryException.class,
			() -> GlobalErrorHandling.handleFatalError(failure));
		assertSame(failure, reported.getCause());
	}
}
