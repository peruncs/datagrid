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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the foundation's single close boundary. */
class ClusterFoundationLifecycleTest
{
	@Test
	void closeIsIdempotentAndPreventsRestart() throws Exception
	{
		final ClusterFoundation<?> foundation = ClusterFoundation.New();
		foundation.close();
		foundation.close();

		assertThrows(IllegalStateException.class, foundation::startStorageManager);
	}
}
