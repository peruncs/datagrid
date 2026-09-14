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

/** File names shared by local and network backup archives. */
final class BackupFileNames
{
	static final String STORAGE = "storage";
	static final String MANIFEST = "manifest";
	static final String READY = "ready";
	static final String USER_UPLOADED_STORAGE = "user-uploaded-storage";

	private BackupFileNames()
	{
	}
}
