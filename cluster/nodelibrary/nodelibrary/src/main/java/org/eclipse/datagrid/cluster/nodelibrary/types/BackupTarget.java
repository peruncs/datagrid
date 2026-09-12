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


	/** Identifies the local or remote place from which a backup is served. */
	public enum BackupTarget
{
		/** Backup served by the hosted service. */
		SAAS,
		/** Backup served by the local installation. */
		ONPREM;

	/**
	 * Tries to parse the string into the appropriate backup target. If it fails
	 * {@code null} is returned.
	 *
	 * @param s target name
	 * @return parsed target, or {@code null}
	 */
	public static BackupTarget parse(final String s)
	{
		if (s == null)
		{
			return null;
		}

		return switch (s)
		{
		case "SAAS" -> SAAS;
		case "ONPREM" -> ONPREM;
		default -> null;
		};
	}
}
