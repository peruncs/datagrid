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

/** Metadata returned by the remote backup service. */
public class BackupMetadataDto
{
	private String name;
	private long size;

	/** Creates empty metadata for a JSON mapper. */
	public BackupMetadataDto()
	{
	}

	/** Creates metadata for one backup.
	 *
	 * @param name backup name
	 * @param size backup size in bytes
	 */
	public BackupMetadataDto(final String name, final long size)
	{
		this.name = name;
		this.size = size;
	}

	/** Returns the backup name.
	 *
	 * @return backup name
	 */
	public String getName()
	{
		return this.name;
	}

	/** Sets the backup name.
	 *
	 * @param name backup name
	 */
	public void setName(final String name)
	{
		this.name = name;
	}

	/** Returns the backup size in bytes.
	 *
	 * @return backup size
	 */
	public long getSize()
	{
		return this.size;
	}

	/** Sets the backup size in bytes.
	 *
	 * @param size backup size
	 */
	public void setSize(final long size)
	{
		this.size = size;
	}
}
