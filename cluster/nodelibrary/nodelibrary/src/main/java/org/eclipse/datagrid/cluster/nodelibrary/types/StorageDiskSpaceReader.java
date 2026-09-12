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


import org.eclipse.serializer.afs.types.ADirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This reader measures the bytes currently used by a storage directory.
 *
 * <p>A running Store may remove a file while the directory is being visited.
 * Implementations therefore treat that individual file as unavailable and
 * continue the measurement.</p>
 */
public interface StorageDiskSpaceReader
{
	/** Reads used bytes in the storage directory.
	 * @return used bytes
	 */
	long readUsedDiskSpaceBytes();

	/** Creates a disk-space reader.
	 * @param storageDir storage directory
	 * @return disk-space reader
	 */
	static StorageDiskSpaceReader New(final ADirectory storageDir)
	{
		return new Default(notNull(storageDir));
	}

	/** Recursively measures the configured Store directory. */
	class Default implements StorageDiskSpaceReader
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageDiskSpaceReader.class);
		private final ADirectory storageDir;

		private long lastLog = System.currentTimeMillis();

		private Default(final ADirectory storageDir)
		{
			this.storageDir = storageDir;
		}

		@Override
		public long readUsedDiskSpaceBytes()
		{
			final long sizeBytes = this.totalSize(this.storageDir);
			if (LOG.isTraceEnabled() && System.currentTimeMillis() - this.lastLog > 600_000L)
			{
				LOG.trace("Read current storage disk space ({})", sizeBytes);
				this.lastLog = System.currentTimeMillis();
			}
			return sizeBytes;
		}

		private long totalSize(final ADirectory dir)
		{
			final long[] total = {
				0L
			};
			dir.iterateFiles(f ->
			{
				try
				{
					total[0] += f.size();
				}
				catch (final RuntimeException e)
				{
					// the running storage might delete some files
					// TODO: Better way would be to queue a storage task
				}
			});
			dir.iterateDirectories(d -> total[0] += this.totalSize(d));
			return total[0];
		}
	}
}
