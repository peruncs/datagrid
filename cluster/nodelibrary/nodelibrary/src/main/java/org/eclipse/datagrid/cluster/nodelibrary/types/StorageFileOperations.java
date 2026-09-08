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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Shared filesystem operations used by backup backends. */
final class StorageFileOperations
{
	private StorageFileOperations()
	{
	}

	static void deleteDirectory(final Path path) throws NodelibraryException
	{
		try (final var files = Files.walk(path))
		{
			files.sorted(Comparator.reverseOrder()).forEach(file ->
			{
				try
				{
					Files.delete(file);
				}
				catch (final IOException e)
				{
					throw new NodelibraryException("Failed to delete file at " + file, e);
				}
			});
		}
		catch (final IOException e)
		{
			throw new NodelibraryException("Failed to iterate files at " + path, e);
		}
	}
}
