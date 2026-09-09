package org.eclipse.datagrid.cluster.nodelibrary.types.crashtest;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

/** Captures bounded, reproducible evidence when a process cell fails. */
final class DiagnosticCollector
{
	private DiagnosticCollector()
	{
	}

	static Path collect(final Path work, final String reason) throws IOException
	{
		final Path evidence = work.resolveSibling(work.getFileName() + ".evidence");
		Files.createDirectories(evidence);
		Files.writeString(evidence.resolve("reason.txt"), reason + System.lineSeparator(),
			StandardCharsets.UTF_8);
		final Path control = work.resolve("control");
		if (Files.exists(control))
		{
			try (var paths = Files.list(control))
			{
				for (final Path path : paths.toList())
				{
					if (Files.isRegularFile(path))
					{
						Files.copy(path, evidence.resolve(path.getFileName().toString()),
							java.nio.file.StandardCopyOption.REPLACE_EXISTING);
					}
				}
			}
		}
		final String listing;
		try (var paths = Files.walk(work))
		{
			listing = paths.sorted().map(path ->
			{
				try
				{
					return work.relativize(path) + "\t" + Files.size(path);
				}
				catch (final IOException failure)
				{
					return work.relativize(path) + "\t<error:" + failure + ">";
				}
			}).collect(Collectors.joining(System.lineSeparator()));
		}
		Files.writeString(evidence.resolve("directory-listing.txt"), listing + System.lineSeparator(),
			StandardCharsets.UTF_8);
		return evidence;
	}
}
