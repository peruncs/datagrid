package org.eclipse.datagrid.storage.distributed.aeron.crashtest;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import org.eclipse.datagrid.storage.distributed.types.AtomicFileStore;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Child process that holds a checkpoint write open until the parent kills it. */
public final class AeronCrashChildMain
{
	private AeronCrashChildMain()
	{
	}

	public static void main(final String[] arguments) throws Exception
	{
		final Path base = Path.of(System.getProperty("dg.crash.base"));
		final Path control = base.resolve("control");
		final Path checkpoint = base.resolve("checkpoint.bin");
		Files.createDirectories(control);
		if ("baseline".equals(System.getProperty("dg.crash.mode")))
		{
			atomicWrite(checkpoint, channel -> write(channel, "baseline"));
			mark(control.resolve("ready"));
			return;
		}
		if ("crash-write".equals(System.getProperty("dg.crash.mode")))
		{
			mark(control.resolve("ready"));
			atomicWrite(checkpoint, channel ->
			{
				write(channel, "replacement");
				mark(control.resolve("after-temp-write"));
				awaitKill(control.resolve("release"));
			});
			return;
		}
		if ("recover".equals(System.getProperty("dg.crash.mode")))
		{
			final String value = Files.readString(checkpoint, StandardCharsets.UTF_8);
			Files.writeString(control.resolve("outcome"), "OUTCOME=" + value,
				StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.WRITE);
			return;
		}
		throw new IllegalArgumentException("unknown dg.crash.mode");
	}

	private static void atomicWrite(final Path destination, final AtomicFileStore.Encoder encoder)
		throws java.io.IOException
	{
		AtomicFileStore.write(destination, encoder);
	}

	private static void write(final FileChannel channel, final String value) throws java.io.IOException
	{
		final ByteBuffer bytes = StandardCharsets.UTF_8.encode(value);
		while (bytes.hasRemaining()) channel.write(bytes);
	}

	private static void mark(final Path path) throws java.io.IOException
	{
		final Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
		Files.writeString(temporary, "ready", StandardCharsets.UTF_8,
			StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
		try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE))
		{
			channel.force(true);
		}
		Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
			java.nio.file.StandardCopyOption.REPLACE_EXISTING);
	}

	private static void awaitKill(final Path release) throws java.io.IOException
	{
		while (!Files.exists(release))
		{
			try { Thread.sleep(10L); }
			catch (final InterruptedException interrupted)
			{
				Thread.currentThread().interrupt();
				throw new java.io.IOException("crash child interrupted", interrupted);
			}
		}
	}
}
