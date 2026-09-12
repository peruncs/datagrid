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

import org.eclipse.datagrid.storage.distributed.types.AtomicFileStoreCrashHook;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests stored message info manager behavior. */
class StoredMessageInfoManagerTest
{
	/** Verifies atomic path round trips and replaces complete records. */
	@Test
	void atomicPathRoundTripsAndReplacesCompleteRecords() throws Exception
	{
		final Path directory = Files.createTempDirectory("message-info-");
		final Path path = directory.resolve("offset");
		final UUID generation = UUID.randomUUID();
		final MessageInfo expected = MessageInfo.New(17L, "aeron", generation, new byte[] { 1, 2, 3 });
		try
		{
			try (StoredMessageInfoManager manager = StoredMessageInfoManager.NewAtomic(path, MessageInfoParser.New()))
			{
				manager.set(expected);
				assertEquals(expected.messageIndex(), manager.get().messageIndex());
			}
			try (StoredMessageInfoManager manager = StoredMessageInfoManager.NewAtomic(path, MessageInfoParser.New()))
			{
				final MessageInfo restored = manager.get();
				assertEquals(expected.messageIndex(), restored.messageIndex());
				assertEquals(expected.transport(), restored.transport());
				assertEquals(expected.storeGeneration(), restored.storeGeneration());
				assertArrayEquals(expected.providerPosition(), restored.providerPosition());
			}
			try (var files = Files.list(directory))
			{
				assertEquals(1L, files.count(), "atomic replacement must not leave a temp file");
			}
		}
		finally
		{
			try (var files = Files.walk(directory))
			{
				files.sorted(java.util.Comparator.reverseOrder()).forEach(file ->
				{
					try { Files.deleteIfExists(file); }
					catch (final java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
				});
			}
		}
	}

	/** Verifies production MessageInfo writes use the shared atomic-file boundary. */
	@Test
	void atomicMessageInfoWriteUsesAtomicFileStore() throws Exception
	{
		final Path directory = Files.createTempDirectory("message-info-hook-");
		final Path path = directory.resolve("offset");
		final AtomicReference<String> phase = new AtomicReference<>();
		try
		{
			AtomicFileStoreCrashHook.install((name, ignored) -> phase.compareAndSet(null, name));
			try (StoredMessageInfoManager manager = StoredMessageInfoManager.NewAtomic(path, MessageInfoParser.New()))
			{
				manager.set(MessageInfo.New(1L, "aeron", UUID.randomUUID(), new byte[] { 4 }));
			}
			assertEquals("BEFORE_TEMP_WRITE", phase.get());
		}
		finally
		{
			AtomicFileStoreCrashHook.clear();
			try (var files = Files.walk(directory))
			{
				files.sorted(java.util.Comparator.reverseOrder()).forEach(file ->
				{
					try { Files.deleteIfExists(file); }
					catch (final java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
				});
			}
		}
	}
}
