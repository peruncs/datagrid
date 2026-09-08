package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.util.X;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Proves the Store-level replacement contract used by at-least-once replay. */
class StorageBinaryImportIntegrationTest
{
	@Test
	void importsTheSameBinaryTransactionsTwiceAndSurvivesRestart() throws Exception
	{
		final Path root = Files.createTempDirectory("datagrid-store-import-");
		final Path sourcePath = root.resolve("source");
		final Path readerPath = root.resolve("reader");
			final CapturingDistributor capture = new CapturingDistributor();
		try
		{
			final EmbeddedStorageManager writer = start(sourcePath, new Root(), capture);
			writer.storeRoot();
			writer.shutdown();
			copyDirectory(sourcePath, readerPath);
			capture.transactions.clear();

			final EmbeddedStorageManager resumedWriter = startExisting(sourcePath, capture);
			final Root resumedRoot = (Root)resumedWriter.root();
			resumedRoot.values.add("one");
			resumedWriter.store(resumedRoot.values);
			resumedRoot.values.add("two");
			resumedWriter.store(resumedRoot.values);
			resumedWriter.shutdown();
			assertTrue(capture.transactions.size() >= 2, "writer must produce replayable Store transactions");

			final EmbeddedStorageManager reader = EmbeddedStorage.Foundation(readerPath).start();
			final StorageConnection connection = reader.createConnection();
			for (final List<ByteBuffer> transaction : capture.transactions)
			{
				connection.importData(X.Enum(copy(transaction)));
				connection.importData(X.Enum(copy(transaction)));
			}
			reader.shutdown();

			final EmbeddedStorageManager restarted = EmbeddedStorage.Foundation(readerPath).start();
			assertTrue(((Root)restarted.root()).values.containsAll(List.of("one", "two")));
			restarted.shutdown();
		}
		finally
		{
			delete(root);
		}
	}

	private static EmbeddedStorageManager startExisting(final Path path, final CapturingDistributor capture)
	{
		final var foundation = EmbeddedStorage.Foundation(path);
		DistributedStorage.configureWriting(foundation, capture);
		return foundation.start();
	}

	private static EmbeddedStorageManager start(
		final Path path,
		final Root initialRoot,
		final CapturingDistributor capture
	)
	{
		final var foundation = EmbeddedStorage.Foundation(path);
		DistributedStorage.configureWriting(foundation, capture);
		return foundation.start(initialRoot);
	}

	private static List<ByteBuffer> copy(final List<ByteBuffer> source)
	{
		final List<ByteBuffer> copy = new ArrayList<>(source.size());
		for (final ByteBuffer value : source)
		{
			final ByteBuffer duplicate = value.duplicate();
			final ByteBuffer direct = ByteBuffer.allocateDirect(duplicate.remaining());
			direct.put(duplicate).flip();
			copy.add(direct);
		}
		return copy;
	}

	private static void delete(final Path path) throws Exception
	{
		if (Files.exists(path))
		{
			try (var paths = Files.walk(path))
			{
				paths.sorted(java.util.Comparator.reverseOrder()).forEach(value ->
				{
					try { Files.deleteIfExists(value); } catch (final Exception ignored) { }
				});
			}
		}
	}

	private static void copyDirectory(final Path source, final Path target) throws Exception
	{
		try (var paths = Files.walk(source))
		{
			for (final Path path : paths.toList())
			{
				final Path destination = target.resolve(source.relativize(path));
				if (Files.isDirectory(path))
				{
					Files.createDirectories(destination);
				}
				else
				{
					Files.createDirectories(destination.getParent());
					Files.copy(path, destination);
				}
			}
		}
	}

	private static final class CapturingDistributor implements StorageBinaryDataDistributor
	{
		private final List<List<ByteBuffer>> transactions = new ArrayList<>();
		private int dictionariesSinceTransaction;

		@Override
		public synchronized void distributeData(final Binary data)
		{
			if (this.dictionariesSinceTransaction > 1)
			{
				throw new AssertionError("Serializer must coalesce type dictionary export per Store commit");
			}
			this.dictionariesSinceTransaction = 0;
			final List<ByteBuffer> copy = new ArrayList<>();
			data.iterateChannelChunks(chunk ->
			{
				for (final ByteBuffer buffer : chunk.buffers())
				{
					copy.add(copy(List.of(buffer)).get(0));
				}
			});
			this.transactions.add(copy);
		}

		@Override public synchronized void distributeTypeDictionary(final String ignored)
		{
			this.dictionariesSinceTransaction++;
		}
		@Override public void dispose() { }
	}

	public static final class Root
	{
		public final List<String> values = new ArrayList<>();
	}
}
