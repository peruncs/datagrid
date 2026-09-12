package org.eclipse.datagrid.cluster.nodelibrary.aeron;

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterReplicationTransport;
import org.eclipse.datagrid.cluster.nodelibrary.types.NodelibraryPropertiesProvider;
import org.eclipse.datagrid.storage.distributed.types.DistributedStorage;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

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

/** Exercises the Aeron target with a real four-channel Embedded Store across a restart. */
class AeronStoreIntegrationIT
{
	@Test
	void fourChannelStoreTransactionSurvivesProviderRestart() throws Exception
	{
		final Path root = Files.createTempDirectory("dg-aeron-store-");
		final Path storePath = root.resolve("store");
		final UUID clusterId = UUID.randomUUID();
		final UUID nodeId = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		final NodelibraryPropertiesProvider properties = properties(root, clusterId, nodeId, generation);
		try
		{
			final Root value = new Root();
			value.values.addAll(List.of("one", "two", "three", "four"));
			for (int i = 0; i < 512; i++) value.objects.add(new NewType("channel-object-" + i));
			final AtomicBoolean sawFourChannels = new AtomicBoolean();
			long firstSequence;
			try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties))
			{
				final StorageBinaryDataDistributor distributor = transport.distributor("store", false);
				final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory = delegate ->
					transport.persistenceTargetFactory("store", distributor).apply(new PersistenceTarget<>()
					{
						@Override public void write(final Binary data)
						{
							final int[] channels = { 0 };
							data.iterateChannelChunks(ignored -> channels[0]++);
							if (channels[0] >= 4) sawFourChannels.set(true);
							delegate.write(data);
						}
						@Override public boolean isWritable() { return delegate.isWritable(); }
					});
				final EmbeddedStorageManager manager = start(storePath, value, distributor,
					targetFactory);
				try
				{
					manager.storeRoot();
					/* A handful of objects can all hash to channel zero.  Store many
					 * independent entities in one commit to force the configured
					 * four-channel storer to emit every channel in that transaction. */
					for (final NewType object : value.objects) object.value = object.value + "-updated";
					manager.storeAll(value.objects);
				}
				finally
				{
					manager.shutdown();
				}
				assertTrue(sawFourChannels.get(), "the real Store transaction must cross all four configured channels");
				firstSequence = transport.positionProvider("store").latestSequence();
				assertTrue(firstSequence >= 0, "real Store write did not reach the Aeron terminal checkpoint");
			}

			try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties))
			{
				final StorageBinaryDataDistributor distributor = transport.distributor("store", false);
				final EmbeddedStorageManager manager = startExisting(storePath, distributor,
					transport.persistenceTargetFactory("store", distributor));
			final Root resumed = manager.root();
				resumed.values.add("after-restart");
				manager.store(resumed.values);
				assertTrue(transport.positionProvider("store").latestSequence() > firstSequence,
					"the restarted real Store did not publish a later Aeron transaction");
				manager.shutdown();
			}
		}
		finally
		{
			delete(root);
		}
	}

	@Test
	void realStoreRetryRepublishesDictionaryAfterLocalRejection() throws Exception
	{
		final Path root = Files.createTempDirectory("dg-aeron-dictionary-");
		final UUID clusterId = UUID.randomUUID();
		final UUID nodeId = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		final NodelibraryPropertiesProvider properties = properties(root, clusterId, nodeId, generation);
		try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties))
		{
			final AtomicInteger dictionaryChunks = new AtomicInteger();
			AeronCrashHooks.install((name, ignored) ->
			{
				if ("AFTER_DICTIONARY_CHUNKS".equals(name)) dictionaryChunks.incrementAndGet();
			});
			try
			{
			final StorageBinaryDataDistributor distributor = transport.distributor("store", false);
			final AtomicBoolean rejectNext = new AtomicBoolean();
			final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory = delegate ->
				transport.persistenceTargetFactory("store", distributor).apply(new PersistenceTarget<>()
				{
					@Override public void write(final Binary data)
					{
						if (rejectNext.compareAndSet(true, false)) throw new IllegalStateException("injected Store rejection");
						delegate.write(data);
					}
					@Override public boolean isWritable() { return delegate.isWritable(); }
				});
			final Path storePath = root.resolve("store");
			final Root value = new Root();
			final EmbeddedStorageManager manager = start(storePath, value, distributor, targetFactory);
			try
			{
				manager.storeRoot();
				value.objects.add(new NewType("registered-before-rejection"));
				rejectNext.set(true);
				org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> manager.store(value.objects));
				manager.store(value.objects);
				assertTrue(dictionaryChunks.get() >= 2,
					"the new type dictionary must be published again on retry");
			}
			finally
			{
				manager.shutdown();
			}
			}
			finally
			{
				AeronCrashHooks.clear();
			}
		}
		finally
		{
			delete(root);
		}
	}

	@Test
	void forkedRealStoreWriterRestartsAndRetriesAcrossFourChannels() throws Exception
	{
		final Path root = Files.createTempDirectory("dg-aeron-store-process-");
		final UUID clusterId = UUID.randomUUID();
		final UUID nodeId = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		try
		{
			final String initial = forkStoreChild(root, clusterId, nodeId, generation, "initial");
			assertTrue(initial.contains("channels=true"), initial);
			final long firstSequence = sequence(initial);
			final String restart = forkStoreChild(root, clusterId, nodeId, generation, "restart");
			assertTrue(sequence(restart) > firstSequence, restart);
			final String dictionary = forkStoreChild(root, clusterId, nodeId, generation, "dictionary");
			assertTrue(sequence(dictionary) > sequence(restart), dictionary);
			assertTrue(dictionaryCount(dictionary) >= 2,
				"a rejected real-Store write must resend its dictionary on retry: " + dictionary);
		}
		finally
		{
			delete(root);
		}
	}

	private static String forkStoreChild(
		final Path root, final UUID clusterId, final UUID nodeId, final UUID generation, final String mode)
		throws Exception
	{
		final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		final Process child = new ProcessBuilder(java, "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
			"-cp", classpath,
			"-Ddg.aeron.store.root=" + root,
			"-Ddg.aeron.store.cluster=" + clusterId,
			"-Ddg.aeron.store.node=" + nodeId,
			"-Ddg.aeron.store.generation=" + generation,
			AeronStoreProcessChildMain.class.getName(), mode)
			.redirectErrorStream(true).start();
		if (!child.waitFor(60, TimeUnit.SECONDS))
		{
			child.destroyForcibly();
			throw new AssertionError("Store process child timed out: " + mode);
		}
		final String output = new String(child.getInputStream().readAllBytes());
		assertTrue(child.exitValue() == 0, mode + " child failed: " + output);
		return Files.readString(root.resolve("control").resolve(mode));
	}

	private static long sequence(final String marker)
	{
		return Long.parseLong(marker.substring(marker.indexOf("sequence=") + "sequence=".length()).trim());
	}

	private static int dictionaryCount(final String marker)
	{
		final String prefix = "dictionaries=";
		final int start = marker.indexOf(prefix);
		if (start < 0) throw new AssertionError("child did not report dictionary publications: " + marker);
		final int end = marker.indexOf(';', start + prefix.length());
		return Integer.parseInt(marker.substring(start + prefix.length(), end < 0 ? marker.length() : end).trim());
	}

	private static EmbeddedStorageManager start(
		final Path path,
		final Root root,
		final StorageBinaryDataDistributor distributor,
		final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory
	)
	{
		final EmbeddedStorageFoundation<?> foundation = foundation(path);
		DistributedStorage.configureWriting(foundation, distributor, targetFactory);
		return foundation.start(root);
	}

	private static EmbeddedStorageManager startExisting(
		final Path path,
		final StorageBinaryDataDistributor distributor,
		final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory
	)
	{
		final EmbeddedStorageFoundation<?> foundation = foundation(path);
		DistributedStorage.configureWriting(foundation, distributor, targetFactory);
		return foundation.start();
	}

	private static EmbeddedStorageFoundation<?> foundation(final Path path)
	{
		final StorageConfiguration configuration = StorageConfiguration.Builder()
			.setStorageFileProvider(Storage.FileProvider(path))
			.setChannelCountProvider(Storage.ChannelCountProvider(4))
			.createConfiguration();
		return EmbeddedStorage.Foundation(configuration);
	}

	private static NodelibraryPropertiesProvider properties(
		final Path root, final UUID clusterId, final UUID nodeId, final UUID generation)
	{
		return new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return "writer"; }
			@Override public boolean replicationRoleConfigured() { return true; }
			@Override public String replicationProperty(final String name)
			{
				return switch (name)
				{
				case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> clusterId.toString();
				case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> nodeId.toString();
				case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
				case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
				case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
				case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> root.resolve("checkpoint/writer.checkpoint").toString();
				default -> null;
				};
			}
		};
	}

	private static void delete(final Path root) throws Exception
	{
		if (!Files.exists(root)) return;
		try (var paths = Files.walk(root))
		{
			for (final Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
		}
	}

	public static final class Root
	{
		public final List<String> values = new ArrayList<>();
		public final List<NewType> objects = new ArrayList<>();
	}

	public static final class NewType
	{
		public String value;

		public NewType() { }
		NewType(final String value) { this.value = value; }
	}
}
