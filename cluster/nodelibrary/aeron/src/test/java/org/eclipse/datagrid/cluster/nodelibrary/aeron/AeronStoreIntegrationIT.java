package org.eclipse.datagrid.cluster.nodelibrary.aeron;

import org.eclipse.datagrid.cluster.nodelibrary.types.*;
import org.eclipse.datagrid.storage.distributed.types.DistributedStorage;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
	void ordinaryAndBackupReadersImportRealStoreDataAndResumeFromAtomicCursors() throws Exception
	{
		final Path root = Files.createTempDirectory("dg-aeron-store-readers-");
		final UUID clusterId = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		final int controlPort = freePort();
		final int livePort = freePort();
		final int watermarkPort = freePort();
		final UUID ordinaryReaderId = UUID.randomUUID();
		final UUID backupReaderId = UUID.randomUUID();
		final Set<UUID> retentionReaders = Set.of(ordinaryReaderId, backupReaderId);
		final String retentionSecret = Base64.getEncoder().encodeToString(
			"store-integration-retention-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8));
		final Path writerStore = root.resolve("writer-store");
		final Path ordinaryStore = root.resolve("ordinary-store");
		final Path backupStore = root.resolve("backup-store");
		try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
			properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
				controlPort, livePort, watermarkPort, retentionSecret, retentionReaders)))
		{
			final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
			final Root initial = new Root();
			initial.values.add("baseline");
			final EmbeddedStorageManager seeded = start(writerStore, initial, distributor,
				writerTransport.persistenceTargetFactory("store", distributor));
			seeded.storeRoot();
			seeded.shutdown();
			final ReplicationCursor baseline = writerTransport.positionProvider("store").latest();
			copyDirectory(writerStore, ordinaryStore);
			copyDirectory(writerStore, backupStore);

			final EmbeddedStorageManager writer = startExisting(writerStore, distributor,
				writerTransport.persistenceTargetFactory("store", distributor));
			final Root writerRoot = writer.root();
			writerRoot.values.add("ordinary-update");
			writerRoot.objects.add(new NewType("dictionary-update"));
			writerRoot.payload = new byte[256 * 1024];
			java.util.Arrays.fill(writerRoot.payload, (byte)0x5a);
			writer.storeAll(List.of(writerRoot, writerRoot.values, writerRoot.objects));
			final ReplicationCursor firstTarget = writerTransport.positionProvider("store").latest();

			replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
				clusterId, generation, baseline, firstTarget, controlPort, livePort, watermarkPort,
				retentionSecret, retentionReaders, "ordinary-update", true);

			writerRoot.values.add("restart-update");
			writer.store(writerRoot.values);
			final ReplicationCursor secondTarget = writerTransport.positionProvider("store").latest();
			final Path ordinaryCursor = root.resolve("ordinary-reader/cursor");
			final MessageInfo persisted;
			try (StoredMessageInfoManager cursorManager = StoredMessageInfoManager.NewAtomic(
				ordinaryCursor, MessageInfoParser.New()))
			{
				persisted = cursorManager.get();
			}
			replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
				clusterId, generation, cursor(persisted), secondTarget, controlPort, livePort, watermarkPort,
				retentionSecret, retentionReaders, "restart-update", false);

			replicateAndVerify(root.resolve("backup-reader"), backupStore, "backup-reader", backupReaderId,
				clusterId, generation, baseline, secondTarget, controlPort, livePort, watermarkPort,
				retentionSecret, retentionReaders, "restart-update", true);
			final long watermarkDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (!writerTransport.retention().isSupported() && System.nanoTime() < watermarkDeadline)
			{
				java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
			}
			assertTrue(writerTransport.retention().isSupported(),
				"writer did not durably assemble the ordinary+backup reader watermark quorum");
			assertEquals(
				org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationLogRetention.MaintenanceResult.Status.DELETED,
				writerTransport.retention().deleteThrough(secondTarget).status(),
				"authenticated quorum must permit online purge at a complete segment boundary");
			final MessageInfo postRetentionStart;
			try (StoredMessageInfoManager cursorManager = StoredMessageInfoManager.NewAtomic(
				ordinaryCursor, MessageInfoParser.New()))
			{
				postRetentionStart = cursorManager.get();
			}
			writerRoot.values.add("post-retention-update");
			writer.store(writerRoot.values);
			final ReplicationCursor postRetentionTarget = writerTransport.positionProvider("store").latest();
			replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
				clusterId, generation, cursor(postRetentionStart), postRetentionTarget, controlPort, livePort, watermarkPort,
				retentionSecret, retentionReaders, "post-retention-update", false);
			writer.shutdown();
		}
		finally
		{
			delete(root);
		}
	}

	private static void replicateAndVerify(
		final Path readerRoot,
		final Path storePath,
		final String role,
		final UUID nodeId,
		final UUID clusterId,
		final UUID generation,
		final ReplicationCursor startingCursor,
		final ReplicationCursor target,
		final int controlPort,
		final int livePort,
		final int watermarkPort,
		final String retentionSecret,
		final Set<UUID> retentionReaders,
		final String expectedValue,
		final boolean expectDictionary
	) throws Exception
	{
		Files.createDirectories(readerRoot);
		final Path cursorPath = readerRoot.resolve("cursor");
		try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(
			properties(readerRoot, clusterId, nodeId, generation, role, -1L,
				controlPort, livePort, watermarkPort, retentionSecret, retentionReaders));
			StoredMessageInfoManager cursorManager = StoredMessageInfoManager.NewAtomic(cursorPath, MessageInfoParser.New()))
		{
			final EmbeddedStorageFoundation<?> readerFoundation = foundation(storePath);
			final EmbeddedStorageManager reader = readerFoundation.start();
			final ClusterStorageBinaryDataMerger merger = ClusterStorageBinaryDataMerger.New(
				readerFoundation.getConnectionFoundation(), reader.createConnection(),
				ObjectGraphUpdateHandler.Synchronized(), 0L, 1L);
			final ClusterStorageBinaryDataPacketAcceptor acceptor = ClusterStorageBinaryDataPacketAcceptor.New(merger);
			final ClusterStorageBinaryDataClient client = transport.client(acceptor, "store", new AfterDataMessageConsumedListener()
			{
				@Override public void onChange(final MessageInfo info) { cursorManager.set(info); }
				@Override public void close() { }
			},
				startingCursor, "backup-reader".equals(role));
			try
			{
				client.start();
				final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
				while (client.messageInfo().messageIndex() < target.logicalSequence() && client.failure() == null &&
					System.nanoTime() < deadline)
				{
					Thread.onSpinWait();
					java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
				}
				if (client.failure() != null) throw client.failure();
				assertEquals(target.logicalSequence(), client.messageInfo().messageIndex(),
					role + " did not reach the writer boundary");
				acceptor.awaitApplied();
				client.stopAtLatestMessage();
				final long stopDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
				while (client.isRunning() && System.nanoTime() < stopDeadline)
				{
					java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
				}
				assertEquals(org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY,
					client.stopOutcome(), role + " did not stop at a resolved transaction boundary");
			}
			finally
			{
				client.dispose();
				acceptor.dispose();
				reader.shutdown();
			}
			assertEquals(target.logicalSequence(), cursorManager.get().messageIndex(),
				role + " did not persist its atomic cursor");
		}

		final EmbeddedStorageManager restarted = foundation(storePath).start();
		try
		{
			final Root imported = restarted.root();
			assertTrue(imported.values.contains(expectedValue), role + " Store missed " + expectedValue);
			if (expectDictionary)
			{
				assertEquals("dictionary-update", imported.objects.get(0).value,
					role + " did not materialize the newly introduced type");
			}
		}
		finally
		{
			restarted.shutdown();
		}
	}

	/** Verifies one writer broadcasts the same Store transaction to two live reader nodes. */
	@Test
	void oneWriterBroadcastsToConcurrentOrdinaryAndBackupReaders() throws Exception
	{
		final Path root = Files.createTempDirectory("dg-aeron-concurrent-readers-");
		final UUID clusterId = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		final int controlPort = freePort();
		final int livePort = freePort();
		final int watermarkPort = freePort();
		final Path writerStore = root.resolve("writer-store");
		final Path ordinaryStore = root.resolve("ordinary-store");
		final Path backupStore = root.resolve("backup-store");
		try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
			properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
				controlPort, livePort, watermarkPort)))
		{
			final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
			final Root initial = new Root();
			initial.values.add("baseline");
			final EmbeddedStorageManager seeded = start(writerStore, initial, distributor,
				writerTransport.persistenceTargetFactory("store", distributor));
			seeded.storeRoot();
			seeded.shutdown();
			final ReplicationCursor baseline = writerTransport.positionProvider("store").latest();
			copyDirectory(writerStore, ordinaryStore);
			copyDirectory(writerStore, backupStore);

			final EmbeddedStorageManager writer = startExisting(writerStore, distributor,
				writerTransport.persistenceTargetFactory("store", distributor));
			try (ReaderNode ordinary = ReaderNode.open(root.resolve("ordinary-reader"), ordinaryStore,
				"reader", UUID.randomUUID(), clusterId, generation, baseline, controlPort, livePort, watermarkPort);
				ReaderNode backup = ReaderNode.open(root.resolve("backup-reader"), backupStore,
					"backup-reader", UUID.randomUUID(), clusterId, generation, baseline,
					controlPort, livePort, watermarkPort))
			{
				ordinary.start();
				backup.start();
				ordinary.awaitLive();
				backup.awaitLive();

				final Root writerRoot = writer.root();
				writerRoot.values.add("concurrent-broadcast");
				writerRoot.objects.add(new NewType("concurrent-dictionary"));
				writer.storeAll(List.of(writerRoot, writerRoot.values, writerRoot.objects));
				final ReplicationCursor firstTarget = writerTransport.positionProvider("store").latest();

				ordinary.await(firstTarget);
				backup.await(firstTarget);
				assertTrue(ordinary.root().values.contains("concurrent-broadcast"));
				assertTrue(backup.root().values.contains("concurrent-broadcast"));
				assertEquals("concurrent-dictionary", ordinary.root().objects.get(0).value);
				assertEquals("concurrent-dictionary", backup.root().objects.get(0).value);
				assertEquals(firstTarget.logicalSequence(), ordinary.persistedCursor().messageIndex());
				assertEquals(firstTarget.logicalSequence(), backup.persistedCursor().messageIndex());

				/* A stopped reader must not affect another reader's live subscription. */
				ordinary.stopAtLatest();
				ordinary.close();
				writerRoot.values.add("surviving-reader-broadcast");
				writer.store(writerRoot.values);
				final ReplicationCursor secondTarget = writerTransport.positionProvider("store").latest();
				backup.await(secondTarget);
				assertTrue(backup.root().values.contains("surviving-reader-broadcast"));
				assertEquals(secondTarget.logicalSequence(), backup.persistedCursor().messageIndex());
				backup.stopAtLatest();
			}
			finally
			{
				writer.shutdown();
			}
		}
		finally
		{
			delete(root);
		}
	}

	static ReplicationCursor cursor(final MessageInfo info)
	{
		return new ReplicationCursor(info.transport(), info.storeGeneration(), info.messageIndex(), info.providerPosition());
	}


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
        assertEquals(0, child.exitValue(), mode + " child failed: " + output);
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

	static EmbeddedStorageFoundation<?> foundation(final Path path)
	{
		final StorageConfiguration configuration = StorageConfiguration.Builder()
			.setStorageFileProvider(Storage.FileProvider(path))
			.setChannelCountProvider(Storage.ChannelCountProvider(4))
			.createConfiguration();
		return EmbeddedStorage.Foundation(configuration);
	}

	static NodelibraryPropertiesProvider properties(
		final Path root, final UUID clusterId, final UUID nodeId, final UUID generation)
	{
		return properties(root, clusterId, nodeId, generation, "writer", -1L, 40124, 40123, 40125);
	}

	static NodelibraryPropertiesProvider properties(
		final Path root,
		final UUID clusterId,
		final UUID nodeId,
		final UUID generation,
		final String role,
		final long recordingId,
		final int controlPort,
		final int livePort,
		final int watermarkPort
	)
	{
		return properties(root, clusterId, nodeId, generation, role, recordingId,
			controlPort, livePort, watermarkPort, null, Set.of());
	}

	static NodelibraryPropertiesProvider properties(
		final Path root,
		final UUID clusterId,
		final UUID nodeId,
		final UUID generation,
		final String role,
		final long recordingId,
		final int controlPort,
		final int livePort,
		final int watermarkPort,
		final String retentionSecret,
		final Set<UUID> retentionReaders
	)
	{
		return new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return role; }
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
					case "ECLIPSE_DATAGRID_AERON_RECORDING_ID" -> Long.toString(recordingId);
					case "ECLIPSE_DATAGRID_AERON_TERM_LENGTH" -> "65536";
					case "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE" -> "4096";
					case "ECLIPSE_DATAGRID_AERON_ARCHIVE_SEGMENT_FILE_LENGTH" -> "65536";
					case "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE" -> Boolean.toString(!"writer".equals(role));
					case "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL" -> "writer".equals(role)
						? "aeron:udp?control=localhost:" + livePort +
							"|control-mode=dynamic|fc=max|alias=datagrid-" + clusterId
						: "aeron:udp?endpoint=localhost:0|control=localhost:" + livePort +
							"|control-mode=dynamic|alias=datagrid-" + clusterId;
					case "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL" ->
						"aeron:udp?endpoint=localhost:" + controlPort;
					case "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
						"ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL" -> "aeron:udp?endpoint=localhost:0";
						case "ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL" ->
							"aeron:udp?endpoint=localhost:" + watermarkPort;
						case "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET" -> retentionSecret;
						case "ECLIPSE_DATAGRID_AERON_RETENTION_READERS" -> retentionReaders.stream()
							.sorted().map(UUID::toString).collect(java.util.stream.Collectors.joining(","));
					default -> null;
				};
			}
		};
	}

	static int freePort() throws Exception
	{
		try (ServerSocket socket = new ServerSocket(0))
		{
			return socket.getLocalPort();
		}
	}

	static void copyDirectory(final Path source, final Path target) throws Exception
	{
		try (var paths = Files.walk(source))
		{
			for (final Path path : paths.toList())
			{
				final Path destination = target.resolve(source.relativize(path));
				if (Files.isDirectory(path)) Files.createDirectories(destination);
				else Files.copy(path, destination);
			}
		}
	}

	static void delete(final Path root) throws Exception
	{
		if (!Files.exists(root)) return;
		try (var paths = Files.walk(root))
		{
			for (final Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
		}
	}

	private static final class ReaderNode implements AutoCloseable
	{
		private final ClusterReplicationTransport transport;
		private final StoredMessageInfoManager cursorManager;
		private final EmbeddedStorageManager storage;
		private final ClusterStorageBinaryDataPacketAcceptor acceptor;
		private final ClusterStorageBinaryDataClient client;
		private boolean closed;

		private ReaderNode(
			final Path nodeRoot,
			final Path storePath,
			final String role,
			final UUID nodeId,
			final UUID clusterId,
			final UUID generation,
			final ReplicationCursor startingCursor,
			final int controlPort,
			final int livePort,
			final int watermarkPort
		)
		{
			this.transport = new AeronClusterReplicationTransportProvider().create(properties(
				nodeRoot, clusterId, nodeId, generation, role, -1L, controlPort, livePort, watermarkPort));
			try
			{
				this.cursorManager = StoredMessageInfoManager.NewAtomic(
					nodeRoot.resolve("cursor"), MessageInfoParser.New());
				final EmbeddedStorageFoundation<?> foundation = foundation(storePath);
				this.storage = foundation.start();
				final ClusterStorageBinaryDataMerger merger = ClusterStorageBinaryDataMerger.New(
					foundation.getConnectionFoundation(), this.storage.createConnection(),
					ObjectGraphUpdateHandler.Synchronized(), 0L, 1L);
				this.acceptor = ClusterStorageBinaryDataPacketAcceptor.New(merger);
				this.client = this.transport.client(this.acceptor, "store", new AfterDataMessageConsumedListener()
				{
					@Override public void onChange(final MessageInfo info) { ReaderNode.this.cursorManager.set(info); }
					@Override public void close() { }
				}, startingCursor, "backup-reader".equals(role));
			}
			catch (final RuntimeException | Error failure)
			{
				try { this.transport.close(); }
				catch (final RuntimeException closeFailure) { failure.addSuppressed(closeFailure); }
				throw failure;
			}
		}

		static ReaderNode open(
			final Path nodeRoot,
			final Path storePath,
			final String role,
			final UUID nodeId,
			final UUID clusterId,
			final UUID generation,
			final ReplicationCursor startingCursor,
			final int controlPort,
			final int livePort,
			final int watermarkPort
		) throws Exception
		{
			Files.createDirectories(nodeRoot);
			return new ReaderNode(nodeRoot, storePath, role, nodeId, clusterId, generation,
				startingCursor, controlPort, livePort, watermarkPort);
		}

		void start()
		{
			this.client.start();
		}

		void awaitLive()
		{
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
			while (!this.client.isLive() && this.client.failure() == null && System.nanoTime() < deadline)
			{
				Thread.onSpinWait();
				java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
			}
			if (this.client.failure() != null) throw this.client.failure();
			assertTrue(this.client.isLive(), "reader did not join the writer's live publication");
		}

		void await(final ReplicationCursor target)
		{
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
			while (this.client.messageInfo().messageIndex() < target.logicalSequence() &&
				this.client.failure() == null && System.nanoTime() < deadline)
			{
				Thread.onSpinWait();
				java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
			}
			if (this.client.failure() != null) throw this.client.failure();
			assertEquals(target.logicalSequence(), this.client.messageInfo().messageIndex(),
				"reader did not resolve the writer transaction");
			this.acceptor.awaitApplied();
		}

		void stopAtLatest()
		{
			this.client.stopAtLatestMessage();
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
			while (this.client.isRunning() && System.nanoTime() < deadline)
			{
				java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
			}
			assertEquals(StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY,
				this.client.stopOutcome(), "reader did not stop at a resolved boundary");
		}

		Root root()
		{
			return this.storage.root();
		}

		MessageInfo persistedCursor()
		{
			return this.cursorManager.get();
		}

		@Override
		public void close()
		{
			if (this.closed) return;
			this.closed = true;
			RuntimeException failure = null;
			try { this.client.dispose(); }
			catch (final RuntimeException closeFailure) { failure = closeFailure; }
			try { this.acceptor.dispose(); }
			catch (final RuntimeException closeFailure) { failure = append(failure, closeFailure); }
			try { this.storage.shutdown(); }
			catch (final RuntimeException closeFailure) { failure = append(failure, closeFailure); }
			try { this.cursorManager.close(); }
			catch (final RuntimeException closeFailure) { failure = append(failure, closeFailure); }
			try { this.transport.close(); }
			catch (final RuntimeException closeFailure) { failure = append(failure, closeFailure); }
			if (failure != null) throw failure;
		}

		private static RuntimeException append(final RuntimeException current, final RuntimeException additional)
		{
			if (current == null) return additional;
			if (current != additional) current.addSuppressed(additional);
			return current;
		}
	}

	public static final class Root
	{
		public final List<String> values = new ArrayList<>();
		public final List<NewType> objects = new ArrayList<>();
		public byte[] payload = new byte[0];
	}

	public static final class NewType
	{
		public String value;

		NewType(final String value) { this.value = value; }
	}
}
