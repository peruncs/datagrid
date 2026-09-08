package org.eclipse.datagrid.cluster.nodelibrary.types;

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

import io.aeron.Aeron;
import io.aeron.Subscription;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpointStore;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AeronProviderCheckpointTest
{
	@Test
	void persistenceTargetWritesCommittedWriterCheckpoint() throws Exception
	{
		final Path directory = Files.createTempDirectory("datagrid-aeron-provider-");
		final Path archive = directory.resolveSibling(directory.getFileName() + ".archive");
		final Path checkpoint = directory.resolveSibling(directory.getFileName() + ".writer.checkpoint");
		final int controlPort = freePort();
		final int livePort = freePort();
		final String clusterId = UUID.randomUUID().toString();
		final NodelibraryPropertiesProvider properties = new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return "writer"; }
			@Override public boolean replicationRoleConfigured() { return true; }
			@Override public String replicationProperty(final String name)
			{
				return switch (name)
				{
					case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> clusterId;
					case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> UUID.nameUUIDFromBytes(directory.toString().getBytes()).toString();
					case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> UUID.nameUUIDFromBytes(
						("generation:" + directory).getBytes()).toString();
					case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> directory.toString();
					case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> archive.toString();
					case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> checkpoint.toString();
					case "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL" -> "aeron:udp?endpoint=localhost:" + livePort;
					case "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL" -> "aeron:udp?endpoint=localhost:" + controlPort;
					case "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL" -> "aeron:udp?endpoint=localhost:0";
					case "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL" -> "aeron:udp?endpoint=localhost:0";
					case "ECLIPSE_DATAGRID_AERON_TERM_LENGTH" -> "1048576";
					case "ECLIPSE_DATAGRID_AERON_MTU_LENGTH" -> "1024";
					case "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE" -> "16384";
					case "ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES" -> "262144";
					default -> null;
				};
			}
		};
		ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties);
		final ClusterStorageBinaryDataDistributor distributor = transport.distributor("stream", false);
		final PersistenceTarget<Binary> target = transport.persistenceTargetFactory("stream", distributor)
			.apply(new PersistenceTarget<>()
			{
				@Override public void write(final Binary ignored) { }
				@Override public boolean isWritable() { return true; }
			});
		try (Aeron subscriberAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory.toString()));
			Subscription subscriber = subscriberAeron.addSubscription("aeron:udp?endpoint=localhost:" + livePort, 1001))
		{
			target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] { 1, 2, 3 })));
			final AeronReplicationCheckpoint saved = AeronReplicationCheckpointStore.read(checkpoint);
			assertEquals(AeronReplicationCheckpoint.State.COMMITTED, saved.state());
			assertEquals(0, saved.transactionSequence());
			assertTrue(saved.recordingId() >= 0);
			transport.close();
			transport = new AeronClusterReplicationTransportProvider().create(properties);
			final ClusterStorageBinaryDataDistributor resumedDistributor = transport.distributor("stream", false);
			final PersistenceTarget<Binary> resumedTarget = transport.persistenceTargetFactory("stream", resumedDistributor)
				.apply(new PersistenceTarget<>()
				{
					@Override public void write(final Binary ignored) { }
					@Override public boolean isWritable() { return true; }
				});
			try (Aeron resumedAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory.toString()));
				Subscription resumedSubscription = resumedAeron.addSubscription("aeron:udp?endpoint=localhost:" + livePort, 1001))
			{
				resumedTarget.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] { 4, 5 })));
			}
			final AeronReplicationCheckpoint resumed = AeronReplicationCheckpointStore.read(checkpoint);
			assertEquals(AeronReplicationCheckpoint.State.COMMITTED, resumed.state());
			assertEquals(1, resumed.transactionSequence());
			assertEquals(saved.recordingId(), resumed.recordingId());
		}
		finally
		{
            transport.close();
			try (var paths = Files.walk(directory))
			{
				paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
				{
					try { Files.deleteIfExists(path); } catch (final Exception ignored) { }
				});
			}
			Files.deleteIfExists(checkpoint);
			if (Files.exists(archive))
			{
				try (var paths = Files.walk(archive))
				{
					paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
					{
						try { Files.deleteIfExists(path); } catch (final Exception ignored) { }
					});
				}
			}
		}
	}

	private static int freePort() throws Exception
	{
		try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
	}
}
