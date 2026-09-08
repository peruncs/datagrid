package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.reader.StorageBinaryDataClientAeronArchive;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

class AeronArchiveReplicationIT
{
	@Test
	void replaysRecordedUdpMessagesAndJoinsLive() throws Exception
	{
		final int controlPort = freePort();
		final String directory = Files.createTempDirectory("datagrid-aeron-").toString();
		final File archiveDirectory = new File(directory, "archive");
		final String controlChannel = "aeron:udp?endpoint=localhost:" + controlPort;
		final String controlResponseChannel = "aeron:udp?endpoint=localhost:0";
		// Archive control and replay are UDP; IPC is used only for the local
		// recorded publication so this test is deterministic on CI hosts.
		final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(1024 * 1024)
			.mtuLength(1408)
			.chunkSize(16 * 1024)
			.maxTransactionBytes(256 * 1024)
			.offerTimeoutNanos(10_000_000_000L)
			.build();
		final UUID clusterId = UUID.randomUUID();
		final RecordingReceiver receiver = new RecordingReceiver();

		final MediaDriver.Context mediaContext = new MediaDriver.Context()
			.aeronDirectoryName(directory)
			.threadingMode(ThreadingMode.SHARED)
			.dirDeleteOnStart(true)
			.dirDeleteOnShutdown(true);
		final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
			.aeronDirectoryName(directory)
			.controlRequestChannel(controlChannel)
			.controlResponseChannel(controlResponseChannel)
			.messageTimeoutNs(10_000_000_000L);
		final Archive.Context archiveContext = new Archive.Context()
			.aeronDirectoryName(directory)
			.archiveDir(archiveDirectory)
			.deleteArchiveOnStart(true)
			.threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED)
			.controlChannel(controlChannel)
			.replicationChannel("aeron:udp?endpoint=localhost:0");

		try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(mediaContext, archiveContext);
			AeronArchive archive = AeronArchive.connect(archiveClientContext))
		{
			final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
				archive, liveChannel, 1001, configuration, clusterId, 2, 0
			);
			await(publisher.publication()::isConnected, 10_000);
			final byte[] data = new byte[70_000];
			for (int i = 0; i < data.length; i++)
			{
				data[i] = (byte)(i * 13);
			}
			publisher.publishTransaction(
				"recorded.Type".getBytes(java.nio.charset.StandardCharsets.UTF_8),
				new ByteBuffer[] { ByteBuffer.wrap(data) }
			);
			final long recordingId = awaitRecordingId(publisher, 10_000);
			final long firstStop = publisher.publication().position();
			assertTrue(publisher.recordingIsActive(), "local Archive recording must be active without an external reader");
			assertTrue(publisher.publication().position() > 0, "writer publication must progress with zero external readers");
			publisher.close();
			await(() -> archive.getStopPosition(recordingId) >= firstStop, 10_000);
			assertThrows(IllegalArgumentException.class, () -> AeronArchiveReplicationPublisher.Extend(
				archive, recordingId, 1002, configuration, clusterId, 2, 1));
			final byte[] resumedData = new byte[] { 8, 6, 7, 5 };
			final AeronArchiveReplicationPublisher resumed = AeronArchiveReplicationPublisher.Extend(
				archive, recordingId, 1001, configuration, clusterId, 2, 1
			);
			await(resumed.publication()::isConnected, 10_000);
			resumed.publishTransaction(null, new ByteBuffer[] { ByteBuffer.wrap(resumedData) });
			assertEquals(recordingId, awaitRecordingId(resumed, 10_000));
			final StorageBinaryDataClientAeronArchive client = StorageBinaryDataClientAeronArchive.New(
				archive.context().aeron(),
				new AeronArchive.Context()
					.aeronDirectoryName(directory)
					.controlRequestChannel(controlChannel)
					.controlResponseChannel(controlResponseChannel)
					.messageTimeoutNs(10_000_000_000L),
				recordingId,
				io.aeron.archive.client.PersistentSubscription.FROM_START,
				liveChannel,
				1001,
				"aeron:udp?endpoint=localhost:0",
				1002,
				configuration,
				clusterId,
				2,
				-1,
				receiver,
				() -> { }
			);
			client.start();

			await(() -> client.lastResolvedSequence() == 1 || client.failure() != null, 15_000);
			if (client.failure() != null)
			{
				throw client.failure();
			}
			assertEquals("recorded.Type", receiver.dictionary);
			assertArrayEquals(resumedData, receiver.data);
			assertNull(client.failure());
			final long restartPosition = client.lastResolvedPosition();
			final long restartSequence = client.lastResolvedSequence();
			client.dispose();
			final RecordingReceiver restartedReceiver = new RecordingReceiver();
			final StorageBinaryDataClientAeronArchive restarted = StorageBinaryDataClientAeronArchive.New(
				archive.context().aeron(),
				new AeronArchive.Context().aeronDirectoryName(directory)
					.controlRequestChannel(controlChannel).controlResponseChannel(controlResponseChannel)
					.messageTimeoutNs(10_000_000_000L),
				recordingId, restartPosition, liveChannel, 1001, "aeron:udp?endpoint=localhost:0", 1002,
				configuration, clusterId, 2, restartSequence, restartedReceiver, () -> { });
			restarted.start();
			final byte[] thirdData = new byte[] { 1, 3, 3, 7 };
			resumed.publishTransaction(null, new ByteBuffer[] { ByteBuffer.wrap(thirdData) });
			await(() -> restarted.lastResolvedSequence() == 2 || restarted.failure() != null, 15_000);
			if (restarted.failure() != null)
			{
				throw restarted.failure();
			}
			assertArrayEquals(thirdData, restartedReceiver.data);
			restarted.dispose();
			resumed.close();
		}
		finally
		{
			delete(archiveDirectory);
			delete(new File(directory));
		}
	}

	private static long awaitRecordingId(final AeronArchiveReplicationPublisher publisher, final long timeout)
		throws Exception
	{
		final long deadline = System.nanoTime() + timeout * 1_000_000L;
		long recordingId;
		do
		{
			recordingId = publisher.recordingId();
			if (recordingId >= 0)
			{
				return recordingId;
			}
			LockSupport.parkNanos(1_000_000L);
		}
		while (System.nanoTime() < deadline);
		throw new AssertionError("recording counter was not created");
	}

	private static int freePort() throws Exception
	{
		try (ServerSocket socket = new ServerSocket(0))
		{
			return socket.getLocalPort();
		}
	}

	private static void await(final Check check, final long timeoutMillis) throws Exception
	{
		final long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
		while (!check.value())
		{
			if (System.nanoTime() >= deadline)
			{
				throw new AssertionError("timed out waiting for Archive state");
			}
			LockSupport.parkNanos(1_000_000L);
		}
	}

	private static void delete(final File file) throws Exception
	{
		if (file.exists())
		{
			try (var paths = Files.walk(file.toPath()))
			{
				paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
				{
					try
					{
						Files.deleteIfExists(path);
					}
					catch (final Exception ignored)
					{
					}
				});
			}
		}
	}

	@FunctionalInterface
	private interface Check
	{
		boolean value();
	}

	private static final class RecordingReceiver implements StorageBinaryDataReceiver
	{
		private volatile String dictionary;
		private volatile byte[] data;

		@Override
		public void receiveData(final Binary value)
		{
			final ByteBuffer source = value.buffers()[0].duplicate();
			this.data = new byte[source.remaining()];
			source.get(this.data);
		}

		@Override
		public void receiveTypeDictionary(final String value)
		{
			this.dictionary = value;
		}
	}
}
