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

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpointStore;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronArchiveReplicationPublisher;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises reader restart boundaries with a real child process and Archive. */
class AeronReaderCrashMatrixIT
{
	private static final UUID CLUSTER_ID = UUID.nameUUIDFromBytes("reader-crash-cluster".getBytes(StandardCharsets.UTF_8));
	private static final long EPOCH = 2L;

	/** Verifies replay before import leaves uncertain marker and requires reseed. */
	@Test
	void replayBeforeImportLeavesUncertainMarkerAndRequiresReseed() throws Exception
	{
		this.assertReseed("REPLAY_BEFORE_FIRST_IMPORT", false);
	}

	/** Verifies import boundary leaves uncertain marker and requires reseed. */
	@Test
	void importBoundaryLeavesUncertainMarkerAndRequiresReseed() throws Exception
	{
		this.assertReseed("DURING_STORE_IMPORT", false);
	}

	/** Verifies an injected Store import failure leaves the reader uncertain. */
	@Test
	void importFailureLeavesUncertainMarkerAndRequiresReseed() throws Exception
	{
		this.assertReseed("DURING_STORE_IMPORT_FAILURE", false);
	}

	/** Verifies import before cursor boundary leaves store record and requires reseed. */
	@Test
	void importBeforeCursorBoundaryLeavesStoreRecordAndRequiresReseed() throws Exception
	{
		this.assertReseed("AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE", true);
	}

	private void assertReseed(final String point, final boolean expectStoreRecord)
		throws IOException, InterruptedException
	{
		final Path base = Files.createTempDirectory("dg-reader-crash-");
		final int controlPort = freePort();
		final Path mediaDirectory = base.resolve("archive-aeron");
		final Path archiveDirectory = base.resolve("archive");
		final String controlChannel = "aeron:udp?endpoint=localhost:" + controlPort;
		final String controlResponseChannel = "aeron:udp?endpoint=localhost:0";
		final String liveChannel = "aeron:ipc?term-length=1048576|mtu=1408";
		final String replayChannel = "aeron:udp?endpoint=localhost:0";
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(1024 * 1024).mtuLength(1408).chunkSize(16 * 1024)
			.maxTransactionBytes(256 * 1024).offerTimeoutNanos(10_000_000_000L).build();
		final MediaDriver.Context mediaContext = new MediaDriver.Context()
			.aeronDirectoryName(mediaDirectory.toString())
			.threadingMode(ThreadingMode.SHARED)
			.dirDeleteOnStart(true).dirDeleteOnShutdown(true);
		final Archive.Context archiveContext = new Archive.Context()
			.aeronDirectoryName(mediaDirectory.toString())
			.archiveDir(archiveDirectory.toFile())
			.deleteArchiveOnStart(true)
			.threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED)
			.controlChannel(controlChannel)
			.replicationChannel(replayChannel);
		Process child = null;
		Process recovery = null;
		try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(mediaContext, archiveContext);
			 AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
				.aeronDirectoryName(mediaDirectory.toString())
				.controlRequestChannel(controlChannel)
				.controlResponseChannel(controlResponseChannel)
				.messageTimeoutNs(configuration.offerTimeoutNanos())))
		{
			try (final AeronArchiveReplicationPublisher publisher = AeronArchiveReplicationPublisher.New(
				archive, liveChannel, 1001, configuration, CLUSTER_ID, EPOCH, 0))
			{
				publisher.publishTransaction(null, new ByteBuffer[] { ByteBuffer.wrap(payload(0)) });
				publisher.publishTransaction(null, new ByteBuffer[] { ByteBuffer.wrap(payload(1)) });
				final long recordingId = awaitRecordingId(publisher, 15_000L);
				child = launch(base, "phase1", point, recordingId, controlChannel, controlResponseChannel,
					liveChannel, replayChannel, mediaDirectory);
				awaitFile(base.resolve("control/ready"), child, 30_000L);
				final Path milestonePath = base.resolve("control/milestone.reached");
				awaitFile(milestonePath, child, 30_000L);
				final ReaderMilestone milestone = ReaderMilestone.read(milestonePath);
				assertTrue(point.equals(milestone.point()), "unexpected reader milestone " + milestone);
				assertTrue(milestone.sequence() >= 0, "reader milestone has no sequence");
				child.destroyForcibly();
				assertTrue(child.waitFor(10, TimeUnit.SECONDS), "reader child did not exit after kill");
				recovery = launch(base, "phase2", "NONE", recordingId, controlChannel,
					controlResponseChannel, liveChannel, replayChannel, mediaDirectory);
				awaitFile(base.resolve("control/outcome"), recovery, 30_000L);
				assertTrue(recovery.waitFor(15, TimeUnit.SECONDS), "reader recovery child did not exit");
				final String outcome = Files.readString(base.resolve("control/outcome"));
				assertTrue(outcome.lines().anyMatch(line -> line.equals("OUTCOME=RESEED_REQUIRED")), outcome);
				final Path uncertainty = base.resolve("reader.reader-inflight");
				assertTrue(Files.exists(uncertainty), "uncertainty marker must survive the crash");
				final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(uncertainty);
				assertEquals(AeronReplicationCheckpoint.RecordType.READER_CURSOR, checkpoint.recordType());
				assertEquals(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, checkpoint.state());
				assertEquals(milestone.sequence(), checkpoint.transactionSequence());
				assertEquals(milestone.position(), checkpoint.recordingPosition());
				assertTrue(Files.exists(base.resolve("reader.store")) == expectStoreRecord,
					"unexpected Store fixture state for " + point);
			}
		}
		finally
		{
			if (child != null && child.isAlive()) child.destroyForcibly();
			if (recovery != null && recovery.isAlive()) recovery.destroyForcibly();
			deleteTree(base);
		}
	}

	private static Process launch(final Path base, final String mode, final String point, final long recordingId,
		final String controlChannel, final String controlResponseChannel, final String liveChannel,
		final String replayChannel, final Path aeronDirectory) throws IOException
	{
		final Path control = base.resolve("control");
		Files.createDirectories(control);
		Files.deleteIfExists(control.resolve("ready"));
		Files.deleteIfExists(control.resolve("outcome"));
		Files.deleteIfExists(control.resolve("milestone.reached"));
		Files.deleteIfExists(control.resolve("release"));
		final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		return new ProcessBuilder(java, "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
			"-cp", ChildJava.classpath(),
			"-Ddg.reader.base=" + base,
			"-Ddg.reader.mode=" + mode,
			"-Ddg.reader.barrier=" + point,
			"-Ddg.reader.recordingId=" + recordingId,
			"-Ddg.reader.controlChannel=" + controlChannel,
			"-Ddg.reader.controlResponseChannel=" + controlResponseChannel,
			"-Ddg.reader.liveChannel=" + liveChannel,
			"-Ddg.reader.replayChannel=" + replayChannel,
			"-Ddg.reader.aeronDirectory=" + aeronDirectory,
			"-Ddg.reader.sharedDriver=true",
			ReaderCrashChildMain.class.getName())
			.redirectOutput(control.resolve(mode + "-stdout.log").toFile())
			.redirectError(control.resolve(mode + "-stderr.log").toFile())
			.start();
	}

	private static byte[] payload(final int sequence)
	{
		final byte[] result = new byte[64];
		final byte[] value = ("reader-crash:" + sequence).getBytes(StandardCharsets.UTF_8);
		for (int i = 0; i < result.length; i++) result[i] = value[i % value.length];
		return result;
	}

	private static long awaitRecordingId(final AeronArchiveReplicationPublisher publisher, final long timeout)
		throws InterruptedException
	{
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
		while (System.nanoTime() < deadline)
		{
			final long recordingId = publisher.recordingId();
			if (recordingId >= 0) return recordingId;
			Thread.sleep(10L);
		}
		throw new AssertionError("recording id not available");
	}

	private static void awaitFile(final Path path, final Process process, final long timeout)
		throws IOException, InterruptedException
	{
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
		while (!Files.exists(path) && System.nanoTime() < deadline)
		{
			if (!process.isAlive())
			{
				final Path control = path.getParent();
				throw new AssertionError("child exited before " + path + "\n" +
					readIfExists(control.resolve("phase1-stderr.log")) + "\n" +
					readIfExists(control.resolve("phase1-stdout.log")) + "\nclasspath=" +
					System.getProperty("java.class.path") + "\nmodulepath=" +
					System.getProperty("jdk.module.path"));
			}
			Thread.sleep(10L);
		}
		assertTrue(Files.exists(path), "timed out waiting for " + path);
	}

	private static String readIfExists(final Path path)
	{
		try { return Files.exists(path) ? Files.readString(path) : "<missing " + path + ">"; }
		catch (final IOException failure) { return failure.toString(); }
	}

	private static int freePort() throws IOException
	{
		try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
	}

	private static void deleteTree(final Path root) throws IOException
	{
		if (!Files.exists(root)) return;
		IOException failure = null;
		try (var paths = Files.walk(root))
		{
			for (final Path path : paths.sorted(Comparator.reverseOrder()).toList())
			{
				try { Files.deleteIfExists(path); }
				catch (final IOException deleteFailure)
				{
					if (failure == null) failure = deleteFailure;
					else failure.addSuppressed(deleteFailure);
				}
			}
		}
		if (failure != null) throw failure;
	}

}
