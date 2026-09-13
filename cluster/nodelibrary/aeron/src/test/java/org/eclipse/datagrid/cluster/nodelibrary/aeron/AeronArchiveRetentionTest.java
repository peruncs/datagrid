package org.eclipse.datagrid.cluster.nodelibrary.aeron;

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

import io.aeron.archive.client.ArchiveException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationCursor;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationLogRetention;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronAuthenticatedWatermark;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies retention authentication before any Archive operation is attempted. */
class AeronArchiveRetentionTest
{
	private static final UUID CLUSTER = UUID.randomUUID();
	private static final UUID GENERATION = UUID.randomUUID();
	private static final UUID READER = UUID.randomUUID();
	private static final byte[] SECRET = "retention-test-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8);

	@Test
	void retentionIsUnsupportedUntilTheConfiguredReaderHasReported()
	{
		final AeronArchiveRetention retention = retention(() -> { throw new AssertionError("writer must not start"); });
		assertFalse(retention.isSupported());
		retention.close();
	}

	@Test
	void malformedWatermarkIsRejectedBeforeStartingTheWriter()
	{
		final AtomicBoolean started = new AtomicBoolean();
		final AeronArchiveRetention retention = retention(() -> started.set(true));
		assertThrows(IllegalArgumentException.class, () -> retention.recordReaderWatermark(
			new ReplicationCursor("aeron", GENERATION, 1, new byte[] { 1, 2, 3 })));
		assertFalse(started.get(), "authentication must precede lazy writer startup");
		retention.close();
	}

	@Test
	void authenticatedConfiguredWatermarkCompletesTheQuorum()
	{
		final AeronArchiveRetention retention = retention(() -> { });
		final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
			READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET);
		retention.recordReaderWatermark(new ReplicationCursor("aeron", GENERATION, 4, watermark.encode()));
		assertTrue(retention.isSupported());
		retention.close();
	}

	@Test
	void decodedControlWatermarkCompletesTheQuorumWithoutCursorReencoding()
	{
		final AeronArchiveRetention retention = retention(() -> { });
		retention.recordReaderWatermark(AeronAuthenticatedWatermark.sign(
			READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET));
		assertTrue(retention.isSupported());
		retention.close();
	}

	@Test
	void writerStartupFailureDoesNotAdvanceTheQuorum()
	{
		final AeronArchiveRetention retention = retention(
			() -> { throw new IllegalStateException("writer unavailable"); });
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> retention.recordReaderWatermark(cursor(READER)));
		assertEquals("writer unavailable", failure.getMessage());
		assertFalse(retention.isSupported(),
			"a watermark rejected before writer validation must not complete the quorum");
		retention.close();
	}

	@Test
	void ordinaryBackupCursorCanRequestDeletionAfterAuthenticatedQuorum()
	{
		final AeronArchiveRetention retention = retention(() -> { });
		final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
			READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET);
		retention.recordReaderWatermark(new ReplicationCursor("aeron", GENERATION, 4, watermark.encode()));
		final byte[] ordinaryPosition = new AeronReplicationCursor(
			CLUSTER, UUID.randomUUID(), GENERATION, 1, 17, 4_096, 4).encode();
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> retention.deleteThrough(new ReplicationCursor("aeron", GENERATION, 4, ordinaryPosition)));
		assertEquals("Aeron Archive is not running", failure.getMessage());
		retention.close();
	}

	@Test
	void activeReplayDefersSegmentDeletionWithoutLosingTheQuorum()
	{
		final long acknowledgedPosition = 9L * 1_024 * 1_024;
		final AeronArchiveRetention retention = retentionWithPurger(ignored -> { throw new ArchiveException(
			"invalid detach: replay in progress - state=ACTIVE", ArchiveException.GENERIC); });
		retention.recordReaderWatermark(AeronAuthenticatedWatermark.sign(
			READER, CLUSTER, GENERATION, 1, 17, 4, acknowledgedPosition, SECRET));
		final ReplicationLogRetention.MaintenanceResult result =
			retention.deleteThrough(deletionCursor(acknowledgedPosition));
		assertEquals(ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, result.status());
		assertTrue(retention.isSupported());
		retention.close();
	}

	@Test
	void unrelatedArchiveFailureIsNotMisclassifiedAsAnActiveReplay()
	{
		final long acknowledgedPosition = 9L * 1_024 * 1_024;
		final AeronArchiveRetention retention = retentionWithPurger(ignored -> { throw new ArchiveException(
			"unrelated Archive failure", ArchiveException.GENERIC); });
		retention.recordReaderWatermark(AeronAuthenticatedWatermark.sign(
			READER, CLUSTER, GENERATION, 1, 17, 4, acknowledgedPosition, SECRET));
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> retention.deleteThrough(deletionCursor(acknowledgedPosition)));
		assertInstanceOf(ArchiveException.class, failure.getCause());
		retention.close();
	}

	@Test
	void watermarkAheadOfDurableWriterBoundaryIsRejected()
	{
		final AeronArchiveRetention retention = retention(() -> { });
		final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
			READER, CLUSTER, GENERATION, 1, 17, 5, 4_096, SECRET);
		assertThrows(IllegalStateException.class, () -> retention.recordReaderWatermark(
			new ReplicationCursor("aeron", GENERATION, 5, watermark.encode())));
		assertFalse(retention.isSupported(), "an acknowledgement beyond the writer boundary must not complete quorum");
		retention.close();
	}

	@Test
	void authenticatedReaderProgressSurvivesControllerRestart() throws Exception
	{
		final Path state = Files.createTempFile("aeron-retention-", ".state");
		try
		{
			Files.deleteIfExists(state);
			final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
				READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET);
			final ReplicationCursor cursor = new ReplicationCursor("aeron", GENERATION, 4, watermark.encode());
			final AeronArchiveRetention first = retention(() -> { }, state);
			first.recordReaderWatermark(cursor);
			first.close();
			final AeronArchiveRetention restarted = retention(() -> { }, state);
			assertTrue(restarted.isSupported());
			restarted.close();
		}
		finally
		{
			Files.deleteIfExists(state);
		}
	}

	@Test
	void rejectsDevelopmentRetentionStateFormats() throws Exception
	{
		final Path state = Files.createTempFile("aeron-retention-obsolete-", ".state");
		try
		{
			Files.write(state, ByteBuffer.allocate(Integer.BYTES * 3)
				.putInt(2).putInt(0).putInt(0).array());
			final IllegalStateException failure = assertThrows(IllegalStateException.class,
				() -> retention(() -> { }, state));
			assertInstanceOf(java.io.IOException.class, failure.getCause());
			assertEquals("unsupported retention state version", failure.getCause().getMessage());
		}
		finally
		{
			Files.deleteIfExists(state);
		}
	}

	@Test
	void retiredReaderIsPersistentlyRemovedFromTheQuorum() throws Exception
	{
		final UUID secondReader = UUID.randomUUID();
		final Path state = Files.createTempFile("aeron-retention-retired-", ".state");
		try
		{
			Files.deleteIfExists(state);
			final AeronArchiveRetention first = new AeronArchiveRetention(SECRET, Set.of(READER, secondReader),
				() -> { }, unavailableRecording(), () -> 17, () -> new AeronWriterBoundary(4, 17, 8_192),
				ignored -> 0L,
				CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, state);
			first.retireReader(secondReader);
			first.recordReaderWatermark(new ReplicationCursor("aeron", GENERATION, 4,
				AeronAuthenticatedWatermark.sign(READER, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET).encode()));
			assertTrue(first.isSupported());
			first.close();

			final AeronArchiveRetention restarted = new AeronArchiveRetention(SECRET, Set.of(READER, secondReader),
				() -> { }, unavailableRecording(), () -> 17, () -> new AeronWriterBoundary(4, 17, 8_192),
				ignored -> 0L,
				CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, state);
			assertTrue(restarted.isSupported());
			assertThrows(SecurityException.class, () -> restarted.recordReaderWatermark(new ReplicationCursor(
				"aeron", GENERATION, 4, AeronAuthenticatedWatermark.sign(
					secondReader, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET).encode())));
			restarted.close();
		}
		finally
		{
			Files.deleteIfExists(state);
		}
	}

	@Test
	void reportedReaderCanBeRetiredAndStateReloaded() throws Exception
	{
		final UUID retiredReader = UUID.randomUUID();
		final Path state = Files.createTempFile("aeron-retention-reported-retired-", ".state");
		try
		{
			Files.deleteIfExists(state);
			final Set<UUID> readers = Set.of(READER, retiredReader);
			final AeronArchiveRetention first = retention(readers, state);
			first.recordReaderWatermark(cursor(retiredReader));
			first.retireReader(retiredReader);
			first.recordReaderWatermark(cursor(READER));
			assertTrue(first.isSupported());
			first.close();

			final AeronArchiveRetention restarted = retention(readers, state);
			assertTrue(restarted.isSupported());
			assertThrows(SecurityException.class,
				() -> restarted.recordReaderWatermark(cursor(retiredReader)));
			restarted.close();
		}
		finally
		{
			Files.deleteIfExists(state);
		}
	}

	@Test
	void watermarkPersistenceFailureRollsBackTheQuorum() throws Exception
	{
		final Path directory = Files.createTempDirectory("aeron-retention-rollback-");
		final Path state = directory.resolve("state");
		final AeronArchiveRetention retention = retention(() -> { }, state);
		try
		{
			Files.createDirectory(state);
			assertThrows(IllegalStateException.class,
				() -> retention.recordReaderWatermark(cursor(READER)));
			assertFalse(retention.isSupported(),
				"an acknowledgement that was not persisted must not complete the quorum");
			Files.delete(state);
			retention.recordReaderWatermark(cursor(READER));
			assertTrue(retention.isSupported());
		}
		finally
		{
			retention.close();
			Files.deleteIfExists(state);
			Files.deleteIfExists(directory);
		}
	}

	@Test
	void retirementPersistenceFailureReinstatesTheReader() throws Exception
	{
		final UUID secondReader = UUID.randomUUID();
		final Path directory = Files.createTempDirectory("aeron-retirement-rollback-");
		final Path state = directory.resolve("state");
		final AeronArchiveRetention retention = retention(Set.of(READER, secondReader), state);
		try
		{
			Files.createDirectory(state);
			assertThrows(IllegalStateException.class, () -> retention.retireReader(secondReader));
			Files.delete(state);
			retention.recordReaderWatermark(cursor(READER));
			assertFalse(retention.isSupported(),
				"failed retirement persistence must leave the reader in the quorum");
			retention.recordReaderWatermark(cursor(secondReader));
			assertTrue(retention.isSupported());
		}
		finally
		{
			retention.close();
			Files.deleteIfExists(state);
			Files.deleteIfExists(directory);
		}
	}

	private static AeronArchiveRetention retention(final Runnable ensureWriter)
	{
		return retention(ensureWriter, null);
	}

	private static AeronArchiveRetention retention(final Runnable ensureWriter, final Path state)
	{
		return retention(Set.of(READER), state, ensureWriter);
	}

	private static AeronArchiveRetention retention(final Set<UUID> readers, final Path state)
	{
		return retention(readers, state, () -> { });
	}

	private static AeronArchiveRetention retention(final Set<UUID> readers, final Path state,
		final Runnable ensureWriter)
	{
		return new AeronArchiveRetention(SECRET, readers, ensureWriter, unavailableRecording(), () -> 17,
			() -> new AeronWriterBoundary(4, 17, 8_192), ignored -> 0L,
			CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608,
			() -> true, state);
	}

	private static AeronArchiveRetention.RecordingPositions unavailableRecording()
	{
		return new AeronArchiveRetention.RecordingPositions(
			ignored -> { throw new IllegalStateException("Aeron Archive is not running"); },
			ignored -> { throw new IllegalStateException("Aeron Archive is not running"); },
			ignored -> { throw new IllegalStateException("Aeron Archive is not running"); });
	}

	private static AeronArchiveRetention retentionWithPurger(final java.util.function.LongUnaryOperator purger)
	{
		return new AeronArchiveRetention(SECRET, Set.of(READER), () -> { },
			new AeronArchiveRetention.RecordingPositions(
				ignored -> 0L, ignored -> 16L * 1_024 * 1_024, ignored -> -1L), () -> 17,
			() -> new AeronWriterBoundary(4, 17, 16L * 1_024 * 1_024), purger,
			CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608, () -> true, null);
	}

	private static ReplicationCursor deletionCursor(final long position)
	{
		return new ReplicationCursor("aeron", GENERATION, 4, new AeronReplicationCursor(
			CLUSTER, UUID.randomUUID(), GENERATION, 1, 17, position, 4).encode());
	}

	private static ReplicationCursor cursor(final UUID reader)
	{
		return new ReplicationCursor("aeron", GENERATION, 4, AeronAuthenticatedWatermark.sign(
			reader, CLUSTER, GENERATION, 1, 17, 4, 4_096, SECRET).encode());
	}
}
