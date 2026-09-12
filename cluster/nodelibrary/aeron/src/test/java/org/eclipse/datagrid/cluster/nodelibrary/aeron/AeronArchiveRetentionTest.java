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

import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationCursor;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronAuthenticatedWatermark;
import org.junit.jupiter.api.Test;

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

	private static AeronArchiveRetention retention(final Runnable ensureWriter)
	{
		return retention(ensureWriter, null);
	}

	private static AeronArchiveRetention retention(final Runnable ensureWriter, final Path state)
	{
		return new AeronArchiveRetention(SECRET, Set.of(READER), ensureWriter, () -> null, () -> 17,
			() -> new AeronWriterBoundary(4, 17, 8_192), CLUSTER, GENERATION, 1, () -> 1_048_576, () -> 8_388_608,
			state);
	}
}
