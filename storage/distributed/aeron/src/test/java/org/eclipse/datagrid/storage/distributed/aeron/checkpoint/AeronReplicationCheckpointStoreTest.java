package org.eclipse.datagrid.storage.distributed.aeron.checkpoint;

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

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies that restart records survive only as complete, checksummed files. */
class AeronReplicationCheckpointStoreTest
{
	/** Verifies round-tripping of the fixed recovery record. */
	@Test
	void roundTripsFixedRecoveryRecord() throws Exception
	{
		final Path path = Files.createTempFile("datagrid-checkpoint", ".bin");
		final AeronReplicationCheckpoint expected = checkpoint();
		AeronReplicationCheckpointStore.write(path, expected);
		assertEquals(AeronReplicationCheckpoint.ENCODED_BYTES, Files.size(path));
		assertEquals(expected, AeronReplicationCheckpointStore.read(path));
		Files.deleteIfExists(path);
	}

	/** Verifies rejection of torn and corrupt records. */
	@Test
	void rejectsTornAndCorruptRecords() throws Exception
	{
		final Path path = Files.createTempFile("datagrid-checkpoint", ".bin");
		AeronReplicationCheckpointStore.write(path, checkpoint());
		final byte[] bytes = Files.readAllBytes(path);
		Files.write(path, java.util.Arrays.copyOf(bytes, bytes.length - 1));
		assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
		Files.write(path, bytes);
		bytes[20] ^= 1;
		Files.write(path, bytes);
		assertThrows(java.io.IOException.class, () -> AeronReplicationCheckpointStore.read(path));
		Files.deleteIfExists(path);
	}

	/** Verifies atomically replaces existing checkpoint and creates parent. */
	@Test
	void atomicallyReplacesExistingCheckpointAndCreatesParent() throws Exception
	{
		final Path directory = Files.createTempDirectory("datagrid-checkpoint-parent");
		final Path path = directory.resolve("nested/checkpoint.bin");
		final AeronReplicationCheckpoint first = checkpoint();
		AeronReplicationCheckpointStore.write(path, first);
		final AeronReplicationCheckpoint second = new AeronReplicationCheckpoint(
			first.recordType(), first.durabilityMode(), AeronReplicationCheckpoint.State.COMMITTED,
			first.clusterId(), first.nodeId(), first.storeGeneration(), first.recordingId(),
			first.writerEpoch(), first.transactionSequence() + 1, first.recordingPosition() + 10,
			first.dataLength(), first.dataChunkCount(), 7);
		AeronReplicationCheckpointStore.write(path, second);
		assertEquals(second, AeronReplicationCheckpointStore.read(path));
		try (var paths = Files.walk(directory))
		{
			paths.sorted(java.util.Comparator.reverseOrder()).forEach(p ->
			{
				try { Files.deleteIfExists(p); } catch (final Exception ignored) { }
			});
		}
	}

	/** Verifies rejection of invalid checkpoint fields at construction. */
	@Test
	void rejectsInvalidCheckpointFieldsAtConstruction()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationCheckpoint(
			AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
			AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
			AeronReplicationCheckpoint.State.PREPARING,
			UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), -2, 0, -1, -1, 0, 0, 0));
	}

	private static AeronReplicationCheckpoint checkpoint()
	{
		return new AeronReplicationCheckpoint(
			AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
			AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
			AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
			UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 42, 7, 13, 4096, 12, 1, 99
		);
	}
}
