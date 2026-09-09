package org.eclipse.datagrid.storage.distributed.aeron.writer;

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

import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.crashtest.CrashBarrier;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the writer's terminal-state rules at injected crash boundaries. */
class AeronCrashBoundaryTest
{
	private final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
		.termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).offerTimeoutNanos(5_000_000L).build();

	@AfterEach
	void clearHooks()
	{
		AeronReplicationPublisher.clearCrashHook();
		AeronReplicationWriteCoordinator.clearCrashHook();
		AeronStorageBinaryTargetDistributing.clearCrashHook();
		try
		{
			final var method = Class.forName("org.eclipse.datagrid.storage.distributed.types.AtomicFileStore")
				.getDeclaredMethod("clearTestHook");
			method.setAccessible(true);
			method.invoke(null);
		}
		catch (final ReflectiveOperationException failure)
		{
			throw new AssertionError("cannot clear AtomicFileStore crash hook", failure);
		}
	}

	/** Verifies prepared tail failure always publishes abort and fails closed. */
	@Test
	void preparedTailFailureAlwaysPublishesAbortAndFailsClosed()
	{
		final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
		final AeronReplicationPublisher publisher = publisher(kinds);
		try (CrashBarrier barrier = new CrashBarrier(
			org.eclipse.datagrid.storage.distributed.aeron.crashtest.CrashPoint.AFTER_DATA_CHUNKS,
			true, 1_000_000_000L))
		{
			AeronReplicationPublisher.setCrashHook(barrier::reached);
			assertThrows(CrashBarrier.SimulatedCrash.class, () -> publisher.prepareTransaction(
				null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {1, 2, 3})}));
		}
		assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.ABORT), kinds);
		assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
			null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {4})}));
		publisher.close();
	}

	/** Verifies ambiguous commit fails closed without publishing a second terminal marker. */
	@Test
	void ambiguousCommitFailsClosedWithoutPublishingASecondTerminalMarker()
	{
		final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
		final AeronReplicationPublisher publisher = publisher(kinds);
		try (CrashBarrier barrier = new CrashBarrier(
			org.eclipse.datagrid.storage.distributed.aeron.crashtest.CrashPoint.AFTER_COMMIT_OFFER,
			true, 1_000_000_000L))
		{
			AeronReplicationPublisher.setCrashHook(barrier::reached);
			assertThrows(CrashBarrier.SimulatedCrash.class, () -> publisher.publishTransaction(
				null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {9})}));
		}
		assertEquals(List.of(AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.COMMIT), kinds);
		assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
			null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {8})}));
		publisher.close();
	}

	/** Verifies enqueue fence is created before local write. */
	@Test
	void enqueueFenceIsCreatedBeforeLocalWrite()
	{
		final AtomicInteger localWrites = new AtomicInteger();
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> { offers.incrementAndGet(); return length; },
			configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
		final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
			publisher, ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE,
			(state, sequence, length, chunks, crc, position) -> { });
		final PersistenceTarget<Binary> local = new PersistenceTarget<>()
		{
			@Override public void write(final Binary ignored) { localWrites.incrementAndGet(); }
			@Override public boolean isWritable() { return true; }
		};
		try
		{
			try (CrashBarrier barrier = new CrashBarrier(
				org.eclipse.datagrid.storage.distributed.aeron.crashtest.CrashPoint.AFTER_ENQUEUE_BEFORE_PREPARE,
				true, 1_000_000_000L))
			{
				AeronStorageBinaryTargetDistributing.setCrashHook(barrier::reached);
				assertThrows(CrashBarrier.SimulatedCrash.class, () ->
					new AeronStorageBinaryTargetDistributing(local, coordinator).write(
						ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] {7}))));
			}
			assertEquals(1, localWrites.get());
			assertEquals(0, offers.get());
		}
		finally
		{
			coordinator.dispose();
		}
	}

	/** Verifies recorded commit before checkpoint is converted to uncertainty. */
	@Test
	void recordedCommitBeforeCheckpointIsConvertedToUncertainty()
	{
		final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
		final AeronReplicationPublisher publisher = publisher(new ArrayList<>());
		final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
			publisher, ReplicationDurabilityMode.ARCHIVE_FIRST,
			(state, sequence, length, chunks, crc, position) -> states.add(state));
		AeronReplicationWriteCoordinator.setCrashHook((name, ignored) ->
			{
				if ("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT".equals(name))
				{
					throw new CrashBarrier.SimulatedCrash(
						org.eclipse.datagrid.storage.distributed.aeron.crashtest.CrashPoint.AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT,
						ignored);
				}
			});
		try
		{
			assertThrows(CrashBarrier.SimulatedCrash.class, () -> coordinator.distributeData(
				ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] {5}))));
			assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
				AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
		}
		finally
		{
			coordinator.dispose();
		}
	}

	private AeronReplicationPublisher publisher(final List<AeronReplicationEnvelope.Kind> kinds)
	{
		return new AeronReplicationPublisher(
			(buffer, offset, length) ->
			{
				kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
				return length;
			}, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
	}
}
