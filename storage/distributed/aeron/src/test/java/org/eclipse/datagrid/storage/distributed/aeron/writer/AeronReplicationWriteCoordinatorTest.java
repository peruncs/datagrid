package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.Publication;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

class AeronReplicationWriteCoordinatorTest
{
	@Test
	void reportsPrepareCommitAndAbortCheckpointStates()
	{
		final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
		final UUID cluster = UUID.randomUUID();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> length, 1024,
			AeronReplicationConfiguration.builder().chunkSize(256).maxTransactionBytes(512).build(), cluster, 1, 0);
		final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
			publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
		final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] { 1 })));
		coordinator.commit(prepared);
		final var second = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] { 2 })));
		coordinator.abort(second);
		assertEquals(List.of(
			AeronReplicationCheckpoint.State.PREPARING,
			AeronReplicationCheckpoint.State.COMMITTED,
			AeronReplicationCheckpoint.State.PREPARING,
			AeronReplicationCheckpoint.State.REJECTED), states);
		coordinator.dispose();
	}

	@Test
	void enqueueThenArchiveDoesNotPublishBeforeLocalEnqueue()
	{
		final List<String> events = new ArrayList<>();
		final UUID cluster = UUID.randomUUID();
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
			.durabilityMode(org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
			.build();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) ->
			{
				events.add("archive");
				return length;
			}, configuration.maxMessageLength(), configuration, cluster, 1, 0);
		final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
			publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> events.add(state.name()));
		final PersistenceTarget<Binary> local = new PersistenceTarget<>()
		{
			@Override public void write(final Binary data) { events.add("local"); }
			@Override public boolean isWritable() { return true; }
		};
		new AeronStorageBinaryTargetDistributing(local, coordinator)
			.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] { 7 })));
		assertEquals(List.of("local", "archive", "PREPARING", "ENQUEUED", "archive", "COMMITTED"), events);
		coordinator.dispose();
	}

	@Test
	void enqueuePrepareFailureRecordsTheFailedTransactionDimensions()
	{
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512)
			.durabilityMode(org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
			.build();
		final AtomicInteger uncertainLength = new AtomicInteger(-1);
		final AtomicInteger uncertainChunks = new AtomicInteger(-1);
		final AtomicInteger uncertainCrc = new AtomicInteger(-1);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> offers.getAndIncrement() == 0 ? length : Publication.CLOSED,
			configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
		final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
			publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) ->
			{
				if (state == AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN)
				{
					uncertainLength.set(length);
					uncertainChunks.set(chunks);
					uncertainCrc.set(crc);
				}
			});
		final PersistenceTarget<Binary> local = new PersistenceTarget<>()
		{
			@Override public void write(final Binary data) { }
			@Override public boolean isWritable() { return true; }
		};
		coordinator.distributeTypeDictionary("type");
		assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(local, coordinator)
			.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] {1, 2, 3}))));
		assertEquals(3, uncertainLength.get());
		assertEquals(1, uncertainChunks.get());
		assertEquals(AeronReplicationEnvelope.crc32c(new byte[] {1, 2, 3}), uncertainCrc.get());
		coordinator.dispose();
	}

	@Test
	void localRejectionEmitsAbortAndNeverCommits()
	{
		final List<String> events = new ArrayList<>();
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> { events.add("archive"); return length; },
			configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
		final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
			publisher, (state, sequence, length, chunks, crc, position) -> events.add(state.name()));
		final PersistenceTarget<Binary> failing = new PersistenceTarget<>()
		{
			public void write(final Binary data) { throw new IllegalStateException("local failure"); }
			public boolean isWritable() { return true; }
		};
		assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(failing, coordinator)
			.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] { 1 }))));
		assertEquals(List.of("archive", "PREPARING", "archive", "REJECTED"), events);
		coordinator.dispose();
	}

	@Test
	void commitFailureIsMarkedUncertainAndCoordinatorCannotPretendSuccess()
	{
		final List<AeronReplicationCheckpoint.State> states = new ArrayList<>();
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
			configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
		final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
			publisher, (state, sequence, length, chunks, crc, position) -> states.add(state));
		final var prepared = coordinator.prepare(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] {1})));
		assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING), states);
		assertThrows(IllegalStateException.class, () -> coordinator.commit(prepared));
		coordinator.markCommittingUncertain(prepared);
		assertEquals(List.of(AeronReplicationCheckpoint.State.PREPARING,
			AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN), states);
		coordinator.dispose();
	}

	@Test
	void localDurableFirstIsRejectedUntilStoreExposesDurabilityCallback()
	{
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.durabilityMode(org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode.LOCAL_DURABLE_FIRST)
			.build();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> length, configuration.maxMessageLength(), configuration,
			UUID.randomUUID(), 1, 0);
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationWriteCoordinator(
			publisher, configuration.durabilityMode(), (state, sequence, length, chunks, crc, position) -> { }));
		publisher.close();
	}

	@Test
	void persistenceTargetConsumesDictionaryFromSharedDistributor()
	{
		final List<AeronReplicationEnvelope.Kind> kinds = new ArrayList<>();
		final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
			.chunkSize(256).maxTransactionBytes(1024).build();
		final var publisher = new AeronReplicationPublisher((buffer, offset, length) -> {
			kinds.add(AeronReplicationEnvelope.decode(buffer, offset, length).kind());
			return length;
		}, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
		final var coordinator = new AeronReplicationWriteCoordinator(publisher);
		final var source = new org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor()
		{
			private String dictionary = "type";
			public void distributeData(final Binary ignored) { }
			public void distributeTypeDictionary(final String ignored) { }
			public String consumeTypeDictionary() { final String value = this.dictionary; this.dictionary = null; return value; }
			public void dispose() { }
		};
		final PersistenceTarget<Binary> local = new PersistenceTarget<>()
		{
			public void write(final Binary ignored) { }
			public boolean isWritable() { return true; }
		};
		new AeronStorageBinaryTargetDistributing(local, coordinator, source, ignored -> { }, () -> true)
			.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] {1})));
		assertEquals(List.of(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY,
			AeronReplicationEnvelope.Kind.STORE_BINARY, AeronReplicationEnvelope.Kind.COMMIT), kinds);
		coordinator.dispose();
	}

	@Test
	void ignoredDistributionWritesLocallyWithoutOfferingAeronFrames()
	{
		final AtomicInteger offers = new AtomicInteger();
		final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
			.chunkSize(256).maxTransactionBytes(1024).build();
		final var publisher = new AeronReplicationPublisher((buffer, offset, length) -> {
			offers.incrementAndGet();
			return length;
		}, configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
		final var coordinator = new AeronReplicationWriteCoordinator(publisher);
		final var localWrites = new AtomicInteger();
		final PersistenceTarget<Binary> local = new PersistenceTarget<>()
		{
			public void write(final Binary ignored) { localWrites.incrementAndGet(); }
			public boolean isWritable() { return true; }
		};
		new AeronStorageBinaryTargetDistributing(local, coordinator, null, ignored -> { }, () -> false)
			.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] {1})));
		assertEquals(1, localWrites.get());
		assertEquals(0, offers.get());
		coordinator.dispose();
	}

	@Test
	void committedSequenceCallbackDoesNotAdvanceOnUncertainCommit()
	{
		final AtomicInteger offers = new AtomicInteger();
		final AtomicLong committed = new AtomicLong(-1);
		final var configuration = AeronReplicationConfiguration.builder().termLength(64 * 1024)
			.chunkSize(256).maxTransactionBytes(1024).build();
		final var publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
			configuration.maxMessageLength(), configuration, UUID.randomUUID(), 1, 0);
		final var coordinator = new AeronReplicationWriteCoordinator(publisher);
		final PersistenceTarget<Binary> local = new PersistenceTarget<>()
		{
			public void write(final Binary ignored) { }
			public boolean isWritable() { return true; }
		};
		assertThrows(IllegalStateException.class, () -> new AeronStorageBinaryTargetDistributing(
			local, coordinator, null, committed::set, () -> true).write(
				ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] {1}))));
		assertEquals(-1, committed.get(), "uncertain commit must not advance the local index");
		coordinator.dispose();
	}
}
