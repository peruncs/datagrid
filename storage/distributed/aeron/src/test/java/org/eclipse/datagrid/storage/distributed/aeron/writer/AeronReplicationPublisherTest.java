package org.eclipse.datagrid.storage.distributed.aeron.writer;

import io.aeron.Publication;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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

class AeronReplicationPublisherTest
{
	private static final UUID CLUSTER = UUID.randomUUID();

	@Test
	void retriesBackPressureAndPreservesSourcePosition()
	{
		final AtomicInteger calls = new AtomicInteger();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> calls.getAndIncrement() < 2
				? Publication.BACK_PRESSURED
				: calls.get(),
			configuration.maxMessageLength(), configuration, CLUSTER, 1, 0
		);
		final ByteBuffer source = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
		final int position = source.position();
		publisher.publishTransaction(null, new ByteBuffer[] { source });
		assertTrue(calls.get() >= 4, "data and commit must both be offered after back pressure");
		assertTrue(source.position() == position, "publisher must not consume caller buffers");
	}

	@Test
	void timesOutAndFailsClosedAfterPersistentBackPressure()
	{
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> Publication.BACK_PRESSURED,
			configuration(1_000_000L).maxMessageLength(), configuration(1_000_000L), CLUSTER, 1, 0
		);
		assertThrows(IllegalStateException.class,
			() -> publisher.publishTransaction(null, new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 1 }) }));
		assertThrows(IllegalStateException.class,
			() -> publisher.publishTransaction(null, new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 2 }) }));
	}

	@Test
	void closedAndMaxPositionStatusesAreFatal()
	{
		for (final long status : new long[] { Publication.CLOSED, Publication.MAX_POSITION_EXCEEDED })
		{
			final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
				(buffer, offset, length) -> status,
				configuration(50_000_000L).maxMessageLength(), configuration(50_000_000L), CLUSTER, 1, 0
			);
			assertThrows(IllegalStateException.class,
				() -> publisher.publishTransaction(null, new ByteBuffer[] { ByteBuffer.wrap(new byte[] { 1 }) }));
		}
	}

	@Test
	void rejectsMessageLimitLargerThanConfiguration()
	{
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024).build();
		assertThrows(IllegalArgumentException.class, () -> new AeronReplicationPublisher(
			(buffer, offset, length) -> length, configuration.maxMessageLength() + 1,
			configuration, CLUSTER, 1, 0));
	}

	@Test
	void abortFailureAlsoFailsClosed()
	{
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> Publication.CLOSED,
			configuration(50_000_000L).maxMessageLength(), configuration(50_000_000L), CLUSTER, 1, 0
		);
		assertThrows(IllegalStateException.class, () -> publisher.publishAbort(0, 1, 1));
		assertThrows(IllegalStateException.class, () -> publisher.publishAbort(1, 1, 1));
	}

	@Test
	void emitsDictionaryChunksBeforeDataAndCommit()
	{
		final List<byte[]> messages = new ArrayList<>();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) ->
			{
				final byte[] copy = new byte[length];
				buffer.getBytes(offset, copy);
				messages.add(copy);
				return messages.size();
			}, configuration.maxMessageLength(), configuration, CLUSTER, 4, 10
		);
		final byte[] data = {1, 2, 3};
		final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
			new byte[] {9, 8}, new ByteBuffer[] {ByteBuffer.wrap(data)});
		assertEquals(10, prepared.sequence());
		assertEquals(3, prepared.dataLength());
		assertEquals(2, messages.size());
		assertEquals(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY,
			AeronReplicationEnvelope.decode(new org.agrona.concurrent.UnsafeBuffer(messages.get(0)), 0,
				messages.get(0).length).kind());
		assertEquals(AeronReplicationEnvelope.Kind.STORE_BINARY,
			AeronReplicationEnvelope.decode(new org.agrona.concurrent.UnsafeBuffer(messages.get(1)), 0,
				messages.get(1).length).kind());
		publisher.commit(prepared);
		assertEquals(3, messages.size());
		final AeronReplicationEnvelope.Envelope commit = AeronReplicationEnvelope.decode(
			new org.agrona.concurrent.UnsafeBuffer(messages.get(2)), 0, messages.get(2).length);
		assertEquals(AeronReplicationEnvelope.Kind.COMMIT, commit.kind());
		assertEquals(AeronReplicationEnvelope.crc32c(data), commit.commitCrc32c());
		assertArrayEquals(data, preparedData(messages.get(1)));
	}

	@Test
	void publishesAcrossMultipleSourceBuffersWithoutChangingTheirPositions()
	{
		final List<byte[]> messages = new ArrayList<>();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) ->
			{
				final byte[] copy = new byte[length];
				buffer.getBytes(offset, copy);
				messages.add(copy);
				return messages.size();
			}, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		final ByteBuffer first = ByteBuffer.wrap(new byte[] {1, 2});
		final ByteBuffer second = ByteBuffer.wrap(new byte[] {3, 4});
		final int firstPosition = first.position();
		final int secondPosition = second.position();
		publisher.publishTransaction(null, new ByteBuffer[] {first, second});
		assertEquals(firstPosition, first.position());
		assertEquals(secondPosition, second.position());
		assertArrayEquals(new byte[] {1, 2, 3, 4}, preparedData(messages.get(0)));
	}

	@Test
	void closingAnAbandonedPreparedTransactionPublishesAnAbortMarker()
	{
		final List<byte[]> messages = new ArrayList<>();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) ->
			{
				final byte[] copy = new byte[length];
				buffer.getBytes(offset, copy);
				messages.add(copy);
				return messages.size();
			}, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		try (AeronReplicationPublisher.PreparedTransaction ignored = publisher.prepareTransaction(
			null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {7})}))
		{
		}
		assertEquals(AeronReplicationEnvelope.Kind.ABORT,
			AeronReplicationEnvelope.decode(new org.agrona.concurrent.UnsafeBuffer(messages.get(1)), 0,
				messages.get(1).length).kind());
	}

	@Test
	void publishesAnExplicitEmptyStoreChunk()
	{
		final List<byte[]> messages = new ArrayList<>();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) ->
			{
				final byte[] copy = new byte[length];
				buffer.getBytes(offset, copy);
				messages.add(copy);
				return messages.size();
			}, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		publisher.publishTransaction(null, new ByteBuffer[] {ByteBuffer.allocate(0)});
		assertEquals(2, messages.size());
		final AeronReplicationEnvelope.Envelope data = AeronReplicationEnvelope.decode(
			new org.agrona.concurrent.UnsafeBuffer(messages.get(0)), 0, messages.get(0).length);
		assertEquals(AeronReplicationEnvelope.Kind.STORE_BINARY, data.kind());
		assertEquals(0, data.payloadLength());
		assertEquals(0, data.payload().length);
	}

	@Test
	void rejectsTransactionLargerThanConfiguredLimitBeforeOffering()
	{
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(64 * 1024).chunkSize(256).maxTransactionBytes(512).build();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> { offers.incrementAndGet(); return 1; },
			configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		assertThrows(IllegalArgumentException.class, () -> publisher.prepareTransaction(
			new byte[300], new ByteBuffer[] {ByteBuffer.wrap(new byte[300])}));
		assertEquals(0, offers.get());
	}

	@Test
	void failedCommitLeavesPublisherFailedClosed()
	{
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> offers.incrementAndGet() == 1 ? 1 : Publication.CLOSED,
			configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
			null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {1})});
		assertThrows(IllegalStateException.class, () -> publisher.commit(prepared));
		assertThrows(IllegalStateException.class, () -> publisher.abort(prepared));
	}

	@Test
	void partialPrepareFailureIsTerminalAndCannotSkipTheReservedSequence()
	{
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
			configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		assertThrows(IllegalStateException.class, () -> publisher.prepareTransaction(
			new byte[] {9}, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {1})}));
		assertTrue(offers.get() >= 2, "the failure must occur after a partial publication");
		assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
			null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {2})}));
	}

	@Test
	void abortFailureFailsClosedAfterPreparedChunksWerePublished()
	{
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> offers.incrementAndGet() == 1 ? length : Publication.CLOSED,
			configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		final AeronReplicationPublisher.PreparedTransaction prepared = publisher.prepareTransaction(
			null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {1})});
		assertThrows(IllegalStateException.class, () -> publisher.abort(prepared));
		assertThrows(IllegalStateException.class, () -> publisher.commit(prepared));
	}

	@Test
	void closeIsIdempotentAndPreventsFurtherOffers()
	{
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> length, configuration.maxMessageLength(), configuration, CLUSTER, 1, 0);
		publisher.close();
		publisher.close();
		assertThrows(IllegalStateException.class, () -> publisher.publishTransaction(
			null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {1})}));
	}

	@Test
	void commitWaitsForAndReturnsArchiveRecordedPosition()
	{
		final AeronReplicationConfiguration configuration = configuration(50_000_000L);
		final AtomicInteger offers = new AtomicInteger();
		final AeronReplicationPublisher publisher = new AeronReplicationPublisher(
			(buffer, offset, length) -> offers.incrementAndGet(), configuration.maxMessageLength(), configuration,
			CLUSTER, 1, 0, offeredPosition -> offeredPosition + 100
		);
		assertEquals(102, publisher.publishTransaction(null, new ByteBuffer[] {ByteBuffer.wrap(new byte[] {1})}));
	}

	private static byte[] preparedData(final byte[] message)
	{
		final AeronReplicationEnvelope.Envelope envelope = AeronReplicationEnvelope.decode(
			new org.agrona.concurrent.UnsafeBuffer(message), 0, message.length);
		return envelope.payload();
	}

	private static AeronReplicationConfiguration configuration(final long timeout)
	{
		return AeronReplicationConfiguration.builder()
			.termLength(64 * 1024)
			.chunkSize(256)
			.maxTransactionBytes(1024)
			.offerTimeoutNanos(timeout)
			.build();
	}
}
