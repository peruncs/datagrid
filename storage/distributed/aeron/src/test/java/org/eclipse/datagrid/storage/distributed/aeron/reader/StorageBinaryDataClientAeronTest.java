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

package org.eclipse.datagrid.storage.distributed.aeron.reader;

import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies live-reader ordering and commit-gated Store delivery. */
class StorageBinaryDataClientAeronTest
{
	private static final UUID CLUSTER = UUID.randomUUID();
	private static final long EPOCH = 17;

	/** Verifies delivery of dictionary and store payload only after commit. */
	@Test
	void deliversDictionaryAndStorePayloadOnlyAfterCommit()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		final byte[] data = new byte[] { 4, 3, 2, 1 };
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, 0, 0, 1, 0,
			"types".getBytes(), 5));
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, data.length));

		assertEquals(-1, assembler.lastResolvedSequence());
        assertNull(receiver.data);

		accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			data.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(data), new byte[0]
		));

		assertEquals("types", receiver.dictionary);
		assertArrayEquals(data, receiver.data);
		assertEquals(0, assembler.lastResolvedSequence());

		// A replayed commit is idempotently ignored.
		accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			data.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(data), new byte[0]
		));
		assertEquals(1, receiver.dataCalls);
	}

	/** Verifies rejection of gap and interleaving without delivering partial data. */
	@Test
	void rejectsGapAndInterleavingWithoutDeliveringPartialData()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		assertThrows(IllegalStateException.class, () -> accept(assembler,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 1, 0, 1, 0,
				new byte[] { 1 }, 1)));
		assertEquals(0, receiver.dataCalls);

		final TransactionAssembler second = assembler(receiver, 1024);
		accept(second, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 2, 0,
			new byte[] { 1 }, 2));
		assertThrows(IllegalStateException.class, () -> accept(second,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 1, 0, 1, 0,
				new byte[] { 2 }, 1)));
	}

	/** Verifies live reader fails closed when writer leaves an orphan tail. */
	@Test
	void liveReaderFailsClosedWhenWriterLeavesAnOrphanTail()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			new byte[] { 1 }, 1));

		final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> accept(assembler,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 1, 0, 1, 0,
				new byte[] { 2 }, 1)));

		assertEquals("replication sequence gap: expected 0, received 1", failure.getMessage());
		assertEquals(0, receiver.dataCalls);
		assertNotNull(assembler.failure());
	}

	/** Verifies rejection of sequence regression instead of silently skipping data. */
	@Test
	void rejectsSequenceRegressionInsteadOfSilentlySkippingData()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = new TransactionAssembler(
			AeronReplicationConfiguration.builder().termLength(64 * 1024).chunkSize(256)
				.maxTransactionBytes(1024).build(), CLUSTER, EPOCH, 5, receiver, () -> { });
		final byte[] data = { 1 };
		assertThrows(IllegalStateException.class, () -> accept(assembler,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 4, 0, 1, 0, data, 1)));
	}

	/** Verifies abort advances cursor and does not import. */
	@Test
	void abortAdvancesCursorAndDoesNotImport()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			new byte[] { 1 }, 1));
		accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.ABORT,
			1, 0, 1, 0, 0, new byte[0]
		));
		assertEquals(0, assembler.lastResolvedSequence());
		assertEquals(0, receiver.dataCalls);
	}

	/** Verifies a committed zero-length transaction is delivered as an empty binary. */
	@Test
	void emptyCommitDeliversAnEmptyBinary()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			new byte[0], 0));
		accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			0, 0, 1, 0, 0, new byte[0]
		));

		assertEquals(1, receiver.dataCalls);
		assertNotNull(receiver.data);
		assertEquals(0, receiver.data.length);
		assertEquals(0, assembler.lastResolvedSequence());
	}

	/** Verifies resolution callback runs exactly once for commit and abort. */
	@Test
	void resolutionCallbackRunsExactlyOnceForCommitAndAbort()
	{
		final AtomicInteger callbacks = new AtomicInteger();
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler =
			new TransactionAssembler(
				AeronReplicationConfiguration.builder().termLength(64 * 1024).chunkSize(256)
					.maxTransactionBytes(1024).build(), CLUSTER, EPOCH, -1, receiver, callbacks::incrementAndGet);
		final byte[] data = {1, 2};
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, data.length));
		accept(assembler, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0,
			AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
			AeronReplicationEnvelope.crc32c(data), new byte[0]));
		accept(assembler, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 1,
			AeronReplicationEnvelope.Kind.ABORT, 0, 0, 1, 0, 0, new byte[0]));
		accept(assembler, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 1,
			AeronReplicationEnvelope.Kind.ABORT, 0, 0, 1, 0, 0, new byte[0]));
		assertEquals(2, callbacks.get(), "duplicate resolution must be idempotent");
	}

	/** Verifies delivery boundary leaves uncertain marker when store import fails. */
	@Test
	void deliveryBoundaryLeavesUncertainMarkerWhenStoreImportFails()
	{
		final AtomicInteger before = new AtomicInteger();
		final AtomicInteger after = new AtomicInteger();
		final StorageBinaryDataReceiver receiver = new RecordingReceiver()
		{
			@Override
			public void receiveData(final Binary value)
			{
				throw new IllegalStateException("injected Store import failure");
			}
		};
		final TransactionAssembler assembler = new TransactionAssembler(
			AeronReplicationConfiguration.builder().termLength(64 * 1024).chunkSize(256)
				.maxTransactionBytes(1024).build(), CLUSTER, EPOCH, -1, receiver, () -> { },
			new ReaderDeliveryListener()
			{
				@Override
				public void beforeStoreImport(final long sequence, final long position, final int dataLength,
					final int dataChunkCount, final int crc32c)
				{
					before.incrementAndGet();
				}

				@Override
				public void afterStoreImport()
				{
					after.incrementAndGet();
				}
			}
		);
		final byte[] data = {1, 2, 3};
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, data.length));
		assertThrows(IllegalStateException.class, () -> accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
			AeronReplicationEnvelope.crc32c(data), new byte[0])));
		assertEquals(1, before.get());
		assertEquals(0, after.get());
		assertEquals(-1, assembler.lastResolvedSequence());
		assertNotNull(assembler.failure());
	}

	/** Verifies rejection of oversize and non contiguous chunks. */
	@Test
	void rejectsOversizeAndNonContiguousChunks()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 4);
		assertThrows(IllegalArgumentException.class, () -> accept(assembler,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
				new byte[] { 1 }, 5)));

		final TransactionAssembler second = assembler(receiver, 10);
		accept(second, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 2, 0,
			new byte[] { 1 }, 2));
		assertThrows(IllegalArgumentException.class, () -> accept(second,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 1, 2, 2,
				new byte[] { 2 }, 2)));
	}

	/** Verifies resumes from persisted sequence. */
	@Test
	void resumesFromPersistedSequence()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler =
			new TransactionAssembler(
				AeronReplicationConfiguration.builder()
					.termLength(64 * 1024)
					.chunkSize(256)
					.maxTransactionBytes(1024)
					.build(),
				CLUSTER,
				EPOCH,
				41,
				receiver
			);
		final byte[] data = new byte[] { 9, 8, 7 };
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 42, 0, 1, 0,
			data, data.length));
		accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 42, AeronReplicationEnvelope.Kind.COMMIT,
			data.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(data), new byte[0]
		));
		assertArrayEquals(data, receiver.data);
		assertEquals(42, assembler.lastResolvedSequence());
	}

	/** A reader resumed at the tail retains both cursor components before new data arrives. */
	@Test
	void resumesFromPersistedCursorAtTail()
	{
		final TransactionAssembler assembler = new TransactionAssembler(
			AeronReplicationConfiguration.builder()
				.termLength(64 * 1024)
				.chunkSize(256)
				.maxTransactionBytes(1024)
				.build(),
			CLUSTER,
			EPOCH,
			41,
			987,
			new RecordingReceiver(),
			() -> { },
			null
		);

		assertEquals(new CursorSnapshot(41, 987), assembler.cursorSnapshot());
	}

	/** Disposal releases native storage for a transaction that never reached a terminal marker. */
	@Test
	void disposalReleasesIncompleteTransactionStorage()
	{
		final TransactionAssembler assembler = assembler(new RecordingReceiver(), 1024);
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			new byte[] { 1, 2, 3 }, 3));
		assertTrue(assembler.hasIncompleteTransaction());
		assembler.dispose();
		assertFalse(assembler.hasIncompleteTransaction());
	}

	/** Verifies rejection of an equal sequence commit with a different payload checksum. */
	@Test
	void rejectsAnEqualSequenceCommitWithADifferentPayloadChecksum()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		final byte[] data = { 9, 8, 7 };
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, data.length));
		accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
			AeronReplicationEnvelope.crc32c(data), new byte[0]));

		assertThrows(IllegalStateException.class, () -> accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
			AeronReplicationEnvelope.crc32c(new byte[] { 1, 2, 3 }), new byte[0])));
	}

	/** Contradictory terminal markers for one sequence fail closed. */
	@Test
	void rejectsContradictoryCommitAndAbortTerminals()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler committed = assembler(receiver, 1024);
		final byte[] data = { 1, 2 };
		accept(committed, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, data.length));
		accept(committed, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0,
			AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
			AeronReplicationEnvelope.crc32c(data), new byte[0]));
		assertThrows(IllegalStateException.class, () -> accept(committed,
			AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.ABORT,
			data.length, 0, 1, 0, 0, new byte[0])));

		final TransactionAssembler aborted = assembler(receiver, 1024);
		accept(aborted, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0,
			AeronReplicationEnvelope.Kind.ABORT, data.length, 0, 1, 0, 0, new byte[0]));
		assertThrows(IllegalStateException.class, () -> accept(aborted,
			AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			data.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(data), new byte[0])));
	}

	/** Duplicate terminals must retain their length and chunk-count witness. */
	@Test
	void rejectsTerminalWithChangedMetadata()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			new byte[0], 0));
		accept(assembler, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0,
			AeronReplicationEnvelope.Kind.COMMIT, 0, 0, 1, 0, 0, new byte[0]));
		assertThrows(IllegalStateException.class, () -> accept(assembler,
			AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			1, 0, 1, 0, 0, new byte[0])));
	}

	/** Verifies cursor persistence failure stops further assembly. */
	@Test
	void cursorPersistenceFailureStopsFurtherAssembly()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler =
			new TransactionAssembler(
				AeronReplicationConfiguration.builder().termLength(64 * 1024).chunkSize(256)
					.maxTransactionBytes(1024).build(), CLUSTER, EPOCH, -1, receiver,
				() -> { throw new IllegalStateException("checkpoint failed"); }
			);
		final byte[] data = { 1, 2, 3 };
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, data.length));
		assertThrows(IllegalStateException.class, () -> accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
			AeronReplicationEnvelope.crc32c(data), new byte[0])));
		assertEquals(1, receiver.dataCalls);
		assertEquals("checkpoint failed", assembler.failure().getMessage());
	}

	/** Verifies rejection of wrong cluster and epoch before mutating state. */
	@Test
	void rejectsWrongClusterAndEpochBeforeMutatingState()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		final byte[] wrongCluster = AeronReplicationEnvelope.encode(
			UUID.randomUUID(), EPOCH, 0, AeronReplicationEnvelope.Kind.STORE_BINARY,
			1, 0, 1, 0, 0, new byte[] {1});
		assertThrows(IllegalArgumentException.class, () -> accept(assembler, wrongCluster));
		assertEquals(-1, assembler.lastResolvedSequence());
		assertEquals(0, receiver.dataCalls);

		final TransactionAssembler wrongEpoch = assembler(receiver, 1024);
		final byte[] epochMismatch = AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH + 1, 0, AeronReplicationEnvelope.Kind.STORE_BINARY,
			1, 0, 1, 0, 0, new byte[] {1});
		assertThrows(IllegalArgumentException.class, () -> accept(wrongEpoch, epochMismatch));
		assertEquals(-1, wrongEpoch.lastResolvedSequence());
	}

	/** Verifies rejection of commit checksum mismatch without delivering data. */
	@Test
	void rejectsCommitChecksumMismatchWithoutDeliveringData()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		final byte[] data = {1, 2, 3};
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, data.length));
		assertThrows(IllegalStateException.class, () -> accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			data.length, 0, 1, 0, AeronReplicationEnvelope.crc32c(new byte[] {7, 7, 7}), new byte[0])));
		assertEquals(0, receiver.dataCalls);
		assertEquals(-1, assembler.lastResolvedSequence());
		accept(assembler, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0,
			AeronReplicationEnvelope.Kind.ABORT, data.length, 0, 1, 0, 0, new byte[0]));
		}

	/** Verifies rejection of duplicate or changed data chunk before commit. */
	@Test
	void rejectsDuplicateOrChangedDataChunkBeforeCommit()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		final byte[] first = envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 2, 0,
			new byte[] {1}, 2);
		accept(assembler, first);
		assertThrows(IllegalStateException.class, () -> accept(assembler, first));

		final TransactionAssembler changedCount = assembler(receiver, 1024);
		accept(changedCount, first);
		assertThrows(IllegalArgumentException.class, () -> accept(changedCount,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 1, 3, 1,
				new byte[] {2}, 3)));
	}

	/** Verifies rejection of dictionary after data and data before dictionary completes. */
	@Test
	void rejectsDictionaryAfterDataAndDataBeforeDictionaryCompletes()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler afterData = assembler(receiver, 1024);
		accept(afterData, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			new byte[] {1}, 1));
		assertThrows(IllegalStateException.class, () -> accept(afterData,
			envelope(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, 0, 0, 1, 0,
				new byte[] {2}, 1)));

		final TransactionAssembler beforeDictionary = assembler(receiver, 1024);
		accept(beforeDictionary, envelope(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, 0, 0, 2, 0,
			new byte[] {2}, 2));
		assertThrows(IllegalStateException.class, () -> accept(beforeDictionary,
			envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
				new byte[] {1}, 1)));
	}

	/** Verifies rejection of commit before chunks and incomplete dictionary. */
	@Test
	void rejectsCommitBeforeChunksAndIncompleteDictionary()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler noData = assembler(receiver, 1024);
		final byte[] commit = AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			1, 0, 1, 0, 0, new byte[0]);
		assertThrows(IllegalStateException.class, () -> accept(noData, commit));

		final TransactionAssembler incomplete = assembler(receiver, 1024);
		accept(incomplete, envelope(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, 0, 0, 2, 0,
			new byte[] {1}, 2));
		assertThrows(IllegalStateException.class, () -> accept(incomplete, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT,
			0, 0, 1, 0, 0, new byte[0])));
		assertEquals(0, receiver.dataCalls);
	}

	/** Verifies receiver failure is terminal and does not apply later transactions. */
	@Test
	void receiverFailureIsTerminalAndDoesNotApplyLaterTransactions()
	{
		final TransactionAssembler assembler =
			new TransactionAssembler(
				AeronReplicationConfiguration.builder().termLength(64 * 1024).chunkSize(256)
					.maxTransactionBytes(1024).build(), CLUSTER, EPOCH, -1,
					new StorageBinaryDataReceiver()
					{
						public void receiveData(final Binary value) { throw new IllegalStateException("receiver failed"); }
						public void receiveTypeDictionary(final String value) { }
					}, () -> { });
			final byte[] data = {4};
		accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0,
			data, 1));
		assertThrows(IllegalStateException.class, () -> accept(assembler, AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, 0, AeronReplicationEnvelope.Kind.COMMIT, 1, 0, 1, 0,
			AeronReplicationEnvelope.crc32c(data), new byte[0])));
		assertEquals("receiver failed", assembler.failure().getMessage());
		accept(assembler, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 1,
			AeronReplicationEnvelope.Kind.ABORT, 0, 0, 1, 0, 0, new byte[0]));
		assertEquals(-1, assembler.lastResolvedSequence());
	}

	/** Verifies applies many transactions in order across empty and dictionary payloads. */
	@Test
	void appliesManyTransactionsInOrderAcrossEmptyAndDictionaryPayloads()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 4096);
		for (int sequence = 0; sequence < 32; sequence++)
		{
			final byte[] data = sequence % 7 == 0 ? new byte[0] : new byte[] {
				(byte)sequence, (byte)(sequence * 3), (byte)(sequence ^ 0x5a)
			};
			if (sequence % 4 == 0)
			{
				final byte[] dictionary = ("Type" + sequence).getBytes(java.nio.charset.StandardCharsets.UTF_8);
				accept(assembler, envelope(AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, sequence, 0, 1, 0,
					dictionary, dictionary.length));
			}
			accept(assembler, envelope(AeronReplicationEnvelope.Kind.STORE_BINARY, sequence, 0, 1, 0,
				data, data.length));
			accept(assembler, AeronReplicationEnvelope.encode(CLUSTER, EPOCH, sequence,
				AeronReplicationEnvelope.Kind.COMMIT, data.length, 0, 1, 0,
				AeronReplicationEnvelope.crc32c(data), new byte[0]));
		}
		assertEquals(31, assembler.lastResolvedSequence());
		assertEquals(32, receiver.dataCalls);
		assertEquals("Type28", receiver.dictionary);
	}

	/** Verifies malformed network frame fails closed and cannot skip to later sequence. */
	@Test
	void malformedNetworkFrameFailsClosedAndCannotSkipToLaterSequence()
	{
		final RecordingReceiver receiver = new RecordingReceiver();
		final TransactionAssembler assembler = assembler(receiver, 1024);
		final byte[] malformed = new byte[AeronReplicationEnvelope.HEADER_LENGTH];
		assertThrows(IllegalArgumentException.class, () -> accept(assembler, malformed));
		assertEquals(0, receiver.dataCalls);
		final byte[] later = AeronReplicationEnvelope.encode(CLUSTER, EPOCH, 0,
			AeronReplicationEnvelope.Kind.STORE_BINARY, 1, 0, 1, 0, 0, new byte[] {9});
		accept(assembler, later);
		assertEquals(0, receiver.dataCalls);
		assertEquals(-1, assembler.lastResolvedSequence());
	}

	private static TransactionAssembler assembler(
		final RecordingReceiver receiver,
		final int maxBytes
	)
	{
		return new TransactionAssembler(
			AeronReplicationConfiguration.builder()
				.termLength(64 * 1024)
				.chunkSize(Math.min(256, maxBytes))
				.maxTransactionBytes(maxBytes)
				.build(),
			CLUSTER,
			EPOCH,
			receiver
		);
	}

	private static byte[] envelope(
		final AeronReplicationEnvelope.Kind kind,
		final long sequence,
		final int index,
		final int count,
		final int offset,
		final byte[] payload,
		final int length
	)
	{
		return AeronReplicationEnvelope.encode(
			CLUSTER, EPOCH, sequence, kind, length, index, count, offset, 0, payload
		);
	}

	private static void accept(
		final TransactionAssembler assembler,
		final byte[] bytes
	)
	{
		assembler.onFragment(new UnsafeBuffer(bytes), 0, bytes.length, null);
	}

	private static class RecordingReceiver implements StorageBinaryDataReceiver
	{
		private String dictionary;
		private byte[] data;
		private int dataCalls;

		@Override
		public void receiveData(final Binary value)
		{
			final ByteBuffer buffer = value.buffers()[0].duplicate();
			this.data = new byte[buffer.remaining()];
			buffer.get(this.data);
			this.dataCalls++;
		}

		@Override
		public void receiveTypeDictionary(final String value)
		{
			this.dictionary = value;
		}
	}
}
