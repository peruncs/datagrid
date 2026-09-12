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

import java.util.UUID;

/**
 * The restart record for one writer or reader.
 *
 * <p>The writer stores the last transaction whose commit result is known. The
 * reader stores the Archive position at which replay may resume. The record
 * also carries the cluster, Store image, and recording identities. Those
 * identities matter because a sequence number can be reused after a reseed.
 * Startup refuses an incomplete or mismatched record instead of guessing.</p>
 *
 * @param recordType whether this is writer or reader state
 * @param durabilityMode ordering selected for the writer
 * @param state last durable state transition
 * @param clusterId fixed-topology cluster identity
 * @param nodeId node that owns the record
 * @param storeGeneration Store image identity
 * @param recordingId Aeron Archive recording identity
 * @param writerEpoch writer fencing epoch
 * @param transactionSequence last transaction sequence represented
 * @param recordingPosition recorded Archive position at the transition. It is
 *        always the position returned after the configured recording wait;
 *        writer terminal checkpoints never store an offer-only position.
 * @param dataLength Store binary length represented by the transition
 * @param dataChunkCount Store binary chunk count represented by the transition
 * @param resolutionCrc32c checksum of the represented Store binary; an abort
 *        keeps that source metadata even though its terminal marker has no
 *        payload CRC
 */
public record AeronReplicationCheckpoint(
	RecordType recordType,
	DurabilityMode durabilityMode,
	State state,
	UUID clusterId,
	UUID nodeId,
	UUID storeGeneration,
	long recordingId,
	long writerEpoch,
	long transactionSequence,
	long recordingPosition,
	int dataLength,
	int dataChunkCount,
	int resolutionCrc32c
)
{
	/**
	 * Validates the restart record and keeps its state machine closed over the
	 * writer and reader recovery domains.
	 */
	public AeronReplicationCheckpoint
	{
		if (recordType == null || durabilityMode == null || state == null || clusterId == null ||
			nodeId == null || storeGeneration == null || recordingId < -1 || writerEpoch < 0 ||
				transactionSequence < -1 || transactionSequence == Long.MAX_VALUE || recordingPosition < -1 ||
				dataLength < 0 || dataChunkCount < 0)
		{
			throw new IllegalArgumentException("invalid Aeron replication checkpoint");
		}
		/* Keep the persisted state machine closed over its domain.  Without these
		 * checks a corrupt-but-checksummed record could be accepted and interpreted
		 * as a different kind of recovery evidence (for example a reader cursor
		 * carrying a COMMITTED writer state). */
		if (recordType == RecordType.READER_CURSOR)
		{
			if (state != State.COMMITTING_UNCERTAIN || transactionSequence < 0 || recordingPosition < 0)
			{
				throw new IllegalArgumentException("invalid Aeron reader cursor checkpoint state");
			}
		}
		else
		{
			if (transactionSequence < 0)
			{
				throw new IllegalArgumentException("writer checkpoint must identify a transaction");
			}
			if ((state == State.COMMITTED || state == State.REJECTED) &&
				(recordingId < 0 || recordingPosition < 0))
			{
				throw new IllegalArgumentException("terminal writer checkpoint must identify a durable Archive position");
			}
		}
	}

	/** Distinguishes a writer checkpoint from a reader cursor record. */
	public enum RecordType
	{
	/** A record owned by the single writer. */
		WRITER_CHECKPOINT(1),
	/** A record owned by one reader's replay cursor. */
		READER_CURSOR(2);
		private final int code;
		RecordType(final int code) { this.code = code; }
		static RecordType from(final int code)
		{
			return switch (code)
			{
				case 1 -> WRITER_CHECKPOINT;
				case 2 -> READER_CURSOR;
				default -> throw new IllegalArgumentException("unknown checkpoint record type: " + code);
			};
		}
	}

	/** Describes how the writer waits for replication durability. */
	public enum DurabilityMode
	{
	/** Publish the Archive transaction before accepting it locally. */
		ARCHIVE_FIRST(1),
	/** Accept locally first; an uncertain result requires reseeding. */
		ENQUEUE_THEN_ARCHIVE(2);
		private final int code;
		DurabilityMode(final int code) { this.code = code; }
		static DurabilityMode from(final int code)
		{
			return switch (code)
			{
				case 1 -> ARCHIVE_FIRST;
				case 2 -> ENQUEUE_THEN_ARCHIVE;
				default -> throw new IllegalArgumentException("unknown checkpoint durability mode: " + code);
			};
		}
	}

	/** States in the persisted writer and reader recovery machine. */
	public enum State
	{
	/** Data publication has started but has no terminal result yet. */
		PREPARING(1),
	/** The local Store accepted the transaction. */
		ENQUEUED(2),
	/** The outcome was lost and must not be guessed during restart. */
		COMMITTING_UNCERTAIN(3),
	/** The commit marker reached the Archive recording. */
		COMMITTED(4),
	/** The transaction was explicitly rejected. */
		REJECTED(5);
		private final int code;
		State(final int code) { this.code = code; }
		static State from(final int code)
		{
			return switch (code)
			{
				case 1 -> PREPARING;
				case 2 -> ENQUEUED;
				case 3 -> COMMITTING_UNCERTAIN;
				case 4 -> COMMITTED;
				case 5 -> REJECTED;
				default -> throw new IllegalArgumentException("unknown checkpoint state: " + code);
			};
		}
	}

	static final int MAGIC = 0x44474152; // DGAR
	static final short VERSION = 1;
	static final int ENCODED_BYTES = 108;

	int recordTypeCode() { return this.recordType.code; }
	int durabilityModeCode() { return this.durabilityMode.code; }
	int stateCode() { return this.state.code; }
}
