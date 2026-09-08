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
 * Fixed-size, CRC-protected recovery record for writer state or reader cursors.
 *
 * <p>The record is intentionally independent of the Aeron wire envelope. A
 * writer checkpoint records the last terminal commit ordering state needed
 * after a crash; a reader record records the replay boundary. The store writes
 * this record atomically. Startup currently accepts only terminal writer
 * states and fails closed when the archive identity or checkpoint is
 * inconsistent; it never guesses through a torn transaction.</p>
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
 * @param recordingPosition Aeron position at the transition
 * @param resolutionCrc32c commit/abort witness checksum
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
	int resolutionCrc32c
)
{
	public AeronReplicationCheckpoint
	{
		if (recordType == null || durabilityMode == null || state == null || clusterId == null ||
			nodeId == null || storeGeneration == null || recordingId < -1 || writerEpoch < 0 ||
			transactionSequence < -1 || recordingPosition < -1)
		{
			throw new IllegalArgumentException("invalid Aeron replication checkpoint");
		}
	}

	public enum RecordType
	{
		/** State belonging to the single writer. */
		WRITER_CHECKPOINT(1),
		/** State belonging to one reader's replay cursor. */
		READER_CURSOR(2);
		private final int code;
		RecordType(final int code) { this.code = code; }
		static RecordType from(final int code)
		{
			for (final RecordType value : values()) if (value.code == code) return value;
			throw new IllegalArgumentException("unknown checkpoint record type: " + code);
		}
	}

	public enum DurabilityMode
	{
		/** Archive bytes are prepared before local Store acceptance. */
		ARCHIVE_FIRST(1),
		/** Local enqueue precedes archive publication; recovery is conservative. */
		ENQUEUE_THEN_ARCHIVE(2);
		private final int code;
		DurabilityMode(final int code) { this.code = code; }
		static DurabilityMode from(final int code)
		{
			for (final DurabilityMode value : values()) if (value.code == code) return value;
			throw new IllegalArgumentException("unknown checkpoint durability mode: " + code);
		}
	}

	public enum State
	{
		/** Data chunks have been accepted by the publication. */
		PREPARING(1),
		/** The local Store accepted the transaction. */
		ENQUEUED(2),
		/** Commit outcome was lost and must not be guessed. */
		COMMITTING_UNCERTAIN(3),
		/** Commit marker is durably published. */
		COMMITTED(4),
		/** Transaction was explicitly aborted. */
		REJECTED(5);
		private final int code;
		State(final int code) { this.code = code; }
		static State from(final int code)
		{
			for (final State value : values()) if (value.code == code) return value;
			throw new IllegalArgumentException("unknown checkpoint state: " + code);
		}
	}

	static final int MAGIC = 0x44474152; // DGAR
	static final short VERSION = 1;
	static final int ENCODED_BYTES = 100;

	int recordTypeCode() { return this.recordType.code; }
	int durabilityModeCode() { return this.durabilityMode.code; }
	int stateCode() { return this.state.code; }
}
