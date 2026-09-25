package peruncs.cluster.storage.aeron.checkpoint;

import java.util.UUID;

/// The restart record for one writer or reader.
///
/// The writer stores the last transaction whose commit result is known. The
/// reader stores the Archive position at which replay may resume. The record
/// also carries the cluster, Store image, and recording identities. Those
/// identities matter because a sequence number can be reused after a reseed.
/// Startup refuses an incomplete or mismatched record instead of guessing.
///
/// @param recordType          whether this is writer or reader state
/// The checkpoint format persists the durability-ordering code as a fixed
/// byte; archive-first (code `1`) is the only ordering this build supports.
/// @param state               last durable state transition
/// @param clusterId           fixed-topology cluster identity
/// @param nodeId              node that owns the record
/// @param storeGeneration     Store image identity
/// @param recordingId         Aeron Archive recording identity
/// @param writerEpoch         writer fencing epoch
/// @param fencingToken        writer fencing token; always positive here. Every persisted
///                            checkpoint represents a transaction, so `0` is never valid on
///                            disk: it exists only as the explicit in-memory new-reader
///                            sentinel (an unresolved cursor with sequence `-1`).
/// @param transactionSequence last transaction sequence represented
/// @param recordingPosition   recorded Archive position at the transition. It is
///                            always the position returned after the configured recording wait;
///                            writer terminal checkpoints never store an offer-only position.
/// @param dataLength          Store binary length represented by the transition
/// @param dataChunkCount      Store binary chunk count represented by the transition
/// @param resolutionCrc32c    checksum of the represented Store binary; an abort
///                            keeps that source metadata even though its terminal marker has no
///                            payload CRC
public record AeronReplicationCheckpoint(
        RecordType recordType,
        State state,
        UUID clusterId,
        UUID nodeId,
        UUID storeGeneration,
        long recordingId,
        long writerEpoch,
        long fencingToken,
        long transactionSequence,
        long recordingPosition,
        int dataLength,
        int dataChunkCount,
        int resolutionCrc32c
) {
    static final int MAGIC = 0x44474350; // DGCP
    static final short VERSION = 2;
    /// Persisted durability-ordering code; archive-first is the only mode.
    static final int DURABILITY_ARCHIVE_FIRST = 1;
    /// Shared header, three state bytes, three UUIDs, five longs, the three
    /// data fields, and the trailing CRC32C.
    static final int ENCODED_BYTES = AeronCheckpointCodec.HEADER_LENGTH
            + Byte.BYTES * 3
            + AeronCheckpointCodec.UUID_BYTES * 3
            + Long.BYTES * 5
            + Integer.BYTES * 4;

        /// Validates the restart record and keeps its state machine closed over the
    /// writer and reader recovery domains.
    public AeronReplicationCheckpoint {
        if (recordType == null || state == null || clusterId == null ||
            nodeId == null || storeGeneration == null || recordingId < -1 || writerEpoch < 0 ||
            fencingToken < 0 || transactionSequence < -1 || transactionSequence == Long.MAX_VALUE ||
            recordingPosition < -1 || dataLength < 0 || dataChunkCount < 0) {
            throw new IllegalArgumentException("invalid Aeron replication checkpoint");
        }
        /* A fencing token of 0 is only the explicit new-reader sentinel for an
         * unresolved cursor (sequence -1). Every persisted checkpoint
         * represents a transaction, so writer checkpoints and any record with
         * a resolved sequence must carry a positive token. */
        if (fencingToken <= 0 && (recordType == RecordType.WRITER_CHECKPOINT || transactionSequence >= 0)) {
            throw new IllegalArgumentException("Aeron replication checkpoint carries no writer fencing token");
        }
        /* Keep the persisted state machine closed over its domain.  Without these
         * checks a corrupt-but-checksummed record could be accepted and interpreted
         * as a different kind of recovery evidence (for example a reader cursor
         * carrying a COMMITTED writer state). */
        if (recordType == RecordType.READER_CURSOR) {
            if (state != State.COMMITTING_UNCERTAIN || transactionSequence < 0 || recordingPosition < 0) {
                throw new IllegalArgumentException("invalid Aeron reader cursor checkpoint state");
            }
        } else {
            if (transactionSequence < 0) {
                throw new IllegalArgumentException("writer checkpoint must identify a transaction");
            }
            if ((state == State.COMMITTED || state == State.REJECTED) &&
                (recordingId < 0 || recordingPosition < 0)) {
                throw new IllegalArgumentException("terminal writer checkpoint must identify a durable Archive position");
            }
        }
    }

    int recordTypeCode() {
        return this.recordType.code;
    }

    int stateCode() {
        return this.state.code;
    }

        /// Distinguishes a writer checkpoint from a reader cursor record.
    public enum RecordType {
                /// A record owned by the single writer.
        WRITER_CHECKPOINT(1),
                /// A record owned by one reader's replay cursor.
        READER_CURSOR(2);
        private final int code;

        RecordType(final int code) {
            this.code = code;
        }

        static RecordType from(final int code) {
            return switch (code) {
                case 1 -> WRITER_CHECKPOINT;
                case 2 -> READER_CURSOR;
                default -> throw new IllegalArgumentException("unknown checkpoint record type: %s".formatted(code));
            };
        }
    }

        /// States in the persisted writer and reader recovery machine.
    public enum State {
                /// Data publication has started but has no terminal result yet.
        PREPARING(1),
                /// The outcome was lost and must not be guessed during restart.
        COMMITTING_UNCERTAIN(3),
                /// The commit marker reached the Archive recording.
        COMMITTED(4),
                /// The transaction was explicitly rejected.
        REJECTED(5);
        private final int code;

        State(final int code) {
            this.code = code;
        }

        static State from(final int code) {
            return switch (code) {
                case 1 -> PREPARING;
                case 3 -> COMMITTING_UNCERTAIN;
                case 4 -> COMMITTED;
                case 5 -> REJECTED;
                default -> throw new IllegalArgumentException("unknown checkpoint state: %s".formatted(code));
            };
        }
    }
}
