package peruncs.cluster.storage.aeron.checkpoint;

import peruncs.cluster.storage.Crc32C;

import java.util.Objects;
import java.util.UUID;

import static peruncs.cluster.storage.aeron.checkpoint.AeronCheckpointCodec.*;

/// The reader's durable place in one Archive recording.
///
/// The sequence is paired with the cluster, node, Store image, epoch, and
/// recording identities. A sequence by itself is unsafe after a restart because
/// a new recording may reuse it. The neutral cursor store persists this value;
/// this record carries the Aeron-specific position across the provider
/// boundary.
///
/// @param clusterId         replication cluster identity
/// @param nodeId            node that produced the cursor; replay may transfer it to another node
/// @param storeGeneration   Store image identity
/// @param epoch             writer epoch associated with the recording
/// @param fencingToken      greatest writer fencing token accepted at the cursor. A value of
///                          `0` is the explicit new-reader sentinel and is only valid with
///                          sequence `-1`; any cursor with sequence `>= 0` must carry a
///                          positive token.
/// @param recordingId       Aeron Archive recording identity
/// @param recordingPosition Archive position at the cursor
/// @param sequence          transaction sequence at the cursor
public record AeronReplicationCursor(
        UUID clusterId,
        UUID nodeId,
        UUID storeGeneration,
        long epoch,
        long fencingToken,
        long recordingId,
        long recordingPosition,
        long sequence
) {
    private static final int MAGIC = 0x44474143; // DGAC
    private static final short VERSION = 2;
    private static final int PAYLOAD_LENGTH = AeronCheckpointCodec.HEADER_LENGTH + UUID_BYTES * 3 + Long.BYTES * 5;
    private static final int ENCODED_LENGTH = PAYLOAD_LENGTH + Integer.BYTES;

        /// Validates the identities and position carried by the durable cursor.
    ///
    /// A fencing token of `0` is only the explicit new-reader sentinel for an
    /// unresolved cursor (sequence `-1`); any cursor with sequence `>= 0`
    /// must carry a positive token.
    public AeronReplicationCursor {
        if (clusterId == null || nodeId == null || storeGeneration == null ||
            epoch < 0 || fencingToken < 0 || recordingId < 0 || recordingPosition < -1 || sequence < -1 ||
            sequence == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid Aeron replication cursor");
        }
        if (sequence >= 0 && fencingToken <= 0) {
            throw new IllegalArgumentException("resolved Aeron replication cursor carries no writer fencing token");
        }
    }

        /// Decodes the sole supported Aeron provider-position format.
    ///
    /// @param encoded serialized cursor bytes
    /// @return decoded cursor
    public static AeronReplicationCursor decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length != ENCODED_LENGTH)
            throw new IllegalArgumentException("invalid Aeron cursor encoding length");
        final int expectedCrc = getInt(encoded, PAYLOAD_LENGTH);
        if (expectedCrc != Crc32C.compute(encoded, 0, PAYLOAD_LENGTH)) {
            throw new IllegalArgumentException("Aeron cursor CRC32C mismatch");
        }
        int offset = 0;
        if (getInt(encoded, offset) != MAGIC) {
            throw new IllegalArgumentException("unsupported Aeron cursor format");
        }
        if (getShort(encoded, VERSION_OFFSET) != VERSION) {
            throw new IllegalArgumentException("unsupported Aeron cursor format");
        }
        if (headerFlags(encoded) != 0)
            throw new IllegalArgumentException("unsupported Aeron cursor flags");
        final var reader = new FrameReader(encoded, AeronCheckpointCodec.HEADER_LENGTH);
        final SerializedNodeIdentity identity = reader.readNodeIdentity();
        final UUID clusterId = identity.clusterId();
        final UUID nodeId = identity.nodeId();
        final UUID storeGeneration = identity.storeGeneration();
        final long epoch = reader.readLong();
        final long fencingToken = reader.readLong();
        final long recordingId = reader.readLong();
        final long recordingPosition = reader.readLong();
        final long sequence = reader.readLong();
        return new AeronReplicationCursor(
                clusterId, nodeId, storeGeneration, epoch, fencingToken, recordingId, recordingPosition, sequence);
    }

        /// Encodes the complete provider cursor identity for the neutral cursor store.
    ///
    /// @return serialized cursor bytes
    public byte[] encode() {
        final byte[] encoded = new byte[ENCODED_LENGTH];
        int offset = putHeader(encoded, 0, MAGIC, VERSION);
        offset = putUuid(encoded, offset, this.clusterId);
        offset = putUuid(encoded, offset, this.nodeId);
        offset = putUuid(encoded, offset, this.storeGeneration);
        offset = putLong(encoded, offset, this.epoch);
        offset = putLong(encoded, offset, this.fencingToken);
        offset = putLong(encoded, offset, this.recordingId);
        offset = putLong(encoded, offset, this.recordingPosition);
        offset = putLong(encoded, offset, this.sequence);
        putInt(encoded, offset, Crc32C.compute(encoded, 0, PAYLOAD_LENGTH));
        return encoded;
    }
}
