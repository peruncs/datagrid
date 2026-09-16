package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import peruncs.datagrid.cluster.storage.types.Crc32c;

import java.util.Objects;
import java.util.UUID;

import static peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronCheckpointCodec.*;

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
/// @param recordingId       Aeron Archive recording identity
/// @param recordingPosition Archive position at the cursor
/// @param sequence          transaction sequence at the cursor
public record AeronReplicationCursor(
        UUID clusterId,
        UUID nodeId,
        UUID storeGeneration,
        long epoch,
        long recordingId,
        long recordingPosition,
        long sequence
) {
    private static final int MAGIC = 0x44474143; // DGAC
    private static final short VERSION = 1;
    private static final int PAYLOAD_LENGTH = Integer.BYTES + Short.BYTES * 2 + UUID_BYTES * 3 + Long.BYTES * 4;
    private static final int ENCODED_LENGTH = PAYLOAD_LENGTH + Integer.BYTES;

        /// Validates the identities and position carried by the durable cursor.
    public AeronReplicationCursor {
        if (clusterId == null || nodeId == null || storeGeneration == null ||
            epoch < 0 || recordingId < 0 || recordingPosition < -1 || sequence < -1 || sequence == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid Aeron replication cursor");
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
        if (expectedCrc != Crc32c.compute(encoded, 0, PAYLOAD_LENGTH)) {
            throw new IllegalArgumentException("Aeron cursor CRC32C mismatch");
        }
        int offset = 0;
        if (getInt(encoded, offset) != MAGIC) {
            throw new IllegalArgumentException("unsupported Aeron cursor format");
        }
        offset += Integer.BYTES;
        if (getShort(encoded, offset) != VERSION) {
            throw new IllegalArgumentException("unsupported Aeron cursor format");
        }
        offset += Short.BYTES;
        if (getShort(encoded, offset) != 0)
            throw new IllegalArgumentException("unsupported Aeron cursor format");
        offset += Short.BYTES;
        final UUID clusterId = getUuid(encoded, offset);
        offset += UUID_BYTES;
        final UUID nodeId = getUuid(encoded, offset);
        offset += UUID_BYTES;
        final UUID storeGeneration = getUuid(encoded, offset);
        offset += UUID_BYTES;
        final long epoch = getLong(encoded, offset);
        offset += Long.BYTES;
        final long recordingId = getLong(encoded, offset);
        offset += Long.BYTES;
        final long recordingPosition = getLong(encoded, offset);
        offset += Long.BYTES;
        final long sequence = getLong(encoded, offset);
        return new AeronReplicationCursor(
                clusterId, nodeId, storeGeneration, epoch, recordingId, recordingPosition, sequence);
    }

        /// Encodes the complete provider cursor identity for the neutral cursor store.
    ///
    /// @return serialized cursor bytes
    public byte[] encode() {
        final byte[] encoded = new byte[ENCODED_LENGTH];
        int offset = 0;
        offset = putInt(encoded, offset, MAGIC);
        offset = putShort(encoded, offset, VERSION);
        offset = putShort(encoded, offset, (short) 0);
        offset = putUuid(encoded, offset, this.clusterId);
        offset = putUuid(encoded, offset, this.nodeId);
        offset = putUuid(encoded, offset, this.storeGeneration);
        offset = putLong(encoded, offset, this.epoch);
        offset = putLong(encoded, offset, this.recordingId);
        offset = putLong(encoded, offset, this.recordingPosition);
        offset = putLong(encoded, offset, this.sequence);
        putInt(encoded, offset, Crc32c.compute(encoded, 0, PAYLOAD_LENGTH));
        return encoded;
    }
}
