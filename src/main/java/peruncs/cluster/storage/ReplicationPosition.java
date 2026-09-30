package peruncs.cluster.storage;

import java.util.Objects;
import java.util.UUID;

/// Identifies a Store transaction and its durable Archive position.
///
/// The fields mirror the Store replication mark, with `nodeId` identifying the
/// reader that reports this view. `NONE` represents a node without replication.
public record ReplicationPosition(
        UUID clusterId,
        UUID storeGeneration,
        long epoch,
        long recordingId,
        long sequence,
        long prepareStartPosition,
        long fencingToken,
        UUID nodeId
) {
    public static final ReplicationPosition NONE =
            new ReplicationPosition(null, null, -1L, -1L, -1L, -1L, 0L, null);

    public ReplicationPosition {
        final boolean absent = clusterId == null && storeGeneration == null && nodeId == null &&
                epoch == -1L && recordingId == -1L && sequence == -1L && prepareStartPosition == -1L &&
                fencingToken == 0L;
        if (!absent) {
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(storeGeneration, "storeGeneration");
            Objects.requireNonNull(nodeId, "nodeId");
            if (epoch < 0L || recordingId < 0L || sequence < -1L || sequence == Long.MAX_VALUE ||
                prepareStartPosition < -1L || fencingToken < 0L ||
                (sequence >= 0L && (prepareStartPosition < 0L || fencingToken == 0L))) {
                throw new IllegalArgumentException("invalid replication position");
            }
        }
    }

    public boolean isResolved() {
        return this.sequence >= 0L;
    }
}
