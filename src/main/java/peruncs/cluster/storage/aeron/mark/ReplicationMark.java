package peruncs.cluster.storage.aeron.mark;

import java.util.Objects;
import java.util.UUID;

/// Store-resident replication boundary shared by a writer and its readers.
///
/// The writer updates this same object in each local Store commit. Readers
/// receive those updates with the rest of the replicated graph.
public final class ReplicationMark {
    /// Reserved Store root identifier.
    public static final String ROOT_ID = "peruncs.replication";

    public UUID clusterId;
    public UUID storeGeneration;
    public long epoch;
    public long recordingId;
    public long fencingToken;
    public long sequence = -1L;
    public long prepareStartPosition = -1L;

    /// Serializer constructor. Registered marks are created with the identity constructor.
    public ReplicationMark() {
    }

    /// Creates the mark for one configured cluster image.
    public ReplicationMark(final UUID clusterId, final UUID storeGeneration, final long epoch,
                           final long recordingId) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.storeGeneration = Objects.requireNonNull(storeGeneration, "storeGeneration");
        if (epoch < 0L || recordingId < -1L) throw new IllegalArgumentException("invalid replication mark identity");
        this.epoch = epoch;
        this.recordingId = recordingId;
    }
}
