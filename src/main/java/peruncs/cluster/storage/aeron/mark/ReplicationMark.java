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

    private UUID clusterId;
    private UUID storeGeneration;
    private long epoch;
    private long recordingId;
    private long fencingToken;
    private long sequence = -1L;
    private long prepareStartPosition = -1L;

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

    /// Returns the cluster this image belongs to.
    ///
    /// @return cluster identity, or `null` for a mark that was never initialized
    public UUID clusterId() {
        return this.clusterId;
    }

    /// Returns the Store generation this image belongs to.
    ///
    /// @return Store generation, or `null` for a mark that was never initialized
    public UUID storeGeneration() {
        return this.storeGeneration;
    }

    /// Returns the writer epoch of the last committed transaction.
    ///
    /// @return writer epoch
    public long epoch() {
        return this.epoch;
    }

    /// Returns the Archive recording that carries this image's history.
    ///
    /// @return recording id, or `-1` before the recording exists
    public long recordingId() {
        return this.recordingId;
    }

    /// Returns the fencing token of the writer that committed the last transaction.
    ///
    /// @return fencing token
    public long fencingToken() {
        return this.fencingToken;
    }

    /// Returns the sequence of the last committed transaction.
    ///
    /// @return sequence, or `-1` before the first commit
    public long sequence() {
        return this.sequence;
    }

    /// Returns the Archive position where the last committed transaction's prepare started.
    ///
    /// A restarting reader resumes its replay here.
    ///
    /// @return Archive position, or `-1` before the first commit
    public long prepareStartPosition() {
        return this.prepareStartPosition;
    }

    /// Records the transaction the writer is about to commit with the Store.
    ///
    /// The four values always change together, so they are set by one method.
    ///
    /// @param recordingId          Archive recording that carries the transaction
    /// @param fencingToken         fencing token of the committing writer
    /// @param sequence             sequence of the transaction
    /// @param prepareStartPosition Archive position where the transaction's prepare started
    public void reserve(final long recordingId, final long fencingToken, final long sequence,
                        final long prepareStartPosition) {
        this.recordingId = recordingId;
        this.fencingToken = fencingToken;
        this.sequence = sequence;
        this.prepareStartPosition = prepareStartPosition;
    }
}
