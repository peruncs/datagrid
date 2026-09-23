package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.errors.NodeException;
import peruncs.datagrid.cluster.storage.ReplicationCursor;

import java.util.Objects;
import java.util.UUID;

/// Deletes old Archive segments only after every configured reader has durably passed them.
///
/// Provider-specific retention hook; providers that cannot prove safe reader
/// watermarks retain history and report retention as unsupported.
public interface ReplicationLogRetention extends AutoCloseable {
        /// Returns whether this transport can safely delete replicated history. A
    /// provider that cannot establish authenticated reader watermarks must return
    /// `false`; lifecycle code will retain history and continue backups.
    ///
    /// @return `true` when safe retention is supported
    default boolean isSupported() {
        return true;
    }

        /// Deletes only history proven safe by the provider's cursor/watermark rules.
    ///
    /// @param cursor deletion boundary
    /// @return result of the bounded maintenance attempt
    /// @throws NodeException if deletion fails
    MaintenanceResult deleteThrough(ReplicationCursor cursor) throws NodeException;

        /// Records one authenticated reader acknowledgement for a later aggregate
    /// retention request. Providers without reader-watermark support reject this
    /// operation explicitly.
    ///
    /// @param cursor reader watermark
    default void recordReaderWatermark(final ReplicationCursor cursor) {
        throw new UnsupportedOperationException("reader watermarks are unsupported by this transport");
    }

        /// Permanently removes a decommissioned reader from the retention quorum.
    ///
    /// Implementations must persist the retirement before it can affect a
    /// deletion decision. Transports without reader-watermark support reject
    /// this operation explicitly.
    ///
    /// @param readerId permanently retired reader identity
    default void retireReader(final UUID readerId) {
        throw new UnsupportedOperationException("reader retirement is unsupported by this transport");
    }

        /// Releases the retention resources.
    @Override
    void close();

        /// Outcome of one bounded retention maintenance attempt.
    ///
    /// @param status   result category
    /// @param position Archive position associated with the attempt
    /// @param detail   diagnostic detail, never `null`
    record MaintenanceResult(Status status, long position, String detail) {
                /// Normalizes the result detail and validates the status.
        public MaintenanceResult {
            Objects.requireNonNull(status, "status");
            detail = detail == null ? "" : detail;
        }

                /// Retention maintenance outcome.
        public enum Status {
                        /// One or more complete leading Archive segments were deleted.
            DELETED,
                        /// No complete segment was eligible for deletion.
            NOTHING_TO_DELETE,
                        /// A stopped recording still has a replay using a segment selected for purge.
            DEFERRED_ACTIVE_REPLAY
        }
    }
}
