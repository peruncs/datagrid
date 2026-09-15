package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.node.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;

/// Aeron reader lifecycle. The Aeron reader exposes a [ReplicationCursor].
/// A client must not report a message as consumed until the Store merger has
/// accepted the complete committed binary.
public interface ClusterStorageBinaryDataClient extends StorageBinaryDataClient {
        /// Creates a neutral client for tests and disabled replication.
    ///
    /// @param startingCursor initial cursor, or `null`
    /// @return neutral client
    static ClusterStorageBinaryDataClient NoOp(final ReplicationCursor startingCursor) {
        final ReplicationCursor cursor = startingCursor == null
                ? new ReplicationCursor("none", null, -1, new byte[0])
                : startingCursor;
        return new ClusterStorageBinaryDataClient() {

            @Override
            public void start() {
            }

            @Override
            public void stopAtLatestMessage() {
            }

            @Override
            public ReplicationCursor cursor() {
                return cursor;
            }

            @Override
            public boolean isRunning() {
                return false;
            }

            @Override
            public RuntimeException failure() {
                return null;
            }

            @Override
            public void resume() {
            }

            @Override
            public void dispose() {
            }
        };
    }

        /// Stops at the latest complete message boundary.
    void stopAtLatestMessage();

        /// Returns the latest applied replication cursor.
    ///
    /// @return replication cursor
    ReplicationCursor cursor();

        /// Reports whether the reader is running.
    ///
    /// @return `true` when running
    boolean isRunning();

        /// Returns a terminal reader failure, or `null` while the client is healthy.
    /// Implementations must expose the same terminal failure observed by their
    /// polling/consumer thread; returning a synthetic `null` hides a failed
    /// reader from readiness and backup coordination.
    ///
    /// @return terminal failure, or `null`
    RuntimeException failure();

        /// Returns the latest lifecycle result. Implementations that can distinguish a
    /// resolved transaction boundary should override this method; the fallback
    /// treats a stopped client without a reported failure as a resolved boundary.
    default StopOutcome stopOutcome() {
        if (this.failure() != null) return StopOutcome.FAILED;
        return this.isRunning() ? StopOutcome.RUNNING : StopOutcome.RESOLVED_BOUNDARY;
    }

        /// Returns the stop outcome together with the last resolved cursor.
    ///
    /// @return stop result
    default StopResult stopResult() {
        final ReplicationCursor cursor = this.cursor();
        return new StopResult(this.stopOutcome(), cursor.logicalSequence(), -1L);
    }

        /// Reports whether the reader is live.
    ///
    /// @return `true` when live
    default boolean isLive() {
        return isRunning();
    }

        /// Resumes reading after a stop.
    ///
    /// @throws NodelibraryException if resume fails
    void resume() throws NodelibraryException;
}
