package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.typing.Disposable;

import java.util.Objects;

/// Replication reader lifecycle. The Aeron reader exposes a [ReplicationCursor].
/// A client must not report a message as consumed until the Store merger has
/// accepted the complete committed binary. This port belongs to the storage
/// domain so the node layer can model disabled replication and test readers
/// without importing transport classes.
public interface StorageBinaryDataClient extends Disposable {
        /// Starts reading from the configured transport.
    void start();

        /// Creates a neutral client for tests and disabled replication.
    ///
    /// @param startingCursor initial cursor, or `null` for the fixed `none` cursor
    /// @return neutral client
    static StorageBinaryDataClient NoOp(final ReplicationCursor startingCursor) {
        final ReplicationCursor cursor = startingCursor == null
                ? ReplicationCursor.NONE
                : startingCursor;
        return new StorageBinaryDataClient() {

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

        /// Returns the latest applied logical sequence without materializing a
        /// cursor. The default delegates to [#cursor()]; hot monitoring paths
        /// should override it.
    ///
    /// @return applied logical sequence, or `-1` when none
    default long currentSequence() {
        return this.cursor().logicalSequence();
    }

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
    ///
    /// @return current lifecycle outcome
    default StopOutcome stopOutcome() {
        if (this.failure() != null) return StopOutcome.FAILED;
        return this.isRunning() ? StopOutcome.RUNNING : StopOutcome.RESOLVED_BOUNDARY;
    }

        /// Returns the stop outcome together with the last resolved cursor.
    ///
    /// The fallback is best-effort and allocates only the result record: the
    /// transport position is unknowable without provider state, so it is
    /// reported as `-1` (unknown) and a `null` cursor is normalized to
    /// [ReplicationCursor#NONE]. Implementations that can distinguish a
    /// transport position override this method; callers that poll it must not
    /// assume the position is always available.
    ///
    /// @return stop result
    default StopResult stopResult() {
        final ReplicationCursor cursor = this.cursor();
        final ReplicationCursor resolved = cursor == null ? ReplicationCursor.NONE : cursor;
        return new StopResult(this.stopOutcome(), resolved.logicalSequence(), -1L);
    }

        /// Reports whether the reader is live.
    ///
    /// @return `true` when live
    default boolean isLive() {
        return isRunning();
    }

        /// Resumes reading after a stop.
    ///
    /// Implementations fail with an unchecked transport exception when resume
    /// is not possible; the node layer wraps it for reporting.
    void resume();

        /// Lifecycle outcomes for a replication reader.
    enum StopOutcome {
        /// Client has not started.
        NOT_STARTED,
        /// Client is actively reading.
        RUNNING,
        /// Client is stopping at a boundary.
        STOPPING,
        /// Client stopped at a resolved transaction boundary.
        RESOLVED_BOUNDARY,
        /// Client did not reach a boundary before its deadline.
        TIMED_OUT,
        /// Client stopped because of a terminal failure.
        FAILED,
        /// Client stopped normally.
        STOPPED,
        /// Client has been disposed.
        CLOSED
    }

        /// Immutable result of a stop-at-latest request.
    ///
    /// @param outcome lifecycle outcome
    /// @param sequence last resolved logical sequence
    /// @param position last resolved transport position, or `-1`
    record StopResult(StopOutcome outcome, long sequence, long position) {
        /// Validates the lifecycle outcome.
        public StopResult {
            Objects.requireNonNull(outcome, "outcome");
        }
    }
}
