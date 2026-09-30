package peruncs.cluster.storage.binary;

import org.eclipse.serializer.typing.Disposable;
import peruncs.cluster.storage.ReplicationPosition;

import java.util.Objects;


/// Replays writer transactions in order and applies them to this Store.
///
/// Replication reader lifecycle. The Aeron reader exposes a [ReplicationPosition].
/// A client must not report a message as consumed until the Store merger has
/// accepted the complete committed binary. This port belongs to the storage
/// domain so the node layer can model disabled replication and test readers
/// without importing transport classes.
public interface ReplicationApplier extends Disposable {
        /// Starts reading from the configured transport.
    void start();

    /// Creates a neutral client for tests and disabled replication.
    ///
    /// @return neutral client
    static ReplicationApplier noOp() {
        final ReplicationPosition position = ReplicationPosition.NONE;
        return new ReplicationApplier() {

            @Override
            public void start() {
            }

            @Override
            public void stopAtLatestMessage() {
            }

            @Override
            public ReplicationPosition position() {
                return position;
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

        /// Returns the latest applied replication position.
    ///
    /// @return replication position
    ReplicationPosition position();

        /// Returns the latest applied logical sequence without materializing a
    /// position. The default delegates to [#position()]; hot monitoring paths
        /// should override it.
    ///
    /// @return applied logical sequence, or `-1` when none
    default long currentSequence() {
        return this.position().sequence();
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

        /// Returns the stop outcome together with the last resolved position.
    ///
    /// The fallback is best-effort and allocates only the result record: the
    /// transport position is unknowable without provider state, so it is
    /// reported as `-1` (unknown) and a `null` position is normalized to
    /// [ReplicationPosition#NONE]. Implementations that can distinguish a
    /// transport position override this method; callers that poll it must not
    /// assume the position is always available.
    ///
    /// @return stop result
    default StopResult stopResult() {
        final ReplicationPosition position = this.position();
        final ReplicationPosition resolved = position == null ? ReplicationPosition.NONE : position;
        return new StopResult(this.stopOutcome(), resolved.sequence(), resolved.prepareStartPosition());
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
    /// @param sequence last resolved logical sequence, or `-1` when unknown
    /// @param position last resolved transport position, or `-1` when unknown
    record StopResult(StopOutcome outcome, long sequence, long position) {
        /// Validates the lifecycle outcome.
        public StopResult {
            Objects.requireNonNull(outcome, "outcome");
            if (sequence < -1L || position < -1L) {
                throw new IllegalArgumentException("stop result positions must be -1 when unknown");
            }
        }

        /// Whether the resolved logical sequence is known.
        public boolean hasSequence() {
            return this.sequence >= 0L;
        }

        /// Whether the resolved transport position is known.
        public boolean hasPosition() {
            return this.position >= 0L;
        }
    }
}
