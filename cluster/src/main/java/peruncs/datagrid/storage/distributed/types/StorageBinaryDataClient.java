package peruncs.datagrid.storage.distributed.types;


import org.eclipse.serializer.typing.Disposable;

/** Minimal lifecycle contract for a reader-side binary replication client. */
public interface StorageBinaryDataClient extends Disposable {
    /**
     * Returns the most recent stop outcome, or {@link StopOutcome#NOT_STARTED}.
     *
     * @return current stop outcome
     */
    default StopOutcome stopOutcome() {
        return StopOutcome.NOT_STARTED;
    }

    /**
     * Returns the stop outcome together with the last resolved cursor.
     *
     * @return current stop result
     */
    default StopResult stopResult() {
        return new StopResult(this.stopOutcome(), -1L, -1L);
    }

    /** Starts reading from the configured transport. */
    void start();

    /** Result of a requested stop-at-latest operation. */
    enum StopOutcome {
        /** No stop request has been made. */
        NOT_STARTED,
        /** The client is still reading. */
        RUNNING,
        /** The client is waiting for the stop boundary. */
        STOPPING,
        /** The stop boundary has been found. */
        RESOLVED_BOUNDARY,
        /** The boundary was not found before the deadline. */
        TIMED_OUT,
        /** The stop operation failed. */
        FAILED,
        /** The client has stopped. */
        STOPPED,
        /** The client has been closed. */
        CLOSED
    }

    /**
     * Immutable result of a stop-at-latest request.
     *
     * @param outcome  stop outcome
     * @param sequence last resolved sequence, or {@code -1}
     * @param position last resolved position, or {@code -1}
     */
    record StopResult(StopOutcome outcome, long sequence, long position) {
        /** Validates the stop outcome. */
        public StopResult {
            if (outcome == null) throw new NullPointerException("outcome");
        }
    }
}
