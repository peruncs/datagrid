package peruncs.cluster.errors;

/// Reports a writer-side request rejected before local Store persistence began.
///
/// A caller may safely retry this write, and the graph is not latched. The
/// storage facade gives unrelated [ReplicationException] causes precedence,
/// except for a [ReplicationUnavailableException] retained by a recorded-ABORT
/// rejection, because that ABORT proves the sequence was terminated.
public final class WriteRejectedException extends ReplicationException {
    private final long recordedAbortPosition;

    /// Creates an exception with a message.
    public WriteRejectedException(final String message) {
        super(message);
        this.recordedAbortPosition = -1L;
    }

    /// Creates an exception with a message and the rejection cause.
    public WriteRejectedException(final String message, final Throwable cause) {
        super(message, cause);
        this.recordedAbortPosition = -1L;
    }

    /// Creates a clean rejection after the publisher durably recorded its ABORT marker.
    ///
    /// @param message rejection detail
    /// @param cause original preparation failure
    /// @param abortPosition durable Archive position of the ABORT marker
    /// @return rejection carrying the proof that the reserved sequence was terminated
    /// @throws IllegalArgumentException if the ABORT position is negative
    public static WriteRejectedException afterRecordedAbort(
            final String message, final Throwable cause, final long abortPosition) {
        if (abortPosition < 0L) throw new IllegalArgumentException("abortPosition must be non-negative");
        return new WriteRejectedException(message, cause, abortPosition);
    }

    private WriteRejectedException(final String message, final Throwable cause, final long abortPosition) {
        super(message, cause);
        this.recordedAbortPosition = abortPosition;
    }

    /// Returns the durable ABORT position, or -1 when this was not a recorded-abort rejection.
    public long recordedAbortPosition() {
        return this.recordedAbortPosition;
    }

    /// Whether this rejection follows an Archive-recorded ABORT marker.
    public boolean hasRecordedAbort() {
        return this.recordedAbortPosition >= 0L;
    }
}
