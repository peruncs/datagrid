package peruncs.cluster.errors;

/// Reports that active Store graph work did not drain before node close timed out.
///
/// Admission remains closed, and calling close again retries the drain.
public final class GraphDrainTimeoutException extends NodeException {
    /// Creates the retryable graph-drain failure.
    ///
    /// @param message diagnostic message
    public GraphDrainTimeoutException(final String message) {
        super(message);
    }
}
