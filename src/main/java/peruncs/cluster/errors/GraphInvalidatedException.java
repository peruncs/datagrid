package peruncs.cluster.errors;

/// Reports a Store object graph that may be partially updated after a failed
/// replication or persistence update.
///
/// The graph coordinator latches this failure when a graph mutation section
/// throws, before releasing the write boundary: keeping the durable cursor at
/// the previous boundary protects restart recovery, but it does not protect
/// queries from an already-mutated in-memory graph. Every coordinated read and
/// write on the invalid graph fails closed with this type until the node
/// reloads or reseeds its Store image.
public class GraphInvalidatedException extends ReplicationException {
    /// Creates an exception with a message.
    ///
    /// @param message diagnostic message
    public GraphInvalidatedException(final String message) {
        super(message);
    }

    /// Creates an exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   update failure that invalidated the graph
    public GraphInvalidatedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
