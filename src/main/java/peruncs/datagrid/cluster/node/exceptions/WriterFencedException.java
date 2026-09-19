package peruncs.datagrid.cluster.node.exceptions;

/// Signals that the writer's fencing lease is no longer held, so the write
/// was refused and the writer must stop.
///
/// This type exists to separate genuine fencing loss from every other
/// [IllegalStateException] a publication path can raise (back-pressure
/// timeout, closed publication, interrupted thread). Relabeling those as
/// "lease lost" would corrupt recovery diagnostics and force needless writer
/// restarts, so only the lease gate throws this exception — and every layer,
/// including the Aeron transport below the node packages, shares this one
/// exported type.
///
/// @since 1.0
public final class WriterFencedException extends IllegalStateException {
        /// Creates an exception with a message.
    ///
    /// @param message diagnostic message
    public WriterFencedException(final String message) {
        super(message);
    }

        /// Creates an exception retaining the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public WriterFencedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
