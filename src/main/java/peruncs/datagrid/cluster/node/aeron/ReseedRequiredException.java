package peruncs.datagrid.cluster.node.aeron;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

/// Typed fail-closed signal for recovery evidence that cannot be reconciled.
///
/// Thrown when a node cannot reconstruct authoritative state from its local
/// files and the replication log — for example a reader started from an
/// empty directory without a matching Store+cursor seed. Callers must reseed
/// the node (restore a compatible backup or copy the writer's Store image
/// with its cursor) instead of manufacturing independent state.
///
/// This extends [peruncs.datagrid.cluster.node.exceptions.NodeLibraryException]
/// so node-lifecycle `catch` blocks observe it; the `RESEED_REQUIRED: ` message
/// prefix is kept for log triage.
public final class ReseedRequiredException extends NodeLibraryException {
    /// Creates a reseed-required failure with a diagnostic message.
    ///
    /// @param message recovery evidence diagnostic
    public ReseedRequiredException(final String message) {
        super("RESEED_REQUIRED: %s".formatted(message));
    }

    /// Creates a reseed-required failure with a diagnostic message and cause.
    ///
    /// @param message recovery evidence diagnostic
    /// @param cause underlying failure
    public ReseedRequiredException(final String message, final Throwable cause) {
        super("RESEED_REQUIRED: %s".formatted(message), cause);
    }
}
