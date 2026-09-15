package peruncs.datagrid.cluster.node.aeron;

/// Typed fail-closed signal for recovery evidence that cannot be reconciled.
final class ReseedRequiredException extends IllegalStateException {
    ReseedRequiredException(final String message) {
        super("RESEED_REQUIRED: %s".formatted(message));
    }

    ReseedRequiredException(final String message, final Throwable cause) {
        super("RESEED_REQUIRED: %s".formatted(message), cause);
    }
}
