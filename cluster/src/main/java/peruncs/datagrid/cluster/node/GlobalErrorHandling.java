package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.exceptions.NodelibraryException;

/// Reports an unrecoverable node error without terminating the hosting JVM.
///
/// A node is embedded in an application and must not call
/// `System.exit`.  The application supervisor decides whether a fatal
/// node error warrants process termination.
public final class GlobalErrorHandling {
    private static final System.Logger LOGGER = System.getLogger(GlobalErrorHandling.class.getName());

    private GlobalErrorHandling() {
    }

        /// Handles an error that makes the node unsafe to continue.
    ///
    /// @param t fatal error
    ///
    /// This method never returns. It rethrows errors and runtime exceptions and
    ///          wraps checked failures in a [NodelibraryException].
    public static void handleFatalError(final Throwable t) {
        try {
            LOGGER.log(System.Logger.Level.ERROR, "Shutting down application due to fatal error", t);
        } catch (final Throwable ignored) {
            // ignore any failures here
        }

        if (t instanceof Error error) {
            throw error;
        }
        if (t instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new NodelibraryException("Fatal node error", t);
    }
}
