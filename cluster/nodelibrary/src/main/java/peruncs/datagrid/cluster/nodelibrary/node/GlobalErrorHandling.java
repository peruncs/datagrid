package peruncs.datagrid.cluster.nodelibrary.node;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

/**
 * Reports an unrecoverable node error without terminating the hosting JVM.
 *
 * <p>A nodelibrary is embedded in an application and must not call
 * {@code System.exit}.  The application supervisor decides whether a fatal
 * node error warrants process termination.</p>
 */
public final class GlobalErrorHandling {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalErrorHandling.class);

    private GlobalErrorHandling() {
    }

    /**
     * Handles an error that makes the node unsafe to continue.
     *
     * @param t fatal error
     *          <p>This method never returns. It rethrows errors and runtime exceptions and
     *          wraps checked failures in a {@link NodelibraryException}.</p>
     */
    public static void handleFatalError(final Throwable t) {
        try {
            LOG.error("Shutting down application due to fatal error", t);
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
