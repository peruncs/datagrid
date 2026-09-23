package peruncs.datagrid.cluster.node.aeron;

import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.TimeoutException;

/// Recognizes Aeron 1.53 Archive failures that have no dedicated error code.
final class AeronArchiveFailures {
    private static final String CONTROL_RESPONSE_DISCONNECTED = "control response publication is not connected";
    private static final String REPLAY_IN_PROGRESS_DETACH = "invalid detach: replay in progress";

    private AeronArchiveFailures() {
    }

    static boolean terminalControlResponseWarning(final Throwable failure) {
        return failure instanceof ArchiveEvent && failure.getMessage() != null &&
               failure.getMessage().contains(CONTROL_RESPONSE_DISCONNECTED);
    }

    static boolean replayInProgressDetach(final ArchiveException failure) {
        return failure.errorCode() == ArchiveException.ACTIVE_RECORDING ||
               failure.errorCode() == ArchiveException.GENERIC && failure.getMessage() != null &&
               failure.getMessage().contains(REPLAY_IN_PROGRESS_DETACH);
    }

    static boolean unavailable(final Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof ArchiveException || current instanceof TimeoutException) return true;
            final String message = current.getMessage();
            if (message != null && (message.contains("connection to the archive is no longer available") ||
                                    message.contains("awaiting response"))) return true;
        }
        return false;
    }
}
