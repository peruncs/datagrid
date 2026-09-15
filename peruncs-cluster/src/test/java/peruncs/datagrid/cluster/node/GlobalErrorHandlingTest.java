package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies fatal node errors remain under the embedding application's control.
class GlobalErrorHandlingTest {
    @Test
    void rethrowsRuntimeFailuresWithoutTerminatingTheJvm() {
        final RuntimeException failure = new IllegalStateException("boom");
        assertSame(failure, assertThrows(RuntimeException.class,
                () -> GlobalErrorHandling.handleFatalError(failure)));
    }

    @Test
    void wrapsCheckedFailuresWithoutTerminatingTheJvm() {
        final Exception failure = new Exception("boom");
        final NodeLibraryException reported = assertThrows(NodeLibraryException.class,
                () -> GlobalErrorHandling.handleFatalError(failure));
        assertSame(failure, reported.getCause());
    }
}
