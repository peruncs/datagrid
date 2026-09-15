package peruncs.datagrid.cluster.nodelibrary.node;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the foundation's single close boundary. */
class ClusterFoundationLifecycleTest {
    @Test
    void closeIsIdempotentAndPreventsRestart() throws Exception {
        final ClusterFoundation<?> foundation = ClusterFoundation.New();
        foundation.close();
        foundation.close();

        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }
}
