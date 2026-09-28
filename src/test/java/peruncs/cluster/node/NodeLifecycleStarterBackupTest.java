package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.NodeException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies startup keeps uploads until a starter backup is durably published.
class NodeLifecycleStarterBackupTest {
    /// A failed or bounded-out backup never deletes its source upload.
    @Test
    void failedOrTimedOutBackupKeepsTheUpload() {
        final AtomicBoolean deleted = new AtomicBoolean();

        assertThrows(NodeException.class, () -> NodeLifecycle.awaitStarterBackup(
                CompletableFuture.failedFuture(new IllegalStateException("export failed")), 1_000L,
                () -> deleted.set(true)));
        assertFalse(deleted.get());

        final NodeException timeout = assertThrows(NodeException.class, () -> NodeLifecycle.awaitStarterBackup(
                new CompletableFuture<>(), 1L, () -> deleted.set(true)));
        assertInstanceOf(TimeoutException.class, timeout.getCause());
        assertFalse(deleted.get());

        final CompletableFuture<String> cancelled = new CompletableFuture<>();
        cancelled.cancel(false);
        assertThrows(NodeException.class, () -> NodeLifecycle.awaitStarterBackup(
                cancelled, 1_000L, () -> deleted.set(true)));
        assertFalse(deleted.get());
    }

    /// A successful starter backup releases the retained upload.
    @Test
    void successfulBackupDeletesTheUploadAfterPublication() throws NodeException {
        final AtomicBoolean deleted = new AtomicBoolean();

        NodeLifecycle.awaitStarterBackup(CompletableFuture.completedFuture("published"), 1_000L,
                () -> deleted.set(true));

        assertTrue(deleted.get());
    }
}
