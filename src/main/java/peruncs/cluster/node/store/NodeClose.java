package peruncs.cluster.node.store;

import peruncs.cluster.errors.GraphDrainTimeoutException;

import java.time.Duration;

/// Internal callback running the complete, ordered node teardown.
///
/// Implemented by the owning node lifecycle; invoked when an application
/// shuts the manager down through the public manager's shutdown method.
public interface NodeClose {
    /// Runs the node close.
    ///
    /// @return `true` when this call performed the teardown, `false` after
    /// observing another caller's successful close; concurrent callers wait
    /// for the in-flight attempt and propagate its failure
    boolean close();

    /// Waits for application calls to leave the node-owned Store facade.
    ///
    /// @param timeout maximum drain wait
    /// @throws GraphDrainTimeoutException if application calls remain active
    void awaitAppIdle(Duration timeout);

    /// Rejects admission once the owning node is closed, closing, or
    /// retry-pending after a failed close.
    ///
    /// Persistence entries on the manager check this alongside their own
    /// admission, so a write arriving while the sequencer has already closed
    /// the replication transport can never persist a local-only divergence.
    ///
    /// @throws IllegalStateException while the node is closed or closing
    void checkOpen();
}
