package peruncs.cluster.node.store;

/// Internal callback running the complete, ordered node teardown.
///
/// Implemented by the owning node lifecycle; invoked when an application
/// shuts the manager down through the public manager's shutdown method.
@FunctionalInterface
public interface NodeClose {
    /// Runs the node close.
    ///
    /// @return `true` when this call performed the teardown, `false` after
    /// observing another caller's successful close; concurrent callers wait
    /// for the in-flight attempt and propagate its failure
    boolean close();
}
