package peruncs.datagrid.cluster.node;


/// This manager reports node readiness and starts storage maintenance work.
///
/// Readiness means the node can serve its role. Health also considers whether
/// its active transport is still functioning. Implementations close their own
/// transport and storage collaborators. Borrowers must use [StorageNodeControl]:
/// the foundation owns this manager and closes it on [ClusterFoundation#close].
public interface ClusterNodeManager extends StorageNodeControl, AutoCloseable {
    @Override
    void close();
}
