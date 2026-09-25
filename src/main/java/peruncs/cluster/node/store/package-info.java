/// This package adapts Eclipse Store to cluster duty.
///
/// It owns the guarded storage managers created through
/// [ClusterStorageManagers] (construction policy only — the public contract
/// lives in `peruncs.cluster.api`), the storage task executor, the storage
/// usage gauge, file operations, the storage limit gate, and the storage
/// health check. The managers' `shutdown()` triggers the owning node's full
/// teardown through [NodeClose]; this package owns no independent Store
/// shutdown path.
///
/// @since 1.0
package peruncs.cluster.node.store;
