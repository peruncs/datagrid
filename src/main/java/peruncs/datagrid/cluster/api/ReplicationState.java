package peruncs.datagrid.cluster.api;

/// Observable replication lifecycle without exposing transport internals.
public enum ReplicationState {
    STARTING,
    REPLAYING,
    LIVE,
    DEGRADED,
    RESEED_REQUIRED,
    FAILED
}
