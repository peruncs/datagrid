package peruncs.cluster.api;

/// Observable replication lifecycle without exposing transport internals.
public enum ReplicationState {
    /// The node is starting its local services.
    STARTING,
    /// The node is replaying archived replication data.
    REPLAYING,
    /// The node is caught up and accepting normal replication traffic.
    LIVE,
    /// The node is running with a degraded replication dependency.
    DEGRADED,
    /// The node must be reseeded before it can become live.
    RESEED_REQUIRED,
    /// The node has no replication transport configured.
    NOT_CONFIGURED,
    /// Replication has failed and requires intervention.
    FAILED
}
