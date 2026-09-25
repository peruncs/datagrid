/// Failures an application can use to decide whether to retry, reseed, or stop a node.
///
/// `NodeException` is the shared base for node lifecycle and storage
/// failures; `ReplicationException` is the base for replication-boundary
/// failures, with typed specializations for reader rejection, writer
/// fencing, replication unavailability, corrupt data, and the reseed demand.
/// This package is exported so boundary code can name and map these
/// failures directly.
package peruncs.cluster.errors;
