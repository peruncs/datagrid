package peruncs.cluster.node.aeron;

/// Immutable terminal position published to health, readers, and retention.
///
/// @param sequence    durable replication sequence
/// @param recordingId Archive recording containing the boundary
/// @param position    exact durable Archive position
record AeronWriterRecoveryBoundary(long sequence, long recordingId, long position) {
}
