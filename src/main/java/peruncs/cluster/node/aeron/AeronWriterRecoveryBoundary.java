package peruncs.cluster.node.aeron;

/// Immutable terminal position published to health, readers, and retention.
///
/// @param sequence    durable replication sequence
/// @param recordingId Archive recording containing the boundary
/// @param terminalPosition exact durable Archive position of the end of the terminal frame
record AeronWriterRecoveryBoundary(long sequence, long recordingId, long terminalPosition) {
}
