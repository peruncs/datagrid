package peruncs.cluster.storage.aeron.reader;

/// One immutable reader boundary: the last resolved transaction sequence and
/// the Archive position it was resolved at.
///
/// The two values are captured together under the assembler monitor so a
/// caller can persist them without observing a sequence from one transaction
/// and a position from another.
///
/// @param sequence last resolved transaction sequence, or `-1` before the first
/// @param position Archive position of that transaction, or `-1` before the first
public record CursorSnapshot(long sequence, long position) {
}
