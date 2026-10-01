package peruncs.cluster.storage.aeron.reader;

/// One immutable reader boundary: the last resolved transaction sequence and
/// the Archive position it was resolved at.
///
/// The two values are captured together under the assembler monitor so a
/// caller can persist them without observing a sequence from one transaction
/// and a position from another.
///
/// Two different boundaries use this pair and must not be mixed up. The assembler reports the
/// commit-end position of the last resolved transaction (a reader's live cursor). The reader transport
/// reports the Store mark's sequence and prepare-start position, the point a restart resumes from, which
/// is what the retention watermark and `position()` publish. Publishing the commit end as a watermark
/// would let retention purge a segment a restart still needs.
///
/// @param sequence last resolved transaction sequence, or `-1` before the first
/// @param position Archive position of that transaction, or `-1` before the first
public record CursorSnapshot(long sequence, long position) {
}
