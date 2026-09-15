package peruncs.datagrid.storage.distributed.aeron.reader;

/**
 * Immutable sequence/recording-position pair captured at one reader boundary.
 *
 * @param sequence last resolved transaction sequence
 * @param position Archive position of that transaction
 */
public record CursorSnapshot(long sequence, long position)
{
}
