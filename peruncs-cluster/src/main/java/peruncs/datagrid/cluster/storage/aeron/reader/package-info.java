/// This package reads complete Store transactions from Aeron.
///
/// A reader first replays the Archive and then follows the live stream. It
/// delivers a transaction only after its `COMMIT` marker arrives, so
/// callers never see a partial Store write. An `ABORT` marker advances
/// the replay boundary without materialising data. The reader owns its subscription and must be
/// closed before the Aeron client that created it.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.aeron.reader;
