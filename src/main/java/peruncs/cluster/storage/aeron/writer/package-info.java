/// This package writes complete Store transactions to Aeron.
///
/// The writer publishes data in order and records the same stream for later
/// replay. A `COMMIT` marker is the hand-off point: consumers may apply
/// the transaction only after that marker is durable; an `ABORT` marker
/// resolves a transaction that cannot be applied. The writer must be closed
/// after the final transaction has been published.
///
/// @since 1.0
package peruncs.cluster.storage.aeron.writer;
