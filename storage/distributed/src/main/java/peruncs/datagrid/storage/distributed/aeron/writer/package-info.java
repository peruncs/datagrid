/**
 * This package writes complete Store transactions to Aeron.
 *
 * <p>The writer publishes data in order and records the same stream for later
 * replay. A {@code COMMIT} marker is the hand-off point: consumers may apply
 * the transaction only after that marker is durable; an {@code ABORT} marker
 * resolves a transaction that cannot be applied. The writer must be closed
 * after the final transaction has been published.</p>
 *
 * @since 1.0
 */
package peruncs.datagrid.storage.distributed.aeron.writer;
