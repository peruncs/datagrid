/**
 * This package keeps the durable state needed to resume Aeron replication.
 *
 * <p>A checkpoint names the stream position and recording that a node has
 * accepted. A reader or writer may reuse a recording only after its identity
 * and generation match the checkpoint. A mismatch starts a new safe path
 * instead of silently appending to unrelated data.</p>
 *
 * @since 1.0
 */
package peruncs.datagrid.storage.distributed.aeron.checkpoint;
