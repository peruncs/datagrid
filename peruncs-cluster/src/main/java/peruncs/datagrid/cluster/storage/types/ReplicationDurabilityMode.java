package peruncs.datagrid.cluster.storage.types;

/// Ordering contract between a local Store enqueue and a replication log.
/// Store 5.x exposes enqueue acceptance, not a durable-completion callback.
public enum ReplicationDurabilityMode {
        /// Record the prepared transaction before accepting the local Store enqueue.
    ARCHIVE_FIRST,
        /// Accept the local Store enqueue before recording the transaction; recovery is conservative.
    ENQUEUE_THEN_ARCHIVE
}
