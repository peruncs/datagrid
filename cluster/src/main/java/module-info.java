/// Data Grid node with Aeron replication.
///
/// Applications create the node services, start them in dependency order,
/// and close them in reverse order. The node lifecycle lives in
/// `...cluster.node.node`, storage adaptation in
/// `...cluster.node.store`, backup in
/// `...cluster.node.backup`, replication in
/// `...cluster.node.replication`, and the HTTP surface in
/// `...cluster.node.http`, implemented by the Aeron transport in
/// `...cluster.node.aeron`. Store binary movement lives in
/// `...cluster.storage.types`, carried by
/// `...cluster.storage.aeron.*`, with the embedded Lucene/JVector
/// index policy in `...cluster.storage.index`. The exported packages
/// contain the public contracts and errors used at those boundaries.
/// Aeron is the only transport.
///
/// # Archive-first replication
///
/// Readers must never apply a transaction the writer did not durably
/// record. Every write therefore follows Archive prepare chunks, then the
/// local Store enqueue, then the Archive commit. The transport keeps
/// Eclipse Serializer `Binary` bytes opaque behind a versioned envelope
/// carrying only cluster identity, sequence, chunking, CRC32C, and
/// commit/abort markers — framing only, no second object-graph encoding.
/// Readers replay from the Archive, join the live stream, and reconnect
/// from a durable cursor.
///
/// # Fixed roles without consensus
///
/// Writer election and failover need fencing and consensus, a separate
/// system this transport deliberately avoids. Roles are fixed at
/// configuration: `writer`, `reader`, or `backup-reader`, or `none` for an
/// unreplicated node. A reader owns a persistent subscription with no
/// writer publication path, so promotion is rejected outright instead of
/// producing a distributor that cannot replicate. Fencing and manual
/// promotion stay deployment responsibilities; there is only ever the
/// configured writer.
///
/// # Durable cursors and checkpoints
///
/// Every applied commit advances a durable `ReplicationCursor` of
/// transport, Store generation, logical sequence, and provider position,
/// while writers persist an authenticated checkpoint binding cluster,
/// Store generation, epoch, recording, and sequence. Startup reconciles
/// the two so a cursor from another Store generation never resumes an
/// unrelated recording. Restarts stay routine instead of reseeds as long
/// as Archive and cursor survive together; cursor and checkpoint writes
/// are CRC-protected and atomic, and torn files are rejected.
///
/// # Quorum-gated retention
///
/// Deleting Archive segments a slow reader still needs destroys data no
/// replay can recover. Readers therefore advertise their durable boundary
/// as HMAC-signed watermarks, and the writer deletes history only through
/// the complete configured reader quorum — pausing admission, stopping the
/// recording, purging only complete segments, and extending the same
/// recording at its exact stop position. Without the shared secret, with
/// an incomplete quorum, or during an active replay, history is preserved.
/// The quorum, not any single request, authorizes deletion.
///
/// # Indexes live inside the object graph
///
/// Replication ships the object graph, so anything kept outside it would
/// diverge across nodes. Lucene data must live in the graph through the
/// embedded GraphDirectory, and vector data in the persisted in-graph
/// vector store; external directories and on-disk indexes are rejected at
/// the registration boundary.
///
/// @since 1.0
module peruncs.datagrid.cluster
{
    requires org.eclipse.store.storage.embedded;
    requires org.eclipse.serializer.base;
    requires org.eclipse.serializer.persistence;
    requires org.eclipse.serializer.persistence.binary;
    requires org.eclipse.store.storage;
    requires org.eclipse.serializer.afs;
    requires org.eclipse.store.afs.nio;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires java.net.http;
    requires org.apache.commons.compress;
    requires io.aeron.client;
    requires io.aeron.archive;
    requires io.aeron.driver;
    requires org.agrona;
    requires org.eclipse.store.gigamap;
    requires org.eclipse.store.gigamap.lucene;
    // The upstream module name is misspelled; keep the dependency aligned with
    // the published module descriptor.
    requires org.eclipes.store.gigamap.jvector;
    requires org.apache.lucene.core;
    /* Benchmark tests use com.sun.management.ThreadMXBean while production
     * classes have no runtime dependency on the management implementation. */
    requires static jdk.management;

    exports peruncs.datagrid.cluster.node.exceptions;
    exports peruncs.datagrid.cluster.node.backup;
    exports peruncs.datagrid.cluster.node.replication;
    exports peruncs.datagrid.cluster.node.node;
    exports peruncs.datagrid.cluster.node.store;
    exports peruncs.datagrid.cluster.node.http;
    exports peruncs.datagrid.cluster.node.aeron;
    exports peruncs.datagrid.cluster.storage.types;
    exports peruncs.datagrid.cluster.storage.aeron.config;
    exports peruncs.datagrid.cluster.storage.aeron.checkpoint;
    exports peruncs.datagrid.cluster.storage.aeron.reader;
    exports peruncs.datagrid.cluster.storage.aeron.writer;
    exports peruncs.datagrid.cluster.storage.index;
}
