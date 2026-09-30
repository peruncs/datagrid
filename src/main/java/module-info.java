/// Eclipse Store replication for one writer and any number of readers.
///
/// Only `peruncs.cluster.api` and `peruncs.cluster.errors` are exported. Roles
/// (`writer`, `reader`, `backup-reader`, or standalone) are fixed at startup.
/// The writer owns the durable replication mark and Archive; readers need a
/// compatible seeded Store and replay committed transactions from that mark.
/// Writer fencing is local to the Store path, so deployment must configure one
/// writer per cluster.
///
/// Store data and the Aeron driver and Archive directories must use storage
/// exclusive to the node. Shared volumes are for cold backups only. Embedded
/// Lucene and JVector indexes travel with the Store graph; external indexes
/// are rejected. Application graph access and replication share
/// `GraphBoundary`, and uncertain writes invalidate the graph until reload or
/// reseed.
///
/// The protocol has **no node authentication and no transport encryption**;
/// neither is required or permitted. Cluster identifiers and CRC32C provide
/// framing and corruption checks, not authentication or authorization.
/// Separate channels and stream IDs prevent accidental cross-wiring only.
///
/// See `README.md` for configuration and operations, and package documentation
/// for implementation details.
///
/// @since 1.0
module peruncs.cluster
{
    /* Exported API signatures expose Store/Serializer types; re-export only
     * the modules whose types appear directly in exported signatures. */
    requires transitive org.eclipse.store.storage;
    requires transitive org.eclipse.serializer.base;
    requires transitive org.eclipse.store.storage.embedded;
    requires org.eclipse.serializer.persistence;
    requires org.eclipse.serializer.persistence.binary;
    requires org.eclipse.serializer.afs;
    requires org.eclipse.store.afs.nio;
    requires io.aeron.client;
    requires io.aeron.archive;
    requires io.aeron.driver;
    requires org.agrona;
    requires transitive org.eclipse.store.gigamap;
    requires transitive org.eclipse.store.gigamap.lucene;
    requires transitive org.apache.lucene.core;
    // The upstream module name is misspelled; keep the dependency aligned with
    // the published module descriptor.
    requires transitive org.eclipes.store.gigamap.jvector;
    requires transitive jvector;
    exports peruncs.cluster.api;
    exports peruncs.cluster.errors;
    opens peruncs.cluster.storage.aeron.mark to org.eclipse.serializer.persistence.binary;
}
